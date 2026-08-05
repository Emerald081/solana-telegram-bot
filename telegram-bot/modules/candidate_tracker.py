"""
candidate_tracker.py — two-tier lifecycle manager for tracked wallets.

Tiers:
  candidate     — wallet funded by exchange, being watched quietly.
  high_priority — wallet has interacted with a launchpad; 60-day timer
                  resets from first launchpad interaction; gets supplemental
                  HTTP polling on top of the real-time WebSocket subscription.

Events handled per tracked wallet transaction:
  1. Token / mint creation  → send Telegram alert (only alert the system sends)
  2. Launchpad interaction  → upgrade to high_priority, reset timer (no alert)
  3. Preparation activity   → pause expiry countdown (no alert)
  4. Outbound SOL hop       → evaluate recipient as new candidate (no alert)
  5. Unrelated incoming SOL → increment counter; disqualify at limit (no alert)

Startup recovery:
  All active wallets are reloaded from DB and re-subscribed automatically.
"""

import asyncio
import logging
import time
from typing import Optional

from .blockchain_monitor import BlockchainMonitor, LAMPORTS_PER_SOL
from .config_loader import Config
from .database import Database
from .exchange_monitor import ExchangeMonitor  # for hop evaluation
from .launchpad_detector import LaunchpadDetector
from .preparation_detector import PreparationDetector
from .scoring_engine import ScoringEngine
from .telegram_alerts import TelegramAlerts
from .token_detector import TokenDetector

logger = logging.getLogger(__name__)

# Minimum SOL for an outbound transfer to be considered a hop
_HOP_MIN_LAMPORTS = 500_000_000  # 0.5 SOL


class CandidateTracker:
    def __init__(
        self,
        config: Config,
        db: Database,
        blockchain: BlockchainMonitor,
        launchpad_detector: LaunchpadDetector,
        preparation_detector: PreparationDetector,
        token_detector: TokenDetector,
        alerts: TelegramAlerts,
        scoring: ScoringEngine,
        exchange_monitor: Optional["ExchangeMonitor"] = None,
    ):
        self.config      = config
        self.db          = db
        self.blockchain  = blockchain
        self.launchpad   = launchpad_detector
        self.preparation = preparation_detector
        self.tokens      = token_detector
        self.alerts      = alerts
        self.scoring     = scoring
        self.exchange_monitor = exchange_monitor   # injected for hop evaluation

        self.max_unrelated     = config.max_unrelated_transfers
        self.max_tracking_days = config.max_tracking_days
        self.prep_pause_sec    = config.prep_pause_hours * 3600

        # Addresses of all exchange wallets — used to filter unrelated-incoming check
        self.exchange_addresses: set[str] = set()

        # High-priority HTTP polling
        # addr → last_seen_signature (cursor, kept in memory)
        self._hp_cursors: dict[str, Optional[str]] = {}
        self._hp_poll_task: Optional[asyncio.Task] = None
        self._hp_poll_interval = config.high_priority_poll_interval

        self._maintenance_task: Optional[asyncio.Task] = None

    def set_exchange_addresses(self, addresses: set[str]) -> None:
        self.exchange_addresses = addresses

    # ── Startup / recovery ────────────────────────────────────

    async def start(self) -> None:
        # 1. Expire wallets that ran out during downtime
        expired = await self.db.expire_old_wallets(self.prep_pause_sec)
        if expired:
            logger.info(f"[CandidateTracker] Expired {len(expired)} wallet(s) on startup")

        # 2. Re-subscribe all still-active wallets
        active = await self.db.get_active_tracked_wallets()
        for w in active:
            await self.blockchain.subscribe_address(w["wallet_address"], self._on_wallet_tx)
            if w.get("priority") == "high_priority":
                self._hp_cursors[w["wallet_address"]] = None  # will resync on first poll

        candidate_count     = sum(1 for w in active if w.get("priority") != "high_priority")
        high_priority_count = sum(1 for w in active if w.get("priority") == "high_priority")

        logger.info(
            f"[CandidateTracker] Resumed: "
            f"{candidate_count} candidate(s), {high_priority_count} high-priority"
        )

        # 3. Periodic maintenance
        self._maintenance_task = asyncio.create_task(
            self._maintenance_loop(), name="candidate-maintenance"
        )

        # 4. High-priority supplemental polling
        self._hp_poll_task = asyncio.create_task(
            self._hp_poll_loop(), name="hp-poll"
        )

    # ── Public API ────────────────────────────────────────────

    async def add_wallet(self, wallet_address: str) -> None:
        """Called by ExchangeMonitor when a new candidate is saved to DB."""
        await self.blockchain.subscribe_address(wallet_address, self._on_wallet_tx)
        logger.debug(f"[CandidateTracker] Subscribed candidate {wallet_address[:8]}…")

    async def remove_wallet(self, wallet_address: str, reason: str = "removed") -> None:
        await self.blockchain.unsubscribe_address(wallet_address)
        self._hp_cursors.pop(wallet_address, None)
        logger.info(f"[CandidateTracker] Unsubscribed {wallet_address[:8]}… ({reason})")

    # ── WebSocket callback ─────────────────────────────────────

    async def _on_wallet_tx(
        self, signature: str, wallet_address: str, logs: list[str]
    ) -> None:
        if await self.db.is_signature_processed(signature):
            return
        await self.db.mark_signature_processed(signature)

        wallet = await self.db.get_tracked_wallet(wallet_address)
        if not wallet or wallet["status"] != "active":
            return

        # Always fetch the full tx for tracked wallets (they are low-volume
        # by definition — new or barely-used wallets).
        tx = await self.blockchain.get_transaction(signature)
        if not tx or tx.get("meta", {}).get("err"):
            return

        await self._process_tx(tx, signature, wallet_address, wallet, logs)

    # ── Transaction processing ────────────────────────────────

    async def _process_tx(
        self,
        tx: dict,
        signature: str,
        wallet_address: str,
        wallet: dict,
        logs: list[str],
    ) -> None:
        try:
            # ── 1. Token / mint creation (highest priority) ────────
            mint = await self.tokens.detect_token_creation(tx, wallet_address)
            if mint:
                await self._handle_token_creation(
                    wallet_address, mint, signature, wallet, logs
                )
                return   # token creation is the terminal event for this tx

            # ── 2. Launchpad interaction ───────────────────────────
            lp = self.launchpad.detect_from_transaction(tx)
            if lp:
                await self._handle_launchpad_interaction(wallet_address, lp)
                # Don't return — also check for prep activity in same tx

            # ── 3. Preparation activity ───────────────────────────
            prep_detected, prep_reason = self.preparation.detect_from_transaction(
                tx, wallet_address
            )
            if prep_detected:
                await self._handle_preparation_activity(wallet_address, prep_reason)

            # ── 4. Outbound SOL → potential hop ───────────────────
            if self.config.hop_detection_enabled:
                await self._check_hop(tx, signature, wallet_address, wallet)

            # ── 5. Unrelated incoming SOL ─────────────────────────
            if _is_incoming_sol(tx, wallet_address, self.exchange_addresses):
                await self._handle_unrelated_transfer(wallet_address)

        except Exception as e:
            logger.error(
                f"_process_tx error (sig={signature[:12]}… "
                f"wallet={wallet_address[:8]}…): {e}",
                exc_info=True,
            )

    # ── Event handlers ────────────────────────────────────────

    async def _handle_launchpad_interaction(
        self, wallet_address: str, launchpad_name: str
    ) -> None:
        wallet = await self.db.get_tracked_wallet(wallet_address)
        if not wallet:
            return

        now = int(time.time())
        was_candidate = wallet.get("priority", "candidate") == "candidate"

        await self.db.upgrade_to_high_priority(
            wallet_address, launchpad_name, now, self.max_tracking_days
        )

        if was_candidate:
            # Start supplemental HTTP polling for this wallet
            self._hp_cursors[wallet_address] = None
            logger.info(
                f"[CandidateTracker] ⭐ Upgraded to HIGH PRIORITY: "
                f"{wallet_address[:8]}… → {launchpad_name}  "
                f"(60-day timer reset from now)"
            )
        else:
            logger.info(
                f"[CandidateTracker] Launchpad repeat: "
                f"{wallet_address[:8]}… → {launchpad_name}"
            )

    async def _handle_preparation_activity(
        self, wallet_address: str, reason: str
    ) -> None:
        now = int(time.time())
        await self.db.set_preparation_detected(wallet_address, True, now)
        logger.info(
            f"[CandidateTracker] 🔧 Prep activity: "
            f"{wallet_address[:8]}… — {reason}  (expiry paused)"
        )

    async def _handle_unrelated_transfer(self, wallet_address: str) -> None:
        count = await self.db.increment_unrelated_transfers(wallet_address)
        logger.info(
            f"[CandidateTracker] Unrelated transfer #{count} for {wallet_address[:8]}…"
        )
        if count >= self.max_unrelated:
            logger.info(
                f"[CandidateTracker] Disqualifying {wallet_address[:8]}… "
                f"— exceeded {self.max_unrelated} unrelated transfers"
            )
            await self.db.update_wallet_status(wallet_address, "disqualified")
            await self.remove_wallet(wallet_address, "too many unrelated transfers")

    async def _handle_token_creation(
        self,
        wallet_address: str,
        token_mint: str,
        signature: str,
        wallet: dict,
        logs: list[str],
    ) -> None:
        # Guard: never duplicate alerts
        if await self.db.has_alert_been_sent(wallet_address, token_mint):
            return

        # Reload freshest wallet record
        wallet = await self.db.get_tracked_wallet(wallet_address) or wallet

        logger.info(
            f"[CandidateTracker] 🚀 Token creation detected! "
            f"wallet={wallet_address[:8]}… mint={token_mint[:8]}… "
            f"priority={wallet.get('priority', 'candidate')}"
        )

        # Developer history for this address
        dev_history = await self.db.get_developer_history(wallet_address)

        # Launchpad: prefer what's in DB (from prior interaction), else scan logs
        launchpad = (
            wallet.get("launchpad_detected")
            or self.launchpad.detect_from_logs(logs)
            or "Unknown"
        )

        # Final score with complete picture
        score = self.scoring.calculate_score({
            "exchange":              wallet["funding_exchange"],
            "funding_amount":        wallet["funding_amount_sol"],
            "wallet_age_days":       wallet.get("wallet_age_days", 0),
            "historical_tx_count":   wallet.get("historical_tx_count", 0),
            "priority":              wallet.get("priority", "candidate"),
            "preparation_detected":  bool(wallet.get("preparation_detected")),
            "launchpad_interaction": bool(wallet.get("launchpad_detected")),
            "previous_launches":     len(dev_history),
            "unrelated_transfers":   wallet.get("unrelated_transfer_count", 0),
        })

        # Persist the launch record
        await self.db.add_developer_history(
            wallet_address, token_mint, launchpad,
            wallet["funding_exchange"], signature,
        )

        # Update score in DB
        await self.db.update_wallet_score(wallet_address, score)

        # Send alert — the ONLY Telegram message this system sends
        message_id = await self.alerts.send_token_creation_alert(
            wallet=wallet,
            token_mint=token_mint,
            tx_signature=signature,
            launchpad=launchpad,
            dev_history=dev_history,
            score=score,
        )

        # Record for dedup
        await self.db.record_alert(wallet_address, token_mint, message_id)

        # Keep watching — the developer may launch more tokens

    # ── One-hop funding chain detection ───────────────────────

    async def _check_hop(
        self,
        tx: dict,
        signature: str,
        wallet_address: str,
        wallet: dict,
    ) -> None:
        """
        If the tracked wallet forwards most of its received SOL to a new
        wallet within `hop_max_hours` of the initial funding, register the
        recipient as a candidate.
        """
        try:
            meta         = tx.get("meta", {})
            message      = tx.get("transaction", {}).get("message", {})
            account_keys = message.get("accountKeys", [])
            pre_bal      = meta.get("preBalances", [])
            post_bal     = meta.get("postBalances", [])

            if not pre_bal or not post_bal:
                return

            addresses = _extract_addresses(account_keys)
            if wallet_address not in addresses:
                return

            our_idx = addresses.index(wallet_address)
            if our_idx >= len(pre_bal) or our_idx >= len(post_bal):
                return

            sent_lamports = pre_bal[our_idx] - post_bal[our_idx]
            if sent_lamports < _HOP_MIN_LAMPORTS:
                return   # not a significant outbound transfer

            # Check timing window
            funding_time = wallet.get("funding_time", 0)
            block_time   = tx.get("blockTime") or int(time.time())
            hours_elapsed = (block_time - funding_time) / 3600
            if hours_elapsed > self.config.hop_max_hours:
                return

            original_funding_lamports = int(
                wallet.get("funding_amount_sol", 0) * LAMPORTS_PER_SOL
            )
            if original_funding_lamports == 0:
                return

            min_hop_lamports = int(
                original_funding_lamports * self.config.hop_min_fraction
            )

            # Find recipients that received enough SOL
            for i, addr in enumerate(addresses):
                if i == our_idx or not addr:
                    continue
                if addr in self.exchange_addresses or addr == wallet_address:
                    continue
                if i >= len(pre_bal) or i >= len(post_bal):
                    continue

                received_lamports = post_bal[i] - pre_bal[i]
                if received_lamports < min_hop_lamports:
                    continue

                # Don't track if already in our DB
                existing = await self.db.get_tracked_wallet(addr)
                if existing:
                    continue

                received_sol = received_lamports / LAMPORTS_PER_SOL
                logger.info(
                    f"[CandidateTracker] 🔗 Hop detected: "
                    f"{wallet_address[:8]}… → {addr[:8]}… "
                    f"({received_sol:.4f} SOL, {hours_elapsed:.1f}h after funding)"
                )

                asyncio.create_task(
                    self._evaluate_hop_candidate(
                        addr,
                        wallet_address,
                        wallet,
                        received_sol,
                        signature,
                        block_time,
                    )
                )

        except Exception as e:
            logger.debug(f"_check_hop error: {e}")

    async def _evaluate_hop_candidate(
        self,
        hop_addr: str,
        source_addr: str,
        source_wallet: dict,
        received_sol: float,
        funding_tx: str,
        funding_time: int,
    ) -> None:
        """Evaluate the hop recipient and register it as a candidate if it qualifies."""
        try:
            sigs = await self.blockchain.get_signatures_for_address(
                hop_addr, limit=self.config.max_historical_txs + 1
            )
            historical_tx_count = len(sigs)

            if historical_tx_count > self.config.max_historical_txs:
                logger.debug(
                    f"Hop rejected {hop_addr[:8]}… — too many txns ({historical_tx_count})"
                )
                return

            wallet_age_days = 0.0
            if sigs:
                oldest_time = sigs[-1].get("blockTime") or funding_time
                wallet_age_days = max(0.0, (funding_time - oldest_time) / 86400)

            # Inherit the source wallet's exchange metadata
            exchange_name = source_wallet.get("funding_exchange", "Unknown")

            score = self.scoring.calculate_score({
                "exchange":              exchange_name,
                "funding_amount":        received_sol,
                "wallet_age_days":       wallet_age_days,
                "historical_tx_count":   historical_tx_count,
                "launchpad_interaction": False,
                "previous_launches":     0,
                "unrelated_transfers":   0,
            })

            now = int(time.time())
            tracking_expires = now + (self.config.max_tracking_days * 86400)

            added = await self.db.add_tracked_wallet({
                "wallet_address":      hop_addr,
                "funding_exchange":    exchange_name,
                "funding_tx":          funding_tx,
                "funding_amount_sol":  received_sol,
                "funding_time":        funding_time,
                "tracking_start":      now,
                "tracking_expires":    tracking_expires,
                "priority":            "candidate",
                "score":               score,
                "historical_tx_count": historical_tx_count,
                "wallet_age_days":     wallet_age_days,
                "hop_source":          source_addr,
            })

            if added:
                logger.info(
                    f"[CandidateTracker] ✓ Hop candidate added: {hop_addr[:8]}… "
                    f"(via {source_addr[:8]}…, exchange={exchange_name}, "
                    f"sol={received_sol:.4f}, score={score:.0f})"
                )
                await self.add_wallet(hop_addr)

        except Exception as e:
            logger.error(f"_evaluate_hop_candidate error ({hop_addr[:8]}…): {e}")

    # ── High-priority supplemental HTTP polling ────────────────

    async def _hp_poll_loop(self) -> None:
        """
        Periodically poll high-priority wallets via HTTP as a backup for
        any events missed by the WebSocket subscription.
        """
        while True:
            try:
                await asyncio.sleep(self._hp_poll_interval)
                for addr in list(self._hp_cursors.keys()):
                    try:
                        await self._hp_poll_wallet(addr)
                    except Exception as e:
                        logger.debug(f"HP poll error {addr[:8]}…: {e}")
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"HP poll loop error: {e}")

    async def _hp_poll_wallet(self, address: str) -> None:
        """Check for new signatures on a high-priority wallet via HTTP."""
        wallet = await self.db.get_tracked_wallet(address)
        if not wallet or wallet["status"] != "active":
            self._hp_cursors.pop(address, None)
            return

        cursor = self._hp_cursors.get(address)
        params = [address, {"limit": 10}]
        if cursor:
            params[1]["until"] = cursor

        sigs = await self.blockchain._rpc("getSignaturesForAddress", params)
        if not isinstance(sigs, list) or not sigs:
            return

        # Advance cursor
        self._hp_cursors[address] = sigs[0]["signature"]

        # Process oldest → newest
        for sig_info in reversed(sigs):
            sig = sig_info.get("signature")
            if not sig or sig_info.get("err"):
                continue
            if await self.db.is_signature_processed(sig):
                continue
            await self.db.mark_signature_processed(sig)

            tx = await self.blockchain.get_transaction(sig)
            if not tx or tx.get("meta", {}).get("err"):
                continue

            # Reload wallet in case it changed
            wallet = await self.db.get_tracked_wallet(address)
            if not wallet or wallet["status"] != "active":
                return

            await self._process_tx(tx, sig, address, wallet, [])

    # ── Maintenance loop ──────────────────────────────────────

    async def _maintenance_loop(self) -> None:
        while True:
            try:
                await asyncio.sleep(self.config.maintenance_interval)
                await self._run_maintenance()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Maintenance error: {e}")

    async def _run_maintenance(self) -> None:
        # Expire wallets that have passed their deadline
        # (prep-active wallets are automatically skipped in the DB query)
        expired = await self.db.expire_old_wallets(self.prep_pause_sec)
        for addr in expired:
            await self.remove_wallet(addr, "expired")
        if expired:
            logger.info(f"[Maintenance] Expired {len(expired)} wallet(s)")

        # Purge old signature records
        await self.db.cleanup_old_signatures(
            days=self.config.signature_retention_days
        )

        stats = await self.db.get_stats()
        logger.info(
            f"[Maintenance] Active: {stats['active_wallets']} "
            f"(candidate={stats['candidate']}, high_priority={stats['high_priority']}, "
            f"prep_active={stats['prep_activity_active']})"
        )


# ── Module-level helpers ──────────────────────────────────────

def _extract_addresses(account_keys: list) -> list[str]:
    addrs = []
    for key in account_keys:
        if isinstance(key, str):
            addrs.append(key)
        elif isinstance(key, dict):
            addrs.append(key.get("pubkey", ""))
        else:
            addrs.append(str(key))
    return addrs


def _is_incoming_sol(
    tx: dict, wallet_address: str, exchange_addresses: set[str]
) -> bool:
    """
    Returns True if the tracked wallet received SOL from a non-exchange source.
    Exchange-originated transfers are handled by ExchangeMonitor, not here.
    """
    try:
        meta         = tx.get("meta", {})
        message      = tx.get("transaction", {}).get("message", {})
        account_keys = message.get("accountKeys", [])
        pre_bal      = meta.get("preBalances", [])
        post_bal     = meta.get("postBalances", [])

        addresses = _extract_addresses(account_keys)

        if wallet_address not in addresses:
            return False

        idx = addresses.index(wallet_address)
        if idx >= len(pre_bal) or idx >= len(post_bal):
            return False

        received = post_bal[idx] - pre_bal[idx]
        if received <= 0:
            return False

        # If any sender is an exchange wallet, this is not unrelated
        for i, addr in enumerate(addresses):
            if addr in exchange_addresses:
                if i < len(pre_bal) and i < len(post_bal):
                    if pre_bal[i] - post_bal[i] > 0:
                        return False  # Exchange-originated

        return True

    except Exception:
        return False
