"""
exchange_monitor.py — poll exchange hot wallets for qualifying SOL transfers.

Architecture: HTTP polling via getSignaturesForAddress (cursor-based).
  - On startup: fetch the most recent signature for each exchange wallet
    as the cursor (avoids replaying historical transactions).
  - Every POLL_INTERVAL seconds: fetch new signatures since last cursor,
    process each qualifying transfer, advance the cursor.

Why polling instead of WebSocket logsSubscribe:
  Exchange hot wallets generate thousands of transactions per hour.
  logsSubscribe floods the WebSocket connection with irrelevant events
  and stalls subscription confirmations.  HTTP polling with a cursor is
  the standard pattern for high-volume Solana monitoring.
"""

import asyncio
import logging
import time
from typing import TYPE_CHECKING, Optional

from .blockchain_monitor import BlockchainMonitor, LAMPORTS_PER_SOL
from .config_loader import Config
from .database import Database
from .scoring_engine import ScoringEngine

if TYPE_CHECKING:
    from .candidate_tracker import CandidateTracker

logger = logging.getLogger(__name__)

# Poll every N seconds per exchange wallet
POLL_INTERVAL = 15
# Signatures fetched per poll (keeps each HTTP call small)
SIGNATURES_PER_POLL = 25
# Delay between wallets in the same poll cycle (rate-limit friendliness)
INTER_WALLET_DELAY = 0.3


class ExchangeMonitor:
    def __init__(
        self,
        config: Config,
        db: Database,
        blockchain: BlockchainMonitor,
        scoring: ScoringEngine,
    ):
        self.config = config
        self.db = db
        self.blockchain = blockchain
        self.scoring = scoring

        self.min_sol = config.min_sol
        self.max_sol = config.max_sol
        self.exchange_wallets: dict[str, str] = config.exchange_wallets  # addr→name
        self.ignored: set[str] = set(config.ignored_wallets)

        # Injected after construction to avoid circular dependency
        self._candidate_tracker: "CandidateTracker | None" = None

        # Per-wallet cursor: last signature we've processed
        self._cursors: dict[str, Optional[str]] = {}

        self._poll_task: Optional[asyncio.Task] = None

    def set_candidate_tracker(self, tracker: "CandidateTracker") -> None:
        self._candidate_tracker = tracker

    # ── Lifecycle ─────────────────────────────────────────────

    async def start(self) -> None:
        count = len(self.exchange_wallets)
        logger.info(f"ExchangeMonitor: initialising cursors for {count} exchange wallet(s) …")
        await self._initialise_cursors()
        logger.info("ExchangeMonitor: cursors ready, polling started.")
        self._poll_task = asyncio.create_task(self._poll_loop(), name="exchange-poll")

    async def stop(self) -> None:
        if self._poll_task:
            self._poll_task.cancel()

    # ── Cursor initialisation ─────────────────────────────────

    async def _initialise_cursors(self) -> None:
        """
        Set each cursor to the wallet's most recent signature so we only
        process NEW transfers from this point forward.
        """
        tasks = [
            self._fetch_initial_cursor(addr)
            for addr in self.exchange_wallets
        ]
        # Batch concurrently but honour max_concurrent_rpc via the semaphore
        await asyncio.gather(*tasks, return_exceptions=True)

    async def _fetch_initial_cursor(self, address: str) -> None:
        try:
            sigs = await self.blockchain.get_signatures_for_address(address, limit=1)
            self._cursors[address] = sigs[0]["signature"] if sigs else None
            logger.debug(
                f"Cursor set for {address[:8]}…: "
                f"{self._cursors[address] and self._cursors[address][:12]}…"
            )
        except Exception as e:
            logger.warning(f"Could not init cursor for {address[:8]}…: {e}")
            self._cursors[address] = None

    # ── Poll loop ─────────────────────────────────────────────

    async def _poll_loop(self) -> None:
        while True:
            try:
                await self._poll_all_wallets()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"ExchangeMonitor poll error: {e}")
            await asyncio.sleep(POLL_INTERVAL)

    async def _poll_all_wallets(self) -> None:
        for addr, exchange_name in self.exchange_wallets.items():
            try:
                await self._poll_wallet(addr, exchange_name)
            except Exception as e:
                logger.warning(f"Poll failed for {exchange_name} ({addr[:8]}…): {e}")
            await asyncio.sleep(INTER_WALLET_DELAY)

    async def _poll_wallet(self, address: str, exchange_name: str) -> None:
        """
        Fetch signatures newer than the current cursor, process qualifying
        transfers, then advance the cursor.
        """
        cursor = self._cursors.get(address)

        # Fetch signatures UNTIL the cursor (exclusive — only new ones)
        params = [address, {"limit": SIGNATURES_PER_POLL}]
        if cursor:
            params[1]["until"] = cursor

        raw = await self.blockchain._rpc("getSignaturesForAddress", params)
        sigs: list[dict] = raw if isinstance(raw, list) else []

        if not sigs:
            return

        # Advance cursor to the newest signature (first in the list)
        self._cursors[address] = sigs[0]["signature"]

        # Process oldest → newest so we add candidates in time order
        for sig_info in reversed(sigs):
            sig = sig_info.get("signature")
            if not sig or sig_info.get("err"):
                continue
            if await self.db.is_signature_processed(sig):
                continue

            await self.db.mark_signature_processed(sig)
            tx = await self.blockchain.get_transaction(sig)
            if not tx:
                continue

            await self._parse_transfer(tx, sig, address, exchange_name)

    # ── Transfer parsing ──────────────────────────────────────

    async def _parse_transfer(
        self,
        tx: dict,
        signature: str,
        exchange_addr: str,
        exchange_name: str,
    ) -> None:
        """
        Determine whether the exchange wallet sent 1–20 SOL to any address,
        then evaluate each qualifying recipient as a candidate.
        """
        try:
            meta = tx.get("meta", {})
            if meta.get("err"):
                return

            message      = tx.get("transaction", {}).get("message", {})
            account_keys = message.get("accountKeys", [])
            pre_bal      = meta.get("preBalances", [])
            post_bal     = meta.get("postBalances", [])

            if not pre_bal or not post_bal:
                return

            addresses = _extract_addresses(account_keys)

            if exchange_addr not in addresses:
                return

            ex_idx = addresses.index(exchange_addr)
            if ex_idx >= len(pre_bal) or ex_idx >= len(post_bal):
                return

            # Exchange wallet must have SENT SOL (balance decreased, minus fee)
            sent_lamports = pre_bal[ex_idx] - post_bal[ex_idx]
            if sent_lamports <= 0:
                return

            sent_sol = sent_lamports / LAMPORTS_PER_SOL
            # Allow a little headroom for fees on the upper bound
            if not (self.min_sol <= sent_sol <= self.max_sol + 0.002):
                return

            block_time = tx.get("blockTime") or int(time.time())

            # Find all recipients who received SOL in the qualifying range
            for i, addr in enumerate(addresses):
                if i == ex_idx:
                    continue
                if not addr or addr in self.ignored or addr in self.exchange_wallets:
                    continue
                if i >= len(pre_bal) or i >= len(post_bal):
                    continue

                received_lamports = post_bal[i] - pre_bal[i]
                if received_lamports <= 0:
                    continue

                received_sol = received_lamports / LAMPORTS_PER_SOL
                if not (self.min_sol <= received_sol <= self.max_sol):
                    continue

                logger.info(
                    f"[ExchangeMonitor] Transfer detected: {exchange_name} → "
                    f"{addr[:8]}… ({received_sol:.4f} SOL) tx={signature[:16]}…"
                )

                asyncio.create_task(
                    self._evaluate_candidate(
                        addr, exchange_name, received_sol, signature, block_time
                    )
                )

        except Exception as e:
            logger.error(f"_parse_transfer error (sig={signature[:12]}…): {e}")

    # ── Candidate evaluation ──────────────────────────────────

    async def _evaluate_candidate(
        self,
        wallet_addr: str,
        exchange_name: str,
        funding_sol: float,
        funding_tx: str,
        funding_time: int,
    ) -> None:
        try:
            if wallet_addr in self.ignored:
                return

            # Skip if already actively tracked
            existing = await self.db.get_tracked_wallet(wallet_addr)
            if existing and existing["status"] == "active":
                return

            # Fetch transaction history to assess activity level
            sigs = await self.blockchain.get_signatures_for_address(
                wallet_addr, limit=self.config.max_historical_txs + 1
            )
            historical_tx_count = len(sigs)

            if historical_tx_count > self.config.max_historical_txs:
                logger.debug(
                    f"Rejected {wallet_addr[:8]}… — "
                    f"too many txns ({historical_tx_count})"
                )
                return

            # Estimate wallet age from oldest known signature
            wallet_age_days = 0.0
            if sigs:
                oldest_time = sigs[-1].get("blockTime") or funding_time
                wallet_age_days = max(0.0, (funding_time - oldest_time) / 86400)

            score = self.scoring.calculate_score({
                "exchange":              exchange_name,
                "funding_amount":        funding_sol,
                "wallet_age_days":       wallet_age_days,
                "historical_tx_count":   historical_tx_count,
                "launchpad_interaction": False,
                "previous_launches":     0,
                "unrelated_transfers":   0,
            })

            now = int(time.time())
            tracking_expires = now + (self.config.max_tracking_days * 86400)

            added = await self.db.add_tracked_wallet({
                "wallet_address":      wallet_addr,
                "funding_exchange":    exchange_name,
                "funding_tx":          funding_tx,
                "funding_amount_sol":  funding_sol,
                "funding_time":        funding_time,
                "tracking_start":      now,
                "tracking_expires":    tracking_expires,
                "score":               score,
                "historical_tx_count": historical_tx_count,
                "wallet_age_days":     wallet_age_days,
            })

            if added and self._candidate_tracker:
                logger.info(
                    f"[ExchangeMonitor] ✓ Candidate added: {wallet_addr[:8]}… "
                    f"(exchange={exchange_name}, sol={funding_sol:.4f}, "
                    f"hist_txns={historical_tx_count}, score={score:.0f})"
                )
                await self._candidate_tracker.add_wallet(wallet_addr)

        except Exception as e:
            logger.error(f"_evaluate_candidate error ({wallet_addr[:8]}…): {e}")


# ── Helper ────────────────────────────────────────────────────

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
