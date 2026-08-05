"""
telegram_alerts.py — format and send token creation alerts via Telegram.

Only called when a tracked wallet creates or mints a new SPL token.
All other events (funding, candidate added, launchpad interaction) are
logged locally and NEVER generate Telegram messages.
"""

import logging
from datetime import datetime, timezone
from typing import Optional

from telegram import Bot
from telegram.error import TelegramError

logger = logging.getLogger(__name__)


class TelegramAlerts:
    def __init__(self, bot: Bot, chat_id: int, scoring_engine):
        self.bot = bot
        self.chat_id = chat_id
        self.scoring = scoring_engine

    async def send_token_creation_alert(
        self,
        wallet: dict,
        token_mint: str,
        tx_signature: str,
        launchpad: str,
        dev_history: list[dict],
        score: float,
    ) -> Optional[int]:
        """
        Send a formatted HTML alert. Returns the Telegram message_id,
        or None if delivery failed.
        """
        try:
            text = self._build_message(
                wallet, token_mint, tx_signature, launchpad, dev_history, score
            )
            msg = await self.bot.send_message(
                chat_id=self.chat_id,
                text=text,
                parse_mode="HTML",
                disable_web_page_preview=True,
            )
            logger.info(
                f"Alert sent — wallet {wallet['wallet_address'][:8]}… "
                f"mint {token_mint[:8]}… msg_id={msg.message_id}"
            )
            return msg.message_id
        except TelegramError as e:
            logger.error(f"Telegram delivery failed: {e}")
            return None

    # ── Message builder ───────────────────────────────────────

    def _build_message(
        self,
        wallet: dict,
        token_mint: str,
        tx_signature: str,
        launchpad: str,
        dev_history: list[dict],
        score: float,
    ) -> str:
        addr = wallet["wallet_address"]
        addr_short = f"{addr[:6]}…{addr[-4:]}"
        mint_short = f"{token_mint[:6]}…{token_mint[-4:]}"

        score_label = self.scoring.get_label(score)
        funded_at   = _fmt_time(wallet["funding_time"])
        tracked_for = _tracked_duration(wallet["tracking_start"])
        amount      = wallet["funding_amount_sol"]
        exchange    = wallet["funding_exchange"]
        lp_display  = launchpad if launchpad and launchpad != "Unknown" else "Unknown / not yet confirmed"

        # ── Developer history block ───────────────────────────
        if dev_history:
            prev_count = len(dev_history)
            hist_lines = []
            for i, rec in enumerate(dev_history[:5], 1):
                pm = rec["token_mint"]
                pm_short = f"{pm[:6]}…{pm[-4:]}"
                lp   = rec.get("launchpad") or "?"
                when = _fmt_time(rec["created_at"])
                hist_lines.append(
                    f"  {i}. <a href='https://solscan.io/token/{pm}'>{pm_short}</a>"
                    f" via {lp} ({when})"
                )
            if prev_count > 5:
                hist_lines.append(f"  … and {prev_count - 5} more")
            history_block = (
                f"\n\n📜 <b>Developer History</b> ({prev_count} prior launch"
                f"{'es' if prev_count > 1 else ''})\n"
                + "\n".join(hist_lines)
            )
        else:
            history_block = "\n\n📜 <b>Developer History:</b> First detected launch"

        # ── Score bar ─────────────────────────────────────────
        filled = round(score / 10)
        bar    = "█" * filled + "░" * (10 - filled)

        return (
            "🚨 <b>NEW TOKEN CREATED</b>\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"

            f"👛 <b>Wallet</b>\n"
            f"<code>{addr}</code>\n"
            f"<a href='https://solscan.io/account/{addr}'>View on Solscan</a>\n\n"

            f"🏦 <b>Funded by:</b> {exchange}\n"
            f"💰 <b>Amount:</b> {amount:.4f} SOL\n"
            f"⏱ <b>Funded at:</b> {funded_at}\n"
            f"🕐 <b>Tracked for:</b> {tracked_for}\n\n"

            f"🎯 <b>Token Mint</b>\n"
            f"<code>{token_mint}</code>\n\n"

            f"🚀 <b>Launchpad:</b> {lp_display}\n\n"

            f"📊 <b>Confidence Score:</b> {score:.0f}/100  {score_label}\n"
            f"<code>[{bar}]</code>\n"

            f"{history_block}\n\n"

            "🔗 <b>Links</b>\n"
            f"• <a href='https://solscan.io/tx/{tx_signature}'>Transaction</a>\n"
            f"• <a href='https://solscan.io/account/{addr}'>Wallet</a>\n"
            f"• <a href='https://solscan.io/token/{token_mint}'>Token on Solscan</a>\n"
            f"• <a href='https://dexscreener.com/solana/{token_mint}'>DexScreener</a>\n"
            f"• <a href='https://birdeye.so/token/{token_mint}?chain=solana'>Birdeye</a>\n"
            f"• <a href='https://photon-sol.tinyastro.io/en/lp/{token_mint}'>Photon</a>"
        )


# ── Utilities ─────────────────────────────────────────────────

def _fmt_time(ts: Optional[int]) -> str:
    if not ts:
        return "Unknown"
    dt = datetime.fromtimestamp(ts, tz=timezone.utc)
    return dt.strftime("%Y-%m-%d %H:%M UTC")


def _tracked_duration(start_ts: int) -> str:
    import time
    secs = int(time.time()) - start_ts
    if secs < 60:
        return f"{secs}s"
    elif secs < 3600:
        return f"{secs // 60}m"
    elif secs < 86400:
        return f"{secs // 3600}h {(secs % 3600) // 60}m"
    else:
        days = secs // 86400
        hours = (secs % 86400) // 3600
        return f"{days}d {hours}h"
