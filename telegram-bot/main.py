"""
main.py — Solana Tracker Bot entry point.

Starts the tracker subsystems in the correct order, then runs the
Telegram bot. On crash/restart, tracked wallets are automatically
reloaded from the database without losing progress.

Usage:
    python3 telegram-bot/main.py
"""

import asyncio
import logging
import os
import sys

from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

# ── Path resolution ───────────────────────────────────────────
# Allow importing from telegram-bot/modules/ regardless of CWD
sys.path.insert(0, os.path.dirname(__file__))

from modules.config_loader import Config
from modules.database import Database
from modules.blockchain_monitor import BlockchainMonitor
from modules.exchange_monitor import ExchangeMonitor
from modules.candidate_tracker import CandidateTracker
from modules.launchpad_detector import LaunchpadDetector
from modules.preparation_detector import PreparationDetector
from modules.token_detector import TokenDetector
from modules.scoring_engine import ScoringEngine
from modules.telegram_alerts import TelegramAlerts


# ── Logging setup ─────────────────────────────────────────────

def setup_logging(level_str: str = "INFO") -> None:
    level = getattr(logging, level_str.upper(), logging.INFO)
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    # Quiet noisy libraries
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    logging.getLogger("websockets").setLevel(logging.WARNING)
    logging.getLogger("telegram").setLevel(logging.WARNING)


logger = logging.getLogger(__name__)


# ── Tracker orchestrator ──────────────────────────────────────

class TrackerOrchestrator:
    """
    Wires up all subsystems and manages their shared lifecycle.
    """

    def __init__(self):
        self.config: Config | None = None
        self.db: Database | None = None
        self.blockchain: BlockchainMonitor | None = None
        self.exchange_monitor: ExchangeMonitor | None = None
        self.candidate_tracker: CandidateTracker | None = None

    async def start(self) -> None:
        logger.info("=== Solana Tracker Bot starting ===")

        # 1. Load configuration
        self.config = Config()
        setup_logging(self.config.settings.get("logging", {}).get("level", "INFO"))

        # 2. Validate critical config
        try:
            _ = self.config.rpc_http
            _ = self.config.rpc_ws
        except EnvironmentError as e:
            logger.error(f"Configuration error: {e}")
            logger.error(
                "Please set HELIUS_API_KEY as an environment secret.\n"
                "Sign up at https://helius.dev for a free API key.\n"
                "Alternative: set SOLANA_RPC_HTTP and SOLANA_RPC_WS directly."
            )
            raise

        logger.info(f"Exchange wallets loaded: {len(self.config.exchange_wallets)}")
        logger.info(f"Ignored wallets: {len(self.config.ignored_wallets)}")
        logger.info(f"Tracking window: {self.config.min_sol}–{self.config.max_sol} SOL")

        # 3. Database
        self.db = Database(self.config.db_path)
        await self.db.initialize()

        # 4. Blockchain monitor (WebSocket + RPC)
        self.blockchain = BlockchainMonitor(
            ws_endpoint=self.config.rpc_ws,
            rpc_endpoint=self.config.rpc_http,
            max_concurrent_rpc=self.config.settings["rpc"].get("max_concurrent_rpc", 10),
        )
        await self.blockchain.start()

        # 5. Shared helpers
        scoring  = ScoringEngine()
        lp_det   = LaunchpadDetector(self.config.launchpads)
        prep_det = PreparationDetector()
        tok_det  = TokenDetector()

        # 6. Telegram alerts (bot instance shared with command handlers below)
        bot_token = os.environ["TELEGRAM_BOT_TOKEN"]
        from telegram import Bot
        bot_instance = Bot(token=bot_token)
        alerts = TelegramAlerts(bot_instance, self.config.telegram_chat_id, scoring)

        # 7. Exchange monitor (created before candidate_tracker for cross-injection)
        self.exchange_monitor = ExchangeMonitor(
            config=self.config,
            db=self.db,
            blockchain=self.blockchain,
            scoring=scoring,
        )

        # 8. Candidate tracker — receives exchange_monitor reference for hop evaluation
        self.candidate_tracker = CandidateTracker(
            config=self.config,
            db=self.db,
            blockchain=self.blockchain,
            launchpad_detector=lp_det,
            preparation_detector=prep_det,
            token_detector=tok_det,
            alerts=alerts,
            scoring=scoring,
            exchange_monitor=self.exchange_monitor,
        )
        exchange_addr_set = set(self.config.exchange_wallets.keys())
        self.candidate_tracker.set_exchange_addresses(exchange_addr_set)
        self.exchange_monitor.set_candidate_tracker(self.candidate_tracker)

        # 9. Start subsystems in order
        await self.candidate_tracker.start()   # re-loads DB wallets first
        await self.exchange_monitor.start()    # begin polling exchange wallets

        logger.info("=== All subsystems running ===")

    async def stop(self) -> None:
        logger.info("Shutting down …")
        if self.blockchain:
            await self.blockchain.stop()
        if self.db:
            await self.db.close()
        logger.info("Shutdown complete.")

    async def get_stats(self) -> dict:
        if self.db:
            return await self.db.get_stats()
        return {}


# ── Telegram command handlers ─────────────────────────────────

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text("🚀 Solana Tracker Bot is running.")


async def cmd_ping(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text("🏓 Pong!")


async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    orchestrator: TrackerOrchestrator = context.bot_data.get("orchestrator")
    if not orchestrator or not orchestrator.db:
        await update.message.reply_text("⚠️ Tracker not yet initialised.")
        return

    stats = await orchestrator.get_stats()
    config = orchestrator.config

    text = (
        "📊 <b>Tracker Status</b>\n"
        "━━━━━━━━━━━━━━━━━━\n\n"
        f"🟢 <b>Active wallets:</b>      {stats.get('active_wallets', 0)}\n"
        f"   ⭐ High priority:        {stats.get('high_priority', 0)}\n"
        f"   👁 Candidate:            {stats.get('candidate', 0)}\n"
        f"   🔧 Prep activity active: {stats.get('prep_activity_active', 0)}\n\n"
        f"⏱  <b>Expired:</b>             {stats.get('expired_wallets', 0)}\n"
        f"❌ <b>Disqualified:</b>        {stats.get('disqualified', 0)}\n\n"
        f"🎯 <b>Tokens detected:</b>     {stats.get('tokens_detected', 0)}\n"
        f"📨 <b>Alerts sent:</b>         {stats.get('alerts_sent', 0)}\n\n"
        f"🏦 <b>Exchange wallets:</b>    {len(config.exchange_wallets)}\n"
        f"💰 <b>Funding window:</b>      {config.min_sol}–{config.max_sol} SOL\n"
        f"📅 <b>Tracking window:</b>     {config.max_tracking_days} days\n"
        f"🔗 <b>Hop detection:</b>       {'on' if config.hop_detection_enabled else 'off'}\n"
        f"🚫 <b>Max unrelated:</b>       {config.max_unrelated_transfers} transfers"
    )
    await update.message.reply_text(text, parse_mode="HTML")


async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    text = (
        "📖 <b>Available Commands</b>\n\n"
        "/start  — confirm the bot is running\n"
        "/ping   — latency check\n"
        "/status — live tracker statistics\n"
        "/help   — this message\n\n"
        "The bot silently monitors Solana exchange wallets and sends "
        "an alert <b>only</b> when a tracked wallet creates or mints a token."
    )
    await update.message.reply_text(text, parse_mode="HTML")


# ── Main entry point ──────────────────────────────────────────

async def main() -> None:
    orchestrator = TrackerOrchestrator()

    # Start tracker subsystems
    await orchestrator.start()

    # Build Telegram application
    bot_token = os.environ["TELEGRAM_BOT_TOKEN"]
    app = (
        Application.builder()
        .token(bot_token)
        .build()
    )

    # Make orchestrator accessible inside command handlers
    app.bot_data["orchestrator"] = orchestrator

    # Register commands
    app.add_handler(CommandHandler("start",  cmd_start))
    app.add_handler(CommandHandler("ping",   cmd_ping))
    app.add_handler(CommandHandler("status", cmd_status))
    app.add_handler(CommandHandler("help",   cmd_help))

    logger.info("Telegram bot polling started. Waiting for events …")

    try:
        await app.initialize()
        await app.start()
        await app.updater.start_polling(drop_pending_updates=True)

        # Run forever
        await asyncio.Event().wait()

    except (KeyboardInterrupt, SystemExit):
        logger.info("Interrupt received, shutting down …")
    finally:
        await app.updater.stop()
        await app.stop()
        await app.shutdown()
        await orchestrator.stop()


if __name__ == "__main__":
    asyncio.run(main())
