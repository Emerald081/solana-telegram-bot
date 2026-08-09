"""
database.py — async SQLite database layer using aiosqlite.

Tables:
  tracked_wallets       — active and historical tracking records
  developer_history     — confirmed token launches per wallet
  alert_history         — dedup: one alert per wallet+mint
  processed_signatures  — dedup: never process the same tx twice

Schema migrations are applied automatically on startup so the DB
survives bot restarts without manual intervention.
"""

import time
import logging
from typing import Optional
import aiosqlite

logger = logging.getLogger(__name__)

_SCHEMA = """
PRAGMA journal_mode=WAL;

CREATE TABLE IF NOT EXISTS tracked_wallets (
    wallet_address          TEXT PRIMARY KEY,
    funding_exchange        TEXT NOT NULL,
    funding_tx              TEXT,
    funding_amount_sol      REAL NOT NULL,
    funding_time            INTEGER NOT NULL,
    tracking_start          INTEGER NOT NULL,
    tracking_expires        INTEGER NOT NULL,
    priority                TEXT DEFAULT 'candidate',
    first_launchpad_time    INTEGER,
    last_launchpad_time     INTEGER,
    launchpad_detected      TEXT,
    preparation_detected    INTEGER DEFAULT 0,
    preparation_paused_at   INTEGER,
    unrelated_transfer_count INTEGER DEFAULT 0,
    score                   REAL DEFAULT 0,
    status                  TEXT DEFAULT 'active',
    historical_tx_count     INTEGER DEFAULT 0,
    wallet_age_days         REAL DEFAULT 0,
    hop_source              TEXT,
    created_at              INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS developer_history (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    wallet_address   TEXT NOT NULL,
    token_mint       TEXT NOT NULL,
    launchpad        TEXT,
    funding_exchange TEXT,
    tx_signature     TEXT,
    created_at       INTEGER NOT NULL,
    UNIQUE(wallet_address, token_mint)
);

CREATE TABLE IF NOT EXISTS alert_history (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    wallet_address      TEXT NOT NULL,
    token_mint          TEXT NOT NULL,
    alert_time          INTEGER NOT NULL,
    telegram_message_id INTEGER,
    UNIQUE(wallet_address, token_mint)
);

CREATE TABLE IF NOT EXISTS processed_signatures (
    signature    TEXT PRIMARY KEY,
    processed_at INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_tw_status    ON tracked_wallets(status);
CREATE INDEX IF NOT EXISTS idx_dh_wallet    ON developer_history(wallet_address);
CREATE INDEX IF NOT EXISTS idx_ah_wallet    ON alert_history(wallet_address);
CREATE INDEX IF NOT EXISTS idx_ps_time      ON processed_signatures(processed_at);
"""

# Safe migrations applied to existing databases. Each is idempotent.
_MIGRATIONS = [
    "ALTER TABLE tracked_wallets ADD COLUMN priority TEXT DEFAULT 'candidate'",
    "ALTER TABLE tracked_wallets ADD COLUMN first_launchpad_time INTEGER",
    "ALTER TABLE tracked_wallets ADD COLUMN preparation_detected INTEGER DEFAULT 0",
    "ALTER TABLE tracked_wallets ADD COLUMN preparation_paused_at INTEGER",
    "ALTER TABLE tracked_wallets ADD COLUMN hop_source TEXT",
]


class Database:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self._conn: Optional[aiosqlite.Connection] = None

    async def initialize(self) -> None:
        self._conn = await aiosqlite.connect(self.db_path)
        self._conn.row_factory = aiosqlite.Row
        await self._conn.executescript(_SCHEMA)
        await self._conn.commit()
        await self._run_migrations()
        logger.info(f"Database initialized: {self.db_path}")

    async def _run_migrations(self) -> None:
        """Apply schema migrations that are safe to run on an existing database."""
        for sql in _MIGRATIONS:
            try:
                await self._conn.execute(sql)
                await self._conn.commit()
            except Exception:
                # "duplicate column name" → column already exists; safe to ignore
                pass

    async def close(self) -> None:
        if self._conn:
            await self._conn.close()

    # ── Tracked Wallets ───────────────────────────────────────

    async def add_tracked_wallet(self, data: dict) -> bool:
        """Insert a new tracked wallet. Returns False if already exists."""
        try:
            await self._conn.execute(
                """
                INSERT OR IGNORE INTO tracked_wallets
                  (wallet_address, funding_exchange, funding_tx, funding_amount_sol,
                   funding_time, tracking_start, tracking_expires, priority, score,
                   historical_tx_count, wallet_age_days, hop_source, created_at)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    data["wallet_address"],
                    data["funding_exchange"],
                    data.get("funding_tx"),
                    data["funding_amount_sol"],
                    data["funding_time"],
                    data["tracking_start"],
                    data["tracking_expires"],
                    data.get("priority", "candidate"),
                    data.get("score", 0),
                    data.get("historical_tx_count", 0),
                    data.get("wallet_age_days", 0),
                    data.get("hop_source"),
                    int(time.time()),
                ),
            )
            await self._conn.commit()
            # Return True only if a row was actually inserted
            async with self._conn.execute(
                "SELECT changes()"
            ) as cur:
                row = await cur.fetchone()
                return (row[0] if row else 0) > 0
        except Exception as e:
            logger.error(f"add_tracked_wallet error: {e}")
            return False

    async def get_tracked_wallet(self, address: str) -> Optional[dict]:
        async with self._conn.execute(
            "SELECT * FROM tracked_wallets WHERE wallet_address = ?", (address,)
        ) as cur:
            row = await cur.fetchone()
            return dict(row) if row else None

    async def get_active_tracked_wallets(self) -> list[dict]:
        async with self._conn.execute(
            "SELECT * FROM tracked_wallets WHERE status = 'active'"
        ) as cur:
            rows = await cur.fetchall()
            return [dict(r) for r in rows]

    async def get_high_priority_wallets(self) -> list[dict]:
        async with self._conn.execute(
            "SELECT * FROM tracked_wallets WHERE status = 'active' AND priority = 'high_priority'"
        ) as cur:
            rows = await cur.fetchall()
            return [dict(r) for r in rows]

    async def update_wallet_status(self, address: str, status: str) -> None:
        await self._conn.execute(
            "UPDATE tracked_wallets SET status = ? WHERE wallet_address = ?",
            (status, address),
        )
        await self._conn.commit()

    async def upgrade_to_high_priority(
        self, address: str, launchpad: str, ts: int, max_days: int
    ) -> None:
        """
        Upgrade wallet to high_priority and set first_launchpad_time if not already set.
        tracking_expires is reset to first_launchpad_time + max_days.
        Subsequent launchpad interactions update last_launchpad_time only.
        """
        wallet = await self.get_tracked_wallet(address)
        if not wallet:
            return

        first_lp_time = wallet.get("first_launchpad_time") or ts
        new_expires = first_lp_time + (max_days * 86400)

        await self._conn.execute(
            """
            UPDATE tracked_wallets
            SET priority             = 'high_priority',
                first_launchpad_time = COALESCE(first_launchpad_time, ?),
                last_launchpad_time  = ?,
                launchpad_detected   = ?,
                tracking_expires     = ?
            WHERE wallet_address = ?
            """,
            (first_lp_time, ts, launchpad, new_expires, address),
        )
        await self._conn.commit()

    async def set_preparation_detected(
        self, address: str, detected: bool, ts: Optional[int] = None
    ) -> None:
        """
        Record that preparation activity was seen (detected=True) or cleared (False).
        When detected=True, preparation_paused_at is set to `ts` (default: now).
        """
        if detected:
            paused_at = ts or int(time.time())
            await self._conn.execute(
                """
                UPDATE tracked_wallets
                SET preparation_detected = 1, preparation_paused_at = ?
                WHERE wallet_address = ?
                """,
                (paused_at, address),
            )
        else:
            await self._conn.execute(
                """
                UPDATE tracked_wallets
                SET preparation_detected = 0, preparation_paused_at = NULL
                WHERE wallet_address = ?
                """,
                (address,),
            )
        await self._conn.commit()

    async def increment_unrelated_transfers(self, address: str) -> int:
        await self._conn.execute(
            """
            UPDATE tracked_wallets
            SET unrelated_transfer_count = unrelated_transfer_count + 1
            WHERE wallet_address = ?
            """,
            (address,),
        )
        await self._conn.commit()
        async with self._conn.execute(
            "SELECT unrelated_transfer_count FROM tracked_wallets WHERE wallet_address = ?",
            (address,),
        ) as cur:
            row = await cur.fetchone()
            return row[0] if row else 0

    async def expire_old_wallets(self, prep_pause_seconds: int = 172800) -> list[str]:
        """
        Mark expired wallets as 'expired'. Skips wallets with recent
        preparation activity (within prep_pause_seconds of detection).

        Returns list of expired wallet addresses.
        """
        now = int(time.time())
        prep_cutoff = now - prep_pause_seconds  # prep_paused_at must be older than this

        async with self._conn.execute(
            """
            SELECT wallet_address FROM tracked_wallets
            WHERE status = 'active'
              AND tracking_expires < ?
              AND (
                preparation_paused_at IS NULL
                OR preparation_paused_at < ?
              )
            """,
            (now, prep_cutoff),
        ) as cur:
            rows = await cur.fetchall()

        expired = [r[0] for r in rows]
        if expired:
            placeholders = ",".join("?" * len(expired))
            await self._conn.execute(
                f"UPDATE tracked_wallets SET status='expired' WHERE wallet_address IN ({placeholders})",
                expired,
            )
            await self._conn.commit()
        return expired

    async def count_active_wallets(self) -> int:
        async with self._conn.execute(
            "SELECT COUNT(*) FROM tracked_wallets WHERE status='active'"
        ) as cur:
            row = await cur.fetchone()
            return row[0] if row else 0

    async def count_high_priority_wallets(self) -> int:
        async with self._conn.execute(
            "SELECT COUNT(*) FROM tracked_wallets WHERE status='active' AND priority='high_priority'"
        ) as cur:
            row = await cur.fetchone()
            return row[0] if row else 0

    async def update_wallet_score(self, address: str, score: float) -> None:
        await self._conn.execute(
            "UPDATE tracked_wallets SET score = ? WHERE wallet_address = ?",
            (score, address),
        )
        await self._conn.commit()

    # ── Developer History ─────────────────────────────────────

    async def add_developer_history(
        self,
        wallet_address: str,
        token_mint: str,
        launchpad: str,
        funding_exchange: str,
        tx_signature: str,
    ) -> None:
        try:
            await self._conn.execute(
                """
                INSERT OR IGNORE INTO developer_history
                  (wallet_address, token_mint, launchpad, funding_exchange, tx_signature, created_at)
                VALUES (?,?,?,?,?,?)
                """,
                (wallet_address, token_mint, launchpad, funding_exchange, tx_signature, int(time.time())),
            )
            await self._conn.commit()
        except Exception as e:
            logger.error(f"add_developer_history error: {e}")

    async def get_developer_history(self, wallet_address: str) -> list[dict]:
        async with self._conn.execute(
            "SELECT * FROM developer_history WHERE wallet_address = ? ORDER BY created_at DESC",
            (wallet_address,),
        ) as cur:
            rows = await cur.fetchall()
            return [dict(r) for r in rows]

    # ── Alert Dedup ───────────────────────────────────────────

    async def has_alert_been_sent(self, wallet_address: str, token_mint: str) -> bool:
        async with self._conn.execute(
            "SELECT id FROM alert_history WHERE wallet_address=? AND token_mint=?",
            (wallet_address, token_mint),
        ) as cur:
            return await cur.fetchone() is not None

    async def record_alert(
        self, wallet_address: str, token_mint: str, message_id: Optional[int] = None
    ) -> None:
        try:
            await self._conn.execute(
                """
                INSERT OR IGNORE INTO alert_history
                  (wallet_address, token_mint, alert_time, telegram_message_id)
                VALUES (?,?,?,?)
                """,
                (wallet_address, token_mint, int(time.time()), message_id),
            )
            await self._conn.commit()
        except Exception as e:
            logger.error(f"record_alert error: {e}")

    # ── Signature Dedup ───────────────────────────────────────

    async def is_signature_processed(self, signature: str) -> bool:
        async with self._conn.execute(
            "SELECT signature FROM processed_signatures WHERE signature=?", (signature,)
        ) as cur:
            return await cur.fetchone() is not None

    async def mark_signature_processed(self, signature: str) -> None:
        try:
            await self._conn.execute(
                "INSERT OR IGNORE INTO processed_signatures (signature, processed_at) VALUES (?,?)",
                (signature, int(time.time())),
            )
            await self._conn.commit()
        except Exception:
            pass

    async def cleanup_old_signatures(self, days: int = 7) -> None:
        cutoff = int(time.time()) - (days * 86400)
        await self._conn.execute(
            "DELETE FROM processed_signatures WHERE processed_at < ?", (cutoff,)
        )
        await self._conn.commit()

    # ── Stats ─────────────────────────────────────────────────

    async def get_stats(self) -> dict:
        stats = {}
        for label, query in [
            ("active_wallets",       "SELECT COUNT(*) FROM tracked_wallets WHERE status='active'"),
            ("high_priority",        "SELECT COUNT(*) FROM tracked_wallets WHERE status='active' AND priority='high_priority'"),
            ("candidate",            "SELECT COUNT(*) FROM tracked_wallets WHERE status='active' AND priority='candidate'"),
            ("expired_wallets",      "SELECT COUNT(*) FROM tracked_wallets WHERE status='expired'"),
            ("disqualified",         "SELECT COUNT(*) FROM tracked_wallets WHERE status='disqualified'"),
            ("tokens_detected",      "SELECT COUNT(*) FROM developer_history"),
            ("alerts_sent",          "SELECT COUNT(*) FROM alert_history"),
            ("prep_activity_active", "SELECT COUNT(*) FROM tracked_wallets WHERE status='active' AND preparation_detected=1"),
        ]:
            async with self._conn.execute(query) as cur:
                row = await cur.fetchone()
                stats[label] = row[0] if row else 0
        return stats
