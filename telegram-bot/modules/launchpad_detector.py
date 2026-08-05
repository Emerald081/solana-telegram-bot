"""
launchpad_detector.py — detect launchpad / DEX program interactions.

Loads verified program IDs from launchpads.yaml.
Entries with placeholder: true are skipped at runtime until a real
program ID is confirmed and verified: true is set.
"""

import logging
from typing import Optional

logger = logging.getLogger(__name__)


def _is_placeholder(entry: dict) -> bool:
    return bool(entry.get("placeholder")) or not entry.get("verified", False)


class LaunchpadDetector:
    def __init__(self, launchpads_config: dict):
        # Build program_id → name map, skipping placeholders
        self.programs: dict[str, str] = {}
        skipped = []
        for key, entry in launchpads_config.items():
            pid = (entry.get("program_id") or "").strip()
            if not pid:
                continue
            if _is_placeholder(entry):
                skipped.append(entry.get("name", key))
                continue
            self.programs[pid] = entry["name"]

        logger.info(
            f"LaunchpadDetector: {len(self.programs)} verified programs loaded. "
            f"Skipped (placeholder): {skipped}"
        )

    # ── Quick scan (from WebSocket log strings) ───────────────

    def detect_from_logs(self, logs: list[str]) -> Optional[str]:
        """
        Scan raw log messages for a known launchpad program ID.
        Very fast — used before fetching the full transaction.
        Returns the launchpad name or None.
        """
        for log in logs:
            for pid, name in self.programs.items():
                if pid in log:
                    return name
        return None

    # ── Full scan (from parsed transaction) ───────────────────

    def detect_from_transaction(self, tx: dict) -> Optional[str]:
        """
        Scan account keys, instruction program IDs, and log messages
        of a fully fetched transaction.
        Returns the launchpad name or None.
        """
        try:
            message = tx.get("transaction", {}).get("message", {})
            account_keys = message.get("accountKeys", [])
            meta = tx.get("meta", {})
            log_messages = meta.get("logMessages", [])

            # Collect all addresses in the transaction
            addrs: set[str] = set()
            for key in account_keys:
                if isinstance(key, str):
                    addrs.add(key)
                elif isinstance(key, dict):
                    addrs.add(key.get("pubkey", ""))

            # Check addresses against known programs
            for pid, name in self.programs.items():
                if pid in addrs:
                    return name

            # Fallback: scan log messages
            for log in log_messages:
                for pid, name in self.programs.items():
                    if pid in log:
                        return name

        except Exception as e:
            logger.debug(f"detect_from_transaction error: {e}")

        return None

    def program_ids(self) -> set[str]:
        return set(self.programs.keys())
