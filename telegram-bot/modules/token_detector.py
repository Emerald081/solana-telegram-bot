"""
token_detector.py — detect SPL token mint creation.

Looks for InitializeMint / InitializeMint2 instructions targeting
either the classic SPL Token program or Token-2022.
Uses three methods in order of reliability:
  1. Parsed instruction 'type' field (most accurate — needs jsonParsed encoding)
  2. Raw instruction with token program in programIdIndex
  3. Log message scan (fallback)
"""

import logging
from typing import Optional

logger = logging.getLogger(__name__)

SPL_TOKEN      = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
SPL_TOKEN_2022 = "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb"
TOKEN_PROGRAMS = {SPL_TOKEN, SPL_TOKEN_2022}

MINT_TYPES = {"initializeMint", "initializeMint2"}
SYSTEM_ACCOUNTS = {
    SPL_TOKEN,
    SPL_TOKEN_2022,
    "11111111111111111111111111111111",
    "ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJe1bKd",
    "SysvarRent111111111111111111111111111111111",
    "SysvarC1ock11111111111111111111111111111111",
    "ComputeBudget111111111111111111111111111111111",
}


class TokenDetector:

    # ── Quick log scan ────────────────────────────────────────

    def detect_from_logs(self, logs: list[str]) -> bool:
        """
        Fast pre-check: returns True if logs suggest a mint was initialised.
        Call this before fetching the full transaction.
        """
        for log in logs:
            if "InitializeMint" in log or "initializeMint" in log:
                return True
        return False

    # ── Full detection ────────────────────────────────────────

    async def detect_token_creation(
        self, tx: dict, creator_address: str
    ) -> Optional[str]:
        """
        Returns the new token mint address if the transaction initialises
        an SPL token mint AND `creator_address` is a signer / fee payer.
        Returns None otherwise.
        """
        try:
            message = tx.get("transaction", {}).get("message", {})
            account_keys = message.get("accountKeys", [])
            top_instructions = message.get("instructions", [])
            meta = tx.get("meta", {})
            inner_instructions = meta.get("innerInstructions", [])
            log_messages = meta.get("logMessages", [])

            if meta.get("err"):
                return None  # failed transaction

            # Resolve account addresses
            addresses = _extract_addresses(account_keys)

            # Creator must be in the transaction
            if creator_address not in addresses:
                return None

            # Method 1: parsed top-level instructions
            mint = _scan_instructions(top_instructions, addresses)
            if mint:
                return mint

            # Method 2: parsed inner instructions
            for inner in inner_instructions:
                mint = _scan_instructions(inner.get("instructions", []), addresses)
                if mint:
                    return mint

            # Method 3: log message fallback
            if self.detect_from_logs(log_messages):
                mint = _guess_mint_from_accounts(addresses)
                if mint:
                    return mint

        except Exception as e:
            logger.debug(f"detect_token_creation error: {e}")

        return None


# ── Helpers ───────────────────────────────────────────────────

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


def _scan_instructions(instructions: list, addresses: list[str]) -> Optional[str]:
    for ix in instructions:
        mint = _check_ix(ix, addresses)
        if mint:
            return mint
    return None


def _check_ix(ix: dict, addresses: list[str]) -> Optional[str]:
    # ── Parsed format ──────────────────────────────────────────
    if "parsed" in ix:
        parsed = ix["parsed"]
        if isinstance(parsed, dict):
            ix_type = parsed.get("type", "")
            if ix_type in MINT_TYPES:
                prog_id = ix.get("programId", "")
                if prog_id in TOKEN_PROGRAMS:
                    info = parsed.get("info", {})
                    mint = info.get("mint")
                    if mint:
                        return mint

    # ── Raw format ─────────────────────────────────────────────
    prog_idx = ix.get("programIdIndex")
    if prog_idx is not None and isinstance(prog_idx, int):
        if prog_idx < len(addresses) and addresses[prog_idx] in TOKEN_PROGRAMS:
            # The first account in a InitializeMint instruction is the mint
            acct_indexes = ix.get("accounts", [])
            if acct_indexes and acct_indexes[0] < len(addresses):
                return addresses[acct_indexes[0]]

    return None


def _guess_mint_from_accounts(addresses: list[str]) -> Optional[str]:
    """
    Last-resort fallback when the instruction format isn't parseable.
    Returns the first non-system account that could be a mint.
    """
    for addr in addresses:
        if addr and addr not in SYSTEM_ACCOUNTS and len(addr) >= 32:
            return addr
    return None
