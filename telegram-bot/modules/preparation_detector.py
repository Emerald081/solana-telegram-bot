"""
preparation_detector.py — detect meaningful pre-launch preparation activity.

Used to pause the expiry countdown when a tracked wallet is clearly
preparing for a token launch, even if it hasn't touched a launchpad yet.

Detects:
  • Creating Associated Token Accounts (ATA Program)
  • Initialising SPL Token / Token-2022 accounts
  • Compute Budget instructions (common before complex launches)
  • Multi-wallet SOL distribution (setting up sub-wallets for gas)
  • Account re-allocation / extend instructions (pre-launch account prep)

These patterns do NOT generate Telegram alerts — they only pause expiry.
"""

import logging
from typing import Optional

logger = logging.getLogger(__name__)

# ── Well-known program IDs ────────────────────────────────────

ASSOCIATED_TOKEN_PROGRAM = "ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJe1bKd"
SPL_TOKEN                = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
SPL_TOKEN_2022           = "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb"
COMPUTE_BUDGET           = "ComputeBudget111111111111111111111111111111111"
MEMO_PROGRAM             = "MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDLGmfcHr"
SYSTEM_PROGRAM           = "11111111111111111111111111111111"

# These programs appearing in a tx from a tracked wallet are preparation signals
PREPARATION_PROGRAMS = {
    ASSOCIATED_TOKEN_PROGRAM,
    SPL_TOKEN,
    SPL_TOKEN_2022,
    COMPUTE_BUDGET,
}

# Minimum number of SOL recipients to flag as "multi-wallet distribution"
MULTI_WALLET_THRESHOLD = 2

# Instructions that indicate active account preparation
PREP_INSTRUCTION_TYPES = {
    "initializeAccount",
    "initializeAccount2",
    "initializeAccount3",
    "initializeMultisig",
    "initializeMultisig2",
    "createAssociatedTokenAccountIdempotent",
    "createAssociatedTokenAccount",
    "setComputeUnitLimit",
    "setComputeUnitPrice",
}

# Log keywords that indicate preparation
PREP_LOG_KEYWORDS = [
    "ATokenGPvb",          # ATA program
    "TokenkegQf",          # SPL Token
    "TokenzQdBN",          # Token 2022
    "ComputeBudget",
    "createAssociated",
    "initializeAccount",
    "initializeMultisig",
]


class PreparationDetector:

    # ── Quick log scan ────────────────────────────────────────

    def detect_from_logs(self, logs: list[str]) -> bool:
        """
        Fast pre-check: returns True if logs suggest preparation activity.
        Call before fetching the full transaction.
        """
        for log in logs:
            for keyword in PREP_LOG_KEYWORDS:
                if keyword in log:
                    return True
        return False

    # ── Full detection ────────────────────────────────────────

    def detect_from_transaction(
        self, tx: dict, wallet_address: str
    ) -> tuple[bool, str]:
        """
        Analyse a fully fetched transaction for preparation activity.

        Returns:
            (detected: bool, reason: str)
        """
        try:
            meta    = tx.get("meta", {})
            message = tx.get("transaction", {}).get("message", {})

            if meta.get("err"):
                return False, ""

            account_keys   = message.get("accountKeys", [])
            instructions   = message.get("instructions", [])
            inner_ixs_list = meta.get("innerInstructions", [])
            log_messages   = meta.get("logMessages", [])
            pre_bal        = meta.get("preBalances", [])
            post_bal       = meta.get("postBalances", [])

            addresses = _extract_addresses(account_keys)

            if wallet_address not in addresses:
                return False, ""

            # ── 1. ATA / Token account creation ───────────────
            all_ixs = list(instructions)
            for inner in inner_ixs_list:
                all_ixs.extend(inner.get("instructions", []))

            for ix in all_ixs:
                prog_id = ix.get("programId", "")
                if prog_id in (ASSOCIATED_TOKEN_PROGRAM, SPL_TOKEN, SPL_TOKEN_2022):
                    parsed = ix.get("parsed", {})
                    if isinstance(parsed, dict):
                        ix_type = parsed.get("type", "")
                        if ix_type in PREP_INSTRUCTION_TYPES or "initialize" in ix_type.lower():
                            return True, f"Token account setup ({ix_type})"
                    else:
                        # Raw format — presence of these programs is enough
                        return True, "Token program interaction"

                if prog_id == COMPUTE_BUDGET:
                    parsed = ix.get("parsed", {})
                    if isinstance(parsed, dict):
                        ix_type = parsed.get("type", "")
                        if ix_type in PREP_INSTRUCTION_TYPES:
                            return True, f"Compute budget config ({ix_type})"

            # ── 2. Program presence in account keys ───────────
            prog_set = set(addresses)
            for prep_prog in PREPARATION_PROGRAMS:
                if prep_prog in prog_set:
                    # Only flag if wallet is a signer / writable in this tx
                    if _wallet_is_signer(account_keys, wallet_address):
                        return True, f"Preparation program in tx ({prep_prog[:12]}…)"

            # ── 3. Multi-wallet SOL distribution ──────────────
            if wallet_address in addresses:
                our_idx = addresses.index(wallet_address)
                if our_idx < len(pre_bal) and our_idx < len(post_bal):
                    our_delta = pre_bal[our_idx] - post_bal[our_idx]
                    if our_delta > 0:  # we sent SOL
                        recipients = 0
                        for i, addr in enumerate(addresses):
                            if i == our_idx or not addr:
                                continue
                            if i < len(post_bal) and i < len(pre_bal):
                                if post_bal[i] - pre_bal[i] > 0:
                                    recipients += 1
                        if recipients >= MULTI_WALLET_THRESHOLD:
                            return True, f"Multi-wallet SOL distribution ({recipients} recipients)"

            # ── 4. Log message scan ────────────────────────────
            for log in log_messages:
                for keyword in PREP_LOG_KEYWORDS:
                    if keyword in log:
                        return True, f"Preparation keyword in logs ({keyword})"

        except Exception as e:
            logger.debug(f"preparation detect error: {e}")

        return False, ""


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


def _wallet_is_signer(account_keys: list, wallet_address: str) -> bool:
    for key in account_keys:
        if isinstance(key, dict):
            if key.get("pubkey") == wallet_address:
                return key.get("signer", False) or key.get("writable", False)
        elif isinstance(key, str):
            if key == wallet_address:
                return True  # string format → assume signer
    return False
