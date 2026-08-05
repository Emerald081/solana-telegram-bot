"""
scoring_engine.py — Developer Preparation Score (0–100).

The score is used internally to:
  • prioritise which wallets to focus on when resources are limited
  • include a human-readable confidence label in every alert

It is NOT a buy/sell signal. It measures how closely a wallet
matches the typical profile of a token creator funded by an exchange.
"""


class ScoringEngine:

    # Exchange tiers reflect typical withdrawal confirmation security.
    TIER_1 = {"Binance", "Coinbase", "OKX", "Bybit", "Kraken"}
    TIER_2 = {"KuCoin", "MEXC", "Gate.io", "Bitstamp", "Gemini"}
    # All others (MoonPay, Bitget, Bitvavo, Bitfinex, Others) → Tier 3

    def calculate_score(self, factors: dict) -> float:
        """
        factors (dict):
            exchange            str   — exchange name
            funding_amount      float — SOL sent
            wallet_age_days     float — days since first tx (0 = brand new)
            historical_tx_count int   — tx count before funding
            launchpad_interaction bool — interacted with known launchpad
            previous_launches   int   — prior token creations in DB
            unrelated_transfers int   — unrelated incoming SOL after funding
        """
        score = 0.0

        # ── Base: funded by a verified exchange (always true here) ──
        score += 20

        # ── Exchange tier ───────────────────────────────────────────
        exchange = factors.get("exchange", "")
        if exchange in self.TIER_1:
            score += 10
        elif exchange in self.TIER_2:
            score += 6
        else:
            score += 3

        # ── Funding amount ──────────────────────────────────────────
        # Sweet spot: 2–10 SOL (enough to cover fees/creation costs, low noise)
        amount = factors.get("funding_amount", 0.0)
        if 2.0 <= amount <= 5.0:
            score += 15
        elif 5.0 < amount <= 10.0:
            score += 12
        elif 1.0 <= amount < 2.0:
            score += 8
        elif 10.0 < amount <= 15.0:
            score += 5
        else:
            score += 2  # > 15 SOL — unusual, possible but lower signal

        # ── Wallet age ──────────────────────────────────────────────
        age = factors.get("wallet_age_days", 0.0)
        if age <= 1:
            score += 20  # Brand new — very strong signal
        elif age <= 7:
            score += 15
        elif age <= 30:
            score += 8
        elif age <= 90:
            score += 3
        else:
            score += 0   # Old wallet being reactivated — weaker signal

        # ── Historical activity ─────────────────────────────────────
        tx_count = factors.get("historical_tx_count", 0)
        if tx_count == 0:
            score += 20  # Never used before — pristine
        elif tx_count <= 3:
            score += 16
        elif tx_count <= 10:
            score += 10
        elif tx_count <= 20:
            score += 4
        else:
            score -= 5   # Too active — probably not a fresh dev wallet

        # ── Priority tier ───────────────────────────────────────────
        if factors.get("priority") == "high_priority":
            score += 10  # Already confirmed launchpad interest

        # ── Preparation activity detected ───────────────────────────
        if factors.get("preparation_detected"):
            score += 5  # Active pre-launch behaviour seen

        # ── Launchpad interaction during tracking ───────────────────
        if factors.get("launchpad_interaction"):
            score += 15  # Confirmed preparation behaviour

        # ── Developer history ───────────────────────────────────────
        prev = factors.get("previous_launches", 0)
        if prev == 0:
            score += 8   # First launch — high novelty
        elif prev == 1:
            score += 5
        elif prev <= 3:
            score += 3
        else:
            score += 1   # Serial launcher — still relevant but lower novelty

        # ── Penalty: unrelated incoming transfers ───────────────────
        # Each one dilutes the signal (possible bot or noisy wallet)
        unrelated = factors.get("unrelated_transfers", 0)
        score -= unrelated * 5

        return round(max(0.0, min(100.0, score)), 1)

    def get_label(self, score: float) -> str:
        if score >= 80:
            return "🔥 Very High"
        elif score >= 65:
            return "⚡ High"
        elif score >= 50:
            return "✅ Medium"
        elif score >= 35:
            return "⚠️ Low"
        else:
            return "❄️ Very Low"
