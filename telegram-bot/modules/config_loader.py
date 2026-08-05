"""
config_loader.py — loads and merges all configuration files.

Sources (in order of priority):
  telegram-bot/config/settings.yaml     — main settings
  telegram-bot/config/launchpads.yaml   — launchpad / DEX program IDs
  telegram-bot/config/ignored_wallets.yaml — blacklist
  exchange_wallets.json                 — exchange wallet addresses (project root)

Environment variables override YAML values where noted.
"""

import json
import os
from pathlib import Path
import yaml


class Config:
    def __init__(self):
        # Resolve directories relative to this file's location.
        # __file__ = telegram-bot/modules/config_loader.py
        self.bot_dir: Path = Path(__file__).parent.parent          # telegram-bot/
        self.project_root: Path = self.bot_dir.parent              # project root

        self.settings: dict = {}
        self.launchpads: dict = {}
        self.ignored_wallets: list[str] = []
        self.exchange_wallets: dict[str, str] = {}  # address -> exchange name

        self.load_all()

    # ── Public API ────────────────────────────────────────────

    def load_all(self) -> None:
        self.settings = self._load_yaml("config/settings.yaml")
        raw_lp = self._load_yaml("config/launchpads.yaml")
        self.launchpads = raw_lp.get("launchpads", {})
        ignored_data = self._load_yaml("config/ignored_wallets.yaml")
        self.ignored_wallets = [
            w["address"]
            for w in ignored_data.get("wallets", [])
            if isinstance(w, dict) and "address" in w
        ]
        self._load_exchange_wallets()
        self._apply_env_overrides()

    def reload(self) -> None:
        """Hot-reload all config files (e.g. after editing launchpads.yaml)."""
        self.load_all()

    # ── Convenience properties ────────────────────────────────

    @property
    def rpc_http(self) -> str:
        endpoint = self.settings["rpc"]["http_endpoint"]
        return self._interpolate(endpoint)

    @property
    def rpc_ws(self) -> str:
        endpoint = self.settings["rpc"]["ws_endpoint"]
        return self._interpolate(endpoint)

    @property
    def db_path(self) -> str:
        rel = self.settings.get("database", {}).get("path", "tracker.db")
        return str(self.bot_dir / rel)

    @property
    def telegram_chat_id(self) -> int:
        return int(self.settings["telegram"]["chat_id"])

    @property
    def min_sol(self) -> float:
        return float(self.settings["funding"]["min_sol"])

    @property
    def max_sol(self) -> float:
        return float(self.settings["funding"]["max_sol"])

    @property
    def max_tracking_days(self) -> int:
        return int(self.settings["tracking"]["max_days"])

    @property
    def max_unrelated_transfers(self) -> int:
        return int(self.settings["tracking"]["max_unrelated_transfers"])

    @property
    def max_historical_txs(self) -> int:
        return int(self.settings.get("candidate", {}).get("max_historical_txs", 20))

    @property
    def maintenance_interval(self) -> int:
        return int(self.settings.get("maintenance", {}).get("interval_seconds", 3600))

    @property
    def signature_retention_days(self) -> int:
        return int(self.settings.get("maintenance", {}).get("signature_retention_days", 7))

    # ── Hop detection ─────────────────────────────────────────

    @property
    def hop_detection_enabled(self) -> bool:
        return bool(self.settings.get("hop_detection", {}).get("enabled", True))

    @property
    def hop_min_fraction(self) -> float:
        return float(self.settings.get("hop_detection", {}).get("min_forwarded_fraction", 0.70))

    @property
    def hop_max_hours(self) -> int:
        return int(self.settings.get("hop_detection", {}).get("max_hours_after_funding", 24))

    # ── Preparation activity ──────────────────────────────────

    @property
    def prep_pause_hours(self) -> int:
        return int(self.settings.get("preparation_activity", {}).get("pause_expiry_hours", 48))

    # ── High priority ─────────────────────────────────────────

    @property
    def high_priority_poll_interval(self) -> int:
        return int(self.settings.get("high_priority", {}).get("poll_interval_seconds", 30))

    # ── Internal helpers ──────────────────────────────────────

    def _load_yaml(self, relative_path: str) -> dict:
        path = self.bot_dir / relative_path
        with open(path, "r", encoding="utf-8") as f:
            return yaml.safe_load(f) or {}

    def _load_exchange_wallets(self) -> None:
        rel = self.settings.get("exchange_wallets_file", "exchange_wallets.json")
        path = self.project_root / rel
        with open(path, "r", encoding="utf-8") as f:
            data: dict = json.load(f)
        self.exchange_wallets = {}
        for exchange_name, wallets in data.items():
            for addr in wallets:
                addr = addr.strip()
                if addr:
                    self.exchange_wallets[addr] = exchange_name

    def _apply_env_overrides(self) -> None:
        """Allow env vars to override critical config without touching YAML."""
        # HELIUS_API_KEY is interpolated lazily in rpc_http / rpc_ws properties.
        # SOLANA_RPC_HTTP and SOLANA_RPC_WS override the endpoint entirely if set.
        if os.environ.get("SOLANA_RPC_HTTP"):
            self.settings.setdefault("rpc", {})["http_endpoint"] = os.environ["SOLANA_RPC_HTTP"]
        if os.environ.get("SOLANA_RPC_WS"):
            self.settings.setdefault("rpc", {})["ws_endpoint"] = os.environ["SOLANA_RPC_WS"]

    def _interpolate(self, value: str) -> str:
        """Replace {ENV_VAR} placeholders with environment variable values."""
        import re
        def replacer(m):
            key = m.group(1)
            val = os.environ.get(key, "")
            if not val:
                raise EnvironmentError(
                    f"Environment variable '{key}' is not set. "
                    f"Please add it as a secret (HELIUS_API_KEY recommended). "
                    f"Sign up at https://helius.dev to get a free API key."
                )
            return val
        return re.sub(r"\{(\w+)\}", replacer, value)
