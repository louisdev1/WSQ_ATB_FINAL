"""
config.py – Load .env and expose a single Config object.

PATH in .env is the project root. All other paths are derived from it.
Works on Windows and Raspberry Pi without any code changes.
"""

import os
from pathlib import Path
from dotenv import load_dotenv


def _resolve_env_path(raw: str, project_root: Path) -> Path:
    resolved = raw.replace("${PATH}", str(project_root))
    p = Path(resolved)
    if not p.is_absolute():
        p = project_root / p
    return p


def _load() -> "Config":
    here = Path(__file__).resolve().parent.parent
    env_file = here / ".env"
    load_dotenv(dotenv_path=env_file, override=True)

    raw_path = os.getenv("PATH", str(here))
    project_root = Path(raw_path) if Path(raw_path).exists() else here

    def _p(key: str, default: str) -> Path:
        raw = os.getenv(key, default)
        return _resolve_env_path(raw, project_root)

    def _bool(key: str, default: bool) -> bool:
        return os.getenv(key, str(default)).lower() in ("1", "true", "yes")

    def _int(key: str, default: int) -> int:
        return int(os.getenv(key, str(default)))

    def _float(key: str, default: float) -> float:
        return float(os.getenv(key, str(default)))

    return Config(
        project_root=project_root,

        # ── Telegram ──────────────────────────────────────────────────────────
        telegram_api_id=_int("TELEGRAM_API_ID", 0),
        telegram_api_hash=os.getenv("TELEGRAM_API_HASH", ""),
        telegram_phone=os.getenv("TELEGRAM_PHONE", ""),
        telegram_session_name=os.getenv("TELEGRAM_SESSION_NAME", "telegram_session"),
        telegram_group_name=os.getenv("TELEGRAM_GROUP_NAME", ""),

        # ── Bybit ─────────────────────────────────────────────────────────────
        bybit_api_key=os.getenv("BYBIT_API_KEY", ""),
        bybit_api_secret=os.getenv("BYBIT_API_SECRET", ""),
        bybit_testnet=_bool("BYBIT_TESTNET", False),

        # ── Risk & sizing ─────────────────────────────────────────────────────
        default_leverage=_int("DEFAULT_LEVERAGE", 10),
        max_leverage=_int("MAX_LEVERAGE", 10),
        risk_per_trade=_float("RISK_PER_TRADE", 0.10),
        short_size_multiplier=_float("SHORT_SIZE_MULTIPLIER", 1.5),
        dry_run=_bool("DRY_RUN", False),

        # ── Blowthrough cancel ────────────────────────────────────────────────
        # When ON: if price moves to entry_mid while entries pending → cancel all
        # This removes 41% WR "full zone" trades and keeps 88% WR "edge/mid" trades
        blowthrough_cancel=_bool("BLOWTHROUGH_CANCEL", True),

        # ── Signal quality filter ─────────────────────────────────────────────
        # Signals that don't pass are ignored (no orders placed)
        filter_enabled=_bool("FILTER_ENABLED", True),
        filter_min_entry_range_pct=_float("FILTER_MIN_ENTRY_RANGE_PCT", 3.0),
        filter_min_num_targets=_int("FILTER_MIN_NUM_TARGETS", 6),

        # ── RSI filter (Binance API) ──────────────────────────────────────────
        # Core confirmed edge: RSI 1h < 40 gives WR 79.4% Sharpe +0.622
        # Fetched from Binance public API at signal time (no key needed)
        filter_rsi_signal_max=_int("FILTER_RSI_SIGNAL_MAX", 40),
        filter_rsi_tf=os.getenv("FILTER_RSI_TF", "1h"),

        # ── SL distance filters ───────────────────────────────────────────────
        # filter_min_sl_pct: skip signals where SL is too tight (noise wicks)
        # filter_max_sl_pct: skip signals where SL is too wide (bad R ratio)
        filter_min_sl_pct=_float("FILTER_MIN_SL_PCT", 3.0),
        filter_max_sl_pct=_float("FILTER_MAX_SL_PCT", 0.0),   # 0 = disabled

        # ── TP1 R:R maximum ───────────────────────────────────────────────────
        filter_max_tp1_rr=_float("FILTER_MAX_TP1_RR", 1.1),

        # ── BTC weekly filter ─────────────────────────────────────────────────
        filter_btc_weekly_enabled=_bool("FILTER_BTC_WEEKLY_ENABLED", True),

        # ── Paths ─────────────────────────────────────────────────────────────
        log_file=_p("BOT_LOG_FILE", "${PATH}/logs/bot.log"),
        db_path=_p("DB_PATH", "${PATH}/data/bot.db"),
        session_dir=_p("SESSION_DIR", "${PATH}/sessions"),

        # ── Alert bot ─────────────────────────────────────────────────────────
        alert_bot_token=os.getenv("ALERT_BOT_TOKEN", ""),
        alert_chat_id=os.getenv("ALERT_CHAT_ID", ""),
        alert_telegram_seconds=_int("ALERT_TELEGRAM_SECONDS", 120),
        alert_bybit_seconds=_int("ALERT_BYBIT_SECONDS", 180),
        alert_sl_seconds=_int("ALERT_SL_SECONDS", 90),
        alert_tp_seconds=_int("ALERT_TP_SECONDS", 120),
        alert_cooldown_seconds=_int("ALERT_COOLDOWN_SECONDS", 600),
    )


class Config:
    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)

    def ensure_dirs(self):
        for d in (self.log_file.parent, self.db_path.parent, self.session_dir):
            d.mkdir(parents=True, exist_ok=True)


# Singleton – import this everywhere
config: Config = _load()
