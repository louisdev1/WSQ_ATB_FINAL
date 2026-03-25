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
        risk_per_trade=_float("RISK_PER_TRADE", 0.05),
        short_size_multiplier=_float("SHORT_SIZE_MULTIPLIER", 1.0),
        dry_run=_bool("DRY_RUN", False),

        # ── Quality-based sizing ──────────────────────────────────────────────
        # signal_quality.py scores each signal 0-6 and multiplies risk_per_trade
        # HIGH (score ≥5) → 1.5×  |  MED (3-4) → 1.0×  |  LOW (≤2) → 0.7×
        # Confirmed on val set Nov 2024–May 2025: +85% vs flat sizing
        quality_sizing_enabled=_bool("QUALITY_SIZING_ENABLED", True),
        quality_mult_high=_float("QUALITY_MULT_HIGH", 1.5),
        quality_mult_med=_float("QUALITY_MULT_MED", 1.0),
        quality_mult_low=_float("QUALITY_MULT_LOW", 0.7),
        quality_high_threshold=_int("QUALITY_HIGH_THRESHOLD", 5),
        quality_med_threshold=_int("QUALITY_MED_THRESHOLD", 3),

        # ── Blowthrough cancel ────────────────────────────────────────────────
        # If price moves past 35% into the entry zone while entries pending
        # → cancel all entries. Hardcoded 35% threshold in trade_manager.py.
        blowthrough_cancel=_bool("BLOWTHROUGH_CANCEL", True),

        # ── Signal quality filters ────────────────────────────────────────────
        filter_enabled=_bool("FILTER_ENABLED", True),
        filter_min_entry_range_pct=_float("FILTER_MIN_ENTRY_RANGE_PCT", 2.0),
        filter_min_sl_pct=_float("FILTER_MIN_SL_PCT", 3.0),
        filter_max_sl_pct=_float("FILTER_MAX_SL_PCT", 0.0),
        filter_max_tp1_rr=_float("FILTER_MAX_TP1_RR", 1.1),
        filter_min_num_targets=_int("FILTER_MIN_NUM_TARGETS", 6),
        filter_block_8t_long=_bool("FILTER_BLOCK_8T_LONG", True),

        # ── RSI filter ────────────────────────────────────────────────────────
        # Fetched from Binance public API at signal time (no key needed).
        # RSI<40 Sharpe=+0.550. DO NOT raise above 40 — tested, degrades edge.
        filter_rsi_signal_max=_float("FILTER_RSI_SIGNAL_MAX", 40.0),
        filter_rsi_tf=os.getenv("FILTER_RSI_TF", "1h"),

        # ── BTC weekly filter ─────────────────────────────────────────────────
        # Blocks LONG signals when BTC weekly candle is bearish.
        filter_btc_weekly_enabled=_bool("FILTER_BTC_WEEKLY_ENABLED", True),

        # ── Trailing stop (not yet implemented in trade_manager.py) ───────────
        # Uncomment TRAIL_ACTIVATION_TP and TRAIL_PCT in .env when ready.
        trail_activation_tp=_int("TRAIL_ACTIVATION_TP", 0),   # 0 = disabled
        trail_pct=_float("TRAIL_PCT", 0.0),

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
