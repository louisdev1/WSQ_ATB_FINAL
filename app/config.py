"""
config.py – Load .env and expose a single Config object.

The project root is always derived from this file's location on disk —
no PATH or PROJECT_PATH variable is needed or read for path resolution.
This avoids collisions with the system PATH environment variable on all
platforms (Windows, Linux, Raspberry Pi).

Path variables in .env (BOT_LOG_FILE, DB_PATH, SESSION_DIR) still support
the ${PROJECT_PATH} placeholder for explicit overrides, but the defaults
work correctly with no configuration at all.
"""

import os
from pathlib import Path
from dotenv import load_dotenv

# Project root is always the parent of the directory containing this file.
# config.py lives at  <project_root>/app/config.py
# so project_root    = Path(__file__).resolve().parent.parent
_PROJECT_ROOT: Path = Path(__file__).resolve().parent.parent


def _resolve_env_path(raw: str) -> Path:
    """Replace ${PROJECT_PATH} placeholder and return an absolute Path."""
    resolved = raw.replace("${PROJECT_PATH}", str(_PROJECT_ROOT))
    resolved = resolved.replace("${PATH}", str(_PROJECT_ROOT))   # legacy compat
    p = Path(resolved)
    if not p.is_absolute():
        p = _PROJECT_ROOT / p
    return p


def _load() -> "Config":
    env_file = _PROJECT_ROOT / ".env"
    load_dotenv(dotenv_path=env_file, override=True)

    def _p(key: str, default: str) -> Path:
        raw = os.getenv(key, default)
        return _resolve_env_path(raw)

    def _bool(key: str, default: bool) -> bool:
        return os.getenv(key, str(default)).lower() in ("1", "true", "yes")

    def _int(key: str, default: int) -> int:
        return int(os.getenv(key, str(default)))

    def _float(key: str, default: float) -> float:
        return float(os.getenv(key, str(default)))

    return Config(
        project_root=_PROJECT_ROOT,
        # Telegram
        telegram_api_id=_int("TELEGRAM_API_ID", 0),
        telegram_api_hash=os.getenv("TELEGRAM_API_HASH", ""),
        telegram_phone=os.getenv("TELEGRAM_PHONE", ""),
        telegram_session_name=os.getenv("TELEGRAM_SESSION_NAME", "telegram_session"),
        telegram_group_name=os.getenv("TELEGRAM_GROUP_NAME", ""),
        # Bybit
        bybit_api_key=os.getenv("BYBIT_API_KEY", ""),
        bybit_api_secret=os.getenv("BYBIT_API_SECRET", ""),
        bybit_testnet=_bool("BYBIT_TESTNET", False),
        # Risk
        default_leverage=_int("DEFAULT_LEVERAGE", 10),
        max_leverage=_int("MAX_LEVERAGE", 10),
        risk_per_trade=_float("RISK_PER_TRADE", 0.15),
        dry_run=_bool("DRY_RUN", False),
        # Signal filter
        filter_enabled=_bool("FILTER_ENABLED", True),
        filter_min_sl_pct=_float("FILTER_MIN_SL_PCT", 3.0),
        filter_max_tp1_rr=_float("FILTER_MAX_TP1_RR", 1.0),
        filter_min_num_targets=_int("FILTER_MIN_NUM_TARGETS", 5),
        filter_min_entry_range_pct=_float("FILTER_MIN_ENTRY_RANGE_PCT", 3.0),
        filter_qs_threshold=_float("FILTER_QS_THRESHOLD", 5.0),
        filter_qs_multiplier=_float("FILTER_QS_MULTIPLIER", 1.0),
        filter_combo_multiplier=_float("FILTER_COMBO_MULTIPLIER", 2.5),
        filter_vol_multiplier=_float("FILTER_VOL_MULTIPLIER", 1.25),
        filter_rsi_multiplier=_float("FILTER_RSI_MULTIPLIER", 1.75),
        filter_rsi_period=_int("FILTER_RSI_PERIOD", 14),
        filter_vol_period=_int("FILTER_VOL_PERIOD", 20),
        filter_indicator_interval=os.getenv("FILTER_INDICATOR_INTERVAL", "60"),
        filter_skip_after_loss=_bool("FILTER_SKIP_AFTER_LOSS", False),
        filter_half_rapid_hours=_float("FILTER_HALF_RAPID_HOURS", 0),
        filter_auto_scale=_bool("FILTER_AUTO_SCALE", True),
        # Alert bot
        alert_bot_token=os.getenv("ALERT_BOT_TOKEN", ""),
        alert_chat_id=os.getenv("ALERT_CHAT_ID", ""),
        # Paths — defaults are relative to project root, no config needed
        log_file=_p("BOT_LOG_FILE", "${PROJECT_PATH}/logs/bot.log"),
        db_path=_p("DB_PATH", "${PROJECT_PATH}/data/bot.db"),
        session_dir=_p("SESSION_DIR", "${PROJECT_PATH}/sessions"),
        # Alert thresholds
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
        """Create all necessary directories if they don't exist."""
        for d in (
            self.log_file.parent,
            self.db_path.parent,
            self.session_dir,
        ):
            d.mkdir(parents=True, exist_ok=True)


# Singleton – import this everywhere
config: Config = _load()
