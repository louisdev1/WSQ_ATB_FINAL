"""
config.py – Load .env and expose a single Config object.

PATH in .env is the project root. All other paths are derived from it.
Works on Windows and Raspberry Pi without any code changes.
"""

import os
from pathlib import Path
from dotenv import load_dotenv


def _resolve_env_path(raw: str, project_root: Path) -> Path:
    """Replace ${PATH} placeholder and return an absolute Path."""
    resolved = raw.replace("${PATH}", str(project_root))
    p = Path(resolved)
    if not p.is_absolute():
        p = project_root / p
    return p


def _load() -> "Config":
    # Find .env relative to this file's parent's parent (project root)
    here = Path(__file__).resolve().parent.parent
    env_file = here / ".env"
    load_dotenv(dotenv_path=env_file, override=True)

    raw_path = os.getenv("PATH", str(here))
    # On Unix $PATH is the system PATH; detect if it looks like a system path
    # and fall back to the directory containing this file.
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
        # Signal filter (data-driven, 258K+ combos tested)
        filter_enabled=_bool("FILTER_ENABLED", True),
        filter_min_sl_pct=_float("FILTER_MIN_SL_PCT", 3.0),
        filter_max_tp1_rr=_float("FILTER_MAX_TP1_RR", 1.0),
        filter_min_num_targets=_int("FILTER_MIN_NUM_TARGETS", 5),
        filter_skip_after_loss=_bool("FILTER_SKIP_AFTER_LOSS", True),
        filter_half_rapid_hours=_float("FILTER_HALF_RAPID_HOURS", 0),
        filter_auto_scale=_bool("FILTER_AUTO_SCALE", True),
        # Alert bot
        alert_bot_token=os.getenv("ALERT_BOT_TOKEN", ""),
        alert_chat_id=os.getenv("ALERT_CHAT_ID", ""),
        # Paths
        log_file=_p("BOT_LOG_FILE", "${PATH}/logs/bot.log"),
        db_path=_p("DB_PATH", "${PATH}/data/bot.db"),
        session_dir=_p("SESSION_DIR", "${PATH}/sessions"),
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
