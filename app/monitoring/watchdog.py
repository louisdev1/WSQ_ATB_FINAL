"""
watchdog.py – Monitors bot health and fires Telegram alerts for real problems.

Tracks:
- Telegram connectivity
- Bybit API connectivity
- Unprotected positions (no SL)
- Log tail scanning for tracebacks

Sends recovery ("all clear") messages when issues resolve.
"""

import asyncio
import logging
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from app.config import config
from app.monitoring.alerter import send_alert, send_recovery, format_duration

log = logging.getLogger(__name__)


class IssueTracker:
    """Tracks when an issue first appeared."""
    def __init__(self):
        self._issues: dict[str, datetime] = {}

    def mark(self, key: str):
        if key not in self._issues:
            self._issues[key] = datetime.now(timezone.utc)
            log.debug("Issue first seen: %s", key)

    def clear(self, key: str) -> float:
        """Clear an issue and return how long it lasted (0.0 if it wasn't tracked)."""
        if key in self._issues:
            duration = (datetime.now(timezone.utc) - self._issues[key]).total_seconds()
            del self._issues[key]
            log.debug("Issue resolved: %s (lasted %.0fs)", key, duration)
            return duration
        return 0.0

    def is_active(self, key: str) -> bool:
        return key in self._issues

    def age_seconds(self, key: str) -> float:
        if key not in self._issues:
            return 0.0
        return (datetime.now(timezone.utc) - self._issues[key]).total_seconds()


_tracker = IssueTracker()

# ── connectivity flags (set by intake/exchange layers) ────────────────────────

_telegram_ok: bool = True
_bybit_ok: bool = True
# Track previous state so we can detect transitions for recovery messages
_telegram_was_down: bool = False
_bybit_was_down: bool = False


def report_telegram_ok():
    global _telegram_ok
    _telegram_ok = True
    _tracker.clear("telegram_down")


def report_telegram_fail():
    global _telegram_ok
    _telegram_ok = False
    _tracker.mark("telegram_down")


def report_bybit_ok():
    global _bybit_ok
    _bybit_ok = True
    _tracker.clear("bybit_down")


def report_bybit_fail():
    global _bybit_ok
    _bybit_ok = False
    _tracker.mark("bybit_down")


# ── log tail watcher ──────────────────────────────────────────────────────────

_TRACEBACK_RE = re.compile(r"Traceback \(most recent call last\)|CRITICAL|FATAL", re.IGNORECASE)
_last_log_pos: int = 0


async def _check_log_for_tracebacks(db) -> None:
    global _last_log_pos
    log_path: Path = config.log_file
    if not log_path.exists():
        return
    try:
        with open(log_path, "r", encoding="utf-8", errors="replace") as f:
            f.seek(_last_log_pos)
            new_content = f.read()
            _last_log_pos = f.tell()
        if _TRACEBACK_RE.search(new_content):
            snippet = new_content[-800:].strip()
            await send_alert("fatal_error", f"Log contains critical error:\n```\n{snippet}\n```", db)
    except Exception as exc:
        log.error("Log watcher error: %s", exc)


# ── unprotected position check ────────────────────────────────────────────────

async def _check_unprotected_positions(db, bybit) -> None:
    if not bybit:
        return
    try:
        trades = await db.get_active_trades()
        for trade in trades:
            sym = trade["symbol"]
            pos = bybit.fetch_position(sym)
            if not pos:
                continue
            sl = pos.get("stopLoss")
            if not sl or float(sl) == 0:
                _tracker.mark(f"no_sl_{sym}")
                age = _tracker.age_seconds(f"no_sl_{sym}")
                if age > config.alert_sl_seconds:
                    size = float(pos.get("size", 0))
                    avg_price = float(pos.get("avgPrice", 0))
                    mark_price = float(pos.get("markPrice", 0))
                    upnl = float(pos.get("unrealisedPnl", 0))
                    side = pos.get("side", "?")
                    await send_alert(
                        f"no_sl_{sym}",
                        f"⚠️ Position *{sym}* has NO stop-loss for {format_duration(age)}!\n"
                        f"Side: {side} | Size: {size}\n"
                        f"Entry: `{avg_price:.6g}` | Mark: `{mark_price:.6g}`\n"
                        f"PnL: `{upnl:+.2f} USDT`\n\n"
                        f"⚡ _Set a stop-loss immediately!_",
                        db,
                    )
            else:
                # SL is now present — send recovery if it was missing before
                duration = _tracker.clear(f"no_sl_{sym}")
                if duration > config.alert_sl_seconds:
                    await send_recovery(
                        f"no_sl_{sym}",
                        f"Stop-loss restored for *{sym}* (was missing for {format_duration(duration)}).",
                        db,
                    )
    except Exception as exc:
        log.error("Unprotected position check error: %s", exc)


# ── main watchdog loop ────────────────────────────────────────────────────────

async def watchdog_loop(db, bybit, trade_manager):
    """Runs forever in the background. Check interval: 30s."""
    global _telegram_was_down, _bybit_was_down

    log.info("Watchdog started")
    while True:
        try:
            # ── Telegram connectivity ──────────────────────────────────────
            if not _telegram_ok:
                _telegram_was_down = True
                age = _tracker.age_seconds("telegram_down")
                if age > config.alert_telegram_seconds:
                    await send_alert(
                        "telegram_down",
                        f"Telegram listener has been down for {format_duration(age)} and has not recovered.",
                        db,
                    )
            elif _telegram_was_down:
                _telegram_was_down = False
                await send_recovery(
                    "telegram_down",
                    "Telegram listener has reconnected and is receiving signals again.",
                    db,
                )

            # ── Bybit connectivity ─────────────────────────────────────────
            if not _bybit_ok:
                _bybit_was_down = True
                age = _tracker.age_seconds("bybit_down")
                if age > config.alert_bybit_seconds:
                    await send_alert(
                        "bybit_down",
                        f"Bybit API has been unreachable for {format_duration(age)}.",
                        db,
                    )
            elif _bybit_was_down:
                _bybit_was_down = False
                await send_recovery(
                    "bybit_down",
                    "Bybit API connection restored.",
                    db,
                )

            # Log tail scan
            await _check_log_for_tracebacks(db)

            # Unprotected positions
            await _check_unprotected_positions(db, bybit)

            # Sync fills
            if trade_manager:
                await trade_manager.sync_fills()

        except Exception as exc:
            log.error("Watchdog loop error: %s", exc)

        await asyncio.sleep(30)
