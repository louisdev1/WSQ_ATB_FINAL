"""
alerter.py – Telegram alert sender with cooldown.

Provides two functions:
  send_alert()        – for problem alerts (subject to cooldown)
  send_notification()  – for trade lifecycle events (no cooldown)
"""

import asyncio
import logging
from datetime import datetime, timezone
from typing import Optional

import aiohttp

from app.temp_files.config import config

log = logging.getLogger(__name__)

# In-memory cooldown tracker: alert_type → last sent UTC timestamp
_last_sent: dict[str, datetime] = {}


def format_duration(seconds: float) -> str:
    """Convert raw seconds to a human-friendly string like '3m 7s' or '1h 12m'."""
    seconds = int(seconds)
    if seconds < 60:
        return f"{seconds}s"
    minutes, secs = divmod(seconds, 60)
    if minutes < 60:
        return f"{minutes}m {secs}s" if secs else f"{minutes}m"
    hours, mins = divmod(minutes, 60)
    if hours < 24:
        return f"{hours}h {mins}m" if mins else f"{hours}h"
    days, hrs = divmod(hours, 24)
    return f"{days}d {hrs}h" if hrs else f"{days}d"


async def _send_telegram(text: str, db=None, alert_type: str = "") -> bool:
    """Low-level Telegram send. Used by both send_alert and send_notification."""
    if not config.alert_bot_token or not config.alert_chat_id:
        log.warning("Alert bot not configured – cannot send: %s", text[:80])
        return False

    url = f"https://api.telegram.org/bot{config.alert_bot_token}/sendMessage"
    payload = {
        "chat_id": config.alert_chat_id,
        "text": text,
        "parse_mode": "Markdown",
    }

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status == 200:
                    log.info("Telegram message sent: %s", alert_type or "notification")
                    if db and alert_type:
                        await db.save_alert(alert_type, text)
                    return True
                else:
                    body = await resp.text()
                    log.error("Telegram send failed (HTTP %s): %s", resp.status, body)
    except Exception as exc:
        log.error("Telegram send exception: %s", exc)

    return False


async def send_alert(alert_type: str, message: str, db=None) -> bool:
    """
    Send a Telegram alert if cooldown has elapsed.
    db is optional – used to persist alert history.
    """
    if not config.alert_bot_token or not config.alert_chat_id:
        log.warning("Alert bot not configured – cannot send: %s", message)
        return False

    now = datetime.now(timezone.utc)
    last = _last_sent.get(alert_type)
    if last:
        elapsed = (now - last).total_seconds()
        if elapsed < config.alert_cooldown_seconds:
            log.debug("Alert %s suppressed (cooldown %.0fs remaining)",
                      alert_type, config.alert_cooldown_seconds - elapsed)
            return False

    text = f"🚨 *TradingBot Alert*\n`{alert_type}`\n\n{message}"
    ok = await _send_telegram(text, db, alert_type)
    if ok:
        _last_sent[alert_type] = now
    return ok


async def send_notification(message: str, db=None) -> bool:
    """
    Send a trade lifecycle notification (no cooldown).
    Used for: trade opened, TP hit, SL hit, trade closed, recovery.
    """
    text = f"📋 *TradingBot*\n\n{message}"
    return await _send_telegram(text, db)


async def send_recovery(alert_type: str, message: str, db=None) -> bool:
    """
    Send an all-clear recovery message and reset the cooldown for that alert type.
    Only sends if the alert_type was previously fired.
    """
    if alert_type not in _last_sent:
        return False

    text = f"✅ *Resolved*\n`{alert_type}`\n\n{message}"
    ok = await _send_telegram(text, db, alert_type)
    if ok:
        # Clear cooldown so future alerts for this type aren't suppressed
        _last_sent.pop(alert_type, None)
    return ok
