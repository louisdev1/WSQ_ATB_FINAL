"""
admin_listener.py – Private Telegram bot command listener.

Listens to the ALERT_CHAT_ID chat (where you receive bot notifications) for
admin commands sent by you.  Only messages from ALERT_CHAT_ID are processed —
everything else is silently ignored.

Supported commands (just type these in the chat):

  Signal format (identical to your signal group)
    Coin: #LTCUSDT
    Direction: Long
    Entry: 57.6 – 57.6
    Stop Loss: 54.6
    Targets: 58.8 - 60 - 61.2 - 62.6 - 66
    Leverage: 10x
    → registers the trade and starts managing it immediately

  close-SYMBOL        e.g.  close-LTCUSDT
    → market-close position and remove from tracking

  see-all-trades
    → list every active trade the bot is managing

  status-update
    → PnL and win/loss stats for today / this week / this month / this year
       plus current open positions summary
"""

import asyncio
import logging
import re
from datetime import datetime, timezone, timedelta
from typing import Callable, Awaitable, Optional

import aiohttp

from app.config import config
from app.parsing.parser import parse_message
from app.parsing.models import MessageType, NewSignal

log = logging.getLogger(__name__)


# ── Telegram Bot API helpers ──────────────────────────────────────────────────

async def _send(text: str) -> None:
    """Send a message to ALERT_CHAT_ID via the alert bot."""
    if not config.alert_bot_token or not config.alert_chat_id:
        log.warning("Admin reply skipped — ALERT_BOT_TOKEN/ALERT_CHAT_ID not configured")
        return
    url = f"https://api.telegram.org/bot{config.alert_bot_token}/sendMessage"
    payload = {"chat_id": config.alert_chat_id, "text": text, "parse_mode": "Markdown"}
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=payload,
                                    timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status != 200:
                    body = await resp.text()
                    log.error("Admin send failed (HTTP %s): %s", resp.status, body)
    except Exception as exc:
        log.error("Admin send exception: %s", exc)


async def _get_updates(offset: int) -> list:
    """Poll for new updates from the Telegram Bot API."""
    url = f"https://api.telegram.org/bot{config.alert_bot_token}/getUpdates"
    params = {"offset": offset, "timeout": 20, "allowed_updates": ["message"]}
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, params=params,
                                   timeout=aiohttp.ClientTimeout(total=30)) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return data.get("result", [])
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        log.error("Admin getUpdates error: %s", exc)
    return []


# ── Stats builder ─────────────────────────────────────────────────────────────

def _period_start(period: str) -> str:
    """Return ISO string for the start of today/week/month/year."""
    now = datetime.now(timezone.utc)
    if period == "day":
        dt = now.replace(hour=0, minute=0, second=0, microsecond=0)
    elif period == "week":
        dt = (now - timedelta(days=now.weekday())).replace(
            hour=0, minute=0, second=0, microsecond=0)
    elif period == "month":
        dt = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    else:  # year
        dt = now.replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0)
    return dt.isoformat()


def _stats_block(trades: list, label: str) -> str:
    """Build a stats text block for a list of closed trades."""
    if not trades:
        return f"*{label}:* no closed trades"

    wins   = [t for t in trades if t["state"] == "closed"]
    losses = [t for t in trades if t["state"] == "sl_hit"]
    total  = len(trades)
    win_rate = len(wins) / total * 100 if total else 0

    # PnL — only trades that have realised_pnl recorded
    pnl_trades = [t for t in trades if t.get("realised_pnl") is not None]
    total_pnl  = sum(t["realised_pnl"] for t in pnl_trades)
    best  = max(pnl_trades, key=lambda t: t["realised_pnl"]) if pnl_trades else None
    worst = min(pnl_trades, key=lambda t: t["realised_pnl"]) if pnl_trades else None

    # Average duration (created_at → closed_at)
    durations = []
    for t in trades:
        try:
            ca = datetime.fromisoformat(t["created_at"].replace("Z", "+00:00"))
            cl = datetime.fromisoformat(t["closed_at"].replace("Z", "+00:00"))
            durations.append((cl - ca).total_seconds())
        except Exception:
            pass
    avg_dur = sum(durations) / len(durations) if durations else None

    lines = [f"*{label}*"]
    lines.append(f"  Trades: {total}  |  Wins: {len(wins)}  |  Losses: {len(losses)}  |  Win rate: {win_rate:.0f}%")

    if pnl_trades:
        pnl_emoji = "✅" if total_pnl >= 0 else "❌"
        lines.append(f"  Total PnL: {pnl_emoji} `{total_pnl:+.2f} USDT`")
        if best:
            lines.append(f"  Best trade: `{best['symbol']}` `{best['realised_pnl']:+.2f} USDT`")
        if worst:
            lines.append(f"  Worst trade: `{worst['symbol']}` `{worst['realised_pnl']:+.2f} USDT`")
    else:
        lines.append("  PnL: _no data yet (recorded going forward)_")

    if avg_dur is not None:
        h, rem = divmod(int(avg_dur), 3600)
        m = rem // 60
        lines.append(f"  Avg duration: {h}h {m}m")

    return "\n".join(lines)


# ── Command handlers ──────────────────────────────────────────────────────────

async def _handle_close(symbol: str, trade_manager, bybit) -> None:
    from app.parsing.models import CloseSymbol
    msg = CloseSymbol(
        raw_text=f"close {symbol}",
        message_type=MessageType.CLOSE_SYMBOL,
        symbol=symbol,
    )
    trade = await trade_manager._db.get_trade_by_symbol(symbol)
    if not trade:
        await _send(f"⚠️ No active trade found for `{symbol}`.")
        return
    await trade_manager._handle_close_symbol(msg)


async def _handle_see_all(trade_manager, bybit) -> None:
    trades = await trade_manager._db.get_active_trades()
    if not trades:
        await _send("📋 *Active trades:* none")
        return

    lines = [f"📋 *Active trades ({len(trades)}):*\n"]
    for t in trades:
        sym       = t["symbol"]
        direction = t["direction"].upper()
        state     = t["state"]
        tp_hit    = t.get("highest_tp_hit", 0) or 0
        targets   = t.get("targets", [])
        sl        = t.get("stop_loss", 0) or 0

        pos       = bybit.fetch_position(sym)
        live_size = float(pos.get("size", 0))      if pos else 0.0
        entry     = float(pos.get("avgPrice", 0))  if pos else t.get("avg_entry_price", 0) or 0
        upnl      = float(pos.get("unrealisedPnl", 0)) if pos else 0.0
        live_sl   = float(pos.get("stopLoss") or 0) if pos else 0.0

        pnl_emoji = "✅" if upnl >= 0 else "❌"
        lines.append(
            f"*{sym}* {direction} `[{state}]`\n"
            f"  Size: `{live_size}` @ `{entry:.4f}`\n"
            f"  SL: `{live_sl or sl:.4f}` | PnL: {pnl_emoji} `{upnl:+.2f} USDT`\n"
            f"  TPs hit: {tp_hit}/{len(targets)}"
        )

    await _send("\n\n".join(lines))


async def _handle_status(trade_manager, bybit) -> None:
    db = trade_manager._db

    # Closed trades per period
    now_iso = datetime.now(timezone.utc).isoformat()
    day_trades   = await db.get_closed_trades_since(_period_start("day"))
    week_trades  = await db.get_closed_trades_since(_period_start("week"))
    month_trades = await db.get_closed_trades_since(_period_start("month"))
    year_trades  = await db.get_closed_trades_since(_period_start("year"))

    stats_lines = [
        "📊 *Status update*\n",
        _stats_block(day_trades,   "Today"),
        "",
        _stats_block(week_trades,  "This week"),
        "",
        _stats_block(month_trades, "This month"),
        "",
        _stats_block(year_trades,  "This year"),
    ]

    # Current open positions
    active = await db.get_active_trades()
    balance = bybit.fetch_wallet_balance()
    stats_lines.append(f"\n💰 *Balance:* `{balance:.2f} USDT`")

    if active:
        stats_lines.append(f"\n📈 *Open positions ({len(active)}):*")
        for t in active:
            sym   = t["symbol"]
            pos   = bybit.fetch_position(sym)
            upnl  = float(pos.get("unrealisedPnl", 0)) if pos else 0.0
            size  = float(pos.get("size", 0))          if pos else 0.0
            entry = float(pos.get("avgPrice", 0))      if pos else 0.0
            pnl_emoji = "✅" if upnl >= 0 else "❌"
            stats_lines.append(
                f"  • *{sym}* {t['direction'].upper()} "
                f"`{size}` @ `{entry:.4f}` "
                f"PnL: {pnl_emoji} `{upnl:+.2f}`"
            )
    else:
        stats_lines.append("\n📈 *Open positions:* none")

    await _send("\n".join(stats_lines))


async def _handle_cancel_entries(symbol: str, trade_manager) -> None:
    from app.parsing.models import CancelRemainingEntries
    trade = await trade_manager._db.get_trade_by_symbol(symbol)
    if not trade:
        await _send(f"⚠️ No active trade found for `{symbol}`.")
        return
    msg = CancelRemainingEntries(
        raw_text=f"cancel remaining entries {symbol}",
        message_type=MessageType.CANCEL_REMAINING_ENTRIES,
        symbol=symbol,
    )
    await trade_manager._handle_cancel_entries(msg)
    await _send(f"✅ Remaining entry orders cancelled for `{symbol}`.")


async def _handle_close_all(trade_manager, bybit) -> None:
    from app.parsing.models import CloseAll
    trades = await trade_manager._db.get_active_trades()
    if not trades:
        await _send("ℹ️ No active trades to close.")
        return
    symbols = ", ".join(f"`{t['symbol']}`" for t in trades)
    await _send(f"🔒 Closing all positions: {symbols}…")
    msg = CloseAll(raw_text="close all", message_type=MessageType.CLOSE_ALL)
    await trade_manager._handle_close_all(msg)


async def _handle_balance(bybit) -> None:
    balance = bybit.fetch_wallet_balance()
    await _send(f"💰 *Balance:* `{balance:.2f} USDT`")


async def _handle_help() -> None:
    await _send(
        "📖 *Available commands:*\n\n"
        "*Signals*\n"
        "• Send a signal in the standard group format → registers the trade\n\n"
        "*Trade control*\n"
        "• `close-SYMBOL` — market-close a position and stop tracking it\n"
        "• `close-all` — emergency market-close every open position\n"
        "• `cancel-entries-SYMBOL` — cancel unfilled entry orders (keep position)\n\n"
        "*Information*\n"
        "• `see-all-trades` — list every active trade with live PnL\n"
        "• `status-update` — win/loss stats + PnL for today/week/month/year\n"
        "• `balance` — current USDT wallet balance\n"
        "• `help` — show this message"
    )


async def _handle_new_signal(text: str, msg_id: int, trade_manager) -> None:
    """Parse and execute a signal sent from the admin chat."""
    parsed = parse_message(text, msg_id)
    if parsed.message_type != MessageType.NEW_SIGNAL:
        await _send(
            f"⚠️ Could not parse as a new signal (detected: `{parsed.message_type.value}`).\n"
            "Make sure to include Coin, Direction, Entry, Stop Loss, and Targets."
        )
        return

    sig: NewSignal = parsed
    if not sig.symbol or not sig.direction or not sig.stop_loss or not sig.targets:
        await _send(
            "⚠️ Signal parsed but missing required fields.\n"
            f"Got: symbol=`{sig.symbol}` direction=`{sig.direction}` "
            f"sl=`{sig.stop_loss}` targets={len(sig.targets)}"
        )
        return

    # Check if already tracking this symbol
    existing = await trade_manager._db.get_trade_by_symbol(sig.symbol)
    if existing:
        await _send(
            f"⚠️ Already tracking an active trade for `{sig.symbol}` "
            f"(state: `{existing['state']}`). Close it first."
        )
        return

    await _send(
        f"📥 Signal received for `{sig.symbol}` — processing…\n"
        f"Direction: {sig.direction.value.upper()} | "
        f"Entry: `{sig.entry_low:.4g}`–`{sig.entry_high:.4g}` | "
        f"SL: `{sig.stop_loss:.4g}` | Targets: {len(sig.targets)}"
    )
    await trade_manager.handle(parsed)


# ── Main polling loop ─────────────────────────────────────────────────────────

async def admin_loop(trade_manager, bybit) -> None:
    """
    Long-poll the Telegram Bot API for messages in ALERT_CHAT_ID.
    Only processes messages from the configured chat ID.
    Runs forever alongside the main bot tasks.
    """
    if not config.alert_bot_token or not config.alert_chat_id:
        log.warning("AdminListener: ALERT_BOT_TOKEN or ALERT_CHAT_ID not set — admin commands disabled")
        return

    log.info("AdminListener started — listening for commands in chat %s", config.alert_chat_id)
    offset = 0
    allowed_chat = str(config.alert_chat_id).strip()

    while True:
        try:
            updates = await _get_updates(offset)
            for update in updates:
                offset = update["update_id"] + 1
                msg = update.get("message", {})
                if not msg:
                    continue

                # Security: only accept from the configured chat
                chat_id = str(msg.get("chat", {}).get("id", ""))
                if chat_id != allowed_chat:
                    continue

                text = (msg.get("text") or "").strip()
                if not text:
                    continue

                msg_id = msg.get("message_id", 0)
                log.info("Admin command received: %s", text[:80])

                # ── Route the command ─────────────────────────────────────
                text_lower = text.lower()

                if text_lower.startswith("close-") and text_lower != "close-all":
                    symbol = text[6:].strip().upper()
                    await _handle_close(symbol, trade_manager, bybit)

                elif text_lower == "close-all":
                    await _handle_close_all(trade_manager, bybit)

                elif text_lower.startswith("cancel-entries-"):
                    symbol = text[15:].strip().upper()
                    await _handle_cancel_entries(symbol, trade_manager)

                elif text_lower == "see-all-trades":
                    await _handle_see_all(trade_manager, bybit)

                elif text_lower == "status-update":
                    await _handle_status(trade_manager, bybit)

                elif text_lower == "balance":
                    await _handle_balance(bybit)

                elif text_lower == "help":
                    await _handle_help()

                elif parse_message(text, msg_id).message_type == MessageType.NEW_SIGNAL:
                    await _handle_new_signal(text, msg_id, trade_manager)

                else:
                    await _send(
                        "❓ Unknown command. Send `help` to see all available commands."
                    )

        except asyncio.CancelledError:
            raise
        except Exception as exc:
            log.error("AdminListener error: %s", exc)
            await asyncio.sleep(5)
