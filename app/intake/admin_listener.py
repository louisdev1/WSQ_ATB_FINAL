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

  close-all
    → emergency market-close every open position

  cancel-entries-SYMBOL
    → cancel unfilled entry orders (keep position)

  be-SYMBOL           e.g.  be-ETHUSDT
    → move SL to break-even for a specific symbol

  see-all-trades
    → list every active trade the bot is managing

  detail-SYMBOL       e.g.  detail-BTCUSDT
    → deep dive on a single trade: entry, PnL, quality score, filters, sizing

  status-update
    → PnL and win/loss stats for today / this week / this month / this year
       plus current open positions summary

  pnl
    → quick PnL snapshot: realised + unrealised + balance

  orders-SYMBOL       e.g.  orders-ETHUSDT
    → show all open Bybit orders for a symbol

  rejected
    → last 10 signals the bot filtered out and why

  config
    → show current live bot configuration

  balance
    → current USDT wallet balance

  help
    → show all available commands
"""

import asyncio
import logging
import re
from datetime import datetime, timezone, timedelta
from typing import Callable, Awaitable, Optional

import aiohttp

from app.config import config
from app.parsing.parser import parse_message
from app.parsing.models import MessageType, NewSignal, MoveSLBreakEven

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


# ── Helpers ───────────────────────────────────────────────────────────────────

def _format_duration(seconds: float) -> str:
    """Convert raw seconds to a human-friendly string."""
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
            # Use closed_at if available, otherwise fall back to updated_at
            close_field = t.get("closed_at") or t.get("updated_at")
            if not close_field:
                continue
            cl = datetime.fromisoformat(close_field.replace("Z", "+00:00"))
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
        lines.append(f"  Avg duration: {_format_duration(avg_dur)}")

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


# ── NEW: be-SYMBOL — move SL to break-even ──────────────────────────────────

async def _handle_break_even(symbol: str, trade_manager) -> None:
    trade = await trade_manager._db.get_trade_by_symbol(symbol)
    if not trade:
        await _send(f"⚠️ No active trade found for `{symbol}`.")
        return
    msg = MoveSLBreakEven(
        raw_text=f"be {symbol}",
        message_type=MessageType.MOVE_SL_BREAK_EVEN,
        symbol=symbol,
    )
    await trade_manager._handle_move_sl_be(msg)
    # Fetch the updated SL to confirm
    pos = trade_manager._bybit.fetch_position(symbol)
    live_sl = float(pos.get("stopLoss") or 0) if pos else 0
    avg_entry = float(pos.get("avgPrice", 0)) if pos else 0
    await _send(
        f"✅ SL moved to break-even for `{symbol}`\n"
        f"  Entry: `{avg_entry:.6g}` | SL: `{live_sl:.6g}`"
    )


# ── NEW: detail-SYMBOL — deep trade info ─────────────────────────────────────

async def _handle_detail(symbol: str, trade_manager, bybit) -> None:
    trade = await trade_manager._db.get_trade_by_symbol(symbol)
    if not trade:
        await _send(f"⚠️ No active trade found for `{symbol}`.")
        return

    direction = trade["direction"].upper()
    targets   = trade.get("targets", [])
    tp_hit    = trade.get("highest_tp_hit", 0) or 0
    q_score   = trade.get("quality_score")
    q_mult    = trade.get("quality_multiplier")
    q_tier    = trade.get("tier", "BASE")
    rsi_val   = trade.get("rsi_at_entry") or trade.get("rsi_at_signal")
    btc_wk    = trade.get("btc_weekly_at_entry")
    f_reason  = trade.get("filter_reason", "n/a")
    created   = trade.get("created_at", "")

    pos       = bybit.fetch_position(symbol)
    live_size = float(pos.get("size", 0))          if pos else 0.0
    entry     = float(pos.get("avgPrice", 0))      if pos else trade.get("avg_entry_price", 0) or 0
    mark      = float(pos.get("markPrice", 0))     if pos else 0.0
    upnl      = float(pos.get("unrealisedPnl", 0)) if pos else 0.0
    live_sl   = float(pos.get("stopLoss") or 0)    if pos else trade.get("stop_loss", 0) or 0

    # Calculate SL distance %
    sl_dist_pct = abs(entry - live_sl) / entry * 100 if entry > 0 and live_sl > 0 else 0
    # Time in trade
    time_str = ""
    if created:
        try:
            ca = datetime.fromisoformat(created.replace("Z", "+00:00"))
            elapsed = (datetime.now(timezone.utc) - ca).total_seconds()
            time_str = _format_duration(elapsed)
        except Exception:
            pass

    pnl_emoji = "✅" if upnl >= 0 else "❌"

    lines = [
        f"🔍 *Detail: {symbol}* {direction} `[{trade['state']}]`\n",
        f"*Position:*",
        f"  Size: `{live_size}` @ `{entry:.6g}`",
        f"  Mark: `{mark:.6g}` | PnL: {pnl_emoji} `{upnl:+.2f} USDT`",
        f"  SL: `{live_sl:.6g}` ({sl_dist_pct:.1f}% away)",
        f"  Leverage: `{trade.get('leverage', 'n/a')}×`",
    ]
    if time_str:
        lines.append(f"  Time in trade: `{time_str}`")

    lines.append(f"\n*Sizing:*")
    if q_score is not None:
        lines.append(f"  Quality score: `{q_score}/6` [{q_tier}] → `{q_mult:.1f}×` multiplier")
        base_risk = config.risk_per_trade * 100
        eff_risk  = base_risk * (q_mult or 1.0)
        lines.append(f"  Base risk: `{base_risk:.1f}%` → Effective: `{eff_risk:.1f}%`")
    else:
        lines.append(f"  Quality: _not recorded (trade predates v2)_")

    lines.append(f"\n*Filters at entry:*")
    lines.append(f"  RSI (1h): `{rsi_val:.1f}`" if rsi_val else "  RSI: _n/a_")
    lines.append(f"  BTC weekly: `{btc_wk or 'n/a'}`")
    lines.append(f"  Result: _{f_reason}_")

    lines.append(f"\n*Targets:*")
    for i, tp in enumerate(targets):
        if i < tp_hit:
            marker = "✅"
        else:
            marker = "⬜"
        lines.append(f"  {marker} TP{i+1}: `{tp:.6g}`")
    lines.append(f"  Hit: {tp_hit}/{len(targets)}")

    # Open orders
    open_orders = bybit.fetch_open_orders(symbol)
    if open_orders:
        lines.append(f"\n*Open orders ({len(open_orders)}):*")
        for o in open_orders:
            reduce_only = str(o.get("reduceOnly", "false")).lower() == "true"
            otype = "TP" if reduce_only else "Entry"
            lines.append(
                f"  {otype}: `{float(o.get('price', 0)):.6g}` "
                f"qty `{float(o.get('qty', 0))}` ({o.get('side', '?')})"
            )

    await _send("\n".join(lines))


# ── NEW: pnl — quick PnL snapshot ────────────────────────────────────────────

async def _handle_pnl(trade_manager, bybit) -> None:
    db = trade_manager._db

    # Realised PnL (all time from DB)
    all_closed = await db.get_closed_trades_since("2000-01-01T00:00:00")
    pnl_trades = [t for t in all_closed if t.get("realised_pnl") is not None]
    total_realised = sum(t["realised_pnl"] for t in pnl_trades)

    # Unrealised PnL (sum of open positions)
    active = await db.get_active_trades()
    total_unrealised = 0.0
    for t in active:
        pos = bybit.fetch_position(t["symbol"])
        if pos:
            total_unrealised += float(pos.get("unrealisedPnl", 0))

    balance = bybit.fetch_wallet_balance()
    combined = total_realised + total_unrealised

    r_emoji = "✅" if total_realised >= 0 else "❌"
    u_emoji = "✅" if total_unrealised >= 0 else "❌"
    c_emoji = "✅" if combined >= 0 else "❌"

    await _send(
        f"💰 *PnL snapshot*\n\n"
        f"Realised:   {r_emoji} `{total_realised:+.2f} USDT` ({len(pnl_trades)} trades)\n"
        f"Unrealised: {u_emoji} `{total_unrealised:+.2f} USDT` ({len(active)} open)\n"
        f"Combined:   {c_emoji} `{combined:+.2f} USDT`\n\n"
        f"Balance: `{balance:.2f} USDT`"
    )


# ── NEW: orders-SYMBOL — show open orders ────────────────────────────────────

async def _handle_orders(symbol: str, bybit) -> None:
    open_orders = bybit.fetch_open_orders(symbol)
    if not open_orders:
        await _send(f"📋 No open orders for `{symbol}`.")
        return

    lines = [f"📋 *Open orders for {symbol} ({len(open_orders)}):*\n"]
    for o in open_orders:
        reduce_only = str(o.get("reduceOnly", "false")).lower() == "true"
        otype = o.get("orderType", "?")
        side  = o.get("side", "?")
        price = float(o.get("price", 0))
        qty   = float(o.get("qty", 0))
        status = o.get("orderStatus", "?")
        label = "TP/Close" if reduce_only else "Entry"

        lines.append(
            f"  • `{label}` {side} {otype}\n"
            f"    Price: `{price:.6g}` | Qty: `{qty}` | Status: `{status}`"
        )

    await _send("\n".join(lines))


# ── NEW: rejected — show recently filtered signals ───────────────────────────

async def _handle_rejected(trade_manager) -> None:
    db = trade_manager._db
    rejected = await db.get_recent_rejected(10)
    if not rejected:
        await _send("📋 *Rejected signals:* none recorded yet.")
        return

    lines = [f"🚫 *Last {len(rejected)} rejected signals:*\n"]
    for r in rejected:
        ts = r.get("rejected_at", "?")
        # Shorten timestamp to just date + time
        if len(ts) > 16:
            ts = ts[:16]
        lines.append(
            f"*{r.get('symbol', '?')}* {(r.get('direction') or '?').upper()}\n"
            f"  Reason: _{r.get('reason', 'unknown')}_\n"
            f"  RSI: `{r.get('rsi_value') or 'n/a'}` | BTC wk: `{r.get('btc_weekly') or 'n/a'}` "
            f"| Q: `{r.get('quality_score', '?')}/6`\n"
            f"  `{ts}`"
        )

    await _send("\n\n".join(lines))


# ── NEW: config — show live config ───────────────────────────────────────────

async def _handle_config() -> None:
    lines = [
        "⚙️ *Bot configuration:*\n",
        f"*Risk & Sizing:*",
        f"  Risk/trade: `{config.risk_per_trade*100:.1f}%`",
        f"  Max leverage: `{config.max_leverage}×`",
        f"  Default leverage: `{config.default_leverage}×`",
        f"  Dry run: `{config.dry_run}`",
        f"  Testnet: `{config.bybit_testnet}`",
        f"\n*Quality Sizing:*",
        f"  Enabled: `{getattr(config, 'quality_sizing_enabled', True)}`",
        f"  HIGH (≥{getattr(config, 'quality_high_threshold', 5)}): `{getattr(config, 'quality_mult_high', 1.5):.1f}×`",
        f"  MED (≥{getattr(config, 'quality_med_threshold', 3)}): `{getattr(config, 'quality_mult_med', 1.0):.1f}×`",
        f"  LOW: `{getattr(config, 'quality_mult_low', 0.7):.1f}×`",
        f"\n*Filters:*",
        f"  Filter enabled: `{getattr(config, 'filter_enabled', True)}`",
        f"  Min zone width: `{getattr(config, 'filter_min_entry_range_pct', 2.0)}%`",
        f"  Min SL distance: `{getattr(config, 'filter_min_sl_pct', 3.0)}%`",
        f"  Max TP1 R:R: `{getattr(config, 'filter_max_tp1_rr', 1.1)}`",
        f"  Min targets: `{getattr(config, 'filter_min_num_targets', 6)}`",
        f"  Block 8T LONG: `{getattr(config, 'filter_block_8t_long', True)}`",
        f"  RSI max ({getattr(config, 'filter_rsi_tf', '1h')}): `{getattr(config, 'filter_rsi_signal_max', 40.0)}`",
        f"  BTC weekly filter: `{getattr(config, 'filter_btc_weekly_enabled', True)}`",
        f"\n*Blowthrough:*",
        f"  Cancel enabled: `{getattr(config, 'blowthrough_cancel', True)}`",
        f"  Depth threshold: `35%`",
    ]
    await _send("\n".join(lines))


# ── help ──────────────────────────────────────────────────────────────────────

async def _handle_help() -> None:
    await _send(
        "📖 *Available commands:*\n\n"
        "*Signals*\n"
        "• Paste a signal in the standard group format → registers the trade\n\n"
        "*Trade control*\n"
        "• `close-SYMBOL` — market-close a position\n"
        "• `close-all` — emergency market-close all positions\n"
        "• `cancel-entries-SYMBOL` — cancel unfilled entries (keep position)\n"
        "• `be-SYMBOL` — move SL to break-even\n\n"
        "*Information*\n"
        "• `detail-SYMBOL` — deep dive: entry, PnL, quality, filters, sizing\n"
        "• `see-all-trades` — all active trades with live PnL\n"
        "• `orders-SYMBOL` — open Bybit orders for a symbol\n"
        "• `status-update` — win/loss stats + PnL (day/week/month/year)\n"
        "• `pnl` — quick PnL snapshot (realised + unrealised)\n"
        "• `rejected` — last 10 filtered-out signals with reasons\n"
        "• `config` — current bot configuration\n"
        "• `balance` — USDT wallet balance\n"
        "• `help` — this message"
    )


# ── Signal passthrough ───────────────────────────────────────────────────────

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

                elif text_lower.startswith("be-"):
                    symbol = text[3:].strip().upper()
                    await _handle_break_even(symbol, trade_manager)

                elif text_lower.startswith("detail-"):
                    symbol = text[7:].strip().upper()
                    await _handle_detail(symbol, trade_manager, bybit)

                elif text_lower.startswith("orders-"):
                    symbol = text[7:].strip().upper()
                    await _handle_orders(symbol, bybit)

                elif text_lower == "see-all-trades":
                    await _handle_see_all(trade_manager, bybit)

                elif text_lower == "status-update":
                    await _handle_status(trade_manager, bybit)

                elif text_lower == "pnl":
                    await _handle_pnl(trade_manager, bybit)

                elif text_lower == "rejected":
                    await _handle_rejected(trade_manager)

                elif text_lower == "config":
                    await _handle_config()

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
