"""
trade_manager.py – Central domain layer.

All trade lifecycle decisions live here.
Exchange layer is called for execution only.

Entry ladder (dynamic by range %):
  range_pct = (entry_high - entry_low) / midpoint
  < 0.7%        →  80 / 15 / 5
  0.7% – 1.5%   →  70 / 20 / 10
  1.5% – 3%     →  65 / 25 / 10
  > 3%          →  50 / 30 / 20

  Distribution:
    Entry_high → largest fraction  (soonest to fill)
    Entry_mid  → middle fraction
    Entry_low  → smallest fraction (best price, least likely)

TP orders:    placed on first fill, qty split equally across remaining targets
SL:           attached directly to each entry order (activates on fill)
              SL never moves after a TP fills — backtest showed static SL
              outperforms ratcheting by +0.76R/trade (270 trades, 2025-2026)
TP1 fills:    cancel remaining entry orders only (no SL move)
Break-even:   moves SL to avg entry and cancels remaining entry orders (manual command only)
"""

import logging
import math
from datetime import datetime
from typing import Optional, List, Tuple

from app.config import config
from app.exchange.bybit_client import BybitClient
from app.storage.database import Database
from app.domain.signal_filter import evaluate_signal
from app.monitoring.alerter import send_notification
from app.parsing.models import (
    ParsedMessage, MessageType, Direction,
    NewSignal, CloseAll, CloseSymbol, CancelRemainingEntries,
    MoveSLBreakEven, MoveSLPrice, UpdateTargets, AddEntries,
    MarketEntry, PartialClose, CancelSignal,
)

log = logging.getLogger(__name__)


# ── dynamic entry ladder ──────────────────────────────────────────────────────

def _calc_ladder(entry_low: float, entry_high: float) -> List[Tuple]:
    """
    Returns a list of (price, fraction) tuples for entry orders.

    Range-based distribution (% of total qty):
      < 0.7%       →  80 / 15 / 5
      0.7% – 1.5%  →  70 / 20 / 10
      1.5% – 3%    →  65 / 25 / 10
      > 3%         →  50 / 30 / 20

    Entry_high gets the largest fraction (fills soonest).
    Collapses to a single order at full qty for single-price signals.
    """
    if entry_low <= 0 or entry_high <= entry_low:
        price = entry_high if entry_high > 0 else entry_low
        return [(price, 1.0)]

    midpoint = (entry_low + entry_high) / 2
    range_pct = (entry_high - entry_low) / midpoint * 100

    if range_pct < 0.7:
        fracs = (0.80, 0.15, 0.05)
    elif range_pct < 1.5:
        fracs = (0.70, 0.20, 0.10)
    elif range_pct < 3.0:
        fracs = (0.65, 0.25, 0.10)
    else:
        fracs = (0.50, 0.30, 0.20)

    return [
        (entry_high, fracs[0]),
        (midpoint,   fracs[1]),
        (entry_low,  fracs[2]),
    ]


def _opposite_side(direction: str) -> str:
    return "Sell" if direction.lower() in ("long", "buy") else "Buy"


def _entry_side(direction: str) -> str:
    return "Buy" if direction.lower() in ("long", "buy") else "Sell"


def _calc_qty(balance: float, risk_fraction: float, entry_price: float,
              stop_loss: float, leverage: int) -> float:
    """
    Position sizing: fixed margin percentage of balance.
    margin = balance × margin_pct
    qty = (margin × leverage) / entry_price

    Auto-scales margin by account size when filter_auto_scale is enabled:
      $0–500:     15% margin  (aggressive growth)
      $500–2000:  13% margin
      $2000–5000: 10% margin
      $5000+:      7% margin  (capital preservation)
    """
    if entry_price <= 0:
        return 0.0

    # Dynamic margin scaling based on account size
    margin_pct = risk_fraction  # default from config (RISK_PER_TRADE)
    if getattr(config, "filter_auto_scale", True):
        if balance >= 5000:
            margin_pct = 0.07
        elif balance >= 2000:
            margin_pct = 0.10
        elif balance >= 500:
            margin_pct = 0.13
        else:
            margin_pct = 0.15

    margin = balance * margin_pct
    qty = (margin * leverage) / entry_price
    return math.floor(qty * 1000) / 1000


def _floor3(v: float) -> float:
    return math.floor(v * 1000) / 1000


# ── TP distribution table ─────────────────────────────────────────────────────
# Keyed by total number of TP targets in the signal.
# Values are percentage weights (sum to ~100).
# BALANCED BACK-LOADED: optimized from 258K+ backtested combinations.
# Later TPs get more weight — safe because trailing SL protects the position.

_TP_DIST: dict = {
    1:  [100],
    2:  [14, 86],
    3:  [41, 21, 65],
    4:  [14, 10, 16, 60],
    5:  [10, 12, 18, 25, 35],
    6:  [5, 7, 10, 15, 25, 38],
    7:  [4, 5, 7, 10, 15, 25, 34],
    8:  [3, 4, 5, 7, 10, 16, 25, 30],
    9:  [3, 3, 4, 6, 8, 12, 17, 22, 25],
    10: [3, 3, 4, 5, 7, 10, 13, 17, 20, 18],
    11: [3, 3, 3, 4, 6, 8, 11, 14, 17, 16, 15],
    12: [2, 3, 3, 4, 5, 7, 9, 12, 14, 15, 14, 12],
    13: [2, 2, 3, 4, 5, 6, 8, 10, 12, 13, 13, 12, 10],
    14: [2, 2, 3, 3, 4, 5, 7, 9, 11, 12, 13, 12, 10, 7],
    15: [2, 2, 2, 3, 4, 5, 6, 8, 10, 11, 12, 12, 10, 7, 6],
}


def _tp_fractions(n: int) -> list:
    """Return a list of fractions (0.0–1.0) for n TP targets."""
    if n in _TP_DIST:
        pcts = _TP_DIST[n]
    else:
        # Equal split for anything beyond 15
        pcts = [100 / n] * n
    total = sum(pcts)
    return [p / total for p in pcts]


class TradeManager:
    def __init__(self, db: Database, bybit: BybitClient):
        self._db    = db
        self._bybit = bybit

    async def _notify(self, message: str):
        """Convenience wrapper — fires send_notification with the DB reference."""
        try:
            await send_notification(message, db=self._db)
        except Exception as exc:
            log.error("Notification send failed: %s", exc)

    # ── startup position sync ─────────────────────────────────────────────────

    async def startup_position_sync(self):
        """
        Called once at boot. Reconciles DB ↔ Bybit in three passes:

        Pass 1 – known DB trades
          • Active DB trade + live position  → update filled_size/avg_entry, re-sync
            TP orders and SL so management continues seamlessly after restart.
          • Active DB trade + no live position → position closed while bot was
            offline; mark closed (WIN if any TP was hit, else LOSS).

        Pass 2 – live positions with no DB record (bot started a trade, crashed
          before writing to DB, OR position was opened on Bybit manually but a
          DB record already exists from a previous bot run that got corrupted).
          These are skipped with a warning — we cannot safely reconstruct targets.

        Pass 3 – live Bybit positions with no matching DB trade at all
          → logged as "untracked position" so the user knows about them, but
          the bot does NOT attempt to manage them (no targets/SL in DB).
        """
        db_trades   = await self._db.get_active_trades()
        all_live    = self._bybit.fetch_all_positions()   # size > 0
        live_by_sym = {p["symbol"]: p for p in all_live}

        synced_closed   = []
        synced_live     = []
        synced_restored = []

        # ── Pass 1: reconcile known DB trades ─────────────────────────────────
        for trade in db_trades:
            symbol = trade["symbol"]
            pos    = live_by_sym.get(symbol)
            live_size = float(pos.get("size", 0)) if pos else 0.0

            if live_size <= 0:
                # Position gone → closed while offline
                highest_tp = trade.get("highest_tp_hit", 0) or 0
                result = "WIN" if highest_tp > 0 else "LOSS"
                await self._db.set_last_trade_result(result)
                await self._db.close_trade(trade["id"], "closed")
                synced_closed.append(f"{symbol} → {result} (TP{highest_tp})")
                log.info(
                    "Startup sync: %s no live position (TP%d) → closed (%s)",
                    symbol, highest_tp, result,
                )
            else:
                # Position still open → update DB and resume management
                avg_price = float(pos.get("avgPrice", 0))
                upnl      = float(pos.get("unrealisedPnl", 0))
                live_sl   = float(pos.get("stopLoss") or 0)

                await self._db.update_trade(
                    trade["id"],
                    filled_size=live_size,
                    avg_entry_price=avg_price,
                )

                # ── Re-sync SL ─────────────────────────────────────────────
                # Use the live Bybit SL as source-of-truth if it is set
                # (ratchet may have moved it while bot was offline).
                # If Bybit has no SL, re-apply the DB value to protect position.
                db_sl = trade.get("stop_loss", 0) or 0.0
                if live_sl > 0 and abs(live_sl - db_sl) > 0.000001:
                    # Bybit SL differs from DB — update DB to match live
                    log.info(
                        "Startup sync: %s SL mismatch — Bybit=%.5f DB=%.5f → updating DB",
                        symbol, live_sl, db_sl,
                    )
                    await self._db.update_trade(trade["id"], stop_loss=live_sl)
                elif live_sl == 0 and db_sl > 0:
                    # No SL on Bybit but we have one in DB → re-apply it
                    log.info(
                        "Startup sync: %s no SL on Bybit, re-applying DB SL %.5f",
                        symbol, db_sl,
                    )
                    self._bybit.move_stop_loss(symbol, db_sl)

                # ── Re-sync TP orders ──────────────────────────────────────
                # Detect how many TPs have filled by comparing live open
                # reduce-only orders on Bybit vs what we expect.
                # This keeps the ratchet correct after a restart mid-trade.
                targets     = trade.get("targets", [])
                highest_tp  = trade.get("highest_tp_hit", 0) or 0
                if targets and live_size > 0:
                    highest_tp = await self._detect_highest_tp_hit(
                        trade, live_size, highest_tp
                    )
                    if highest_tp != (trade.get("highest_tp_hit", 0) or 0):
                        log.info(
                            "Startup sync: %s highest_tp_hit updated %d → %d",
                            symbol,
                            trade.get("highest_tp_hit", 0) or 0,
                            highest_tp,
                        )
                        await self._db.update_trade(
                            trade["id"], highest_tp_hit=highest_tp
                        )
                    # Refresh DB open-order records and re-place missing TP orders
                    await self._resync_tp_orders_on_bybit(trade, live_size, highest_tp)

                synced_live.append(
                    f"{symbol} size={live_size:.4f} "
                    f"entry={avg_price:.4f} "
                    f"SL={live_sl or db_sl:.4f} "
                    f"PnL={upnl:+.2f}"
                )
                synced_restored.append(symbol)
                log.info(
                    "Startup sync: %s RESUMED — size=%.4f avg=%.4f TP_hit=%d",
                    symbol, live_size, avg_price, highest_tp,
                )

        # ── Pass 2: live positions that have no DB trade ───────────────────────
        known_symbols = {t["symbol"] for t in db_trades}
        for sym, pos in live_by_sym.items():
            if sym not in known_symbols:
                size  = float(pos.get("size", 0))
                side  = pos.get("side", "?")
                entry = float(pos.get("avgPrice", 0))
                upnl  = float(pos.get("unrealisedPnl", 0))
                log.warning(
                    "Startup sync: UNTRACKED position %s %s size=%.4f entry=%.4f "
                    "— not managed by bot (no DB record)",
                    sym, side, size, entry,
                )
                synced_live.append(
                    f"{sym} ⚠ UNTRACKED {side} size={size:.4f} "
                    f"entry={entry:.4f} PnL={upnl:+.2f}"
                )

        # ── Startup summary notification ───────────────────────────────────────
        balance = self._bybit.fetch_wallet_balance()
        mode = "DRY RUN" if config.dry_run else "LIVE"
        lines = [f"🟢 *Bot started* ({mode})", f"Balance: `{balance:.2f} USDT`"]
        if synced_restored:
            lines.append(f"\n▶️ *Resumed management ({len(synced_restored)}):*")
            for s in synced_live[:len(synced_restored)]:
                lines.append(f"  • {s}")
        if synced_closed:
            lines.append(f"\n🗑 *Closed during offline ({len(synced_closed)}):*")
            for s in synced_closed:
                lines.append(f"  • {s}")
        untracked = [s for s in synced_live[len(synced_restored):]]
        if untracked:
            lines.append(f"\n⚠️ *Untracked positions (manual):*")
            for s in untracked:
                lines.append(f"  • {s}")
        if not synced_restored and not synced_closed and not untracked:
            lines.append("No active trades.")
        await self._notify("\n".join(lines))

    async def _detect_highest_tp_hit(
        self, trade: dict, live_size: float, current_highest: int
    ) -> int:
        """
        Estimate how many TPs have filled by comparing the live position size
        to the original total qty.  We can't know for certain without order
        history, but we can use the remaining qty fraction to infer a lower bound.

        Also cross-checks against open reduce-only orders on Bybit: if a TP
        order is missing from the open orders we know at minimum that level fired.
        """
        targets    = trade.get("targets", [])
        total_tps  = len(targets)
        if not total_tps:
            return current_highest

        fractions = _tp_fractions(total_tps)

        # Qty remaining if N TPs have been hit = original_qty × sum(remaining fractions)
        # We infer original_qty from DB filled_size (set on first fill)
        original_qty = trade.get("filled_size", 0) or live_size
        if original_qty <= 0:
            return current_highest

        # Walk from current_highest upward and see if the observed size is
        # consistent with more TPs having fired.
        best_estimate = current_highest
        remaining_after = [
            original_qty * sum(fractions[n:])
            for n in range(total_tps + 1)
        ]

        for n in range(current_highest, total_tps):
            expected_remaining = remaining_after[n + 1]
            # If live size ≤ expected remaining after TP(n+1), it's possible
            # TP(n+1) has filled.  Use a 5% tolerance for partial fills / rounding.
            if live_size <= expected_remaining * 1.05:
                best_estimate = n + 1
            else:
                break  # sizes are monotonically decreasing; stop when it no longer fits

        return max(current_highest, best_estimate)

    async def _resync_tp_orders_on_bybit(
        self, trade: dict, live_size: float, highest_tp_hit: int
    ):
        """
        After a restart, cancel stale DB TP records and re-place any missing
        TP orders on Bybit so management continues after restart.

        Strategy:
          1. Fetch open reduce-only limit orders from Bybit for this symbol.
          2. Mark any DB tp orders that are no longer open on Bybit as cancelled.
          3. Place any missing TP levels (from highest_tp_hit+1 onward) that
             don't already have a live order.
        """
        symbol    = trade["symbol"]
        trade_id  = trade["id"]
        targets   = trade.get("targets", [])
        direction = trade["direction"]
        if not targets or live_size <= 0:
            return

        # Fetch live reduce-only orders from Bybit
        live_orders = self._bybit.fetch_open_orders(symbol)
        live_reduce_only = {
            float(o.get("price", 0)): o
            for o in live_orders
            if str(o.get("reduceOnly", "false")).lower() == "true"
        }

        # Mark stale DB tp records as cancelled
        db_open = await self._db.get_open_orders_for_trade(trade_id)
        for order in db_open:
            if not order["order_type"].startswith("tp"):
                continue
            price = float(order.get("price", 0))
            if price not in live_reduce_only:
                await self._db.mark_order_status(order["bybit_order_id"], "cancelled")
                log.debug(
                    "Startup sync: marked stale TP order %s as cancelled",
                    order["bybit_order_id"],
                )

        # Determine which TP levels still need live orders
        close_side        = _opposite_side(direction)
        remaining_targets = targets[highest_tp_hit:]
        if not remaining_targets:
            return

        fractions = _tp_fractions(len(targets))
        remaining_fractions = fractions[highest_tp_hit:]
        frac_sum = sum(remaining_fractions)
        if frac_sum <= 0:
            return
        remaining_fractions = [f / frac_sum for f in remaining_fractions]

        # For each remaining TP, place it only if there is no live order
        # at that price (within a small tolerance)
        for i, (tp_price, frac) in enumerate(zip(remaining_targets, remaining_fractions)):
            tp_num = highest_tp_hit + i + 1

            # Check if a live order already covers this TP price
            already_live = any(
                abs(p - tp_price) < tp_price * 0.0005
                for p in live_reduce_only
            )
            if already_live:
                # Ensure the DB record exists for tracking
                log.debug(
                    "Startup sync: TP%d for %s already live on Bybit at %.5f — skipping placement",
                    tp_num, symbol, tp_price,
                )
                continue

            order_qty = _floor3(live_size * frac)
            if tp_price <= 0 or order_qty <= 0:
                continue

            order_id = self._bybit.place_take_profit_order(
                symbol, close_side, order_qty, tp_price
            )
            if order_id:
                await self._db.save_order(
                    trade_id, order_id, symbol, f"tp{tp_num}",
                    close_side, tp_price, order_qty
                )
                log.info(
                    "Startup sync: re-placed TP%d for %s at %.5f qty=%.4f",
                    tp_num, symbol, tp_price, order_qty,
                )

    # ─────────────────────────────────────────────────────────────────────────

    async def handle(self, msg: ParsedMessage):
        t = msg.message_type
        if t == MessageType.NEW_SIGNAL:
            await self._handle_new_signal(msg)
        elif t == MessageType.CLOSE_ALL:
            await self._handle_close_all(msg)
        elif t == MessageType.CLOSE_SYMBOL:
            await self._handle_close_symbol(msg)
        elif t == MessageType.CANCEL_REMAINING_ENTRIES:
            await self._handle_cancel_entries(msg)
        elif t == MessageType.MOVE_SL_BREAK_EVEN:
            await self._handle_move_sl_be(msg)
        elif t == MessageType.MOVE_SL_PRICE:
            await self._handle_move_sl_price(msg)
        elif t == MessageType.UPDATE_TARGETS:
            await self._handle_update_targets(msg)
        elif t == MessageType.MARKET_ENTRY:
            await self._handle_market_entry(msg)
        elif t == MessageType.PARTIAL_CLOSE:
            await self._handle_partial_close(msg)
        elif t == MessageType.CANCEL_SIGNAL:
            await self._handle_cancel_signal(msg)
        elif t == MessageType.ADD_ENTRIES:
            await self._handle_add_entries(msg)
        else:
            log.debug("Ignored message type %s", t)

    # ── new signal ────────────────────────────────────────────────────────────

    async def _handle_new_signal(self, sig: NewSignal):
        if not sig.symbol or not sig.direction:
            log.warning("NewSignal missing symbol or direction – skipped")
            return

        if await self._db.get_trade_by_symbol(sig.symbol):
            log.info("Active trade already exists for %s – skipping", sig.symbol)
            return

        # ── Signal filter ─────────────────────────────────────────────────────
        last_result = await self._db.get_last_trade_result()
        last_signal_time = await self._db.get_last_signal_time()

        # Always record signal time (even if we skip the trade)
        await self._db.set_last_signal_time(datetime.utcnow())

        if getattr(config, "filter_enabled", True):
            action, reason = evaluate_signal(
                symbol=sig.symbol,
                entry_high=sig.entry_high,
                entry_low=sig.entry_low,
                stop_loss=sig.stop_loss,
                targets=sig.targets,
                last_trade_result=last_result,
                last_signal_time=last_signal_time,
            )
            if action == "SKIP":
                log.info("SIGNAL SKIPPED %s %s: %s", sig.symbol, sig.direction.value, reason)
                await self._notify(
                    f"⏭ *Signal skipped*\n"
                    f"Pair: `{sig.symbol}` {sig.direction.value.upper()}\n"
                    f"Reason: {reason}\n"
                    f"Entry: `{sig.entry_low:.6g}` – `{sig.entry_high:.6g}`\n"
                    f"SL: `{sig.stop_loss:.6g}` | Targets: {len(sig.targets)}"
                )
                return
            risk_multiplier = 0.5 if action == "HALF" else 1.0
        else:
            risk_multiplier = 1.0
        # ── End filter ────────────────────────────────────────────────────────

        leverage = min(sig.leverage_max, config.max_leverage)
        self._bybit.set_leverage(sig.symbol, leverage)

        balance = self._bybit.fetch_wallet_balance()
        if balance <= 0 and not config.dry_run:
            log.error("Cannot determine balance – skipping %s", sig.symbol)
            return

        entry_ref = sig.entry_high if sig.entry_high > 0 else sig.entry_low
        qty = _calc_qty(balance, config.risk_per_trade, entry_ref, sig.stop_loss, leverage)

        # Apply risk multiplier from filter (HALF = 0.5×)
        qty = _floor3(qty * risk_multiplier)

        if qty <= 0:
            log.warning("Calculated qty=0 for %s – skipping", sig.symbol)
            return

        trade_id = await self._db.upsert_trade({
            "signal_telegram_id": sig.telegram_message_id,
            "symbol":    sig.symbol,
            "direction": sig.direction.value,
            "leverage":  leverage,
            "entry_low":  sig.entry_low,
            "entry_high": sig.entry_high,
            "stop_loss":  sig.stop_loss,
            "targets":    sig.targets,
            "state":      "pending",
        })

        side   = _entry_side(sig.direction.value)
        ladder = _calc_ladder(sig.entry_low, sig.entry_high)

        # FIX #1: attach SL directly to each entry order so it becomes active
        # the instant any leg fills — no separate move_stop_loss on empty position.
        for price, fraction in ladder:
            price     = round(price, 8)
            order_qty = _floor3(qty * fraction)
            if order_qty <= 0 or price <= 0:
                continue
            order_id = self._bybit.place_limit_order(
                sig.symbol, side, order_qty, price,
                order_type_label="entry",
                stop_loss=sig.stop_loss,          # ← attached to order
            )
            if order_id:
                await self._db.save_order(
                    trade_id, order_id, sig.symbol, "entry", side, price, order_qty
                )

        # NOTE: move_stop_loss deliberately removed here.
        # Bybit rejects set_trading_stop when no position exists yet (ErrCode 10001).
        # The SL above is attached per-order and fires on first fill.

        await self._db.update_trade_state(trade_id, "active")
        log.info(
            "Trade opened: %s %s | qty=%.4f | ladder=%s | sl=%.5f",
            sig.symbol, sig.direction.value, qty,
            " / ".join(f"{p:.5f}({f*100:.0f}%)" for p, f in ladder),
            sig.stop_loss,
        )

        # ── Telegram notification: new trade opened ──────────────────────
        dir_emoji = "🟢" if sig.direction.value.lower() in ("long", "buy") else "🔴"
        size_note = " (HALF size)" if risk_multiplier < 1.0 else ""
        ladder_str = "\n".join(
            f"  `{p:.6g}` — {f*100:.0f}% ({_floor3(qty * f):.4f})"
            for p, f in ladder
        )
        tp_str = "\n".join(
            f"  TP{i+1}: `{t:.6g}`" for i, t in enumerate(sig.targets[:6])
        )
        if len(sig.targets) > 6:
            tp_str += f"\n  … +{len(sig.targets) - 6} more"
        await self._notify(
            f"{dir_emoji} *New trade opened*{size_note}\n"
            f"Pair: `{sig.symbol}` {sig.direction.value.upper()}\n"
            f"Leverage: {leverage}x | Qty: {qty:.4f}\n"
            f"Balance: `{balance:.2f} USDT`\n\n"
            f"📥 *Entry ladder:*\n{ladder_str}\n\n"
            f"🛑 SL: `{sig.stop_loss:.6g}`\n\n"
            f"🎯 *Targets ({len(sig.targets)}):*\n{tp_str}"
        )

    # ── TP order management ───────────────────────────────────────────────────

    async def _refresh_tp_orders(self, trade: dict, filled_qty: float):
        """
        Cancel open TP orders and replace with fresh ones based on actual
        filled position size. Skips TP levels already hit.

        FIX #5: always re-fetch live position size from Bybit rather than
        relying on the caller-supplied filled_qty, to avoid over-placing after
        partial closes or mid-session size changes.
        """
        trade_id       = trade["id"]
        symbol         = trade["symbol"]
        direction      = trade["direction"]
        targets        = trade.get("targets", [])
        highest_tp_hit = trade.get("highest_tp_hit", 0) or 0

        if not targets:
            return

        # FIX #5: authoritative size from Bybit REST
        pos = self._bybit.fetch_position(symbol)
        live_qty = float(pos.get("size", 0)) if pos else filled_qty
        if live_qty <= 0:
            return

        open_orders = await self._db.get_open_orders_for_trade(trade_id)
        for order in open_orders:
            if order["order_type"].startswith("tp"):
                self._bybit.cancel_order(symbol, order["bybit_order_id"])
                await self._db.mark_order_status(order["bybit_order_id"], "cancelled")

        close_side        = _opposite_side(direction)
        remaining_targets = targets[highest_tp_hit:]

        if not remaining_targets:
            log.info("All TP levels already hit for %s", symbol)
            return

        # Weighted TP distribution based on total number of targets in signal
        fractions = _tp_fractions(len(targets))
        remaining_fractions = fractions[highest_tp_hit:]
        frac_sum = sum(remaining_fractions)
        if frac_sum <= 0:
            return
        remaining_fractions = [f / frac_sum for f in remaining_fractions]

        for i, (tp_price, frac) in enumerate(zip(remaining_targets, remaining_fractions)):
            tp_num    = highest_tp_hit + i + 1
            order_qty = _floor3(live_qty * frac)
            if tp_price <= 0 or order_qty <= 0:
                continue
            order_id = self._bybit.place_take_profit_order(
                symbol, close_side, order_qty, tp_price
            )
            if order_id:
                await self._db.save_order(
                    trade_id, order_id, symbol, f"tp{tp_num}",
                    close_side, tp_price, order_qty
                )

        log.info(
            "TP orders refreshed for %s | live=%.4f | remaining=%d | dist=%s",
            symbol, live_qty, len(remaining_targets),
            "/".join(f"{f*100:.0f}%" for f in remaining_fractions),
        )

    # ── TP fill handler ───────────────────────────────────────────────────────

    async def on_tp_filled(self, symbol: str, tp_num: int):
        """
        Called when a TP order fills (via WebSocket or sync_fills).

        Records highest_tp_hit for WIN/LOSS classification.
        On TP1: cancels remaining entry orders (stops adding to position).
        SL is intentionally never moved — static SL outperforms ratcheting
        by +0.76R/trade based on 270-trade backtest (2025-2026, Bybit 1H).
        """
        trade = await self._db.get_trade_by_symbol(symbol)
        if not trade:
            log.info("on_tp_filled: no active trade for %s", symbol)
            return

        await self._db.update_trade(trade["id"], highest_tp_hit=tp_num)

        if tp_num == 1:
            log.info("TP1 filled %s → cancelling remaining entries", symbol)
            await self._handle_cancel_entries(
                CancelRemainingEntries(
                    raw_text="", message_type=MessageType.CANCEL_REMAINING_ENTRIES,
                    symbol=symbol,
                )
            )

    # ── WebSocket execution handler ───────────────────────────────────────────

    async def on_ws_execution(self, msg: dict):
        """Called by BybitStream on every execution (fill) event."""
        data = msg.get("data", [])
        if not data:
            return

        for exec_item in data:
            symbol    = exec_item.get("symbol", "")
            order_id  = exec_item.get("orderId", "")
            exec_type = exec_item.get("execType", "")
            exec_qty  = float(exec_item.get("execQty", 0))
            avg_price = float(exec_item.get("execPrice", 0))

            if exec_type != "Trade" or exec_qty <= 0:
                continue

            order = await self._db.get_order_by_bybit_id(order_id)
            if not order:
                continue

            trade = await self._db.get_trade_by_id(order["trade_id"])
            if not trade:
                continue

            order_type = order.get("order_type", "")
            log.info(
                "WS execution: %s %s qty=%.4f price=%.5f",
                symbol, order_type, exec_qty, avg_price,
            )

            await self._db.mark_order_status(order_id, "filled")

            if order_type == "entry":
                pos       = self._bybit.fetch_position(symbol)
                filled    = float(pos.get("size", 0))    if pos else exec_qty
                pos_avg   = float(pos.get("avgPrice", 0)) if pos else avg_price
                prev_filled = trade.get("filled_size", 0) or 0.0

                await self._db.update_trade(
                    trade["id"], filled_size=filled, avg_entry_price=pos_avg,
                )

                if prev_filled == 0.0:
                    sl_price = trade.get("stop_loss", 0)
                    if sl_price and sl_price > 0:
                        ok = self._bybit.move_stop_loss(symbol, sl_price)
                        log.info("SL enforced on first fill %s → %.5f (ok=%s)", symbol, sl_price, ok)

                fresh_trade = await self._db.get_trade_by_symbol(symbol)
                if fresh_trade:
                    await self._refresh_tp_orders(fresh_trade, filled)

                # ── Telegram notification: entry filled ──────────────────
                first_fill = "🥇 *First fill!*\n" if prev_filled == 0.0 else ""
                upnl = float(pos.get("unrealisedPnl", 0)) if pos else 0.0
                await self._notify(
                    f"📥 *Entry filled*\n"
                    f"{first_fill}"
                    f"Pair: `{symbol}` {trade['direction'].upper()}\n"
                    f"Fill price: `{avg_price:.6g}` | Qty: {exec_qty:.4f}\n"
                    f"Position: {filled:.4f} @ `{pos_avg:.6g}`\n"
                    f"PnL: `{upnl:+.2f} USDT`"
                )

            elif order_type.startswith("tp"):
                try:
                    tp_num = int(order_type[2:])
                except ValueError:
                    tp_num = 1

                await self.on_tp_filled(symbol, tp_num)

                pos       = self._bybit.fetch_position(symbol)
                remaining = float(pos.get("size", 0)) if pos else 0.0
                if remaining <= 0:
                    # Fetch closed PnL from the execution that just filled
                    closed_pnl = float(pos.get("unrealisedPnl", 0)) if pos else None
                    await self._db.set_last_trade_result("WIN")
                    await self._db.close_trade(trade["id"], "closed", realised_pnl=closed_pnl)
                    log.info("All TPs filled for %s – trade closed (WIN)", symbol)
                    await self._notify(
                        f"🏆 *All targets hit — trade closed!*\n"
                        f"Pair: `{symbol}` {trade['direction'].upper()}\n"
                        f"Final TP: {tp_num} @ `{avg_price:.6g}`\n"
                        f"Result: *WIN*"
                    )
                else:
                    fresh_trade = await self._db.get_trade_by_symbol(symbol)
                    upnl = float(pos.get("unrealisedPnl", 0)) if pos else 0.0
                    sl_price = fresh_trade.get("stop_loss", 0) if fresh_trade else 0
                    await self._notify(
                        f"🎯 *TP{tp_num} hit!*\n"
                        f"Pair: `{symbol}` {trade['direction'].upper()}\n"
                        f"TP price: `{avg_price:.6g}` | Closed qty: {exec_qty:.4f}\n"
                        f"Remaining: {remaining:.4f} | PnL: `{upnl:+.2f} USDT`\n"
                        f"SL: `{sl_price:.6g}` (unchanged)"
                    )
                    if fresh_trade:
                        await self._refresh_tp_orders(fresh_trade, remaining)

    # ── WebSocket order status handler ────────────────────────────────────────

    async def on_ws_order(self, msg: dict):
        """Detects SL hit via order status change.

        FIX #3: SL orders are position-level on Bybit and never saved to the
        orders table, so get_order_by_bybit_id always returns None for them.
        We now fall back to looking up the trade by symbol from the WS event.
        """
        data = msg.get("data", [])
        for item in data:
            order_id        = item.get("orderId", "")
            order_status    = item.get("orderStatus", "")
            stop_order_type = item.get("stopOrderType", "")
            symbol          = item.get("symbol", "")

            if order_status == "Filled" and stop_order_type == "StopLoss":
                # Try DB lookup first (handles any edge case where SL was saved)
                order = await self._db.get_order_by_bybit_id(order_id)
                if order:
                    trade = await self._db.get_trade_by_id(order["trade_id"])
                else:
                    # FIX #3: fall back to symbol lookup — position-level SL
                    trade = await self._db.get_trade_by_symbol(symbol) if symbol else None

                if trade:
                    highest_tp = trade.get("highest_tp_hit", 0) or 0
                    result = "WIN" if highest_tp > 0 else "LOSS"
                    sl_pnl = None
                    try:
                        sl_pos = self._bybit.fetch_position(trade["symbol"])
                        if sl_pos is None:  # position gone = closed
                            sl_pnl = float(item.get("cumRealisedPnl", 0) or 0) or None
                    except Exception:
                        pass
                    await self._db.set_last_trade_result(result)
                    await self._db.close_trade(trade["id"], "sl_hit", realised_pnl=sl_pnl)
                    log.warning(
                        "SL hit for %s (TP%d reached) → result=%s",
                        trade["symbol"], highest_tp, result,
                    )
                    result_emoji = "✅" if result == "WIN" else "❌"
                    sl_price  = trade.get("stop_loss", 0) or 0
                    avg_entry = trade.get("avg_entry_price", 0) or 0
                    await self._notify(
                        f"🛑 *Stop-loss hit!*\n"
                        f"Pair: `{trade['symbol']}` {trade['direction'].upper()}\n"
                        f"SL price: `{sl_price:.6g}`\n"
                        f"Avg entry: `{avg_entry:.6g}`\n"
                        f"TPs hit before SL: {highest_tp}\n"
                        f"Result: {result_emoji} *{result}*"
                    )
                else:
                    log.warning(
                        "on_ws_order: SL hit event for %s but no active trade found in DB",
                        symbol,
                    )

    # ── fill-size sync (watchdog fallback) ────────────────────────────────────

    async def sync_fills(self):
        """
        REST polling fallback. Runs every 30s from the watchdog.
        Catches fills the WebSocket may have missed.

        FIX #4: also checks whether any TP orders filled since the last poll
        and calls on_tp_filled so highest_tp_hit stays current.
        """
        trades = await self._db.get_active_trades()
        for trade in trades:
            symbol      = trade["symbol"]
            prev_filled = trade.get("filled_size", 0) or 0.0

            pos       = self._bybit.fetch_position(symbol)
            filled    = float(pos.get("size", 0))    if pos else 0.0
            avg_price = float(pos.get("avgPrice", 0)) if pos else 0.0

            if filled <= 0 and prev_filled > 0:
                await self._db.close_trade(trade["id"], "closed")
                log.info("Sync: trade %s closed externally", symbol)
                await self._notify(
                    f"⚠️ *Trade closed externally*\n"
                    f"Pair: `{symbol}` {trade['direction'].upper()}\n"
                    f"Detected by sync — position gone from Bybit"
                )
                continue

            if filled <= 0:
                continue

            if abs(filled - prev_filled) > 0.0001:
                log.info(
                    "Sync fallback fill %s: %.4f → %.4f (WS may have missed this)",
                    symbol, prev_filled, filled,
                )
                await self._db.update_trade(
                    trade["id"], filled_size=filled, avg_entry_price=avg_price,
                )
                if prev_filled == 0.0:
                    sl_price = trade.get("stop_loss", 0)
                    if sl_price and sl_price > 0:
                        self._bybit.move_stop_loss(symbol, sl_price)
                fresh_trade = await self._db.get_trade_by_symbol(symbol)
                if fresh_trade:
                    await self._refresh_tp_orders(fresh_trade, filled)

            # ── FIX #4: detect missed TP fills via order status ───────────────
            # Check each open TP order in the DB against Bybit's current order status.
            db_open_orders = await self._db.get_open_orders_for_trade(trade["id"])
            for order in db_open_orders:
                if not order["order_type"].startswith("tp"):
                    continue
                oid = order["bybit_order_id"]
                # Skip dry-run fake IDs
                if oid.startswith("DRY-"):
                    continue
                try:
                    resp = self._bybit._session.get_order_history(
                        category="linear",
                        symbol=symbol,
                        orderId=oid,
                        limit=1,
                    )
                    order_list = resp.get("result", {}).get("list", [])
                    if not order_list:
                        continue
                    bybit_status = order_list[0].get("orderStatus", "")
                    if bybit_status == "Filled":
                        log.info(
                            "Sync: detected missed TP fill for %s order %s",
                            symbol, oid,
                        )
                        await self._db.mark_order_status(oid, "filled")
                        try:
                            tp_num = int(order["order_type"][2:])
                        except ValueError:
                            tp_num = 1
                        await self.on_tp_filled(symbol, tp_num)
                        # Refresh TP orders with updated position size
                        fresh_trade = await self._db.get_trade_by_symbol(symbol)
                        if fresh_trade and pos:
                            remaining = float(pos.get("size", 0))
                            if remaining > 0:
                                await self._refresh_tp_orders(fresh_trade, remaining)
                except Exception as exc:
                    log.debug("Sync TP check error for %s order %s: %s", symbol, oid, exc)

    # ── close all ─────────────────────────────────────────────────────────────

    async def _handle_close_all(self, _msg: CloseAll):
        log.warning("CLOSE ALL triggered from Telegram")
        trades = await self._db.get_active_trades()
        closed_symbols = []
        for trade in trades:
            sym = trade["symbol"]
            self._bybit.cancel_orders_for_symbol(sym)
            pos = self._bybit.fetch_position(sym)
            if pos:
                size = float(pos.get("size", 0))
                # FIX #2: use trade direction as authoritative source for close side
                close_side = _opposite_side(trade["direction"])
                self._bybit.close_position(sym, size, close_side)
            await self._db.close_trade(trade["id"], "closed")
            closed_symbols.append(sym)
        log.info("Close-all complete: %d trades closed", len(trades))
        symbols_str = ", ".join(f"`{s}`" for s in closed_symbols) if closed_symbols else "none"
        await self._notify(
            f"🔒 *Close-all executed*\n"
            f"Trades closed: {len(closed_symbols)}\n"
            f"Symbols: {symbols_str}"
        )

    # ── close symbol ──────────────────────────────────────────────────────────

    async def _handle_close_symbol(self, msg: CloseSymbol):
        trade = await self._db.get_trade_by_symbol(msg.symbol)
        if not trade:
            log.info("No active trade for %s – close_symbol ignored", msg.symbol)
            return
        self._bybit.cancel_orders_for_symbol(msg.symbol)
        pos = self._bybit.fetch_position(msg.symbol)
        upnl = 0.0
        if pos:
            size       = float(pos.get("size", 0))
            upnl       = float(pos.get("unrealisedPnl", 0))
            # FIX #2: use trade direction as authoritative source for close side
            close_side = _opposite_side(trade["direction"])
            self._bybit.close_position(msg.symbol, size, close_side)
        close_pnl = upnl if upnl != 0.0 else None
        await self._db.close_trade(trade["id"], "closed", realised_pnl=close_pnl)
        log.info("Closed trade for %s", msg.symbol)
        pnl_emoji = "✅" if upnl >= 0 else "❌"
        await self._notify(
            f"🔒 *Trade closed*\n"
            f"Pair: `{msg.symbol}` {trade['direction'].upper()}\n"
            f"PnL at close: {pnl_emoji} `{upnl:+.2f} USDT`"
        )

    # ── cancel remaining entries ──────────────────────────────────────────────

    async def _handle_cancel_entries(self, msg: CancelRemainingEntries):
        trade = await self._db.get_trade_by_symbol(msg.symbol)
        if not trade:
            log.info("No active trade for %s – cancel_entries ignored", msg.symbol)
            return
        open_orders = await self._db.get_open_orders_for_trade(trade["id"])
        for order in open_orders:
            if order["order_type"] == "entry":
                ok = self._bybit.cancel_order(msg.symbol, order["bybit_order_id"])
                if ok:
                    await self._db.mark_order_status(order["bybit_order_id"], "cancelled")
        await self._db.update_trade(trade["id"], entries_cancelled=1)
        log.info("Cancelled remaining entry orders for %s", msg.symbol)

    # ── move SL to break-even ─────────────────────────────────────────────────

    async def _handle_move_sl_be(self, msg: MoveSLBreakEven):
        symbol = msg.symbol or None
        trades = (
            [await self._db.get_trade_by_symbol(symbol)]
            if symbol
            else await self._db.get_active_trades()
        )
        for trade in trades:
            if not trade:
                continue
            # Guard: can't break-even with no fill yet
            if (trade.get("filled_size", 0) or 0) <= 0:
                log.info(
                    "move_sl_be: %s has no fill yet — skipping break-even",
                    trade["symbol"],
                )
                continue
            pos = self._bybit.fetch_position(trade["symbol"])
            if not pos:
                log.info("No position for %s – cannot move SL to BE", trade["symbol"])
                continue
            avg_entry = float(pos.get("avgPrice", 0) or trade.get("avg_entry_price", 0))
            if avg_entry <= 0:
                avg_entry = (trade["entry_low"] + trade["entry_high"]) / 2

            ok = self._bybit.move_stop_loss(trade["symbol"], avg_entry)
            if ok:
                await self._db.update_trade(
                    trade["id"], break_even_activated=1, stop_loss=avg_entry
                )
                await self._handle_cancel_entries(
                    CancelRemainingEntries(
                        raw_text="", message_type=MessageType.CANCEL_REMAINING_ENTRIES,
                        symbol=trade["symbol"],
                    )
                )
                await self._db.update_trade_state(trade["id"], "break_even")
                log.info("SL moved to break-even %.5f for %s", avg_entry, trade["symbol"])
                upnl = float(pos.get("unrealisedPnl", 0))
                await self._notify(
                    f"🔄 *Break-even activated*\n"
                    f"Pair: `{trade['symbol']}` {trade['direction'].upper()}\n"
                    f"SL moved → `{avg_entry:.6g}` (entry price)\n"
                    f"Remaining entries cancelled\n"
                    f"PnL: `{upnl:+.2f} USDT`"
                )

    # ── move SL to price ──────────────────────────────────────────────────────

    async def _handle_move_sl_price(self, msg: MoveSLPrice):
        symbol = msg.symbol or None
        trades = (
            [await self._db.get_trade_by_symbol(symbol)]
            if symbol
            else await self._db.get_active_trades()
        )
        for trade in trades:
            if not trade:
                continue
            ok = self._bybit.move_stop_loss(trade["symbol"], msg.price)
            if ok:
                old_sl = trade.get("stop_loss", 0) or 0
                await self._db.update_trade(trade["id"], stop_loss=msg.price)
                log.info("SL updated to %.5f for %s", msg.price, trade["symbol"])
                await self._notify(
                    f"🛑 *SL moved*\n"
                    f"Pair: `{trade['symbol']}` {trade['direction'].upper()}\n"
                    f"Old SL: `{old_sl:.6g}` → New SL: `{msg.price:.6g}`"
                )

    # ── update targets ────────────────────────────────────────────────────────

    async def _handle_update_targets(self, msg: UpdateTargets):
        trade = await self._db.get_trade_by_symbol(msg.symbol)
        if not trade:
            log.info("No active trade for %s – update_targets ignored", msg.symbol)
            return
        if not msg.targets:
            log.info("No targets parsed for %s – ignored", msg.symbol)
            return

        open_orders = await self._db.get_open_orders_for_trade(trade["id"])
        for order in open_orders:
            if order["order_type"].startswith("tp"):
                self._bybit.cancel_order(msg.symbol, order["bybit_order_id"])
                await self._db.mark_order_status(order["bybit_order_id"], "cancelled")

        pos       = self._bybit.fetch_position(msg.symbol)
        total_qty = float(pos.get("size", 0)) if pos else trade.get("filled_size", 0)
        if total_qty <= 0:
            log.warning("Cannot place TP orders – unknown qty for %s", msg.symbol)
            return

        await self._db.update_trade(trade["id"], targets=msg.targets, highest_tp_hit=0)
        updated_trade = await self._db.get_trade_by_symbol(msg.symbol)
        await self._refresh_tp_orders(updated_trade, total_qty)
        log.info("Targets updated for %s: %s", msg.symbol, msg.targets)

    # ── add entries ───────────────────────────────────────────────────────────

    async def _handle_add_entries(self, msg: AddEntries):
        trade = await self._db.get_trade_by_symbol(msg.symbol)
        if not trade:
            log.info("No active trade for %s – add_entries ignored", msg.symbol)
            return
        balance   = self._bybit.fetch_wallet_balance()
        leverage  = trade.get("leverage", config.default_leverage)
        entry_mid = (msg.entry_low + msg.entry_high) / 2
        qty = _calc_qty(balance, config.risk_per_trade / 2, entry_mid,
                        trade["stop_loss"], leverage)
        if qty <= 0:
            return
        side   = _entry_side(trade["direction"])
        prices = [msg.entry_low, msg.entry_high] if msg.entry_low != msg.entry_high else [msg.entry_low]
        half_qty = _floor3(qty / len(prices))
        for price in prices:
            order_id = self._bybit.place_limit_order(msg.symbol, side, half_qty, price,
                                                     order_type_label="add_entry")
            if order_id:
                await self._db.save_order(trade["id"], order_id, msg.symbol,
                                          "entry", side, price, half_qty)
        log.info("Added entries for %s at %.5f-%.5f", msg.symbol, msg.entry_low, msg.entry_high)

    # ── market entry ──────────────────────────────────────────────────────────

    async def _handle_market_entry(self, msg: MarketEntry):
        trade = await self._db.get_trade_by_symbol(msg.symbol) if msg.symbol else None
        if not trade:
            log.info("market_entry: no active trade for %s – ignored", msg.symbol)
            return
        balance   = self._bybit.fetch_wallet_balance()
        leverage  = trade.get("leverage", config.default_leverage)
        pos       = self._bybit.fetch_position(msg.symbol)
        current_size = float(pos.get("size", 0)) if pos else 0.0
        entry_mid = (trade["entry_low"] + trade["entry_high"]) / 2
        qty = _calc_qty(balance, config.risk_per_trade, entry_mid, trade["stop_loss"], leverage)
        remaining_qty = max(0, qty - current_size)
        if remaining_qty <= 0:
            log.info("market_entry: position already full for %s", msg.symbol)
            return
        direction = msg.direction.value if msg.direction else trade["direction"]
        side = _entry_side(direction)
        await self._handle_cancel_entries(
            CancelRemainingEntries(
                raw_text="", message_type=MessageType.CANCEL_REMAINING_ENTRIES,
                symbol=msg.symbol,
            )
        )
        order_id = self._bybit.place_market_order(msg.symbol, side, remaining_qty)
        if order_id:
            await self._db.save_order(trade["id"], order_id, msg.symbol,
                                      "entry", side, 0, remaining_qty)
        log.info("Market entry executed for %s", msg.symbol)

    # ── partial close ─────────────────────────────────────────────────────────

    async def _handle_partial_close(self, msg: PartialClose):
        symbol = msg.symbol if msg.symbol else None
        if symbol:
            trade = await self._db.get_trade_by_symbol(symbol)
            if trade:
                await self._partial_close_trade(trade, msg.percent)
            else:
                log.info("partial_close: no active trade for %s", symbol)
        else:
            for t in await self._db.get_active_trades():
                await self._partial_close_trade(t, msg.percent)

    async def _partial_close_trade(self, trade: dict, percent: float):
        symbol = trade["symbol"]
        pos    = self._bybit.fetch_position(symbol)
        if not pos:
            log.info("partial_close: no position for %s", symbol)
            return
        total_size = float(pos.get("size", 0))
        close_qty  = _floor3(total_size * (percent / 100))
        if close_qty <= 0:
            return
        # FIX #2: use trade direction as authoritative source for close side
        close_side = _opposite_side(trade["direction"])
        order_id   = self._bybit.place_market_order(symbol, close_side, close_qty, reduce_only=True)
        if order_id:
            await self._db.save_order(trade["id"], order_id, symbol,
                                      "close", close_side, 0, close_qty)
        remaining = total_size - close_qty
        log.info("Partial close %.0f%% for %s qty=%.4f", percent, symbol, close_qty)
        await self._notify(
            f"📤 *Partial close*\n"
            f"Pair: `{symbol}` {trade['direction'].upper()}\n"
            f"Closed: {percent:.0f}% ({close_qty:.4f})\n"
            f"Remaining: {remaining:.4f}"
        )

    # ── cancel signal ─────────────────────────────────────────────────────────

    async def _handle_cancel_signal(self, msg: CancelSignal):
        trade = await self._db.get_trade_by_symbol(msg.symbol) if msg.symbol else None
        if not trade:
            log.info("cancel_signal: no active trade for %s", msg.symbol)
            return
        pos = self._bybit.fetch_position(msg.symbol)
        if pos is not None and float(pos.get("size", 0)) > 0:
            log.info("cancel_signal: %s already has a live position – not cancelling", msg.symbol)
            return
        self._bybit.cancel_orders_for_symbol(msg.symbol)
        await self._db.update_trade_state(trade["id"], "cancelled")
        log.info("Signal cancelled for %s", msg.symbol)
        await self._notify(
            f"🚫 *Signal cancelled*\n"
            f"Pair: `{msg.symbol}` {trade['direction'].upper()}\n"
            f"All pending orders removed"
        )
