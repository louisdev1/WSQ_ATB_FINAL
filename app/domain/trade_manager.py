"""
trade_manager.py – Central domain layer.

Entry ladder:  65% @ entry_high / 25% @ entry_mid / 10% @ entry_low  (LONG)
               65% @ entry_low  / 25% @ entry_mid / 10% @ entry_high (SHORT)

Blowthrough cancel:
  LONG:  if live price ≤ entry_mid while entries pending → cancel all entries
  SHORT: if live price ≥ entry_mid while entries pending → cancel all entries
  Rationale: price blowing through the zone = 41% WR vs 88% WR at edge/mid.

TP ratchet:   TP1 → cancel entries, SL to avg entry
              TP2+ → SL to previous TP price
"""

import logging
import math
from typing import Optional, List, Tuple

from app.config import config
from app.exchange.bybit_client import BybitClient
from app.storage.database import Database
from app.parsing.models import (
    ParsedMessage, MessageType, Direction,
    NewSignal, CloseAll, CloseSymbol, CancelRemainingEntries,
    MoveSLBreakEven, MoveSLPrice, UpdateTargets, AddEntries,
    MarketEntry, PartialClose, CancelSignal,
)

log = logging.getLogger(__name__)


# ── entry ladder ──────────────────────────────────────────────────────────────

def _is_long(direction: str) -> bool:
    return direction.lower() in ("long", "buy")


def _calc_ladder(entry_low: float, entry_high: float, direction: str) -> List[Tuple]:
    """
    Direction-aware ladder.
    LONG:  65% at entry_high (easiest fill) → 25% mid → 10% entry_low
    SHORT: 65% at entry_low  (easiest fill) → 25% mid → 10% entry_high
    Single-price signals: 100% at that price.
    """
    if entry_low <= 0 or entry_high <= entry_low:
        price = entry_high if entry_high > 0 else entry_low
        return [(price, 1.0)]

    midpoint = (entry_low + entry_high) / 2

    if _is_long(direction):
        return [(entry_high, 0.65), (midpoint, 0.25), (entry_low, 0.10)]
    else:
        return [(entry_low, 0.65), (midpoint, 0.25), (entry_high, 0.10)]


def _opposite_side(direction: str) -> str:
    return "Sell" if _is_long(direction) else "Buy"


def _entry_side(direction: str) -> str:
    return "Buy" if _is_long(direction) else "Sell"


def _calc_qty(balance: float, risk_fraction: float, entry_price: float,
              stop_loss: float, leverage: int) -> float:
    if entry_price <= 0 or stop_loss <= 0 or entry_price == stop_loss:
        return 0.0
    risk_amount = balance * risk_fraction
    distance    = abs(entry_price - stop_loss)
    qty = risk_amount / distance
    max_qty_by_balance = (balance * leverage) / entry_price
    qty = min(qty, max_qty_by_balance)
    return math.floor(qty * 1000) / 1000


def _floor3(v: float) -> float:
    return math.floor(v * 1000) / 1000


# ── TP distribution table ─────────────────────────────────────────────────────

_TP_DIST: dict = {
    1:  [100],
    2:  [70, 30],
    3:  [50, 30, 20],
    4:  [40, 30, 20, 10],
    5:  [35, 25, 20, 12, 8],
    6:  [30, 25, 18, 12, 9, 6],
    7:  [28, 22, 16, 12, 9, 7, 6],
    8:  [25, 20, 15, 12, 10, 8, 6, 4],
    9:  [23, 18, 14, 12, 10, 8, 6, 5, 4],
    10: [20, 17, 14, 12, 10, 8, 6, 5, 4, 4],
    11: [19, 16, 13, 11, 10, 8, 7, 6, 5, 3, 2],
    12: [18, 15, 12, 11, 10, 8, 7, 6, 5, 4, 2, 2],
    13: [17, 14, 12, 11,  9, 8, 7, 6, 5, 4, 3, 2, 2],
    14: [16, 14, 11, 10,  9, 8, 7, 6, 5, 4, 3, 3, 2, 2],
    15: [15, 13, 11, 10,  9, 8, 7, 6, 5, 4, 3, 3, 2, 2, 2],
}


def _tp_fractions(n: int) -> list:
    if n in _TP_DIST:
        pcts = _TP_DIST[n]
    else:
        pcts = [100 / n] * n
    total = sum(pcts)
    return [p / total for p in pcts]


class TradeManager:
    def __init__(self, db: Database, bybit: BybitClient):
        self._db    = db
        self._bybit = bybit

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

        leverage = min(sig.leverage_max, config.max_leverage)
        self._bybit.set_leverage(sig.symbol, leverage)

        balance = self._bybit.fetch_wallet_balance()
        if balance <= 0 and not config.dry_run:
            log.error("Cannot determine balance – skipping %s", sig.symbol)
            return

        entry_ref = sig.entry_high if sig.entry_high > 0 else sig.entry_low
        qty = _calc_qty(balance, config.risk_per_trade, entry_ref, sig.stop_loss, leverage)

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
        # Direction-aware ladder: LONG=65% at entry_high, SHORT=65% at entry_low
        ladder = _calc_ladder(sig.entry_low, sig.entry_high, sig.direction.value)

        for price, fraction in ladder:
            price     = round(price, 8)
            order_qty = _floor3(qty * fraction)
            if order_qty <= 0 or price <= 0:
                continue
            order_id = self._bybit.place_limit_order(
                sig.symbol, side, order_qty, price, order_type_label="entry"
            )
            if order_id:
                await self._db.save_order(
                    trade_id, order_id, sig.symbol, "entry", side, price, order_qty
                )

        self._bybit.move_stop_loss(sig.symbol, sig.stop_loss)

        await self._db.update_trade_state(trade_id, "active")
        log.info(
            "Trade opened: %s %s | qty=%.4f | ladder=%s | sl=%.5f",
            sig.symbol, sig.direction.value, qty,
            " / ".join(f"{p:.5f}({f*100:.0f}%)" for p, f in ladder),
            sig.stop_loss,
        )

    # ── TP order management ───────────────────────────────────────────────────

    async def _refresh_tp_orders(self, trade: dict, filled_qty: float):
        trade_id       = trade["id"]
        symbol         = trade["symbol"]
        direction      = trade["direction"]
        targets        = trade.get("targets", [])
        highest_tp_hit = trade.get("highest_tp_hit", 0) or 0

        if not targets or filled_qty <= 0:
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

        fracs      = _tp_fractions(len(remaining_targets))
        for i, tp_price in enumerate(remaining_targets):
            tp_num    = highest_tp_hit + i + 1
            qty_for_tp = _floor3(filled_qty * fracs[i])
            if tp_price <= 0 or qty_for_tp <= 0:
                continue
            order_id = self._bybit.place_take_profit_order(
                symbol, close_side, qty_for_tp, tp_price
            )
            if order_id:
                await self._db.save_order(
                    trade_id, order_id, symbol, f"tp{tp_num}",
                    close_side, tp_price, qty_for_tp
                )

        log.info(
            "TP orders refreshed for %s | filled=%.4f | remaining=%d",
            symbol, filled_qty, len(remaining_targets),
        )

    # ── TP ratchet ────────────────────────────────────────────────────────────

    async def on_tp_filled(self, symbol: str, tp_num: int):
        trade = await self._db.get_trade_by_symbol(symbol)
        if not trade:
            return

        targets   = trade.get("targets", [])
        avg_entry = trade.get("avg_entry_price", 0) or 0.0

        await self._db.update_trade(trade["id"], highest_tp_hit=tp_num)

        if tp_num == 1:
            log.info("Ratchet TP1 %s → cancel entries, SL→entry %.5f", symbol, avg_entry)
            await self._handle_cancel_entries(
                CancelRemainingEntries(
                    raw_text="", message_type=MessageType.CANCEL_REMAINING_ENTRIES,
                    symbol=symbol,
                )
            )
            if avg_entry > 0:
                ok = self._bybit.move_stop_loss(symbol, avg_entry)
                if ok:
                    await self._db.update_trade(trade["id"], stop_loss=avg_entry)
        else:
            prev_tp_price = targets[tp_num - 2] if len(targets) >= tp_num - 1 else None
            if prev_tp_price and prev_tp_price > 0:
                ok = self._bybit.move_stop_loss(symbol, prev_tp_price)
                if ok:
                    await self._db.update_trade(trade["id"], stop_loss=prev_tp_price)
                    log.info("Ratchet TP%d %s → SL→TP%d %.5f",
                             tp_num, symbol, tp_num - 1, prev_tp_price)

    # ── blowthrough cancel ────────────────────────────────────────────────────

    async def check_blowthrough(self):
        """
        Called every 30s from the watchdog.

        If price has moved to or through entry_mid while entry orders are still
        pending (not yet filled), cancel all remaining entry orders.

        LONG:  blowthrough = live price ≤ entry_mid
               (price dropped from above entry_high all the way to mid = bearish momentum)
        SHORT: blowthrough = live price ≥ entry_mid
               (price rallied from below entry_low all the way to mid = bullish momentum)

        This removes trades where all 3 ladder levels would fill (41% WR)
        and keeps only trades where price barely touched the zone then bounced
        (88-90% WR).
        """
        trades = await self._db.get_active_trades()
        for trade in trades:
            symbol    = trade["symbol"]
            direction = trade["direction"]
            entry_mid = (trade.get("entry_low", 0) + trade.get("entry_high", 0)) / 2

            # Only check trades that still have unfilled entry orders
            if trade.get("entries_cancelled"):
                continue
            if trade.get("filled_size", 0) and trade["filled_size"] > 0:
                # Already filled — blowthrough cancel doesn't apply
                continue

            open_orders = await self._db.get_open_orders_for_trade(trade["id"])
            pending_entries = [o for o in open_orders if o["order_type"] == "entry"]
            if not pending_entries:
                continue

            # Fetch live price
            ticker = self._bybit.fetch_ticker(symbol)
            if not ticker:
                continue
            live_price = ticker

            blowthrough = False
            if _is_long(direction) and live_price <= entry_mid:
                blowthrough = True
            elif not _is_long(direction) and live_price >= entry_mid:
                blowthrough = True

            if blowthrough:
                log.info(
                    "BLOWTHROUGH CANCEL %s %s: live=%.6g mid=%.6g → cancelling entries",
                    symbol, direction, live_price, entry_mid,
                )
                for order in pending_entries:
                    ok = self._bybit.cancel_order(symbol, order["bybit_order_id"])
                    if ok:
                        await self._db.mark_order_status(order["bybit_order_id"], "cancelled")
                await self._db.update_trade(trade["id"], entries_cancelled=1)
                await self._db.update_trade_state(trade["id"], "cancelled")
                log.info("Blowthrough cancel complete for %s — trade cancelled", symbol)

    # ── WebSocket execution handler ───────────────────────────────────────────

    async def on_ws_execution(self, msg: dict):
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
            log.info("WS execution: %s %s qty=%.4f price=%.5f",
                     symbol, order_type, exec_qty, avg_price)

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
                        log.info("SL enforced on first fill %s → %.5f (ok=%s)",
                                 symbol, sl_price, ok)

                fresh_trade = await self._db.get_trade_by_symbol(symbol)
                if fresh_trade:
                    await self._refresh_tp_orders(fresh_trade, filled)

            elif order_type.startswith("tp"):
                try:
                    tp_num = int(order_type[2:])
                except ValueError:
                    tp_num = 1

                await self.on_tp_filled(symbol, tp_num)

                pos       = self._bybit.fetch_position(symbol)
                remaining = float(pos.get("size", 0)) if pos else 0.0
                if remaining <= 0:
                    await self._db.update_trade_state(trade["id"], "closed")
                    log.info("All TPs filled for %s – trade closed", symbol)
                else:
                    fresh_trade = await self._db.get_trade_by_symbol(symbol)
                    if fresh_trade:
                        await self._refresh_tp_orders(fresh_trade, remaining)

    # ── WebSocket order status handler ────────────────────────────────────────

    async def on_ws_order(self, msg: dict):
        data = msg.get("data", [])
        for item in data:
            order_id        = item.get("orderId", "")
            order_status    = item.get("orderStatus", "")
            stop_order_type = item.get("stopOrderType", "")

            if order_status == "Filled" and stop_order_type == "StopLoss":
                order = await self._db.get_order_by_bybit_id(order_id)
                if order:
                    trade = await self._db.get_trade_by_id(order["trade_id"])
                    if trade:
                        await self._db.update_trade_state(trade["id"], "sl_hit")
                        log.warning("SL hit for %s – trade marked sl_hit", trade["symbol"])

    # ── startup sync ──────────────────────────────────────────────────────────

    async def startup_position_sync(self):
        """
        Called once at bot startup.
        Checks all active DB trades against live Bybit positions and
        reconciles any that filled while the bot was offline.
        """
        trades = await self._db.get_active_trades()
        if not trades:
            log.info("Startup sync: no active trades in DB")
            return

        log.info("Startup sync: checking %d active trade(s) against Bybit…", len(trades))
        for trade in trades:
            symbol      = trade["symbol"]
            prev_filled = trade.get("filled_size", 0) or 0.0

            pos       = self._bybit.fetch_position(symbol)
            filled    = float(pos.get("size", 0))    if pos else 0.0
            avg_price = float(pos.get("avgPrice", 0)) if pos else 0.0

            if filled <= 0 and prev_filled > 0:
                log.info("Startup sync: %s position gone – marking closed", symbol)
                await self._db.update_trade_state(trade["id"], "closed")
                continue

            if filled > 0 and abs(filled - prev_filled) > 0.0001:
                log.info(
                    "Startup sync: %s fill updated %.4f → %.4f (avg=%.5f)",
                    symbol, prev_filled, filled, avg_price,
                )
                await self._db.update_trade(
                    trade["id"], filled_size=filled, avg_entry_price=avg_price,
                )
                if prev_filled == 0.0:
                    sl_price = trade.get("stop_loss", 0)
                    if sl_price and sl_price > 0:
                        self._bybit.move_stop_loss(symbol, sl_price)
                fresh = await self._db.get_trade_by_symbol(symbol)
                if fresh:
                    await self._refresh_tp_orders(fresh, filled)
            else:
                log.info("Startup sync: %s OK (filled=%.4f)", symbol, filled)

    # ── fill-size sync (watchdog fallback) ────────────────────────────────────

    async def sync_fills(self):
        trades = await self._db.get_active_trades()
        for trade in trades:
            symbol      = trade["symbol"]
            prev_filled = trade.get("filled_size", 0) or 0.0

            pos       = self._bybit.fetch_position(symbol)
            filled    = float(pos.get("size", 0))    if pos else 0.0
            avg_price = float(pos.get("avgPrice", 0)) if pos else 0.0

            if filled <= 0 and prev_filled > 0:
                await self._db.update_trade_state(trade["id"], "closed")
                log.info("Sync: trade %s closed externally", symbol)
                continue

            if filled <= 0:
                continue

            if abs(filled - prev_filled) > 0.0001:
                log.info("Sync fallback fill %s: %.4f → %.4f", symbol, prev_filled, filled)
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

    # ── close all ─────────────────────────────────────────────────────────────

    async def _handle_close_all(self, _msg: CloseAll):
        log.warning("CLOSE ALL triggered from Telegram")
        trades = await self._db.get_active_trades()
        for trade in trades:
            sym = trade["symbol"]
            self._bybit.cancel_orders_for_symbol(sym)
            pos = self._bybit.fetch_position(sym)
            if pos:
                size       = float(pos.get("size", 0))
                close_side = "Sell" if pos.get("side", "Buy") == "Buy" else "Buy"
                self._bybit.close_position(sym, size, close_side)
            await self._db.update_trade_state(trade["id"], "closed")
        log.info("Close-all complete: %d trades closed", len(trades))

    # ── close symbol ──────────────────────────────────────────────────────────

    async def _handle_close_symbol(self, msg: CloseSymbol):
        trade = await self._db.get_trade_by_symbol(msg.symbol)
        if not trade:
            log.info("No active trade for %s – close_symbol ignored", msg.symbol)
            return
        self._bybit.cancel_orders_for_symbol(msg.symbol)
        pos = self._bybit.fetch_position(msg.symbol)
        if pos:
            size       = float(pos.get("size", 0))
            close_side = "Sell" if pos.get("side", "Buy") == "Buy" else "Buy"
            self._bybit.close_position(msg.symbol, size, close_side)
        await self._db.update_trade_state(trade["id"], "closed")
        log.info("Closed trade for %s", msg.symbol)

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
            pos = self._bybit.fetch_position(trade["symbol"])
            if not pos:
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
                await self._db.update_trade(trade["id"], stop_loss=msg.price)
                log.info("SL updated to %.5f for %s", msg.price, trade["symbol"])

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
            return
        total_size = float(pos.get("size", 0))
        close_qty  = _floor3(total_size * (percent / 100))
        if close_qty <= 0:
            return
        close_side = "Sell" if pos.get("side", "Buy") == "Buy" else "Buy"
        order_id   = self._bybit.place_market_order(symbol, close_side, close_qty, reduce_only=True)
        if order_id:
            await self._db.save_order(trade["id"], order_id, symbol,
                                      "close", close_side, 0, close_qty)
        log.info("Partial close %.0f%% for %s qty=%.4f", percent, symbol, close_qty)

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
