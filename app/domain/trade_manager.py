"""
trade_manager.py – Central domain layer.

Entry:  Single limit order at zone edge (confirmed optimal from analysis)
          LONG:  100% at entry_HIGH
          SHORT: 100% at entry_LOW
        No ladder — 35% blowthrough cancel makes entry_mid unreachable.
        Blowthrough depth threshold: 35% into zone.

TP ratchet:
        TP1 → cancel remaining entries, SL → avg_entry (break-even)
        TP2+ → SL → previous TP price

Quality sizing:
        Each signal is scored 0-6 by signal_quality.py before placing orders.
        HIGH (≥5) → 1.5×  |  MED (3-4) → 1.0×  |  LOW (≤2) → 0.7×
        Confirmed on val set Nov 2024–May 2025: +85% vs flat sizing.

Filters wired in:
        evaluate_signal() from signal_filter.py is called before every trade.
        RSI<40 (1h) + BTC weekly + structural filters all enforced here.
"""

import logging
import math
from typing import Optional, List, Tuple

from app.config import config
from app.domain.signal_filter import evaluate_signal, _fetch_rsi, _binance_symbol, _fetch_btc_weekly_direction
from app.domain.signal_quality import compute_quality_score, quality_risk_multiplier, quality_tier, describe_score
from app.exchange.bybit_client import BybitClient
from app.storage.database import Database
from app.monitoring.alerter import send_notification
from app.parsing.models import (
    ParsedMessage, MessageType, Direction,
    NewSignal, CloseAll, CloseSymbol, CancelRemainingEntries,
    MoveSLBreakEven, MoveSLPrice, UpdateTargets, AddEntries,
    MarketEntry, PartialClose, CancelSignal,
)

log = logging.getLogger(__name__)

# Blowthrough threshold: if price moves this far into the entry zone,
# cancel all remaining entry orders. Confirmed 35% from backtest analysis.
BLOWTHROUGH_DEPTH = 0.35


# ── Helpers ───────────────────────────────────────────────────────────────────

def _is_long(direction: str) -> bool:
    return direction.lower() in ("long", "buy")


def _opposite_side(direction: str) -> str:
    return "Sell" if _is_long(direction) else "Buy"


def _entry_side(direction: str) -> str:
    return "Buy" if _is_long(direction) else "Sell"


def _calc_qty(balance: float, risk_fraction: float, entry_price: float,
              stop_loss: float, leverage: int) -> float:
    """
    Position size based on risk amount and SL distance.
    Capped at (balance × leverage) / entry_price to prevent over-leverage.
    Floored to 3 decimal places.
    """
    if entry_price <= 0 or stop_loss <= 0 or entry_price == stop_loss:
        return 0.0
    risk_amount = balance * risk_fraction
    distance    = abs(entry_price - stop_loss)
    qty         = risk_amount / distance
    max_qty     = (balance * leverage) / entry_price
    return math.floor(min(qty, max_qty) * 1000) / 1000


def _floor3(v: float) -> float:
    return math.floor(v * 1000) / 1000


def _effective_risk(signal: NewSignal) -> float:
    """
    Compute the effective risk fraction for this signal.
    Applies quality multiplier if QUALITY_SIZING_ENABLED=true.
    """
    base_risk = config.risk_per_trade

    if not getattr(config, "quality_sizing_enabled", True):
        return base_risk

    sig_dict = {
        "entry_low":  signal.entry_low,
        "entry_high": signal.entry_high,
        "stop_loss":  signal.stop_loss,
        "targets":    signal.targets,
        "side":       signal.direction.value if signal.direction else "LONG",
    }
    score      = compute_quality_score(sig_dict)
    multiplier = quality_risk_multiplier(score)
    effective  = base_risk * multiplier

    log.info(
        "Quality score %d/6 → %.1f× multiplier → %.1f%% effective risk "
        "(base=%.1f%%)",
        score, multiplier, effective * 100, base_risk * 100,
    )
    return effective


# ── TP distribution table (confirmed optimal from wsq_tp_optimizer.py) ───────

_TP_DIST: dict = {
    5:  [40, 20, 15, 15, 10],
    6:  [14, 10, 15, 19, 20, 22],
    7:  [14, 10, 13, 17, 16, 15, 15],
    8:  [14,  8, 11, 13, 13, 14, 15, 12],
    9:  [14,  8, 10, 11, 12, 12, 12, 11, 10],
    10: [10, 10, 10, 10, 10, 10, 10, 10, 10, 10],
}


def _tp_fractions(n: int) -> list:
    """
    Return TP allocation fractions for n remaining targets.
    Falls back to even split for unusual target counts.
    """
    if n in _TP_DIST:
        pcts = _TP_DIST[n]
    elif n <= 0:
        return []
    else:
        pcts = [round(100 / n)] * n
        pcts[-1] += 100 - sum(pcts)
    total = sum(pcts)
    return [p / total for p in pcts]


# ── TradeManager ──────────────────────────────────────────────────────────────

class TradeManager:
    def __init__(self, db: Database, bybit: BybitClient):
        self._db    = db
        self._bybit = bybit

    async def handle(self, msg: ParsedMessage):
        t = msg.message_type
        if   t == MessageType.NEW_SIGNAL:               await self._handle_new_signal(msg)
        elif t == MessageType.CLOSE_ALL:                await self._handle_close_all(msg)
        elif t == MessageType.CLOSE_SYMBOL:             await self._handle_close_symbol(msg)
        elif t == MessageType.CANCEL_REMAINING_ENTRIES: await self._handle_cancel_entries(msg)
        elif t == MessageType.MOVE_SL_BREAK_EVEN:       await self._handle_move_sl_be(msg)
        elif t == MessageType.MOVE_SL_PRICE:            await self._handle_move_sl_price(msg)
        elif t == MessageType.UPDATE_TARGETS:           await self._handle_update_targets(msg)
        elif t == MessageType.MARKET_ENTRY:             await self._handle_market_entry(msg)
        elif t == MessageType.PARTIAL_CLOSE:            await self._handle_partial_close(msg)
        elif t == MessageType.CANCEL_SIGNAL:            await self._handle_cancel_signal(msg)
        elif t == MessageType.ADD_ENTRIES:              await self._handle_add_entries(msg)
        else:                                           log.debug("Ignored: %s", t)

    # ── New signal ────────────────────────────────────────────────────────────

    async def _handle_new_signal(self, sig: NewSignal):
        if not sig.symbol or not sig.direction:
            log.warning("NewSignal missing symbol or direction – skipped")
            return

        # ── Fetch RSI and BTC weekly early (used for filter, notification, DB) ─
        b_sym   = _binance_symbol(sig.symbol)
        rsi_val = _fetch_rsi(b_sym, getattr(config, "filter_rsi_tf", "1h"))
        btc_dir = _fetch_btc_weekly_direction()

        # ── Compute quality score early (used for notification even on reject) ──
        sig_dict = {
            "entry_low":  sig.entry_low,
            "entry_high": sig.entry_high,
            "stop_loss":  sig.stop_loss,
            "targets":    sig.targets,
            "side":       sig.direction.value if sig.direction else "LONG",
        }
        q_score = compute_quality_score(sig_dict)

        # ── Step 1: Signal filter (RSI, BTC weekly, structural) ───────────────
        decision, reason = evaluate_signal(
            symbol     = sig.symbol,
            direction  = sig.direction.value,
            entry_high = sig.entry_high,
            entry_low  = sig.entry_low,
            stop_loss  = sig.stop_loss,
            targets    = sig.targets,
        )
        if decision != "TAKE":
            log.info("Signal REJECTED %s: %s", sig.symbol, reason)
            # ── Log rejection to DB for the `rejected` command ────────────────
            await self._db.save_rejected_signal({
                "symbol":        sig.symbol,
                "direction":     sig.direction.value if sig.direction else None,
                "reason":        reason,
                "entry_low":     sig.entry_low,
                "entry_high":    sig.entry_high,
                "stop_loss":     sig.stop_loss,
                "n_targets":     len(sig.targets),
                "rsi_value":     rsi_val,
                "btc_weekly":    btc_dir,
                "quality_score": q_score,
            })
            await send_notification(
                f"🚫 *Signal rejected:* `{sig.symbol}` {sig.direction.value.upper()}\n"
                f"Reason: _{reason}_\n"
                f"Quality: {q_score}/6 | RSI: {rsi_val or 'n/a'} | BTC wk: {btc_dir or 'n/a'}"
            )
            return

        # ── Step 2: Duplicate check ───────────────────────────────────────────
        if await self._db.get_trade_by_symbol(sig.symbol):
            log.info("Active trade already exists for %s – skipping", sig.symbol)
            return

        # ── Step 3: Quality score → effective risk ────────────────────────────
        effective_risk = _effective_risk(sig)
        q_mult = quality_risk_multiplier(q_score)
        q_tier = quality_tier(q_score)

        # ── Step 4: Set leverage and fetch balance ────────────────────────────
        leverage = min(sig.leverage_max, config.max_leverage)
        self._bybit.set_leverage(sig.symbol, leverage)

        balance = self._bybit.fetch_wallet_balance()
        if balance <= 0 and not config.dry_run:
            log.error("Cannot determine balance – skipping %s", sig.symbol)
            return

        # ── Step 5: Single limit order at zone edge ───────────────────────────
        # LONG:  100% at entry_HIGH (price drops to zone top)
        # SHORT: 100% at entry_LOW  (price rallies down into zone)
        fill_price = sig.entry_high if _is_long(sig.direction.value) else sig.entry_low
        qty        = _calc_qty(balance, effective_risk, fill_price, sig.stop_loss, leverage)

        if qty <= 0:
            log.warning("Calculated qty=0 for %s – skipping", sig.symbol)
            return

        # ── Step 6: Save to DB with quality metadata ──────────────────────────
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
        # Store quality + filter metadata on the trade row
        await self._db.update_trade(
            trade_id,
            quality_score=q_score,
            quality_multiplier=q_mult,
            tier=q_tier,
            size_mult=q_mult,
            filter_reason=reason,
            rsi_at_entry=rsi_val,
            btc_weekly_at_entry=btc_dir,
            rsi_at_signal=rsi_val,
        )

        # ── Step 7: Place single limit entry order ────────────────────────────
        side     = _entry_side(sig.direction.value)
        order_id = self._bybit.place_limit_order(
            sig.symbol, side, qty, round(fill_price, 8),
            order_type_label="entry",
        )
        if order_id:
            await self._db.save_order(
                trade_id, order_id, sig.symbol,
                "entry", side, fill_price, qty,
            )

        self._bybit.move_stop_loss(sig.symbol, sig.stop_loss)
        await self._db.update_trade_state(trade_id, "active")

        log.info(
            "Trade opened: %s %s | qty=%.4f | entry=%.6g | sl=%.6g | "
            "risk=%.1f%% (quality-adjusted) | filter=%s",
            sig.symbol, sig.direction.value,
            qty, fill_price, sig.stop_loss,
            effective_risk * 100, reason,
        )

        # ── Step 8: Send trade-open notification with sizing breakdown ────────
        desc = describe_score(sig_dict)
        notional = qty * fill_price
        await send_notification(
            f"📈 *Trade opened:* `{sig.symbol}` {sig.direction.value.upper()}\n\n"
            f"*Sizing breakdown:*\n"
            f"  Quality: `{q_score}/6` [{q_tier}] → `{q_mult:.1f}×` multiplier\n"
            f"  Base risk: `{config.risk_per_trade*100:.1f}%` → Effective: `{effective_risk*100:.1f}%`\n"
            f"  Qty: `{qty:.4f}` @ `{fill_price:.6g}` (≈`{notional:.1f}` USDT notional)\n"
            f"  Leverage: `{leverage}×` | Balance: `{balance:.2f}` USDT\n\n"
            f"*Filters passed:*\n"
            f"  RSI ({getattr(config, 'filter_rsi_tf', '1h')}): `{rsi_val:.1f}` {'✅' if rsi_val and rsi_val < getattr(config, 'filter_rsi_signal_max', 40) else '⚠️'}\n"
            f"  BTC weekly: `{btc_dir or 'n/a'}` {'✅' if btc_dir != 'bear' else '❌'}\n\n"
            f"*Signal geometry:*\n"
            f"```\n{desc}\n```\n\n"
            f"Entry: `{sig.entry_low:.6g}`–`{sig.entry_high:.6g}` | "
            f"SL: `{sig.stop_loss:.6g}` | TPs: {len(sig.targets)}"
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

        # Cancel any existing open TP orders
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

        fracs = _tp_fractions(len(remaining_targets))
        for i, tp_price in enumerate(remaining_targets):
            tp_num    = highest_tp_hit + i + 1
            qty_for_tp = _floor3(filled_qty * fracs[i])
            if tp_price <= 0 or qty_for_tp <= 0:
                continue
            order_id = self._bybit.place_take_profit_order(
                symbol, close_side, qty_for_tp, tp_price,
            )
            if order_id:
                await self._db.save_order(
                    trade_id, order_id, symbol,
                    f"tp{tp_num}", close_side, tp_price, qty_for_tp,
                )

        log.info(
            "TP orders placed for %s | filled=%.4f | %d remaining targets",
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
            # TP1: cancel any unfilled entries, move SL to break-even
            log.info("Ratchet TP1 %s → cancel entries, SL → entry %.6g", symbol, avg_entry)
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
            # TP2+: ratchet SL to previous TP price
            prev_tp_price = targets[tp_num - 2] if len(targets) >= tp_num - 1 else None
            if prev_tp_price and prev_tp_price > 0:
                ok = self._bybit.move_stop_loss(symbol, prev_tp_price)
                if ok:
                    await self._db.update_trade(trade["id"], stop_loss=prev_tp_price)
                    log.info(
                        "Ratchet TP%d %s → SL → TP%d %.6g",
                        tp_num, symbol, tp_num - 1, prev_tp_price,
                    )

    # ── Blowthrough cancel ────────────────────────────────────────────────────

    async def check_blowthrough(self):
        """
        Called every 30s from the watchdog.

        Cancels remaining entry orders if price blows through 35% of the
        entry zone from the edge. This removes low-probability full-fill
        scenarios and keeps only trades where price barely touched the zone.

        LONG:  blowthrough price = entry_high - 35% × zone_width
        SHORT: blowthrough price = entry_low  + 35% × zone_width
        """
        if not getattr(config, "blowthrough_cancel", True):
            return

        trades = await self._db.get_active_trades()
        for trade in trades:
            symbol    = trade["symbol"]
            direction = trade["direction"]

            if trade.get("entries_cancelled"):
                continue
            if trade.get("filled_size", 0) and trade["filled_size"] > 0:
                continue

            open_orders = await self._db.get_open_orders_for_trade(trade["id"])
            if not any(o["order_type"] == "entry" for o in open_orders):
                continue

            e_high = trade.get("entry_high", 0)
            e_low  = trade.get("entry_low",  0)
            if e_high <= 0 or e_low <= 0:
                continue

            zone_width        = e_high - e_low
            blowthrough_price = (
                e_high - BLOWTHROUGH_DEPTH * zone_width if _is_long(direction)
                else e_low + BLOWTHROUGH_DEPTH * zone_width
            )

            ticker     = self._bybit.fetch_ticker(symbol)
            if not ticker:
                continue
            live_price = ticker

            blowthrough = (
                (_is_long(direction)  and live_price <= blowthrough_price) or
                (not _is_long(direction) and live_price >= blowthrough_price)
            )

            if blowthrough:
                log.info(
                    "BLOWTHROUGH CANCEL %s %s: live=%.6g threshold=%.6g → cancelling",
                    symbol, direction, live_price, blowthrough_price,
                )
                for order in open_orders:
                    if order["order_type"] == "entry":
                        ok = self._bybit.cancel_order(symbol, order["bybit_order_id"])
                        if ok:
                            await self._db.mark_order_status(order["bybit_order_id"], "cancelled")
                await self._db.update_trade(trade["id"], entries_cancelled=1)
                await self._db.update_trade_state(trade["id"], "cancelled")
                log.info("Blowthrough cancel complete for %s", symbol)

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
            log.info("WS execution: %s %s qty=%.4f price=%.6g",
                     symbol, order_type, exec_qty, avg_price)

            await self._db.mark_order_status(order_id, "filled")

            if order_type == "entry":
                pos         = self._bybit.fetch_position(symbol)
                filled      = float(pos.get("size", 0))     if pos else exec_qty
                pos_avg     = float(pos.get("avgPrice", 0)) if pos else avg_price
                prev_filled = trade.get("filled_size", 0) or 0.0

                await self._db.update_trade(
                    trade["id"], filled_size=filled, avg_entry_price=pos_avg,
                )

                if prev_filled == 0.0:
                    sl_price = trade.get("stop_loss", 0)
                    if sl_price and sl_price > 0:
                        ok = self._bybit.move_stop_loss(symbol, sl_price)
                        log.info("SL enforced on first fill %s → %.6g (ok=%s)",
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
                    # All TPs filled — estimate realised PnL from cumulative PnL field
                    cum_pnl = float(pos.get("cumRealisedPnl", 0)) if pos else None
                    await self._db.close_trade_with_pnl(trade["id"], "closed", cum_pnl)
                    log.info("All TPs filled for %s – trade closed (pnl=%.4f)", symbol, cum_pnl or 0)
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
                        # Estimate PnL: SL was hit, so loss ≈ risk amount
                        # Try to get from position cumRealisedPnl if still available
                        pos = self._bybit.fetch_position(trade["symbol"])
                        pnl = float(pos.get("cumRealisedPnl", 0)) if pos else None
                        await self._db.close_trade_with_pnl(trade["id"], "sl_hit", pnl)
                        log.warning("SL hit for %s – trade marked sl_hit (pnl=%.4f)", trade["symbol"], pnl or 0)

    # ── Startup sync ──────────────────────────────────────────────────────────

    async def startup_position_sync(self):
        trades = await self._db.get_active_trades()
        if not trades:
            log.info("Startup sync: no active trades in DB")
            return

        log.info("Startup sync: checking %d active trade(s)…", len(trades))
        for trade in trades:
            symbol      = trade["symbol"]
            prev_filled = trade.get("filled_size", 0) or 0.0

            pos       = self._bybit.fetch_position(symbol)
            filled    = float(pos.get("size", 0))     if pos else 0.0
            avg_price = float(pos.get("avgPrice", 0)) if pos else 0.0

            if filled <= 0 and prev_filled > 0:
                log.info("Startup sync: %s position gone – marking closed", symbol)
                await self._db.close_trade_with_pnl(trade["id"], "closed")
                continue

            if filled > 0 and abs(filled - prev_filled) > 0.0001:
                log.info("Startup sync: %s fill %.4f → %.4f (avg=%.6g)",
                         symbol, prev_filled, filled, avg_price)
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

    # ── Fill sync (watchdog fallback) ─────────────────────────────────────────

    async def sync_fills(self):
        trades = await self._db.get_active_trades()
        for trade in trades:
            symbol      = trade["symbol"]
            prev_filled = trade.get("filled_size", 0) or 0.0

            pos       = self._bybit.fetch_position(symbol)
            filled    = float(pos.get("size", 0))     if pos else 0.0
            avg_price = float(pos.get("avgPrice", 0)) if pos else 0.0

            if filled <= 0 and prev_filled > 0:
                await self._db.close_trade_with_pnl(trade["id"], "closed")
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

    # ── Close all ─────────────────────────────────────────────────────────────

    async def _handle_close_all(self, _msg: CloseAll):
        log.warning("CLOSE ALL triggered")
        trades = await self._db.get_active_trades()
        for trade in trades:
            sym = trade["symbol"]
            self._bybit.cancel_orders_for_symbol(sym)
            pos = self._bybit.fetch_position(sym)
            pnl = float(pos.get("unrealisedPnl", 0)) if pos else None
            if pos:
                size       = float(pos.get("size", 0))
                close_side = "Sell" if pos.get("side", "Buy") == "Buy" else "Buy"
                self._bybit.close_position(sym, size, close_side)
            await self._db.close_trade_with_pnl(trade["id"], "closed", pnl)
        log.info("Close-all complete: %d trades closed", len(trades))

    # ── Close symbol ──────────────────────────────────────────────────────────

    async def _handle_close_symbol(self, msg: CloseSymbol):
        trade = await self._db.get_trade_by_symbol(msg.symbol)
        if not trade:
            return
        self._bybit.cancel_orders_for_symbol(msg.symbol)
        pos = self._bybit.fetch_position(msg.symbol)
        pnl = float(pos.get("unrealisedPnl", 0)) if pos else None
        if pos:
            size       = float(pos.get("size", 0))
            close_side = "Sell" if pos.get("side", "Buy") == "Buy" else "Buy"
            self._bybit.close_position(msg.symbol, size, close_side)
        await self._db.close_trade_with_pnl(trade["id"], "closed", pnl)
        log.info("Closed trade for %s (pnl=%.4f)", msg.symbol, pnl or 0)

    # ── Cancel remaining entries ──────────────────────────────────────────────

    async def _handle_cancel_entries(self, msg: CancelRemainingEntries):
        trade = await self._db.get_trade_by_symbol(msg.symbol)
        if not trade:
            return
        open_orders = await self._db.get_open_orders_for_trade(trade["id"])
        for order in open_orders:
            if order["order_type"] == "entry":
                ok = self._bybit.cancel_order(msg.symbol, order["bybit_order_id"])
                if ok:
                    await self._db.mark_order_status(order["bybit_order_id"], "cancelled")
        await self._db.update_trade(trade["id"], entries_cancelled=1)

    # ── Move SL to break-even ─────────────────────────────────────────────────

    async def _handle_move_sl_be(self, msg: MoveSLBreakEven):
        symbol = msg.symbol or None
        trades = ([await self._db.get_trade_by_symbol(symbol)] if symbol
                  else await self._db.get_active_trades())
        for trade in trades:
            if not trade:
                continue
            pos       = self._bybit.fetch_position(trade["symbol"])
            avg_entry = float(pos.get("avgPrice", 0) if pos else
                              trade.get("avg_entry_price", 0))
            if avg_entry <= 0:
                avg_entry = (trade["entry_low"] + trade["entry_high"]) / 2
            ok = self._bybit.move_stop_loss(trade["symbol"], avg_entry)
            if ok:
                await self._db.update_trade(trade["id"],
                                            break_even_activated=1, stop_loss=avg_entry)
                await self._handle_cancel_entries(
                    CancelRemainingEntries(
                        raw_text="", message_type=MessageType.CANCEL_REMAINING_ENTRIES,
                        symbol=trade["symbol"],
                    )
                )
                await self._db.update_trade_state(trade["id"], "break_even")
                log.info("SL → break-even %.6g for %s", avg_entry, trade["symbol"])

    # ── Move SL to price ──────────────────────────────────────────────────────

    async def _handle_move_sl_price(self, msg: MoveSLPrice):
        symbol = msg.symbol or None
        trades = ([await self._db.get_trade_by_symbol(symbol)] if symbol
                  else await self._db.get_active_trades())
        for trade in trades:
            if not trade:
                continue
            ok = self._bybit.move_stop_loss(trade["symbol"], msg.price)
            if ok:
                await self._db.update_trade(trade["id"], stop_loss=msg.price)

    # ── Update targets ────────────────────────────────────────────────────────

    async def _handle_update_targets(self, msg: UpdateTargets):
        trade = await self._db.get_trade_by_symbol(msg.symbol)
        if not trade or not msg.targets:
            return

        open_orders = await self._db.get_open_orders_for_trade(trade["id"])
        for order in open_orders:
            if order["order_type"].startswith("tp"):
                self._bybit.cancel_order(msg.symbol, order["bybit_order_id"])
                await self._db.mark_order_status(order["bybit_order_id"], "cancelled")

        pos       = self._bybit.fetch_position(msg.symbol)
        total_qty = float(pos.get("size", 0)) if pos else trade.get("filled_size", 0)
        if total_qty <= 0:
            return

        await self._db.update_trade(trade["id"], targets=msg.targets, highest_tp_hit=0)
        updated = await self._db.get_trade_by_symbol(msg.symbol)
        await self._refresh_tp_orders(updated, total_qty)

    # ── Add entries ───────────────────────────────────────────────────────────

    async def _handle_add_entries(self, msg: AddEntries):
        trade = await self._db.get_trade_by_symbol(msg.symbol)
        if not trade:
            return
        balance  = self._bybit.fetch_wallet_balance()
        leverage = trade.get("leverage", config.default_leverage)
        entry_mid = (msg.entry_low + msg.entry_high) / 2
        qty = _calc_qty(balance, config.risk_per_trade / 2,
                        entry_mid, trade["stop_loss"], leverage)
        if qty <= 0:
            return
        side   = _entry_side(trade["direction"])
        prices = ([msg.entry_low, msg.entry_high]
                  if msg.entry_low != msg.entry_high else [msg.entry_low])
        half   = _floor3(qty / len(prices))
        for price in prices:
            order_id = self._bybit.place_limit_order(
                msg.symbol, side, half, price, order_type_label="add_entry")
            if order_id:
                await self._db.save_order(trade["id"], order_id, msg.symbol,
                                          "entry", side, price, half)

    # ── Market entry ──────────────────────────────────────────────────────────

    async def _handle_market_entry(self, msg: MarketEntry):
        trade = await self._db.get_trade_by_symbol(msg.symbol) if msg.symbol else None
        if not trade:
            return
        balance  = self._bybit.fetch_wallet_balance()
        leverage = trade.get("leverage", config.default_leverage)
        pos      = self._bybit.fetch_position(msg.symbol)
        current  = float(pos.get("size", 0)) if pos else 0.0
        mid      = (trade["entry_low"] + trade["entry_high"]) / 2
        qty      = _calc_qty(balance, config.risk_per_trade, mid, trade["stop_loss"], leverage)
        remaining = max(0, qty - current)
        if remaining <= 0:
            return
        direction = msg.direction.value if msg.direction else trade["direction"]
        side      = _entry_side(direction)
        await self._handle_cancel_entries(
            CancelRemainingEntries(raw_text="",
                                   message_type=MessageType.CANCEL_REMAINING_ENTRIES,
                                   symbol=msg.symbol))
        order_id = self._bybit.place_market_order(msg.symbol, side, remaining)
        if order_id:
            await self._db.save_order(trade["id"], order_id, msg.symbol,
                                      "entry", side, 0, remaining)

    # ── Partial close ─────────────────────────────────────────────────────────

    async def _handle_partial_close(self, msg: PartialClose):
        if msg.symbol:
            trade = await self._db.get_trade_by_symbol(msg.symbol)
            if trade:
                await self._partial_close_trade(trade, msg.percent)
        else:
            for t in await self._db.get_active_trades():
                await self._partial_close_trade(t, msg.percent)

    async def _partial_close_trade(self, trade: dict, percent: float):
        symbol = trade["symbol"]
        pos    = self._bybit.fetch_position(symbol)
        if not pos:
            return
        total    = float(pos.get("size", 0))
        close_qty = _floor3(total * (percent / 100))
        if close_qty <= 0:
            return
        close_side = "Sell" if pos.get("side", "Buy") == "Buy" else "Buy"
        order_id   = self._bybit.place_market_order(symbol, close_side,
                                                    close_qty, reduce_only=True)
        if order_id:
            await self._db.save_order(trade["id"], order_id, symbol,
                                      "close", close_side, 0, close_qty)

    # ── Cancel signal ─────────────────────────────────────────────────────────

    async def _handle_cancel_signal(self, msg: CancelSignal):
        trade = await self._db.get_trade_by_symbol(msg.symbol) if msg.symbol else None
        if not trade:
            return
        pos = self._bybit.fetch_position(msg.symbol)
        if pos is not None and float(pos.get("size", 0)) > 0:
            log.info("cancel_signal: %s has live position – not cancelling", msg.symbol)
            return
        self._bybit.cancel_orders_for_symbol(msg.symbol)
        await self._db.update_trade_state(trade["id"], "cancelled")
        log.info("Signal cancelled for %s", msg.symbol)
