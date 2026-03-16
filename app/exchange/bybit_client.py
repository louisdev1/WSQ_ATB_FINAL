"""
bybit_client.py – Clean Bybit API abstraction.

No strategy logic lives here. Only API calls.
Uses pybit v5 unified trading API.
"""

import logging
import math
from typing import Optional, List, Dict, Any

from pybit.unified_trading import HTTP

log = logging.getLogger(__name__)


class BybitClient:
    def __init__(self, api_key: str, api_secret: str, testnet: bool = False):
        self._session = HTTP(
            testnet=testnet,
            api_key=api_key,
            api_secret=api_secret,
        )
        self._dry_run = False  # Set by caller if needed
        self._on_ok = None    # Optional callback: report_bybit_ok
        self._on_fail = None  # Optional callback: report_bybit_fail
        log.info("BybitClient initialized (testnet=%s)", testnet)

    def set_health_callbacks(self, on_ok, on_fail):
        """Wire up watchdog health reporting. Called once from main."""
        self._on_ok = on_ok
        self._on_fail = on_fail

    def _ok(self):
        if self._on_ok:
            self._on_ok()

    def _fail(self, context: str, exc: Exception):
        log.error("%s: %s", context, exc)
        if self._on_fail:
            self._on_fail()

    # ── leverage ──────────────────────────────────────────────────────────────

    def set_leverage(self, symbol: str, leverage: int) -> bool:
        if self._dry_run:
            log.info("[DRY] set_leverage %s %sx", symbol, leverage)
            return True
        try:
            self._session.set_leverage(
                category="linear",
                symbol=symbol,
                buyLeverage=str(leverage),
                sellLeverage=str(leverage),
            )
            self._ok()
            return True
        except Exception as exc:
            # Bybit returns an error if leverage is already set to the same value — not a real failure
            msg = str(exc)
            if "leverage not modified" in msg.lower() or "110043" in msg:
                log.debug("set_leverage %s: already at %sx (ok)", symbol, leverage)
                return True
            self._fail(f"set_leverage {symbol}", exc)
            return False

    def _fetch_instrument_info(self, symbol: str) -> dict:
        """
        Fetch and cache instrument info for a symbol (single API call).
        Returns a dict with keys: qty_step, tick_size, min_qty.
        Cached for the lifetime of this client instance.
        """
        if not hasattr(self, "_instrument_cache"):
            self._instrument_cache = {}
        if symbol in self._instrument_cache:
            return self._instrument_cache[symbol]
        try:
            resp = self._session.get_instruments_info(category="linear", symbol=symbol)
            info = resp.get("result", {}).get("list", [{}])[0]
            result = {
                "qty_step": float(info.get("lotSizeFilter", {}).get("qtyStep", "0.001")),
                "tick_size": float(info.get("priceFilter", {}).get("tickSize", "0.0001")),
                "min_qty": float(info.get("lotSizeFilter", {}).get("minOrderQty", "0")),
            }
            self._instrument_cache[symbol] = result
            log.debug(
                "instrument_info %s: qty_step=%s tick_size=%s min_qty=%s",
                symbol, result["qty_step"], result["tick_size"], result["min_qty"],
            )
            return result
        except Exception as exc:
            log.warning("_fetch_instrument_info %s: %s — using defaults", symbol, exc)
            return {"qty_step": 0.001, "tick_size": 0.0001, "min_qty": 0.0}

    def get_qty_step(self, symbol: str) -> float:
        """Minimum qty increment for a symbol. Cached via _fetch_instrument_info."""
        return self._fetch_instrument_info(symbol)["qty_step"]

    def get_tick_size(self, symbol: str) -> float:
        """Minimum price tick for a symbol. Cached via _fetch_instrument_info."""
        return self._fetch_instrument_info(symbol)["tick_size"]

    def get_min_qty(self, symbol: str) -> float:
        """Minimum order quantity for a symbol. Cached via _fetch_instrument_info."""
        return self._fetch_instrument_info(symbol)["min_qty"]

    def _round_qty(self, symbol: str, qty: float) -> float:
        """Floor qty to the symbol's allowed step size."""
        step = self.get_qty_step(symbol)
        if step <= 0:
            return qty
        factor = 1.0 / step
        return math.floor(qty * factor) / factor

    def _round_price(self, symbol: str, price: float) -> float:
        """Round price to the symbol's tick size (standard rounding, not floor)."""
        tick = self.get_tick_size(symbol)
        if tick <= 0:
            return price
        factor = 1.0 / tick
        return round(price * factor) / factor

    # ── order placement ───────────────────────────────────────────────────────

    def place_limit_order(self, symbol: str, side: str, qty: float,
                          price: float, reduce_only: bool = False,
                          order_type_label: str = "limit",
                          stop_loss: Optional[float] = None) -> Optional[str]:
        """Returns bybit order_id or None on failure.

        If stop_loss is provided it is attached directly to the order so the
        SL activates the instant the entry fills — no separate move_stop_loss
        call needed and no risk of the "zero position" ErrCode 10001.
        """
        if self._dry_run:
            fake_id = f"DRY-{symbol}-{side}-{price}"
            log.info("[DRY] place_limit_order %s %s qty=%s price=%s sl=%s → %s",
                     symbol, side, qty, price, stop_loss, fake_id)
            return fake_id
        try:
            qty = self._round_qty(symbol, qty)
            min_qty = self.get_min_qty(symbol)
            if qty <= 0 or (min_qty > 0 and qty < min_qty):
                log.warning(
                    "place_limit_order %s: qty=%.6f below min_qty=%.6f — skipping",
                    symbol, qty, min_qty,
                )
                return None
            kwargs = dict(
                category="linear",
                symbol=symbol,
                side=side,
                orderType="Limit",
                qty=str(qty),
                price=str(price),
                reduceOnly=reduce_only,
                timeInForce="GTC",
            )
            if stop_loss and stop_loss > 0:
                kwargs["stopLoss"] = str(self._round_price(symbol, stop_loss))
                kwargs["slTriggerBy"] = "LastPrice"
            resp = self._session.place_order(**kwargs)
            order_id = resp["result"]["orderId"]
            sl_str = f" sl={stop_loss}" if stop_loss else ""
            log.info("Placed limit order %s %s %s qty=%s price=%s%s → orderId=%s",
                     order_type_label, symbol, side, qty, price, sl_str, order_id)
            self._ok()
            return order_id
        except Exception as exc:
            self._fail(f"place_limit_order {symbol}", exc)
            return None

    def place_market_order(self, symbol: str, side: str, qty: float,
                           reduce_only: bool = False) -> Optional[str]:
        if self._dry_run:
            fake_id = f"DRY-MKT-{symbol}-{side}"
            log.info("[DRY] place_market_order %s %s qty=%s → %s", symbol, side, qty, fake_id)
            return fake_id
        try:
            qty = self._round_qty(symbol, qty)
            min_qty = self.get_min_qty(symbol)
            if qty <= 0 or (min_qty > 0 and qty < min_qty):
                log.warning(
                    "place_market_order %s: qty=%.6f below min_qty=%.6f — skipping",
                    symbol, qty, min_qty,
                )
                return None
            resp = self._session.place_order(
                category="linear",
                symbol=symbol,
                side=side,
                orderType="Market",
                qty=str(qty),
                reduceOnly=reduce_only,
            )
            order_id = resp["result"]["orderId"]
            log.info("Placed market order %s %s qty=%s → orderId=%s", symbol, side, qty, order_id)
            self._ok()
            return order_id
        except Exception as exc:
            self._fail(f"place_market_order {symbol}", exc)
            return None

    # ── cancel ────────────────────────────────────────────────────────────────

    def cancel_order(self, symbol: str, order_id: str) -> bool:
        if self._dry_run:
            log.info("[DRY] cancel_order %s %s", symbol, order_id)
            return True
        try:
            self._session.cancel_order(category="linear", symbol=symbol, orderId=order_id)
            log.info("Cancelled order %s %s", symbol, order_id)
            self._ok()
            return True
        except Exception as exc:
            # Order already filled or cancelled — not a real API failure
            msg = str(exc)
            if "order does not exist" in msg.lower() or "110001" in msg:
                log.debug("cancel_order %s %s: already gone (ok)", symbol, order_id)
                return True
            self._fail(f"cancel_order {symbol} {order_id}", exc)
            return False

    def cancel_orders_for_symbol(self, symbol: str) -> bool:
        if self._dry_run:
            log.info("[DRY] cancel_orders_for_symbol %s", symbol)
            return True
        try:
            self._session.cancel_all_orders(category="linear", symbol=symbol)
            log.info("Cancelled all orders for %s", symbol)
            self._ok()
            return True
        except Exception as exc:
            msg = str(exc)
            if "10001" in msg or "symbol not exist" in msg.lower() or "symbol invalid" in msg.lower():
                log.debug("cancel_orders_for_symbol %s: symbol not on Bybit (delisted?) – skipping", symbol)
                return True  # treat as success — nothing to cancel
            self._fail(f"cancel_orders_for_symbol {symbol}", exc)
            return False

    def cancel_entry_orders(self, symbol: str) -> int:
        """
        Cancel only open non-reduceOnly limit orders (entry ladder orders).
        Does NOT touch TP orders or the position-level stop-loss.
        Returns the number of orders successfully cancelled.
        """
        if self._dry_run:
            log.info("[DRY] cancel_entry_orders %s", symbol)
            return 0
        try:
            open_orders = self.fetch_open_orders(symbol)
            entry_orders = [
                o for o in open_orders
                if str(o.get("reduceOnly", "false")).lower() != "true"
                and o.get("orderType") == "Limit"
            ]
            cancelled = 0
            for o in entry_orders:
                order_id = o.get("orderId", "")
                if order_id and self.cancel_order(symbol, order_id):
                    cancelled += 1
            return cancelled
        except Exception as exc:
            self._fail(f"cancel_entry_orders {symbol}", exc)
            return 0

    # ── fetch ─────────────────────────────────────────────────────────────────

    def fetch_open_orders(self, symbol: str) -> List[Dict[str, Any]]:
        try:
            resp = self._session.get_open_orders(category="linear", symbol=symbol)
            self._ok()
            return resp.get("result", {}).get("list", [])
        except Exception as exc:
            self._fail(f"fetch_open_orders {symbol}", exc)
            return []

    def fetch_position(self, symbol: str) -> Optional[Dict[str, Any]]:
        try:
            resp = self._session.get_positions(category="linear", symbol=symbol)
            self._ok()
            positions = resp.get("result", {}).get("list", [])
            for pos in positions:
                if float(pos.get("size", 0)) > 0:
                    return pos
            return None
        except Exception as exc:
            msg = str(exc)
            # ErrCode 10001 = symbol does not exist (delisted / renamed).
            # Treat as "no position" rather than a connectivity failure so the
            # watchdog doesn't fire Bybit-down alerts for stale DB records.
            if "10001" in msg or "symbol not exist" in msg.lower():
                log.debug("fetch_position %s: symbol not found on Bybit (delisted?) – treating as no position", symbol)
                return None
            self._fail(f"fetch_position {symbol}", exc)
            return None

    def fetch_wallet_balance(self) -> float:
        """Return available USDT balance."""
        try:
            resp = self._session.get_wallet_balance(accountType="UNIFIED")
            account = resp.get("result", {}).get("list", [{}])[0]
            # Try per-coin availableToWithdraw first
            coins = account.get("coin", [])
            for coin in coins:
                if coin.get("coin") == "USDT":
                    val = coin.get("availableToWithdraw", "")
                    if val != "":
                        self._ok()
                        return float(val)
            # Fall back to account-level totalAvailableBalance
            val = account.get("totalAvailableBalance", "")
            if val != "":
                self._ok()
                return float(val)
        except Exception as exc:
            self._fail("fetch_wallet_balance", exc)
        return 0.0

    # ── stop-loss / take-profit ───────────────────────────────────────────────

    def move_stop_loss(self, symbol: str, stop_price: float, position_idx: int = 0) -> bool:
        if self._dry_run:
            log.info("[DRY] move_stop_loss %s → %s", symbol, stop_price)
            return True
        try:
            self._session.set_trading_stop(
                category="linear",
                symbol=symbol,
                stopLoss=str(stop_price),
                positionIdx=position_idx,
            )
            log.info("Moved SL for %s to %s", symbol, stop_price)
            self._ok()
            return True
        except Exception as exc:
            self._fail(f"move_stop_loss {symbol}", exc)
            return False

    def place_take_profit_order(self, symbol: str, side: str, qty: float, price: float) -> Optional[str]:
        """Place a limit TP order (reduce-only)."""
        return self.place_limit_order(symbol, side, qty, price,
                                      reduce_only=True, order_type_label="tp")

    # ── close ─────────────────────────────────────────────────────────────────

    def close_position(self, symbol: str, size: float, side: str) -> bool:
        """Market-close a position. side = opposite of position side."""
        if self._dry_run:
            log.info("[DRY] close_position %s size=%s side=%s", symbol, size, side)
            return True
        order_id = self.place_market_order(symbol, side, size, reduce_only=True)
        return order_id is not None

    def fetch_all_positions(self) -> list:
        """Return all open perpetual positions (size > 0)."""
        try:
            resp = self._session.get_positions(category="linear", settleCoin="USDT")
            self._ok()
            return [
                p for p in resp.get("result", {}).get("list", [])
                if float(p.get("size", 0)) > 0
            ]
        except Exception as exc:
            self._fail("fetch_all_positions", exc)
            return []

    def close_all_positions(self) -> bool:
        if self._dry_run:
            log.info("[DRY] close_all_positions")
            return True
        try:
            resp = self._session.get_positions(category="linear")
            self._ok()
            positions = resp.get("result", {}).get("list", [])
            success = True
            for pos in positions:
                size = float(pos.get("size", 0))
                if size <= 0:
                    continue
                sym = pos["symbol"]
                pos_side = pos.get("side", "Buy")
                close_side = "Sell" if pos_side == "Buy" else "Buy"
                ok = self.close_position(sym, size, close_side)
                if not ok:
                    success = False
            return success
        except Exception as exc:
            self._fail("close_all_positions", exc)
            return False
