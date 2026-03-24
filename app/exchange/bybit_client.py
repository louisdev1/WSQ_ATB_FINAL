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

    def get_qty_step(self, symbol: str) -> float:
        """
        Fetch the minimum qty step for a symbol from Bybit instrument info.
        E.g. NEOUSDT = 1.0, BTCUSDT = 0.001, AXLUSDT = 0.1
        Result is cached in memory for the lifetime of this client instance.
        """
        if not hasattr(self, "_qty_step_cache"):
            self._qty_step_cache = {}
        if symbol in self._qty_step_cache:
            return self._qty_step_cache[symbol]
        try:
            resp = self._session.get_instruments_info(category="linear", symbol=symbol)
            info = resp.get("result", {}).get("list", [{}])[0]
            step = float(info.get("lotSizeFilter", {}).get("qtyStep", "0.001"))
            self._qty_step_cache[symbol] = step
            log.debug("qty_step for %s = %s", symbol, step)
            return step
        except Exception as exc:
            log.warning("get_qty_step error %s: %s — defaulting to 0.001", symbol, exc)
            return 0.001

    def _round_qty(self, symbol: str, qty: float) -> float:
        """Floor qty to the symbol's allowed step size."""
        step = self.get_qty_step(symbol)
        if step <= 0:
            return qty
        factor = 1.0 / step
        return math.floor(qty * factor) / factor

    # ── order placement ───────────────────────────────────────────────────────

    def place_limit_order(self, symbol: str, side: str, qty: float,
                          price: float, reduce_only: bool = False,
                          order_type_label: str = "limit") -> Optional[str]:
        """Returns bybit order_id or None on failure."""
        if self._dry_run:
            fake_id = f"DRY-{symbol}-{side}-{price}"
            log.info("[DRY] place_limit_order %s %s qty=%s price=%s → %s",
                     symbol, side, qty, price, fake_id)
            return fake_id
        try:
            qty = self._round_qty(symbol, qty)
            if qty <= 0:
                log.warning("place_limit_order %s: qty rounds to 0 after step adjustment", symbol)
                return None
            resp = self._session.place_order(
                category="linear",
                symbol=symbol,
                side=side,
                orderType="Limit",
                qty=str(qty),
                price=str(price),
                reduceOnly=reduce_only,
                timeInForce="GTC",
            )
            order_id = resp["result"]["orderId"]
            log.info("Placed limit order %s %s %s qty=%s price=%s → orderId=%s",
                     order_type_label, symbol, side, qty, price, order_id)
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
            if qty <= 0:
                log.warning("place_market_order %s: qty rounds to 0 after step adjustment", symbol)
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
            self._fail(f"cancel_orders_for_symbol {symbol}", exc)
            return False

    # ── fetch ─────────────────────────────────────────────────────────────────

    def fetch_open_orders(self, symbol: str) -> List[Dict[str, Any]]:
        try:
            resp = self._session.get_open_orders(category="linear", symbol=symbol)
            self._ok()
            return resp.get("result", {}).get("list", [])
        except Exception as exc:
            self._fail(f"fetch_open_orders {symbol}", exc)
            return []

    def fetch_ticker(self, symbol: str) -> Optional[float]:
        """Return current mark price for a symbol."""
        try:
            resp = self._session.get_tickers(category="linear", symbol=symbol)
            items = resp.get("result", {}).get("list", [])
            if items:
                self._ok()
                return float(items[0].get("markPrice", 0))
        except Exception as exc:
            self._fail(f"fetch_ticker {symbol}", exc)
        return None

    def fetch_btc_weekly_trend(self) -> Optional[str]:
        """
        Returns the BTC weekly candle trend: "bull", "bear", or None (on error).

        Logic:
          - Fetch the last 2 weekly candles for BTCUSDT (interval="W")
          - The most recent CLOSED weekly candle: if close < open → "bear"
          - If close >= open → "bull"
          - We use the second-to-last candle (the last fully closed week)
            because the current week is still forming.

        Data proven: LONG trades during BTC bear weeks = 20% WR.
                     LONG trades during BTC bull weeks = 70%+ WR.
        "Don't buy altcoins when Bitcoin is falling on the weekly" —
        the oldest and most reliable rule in crypto trading.
        """
        try:
            resp = self._session.get_kline(
                category="linear", symbol="BTCUSDT",
                interval="W", limit=3,
            )
            raw = resp.get("result", {}).get("list", [])
            # raw[0] = current (forming) week, raw[1] = last closed week
            if not raw or len(raw) < 2:
                return None
            # Last fully closed weekly candle
            # Bybit kline format: [timestamp, open, high, low, close, volume, turnover]
            closed = raw[1]
            open_price  = float(closed[1])
            close_price = float(closed[4])
            trend = "bull" if close_price >= open_price else "bear"
            self._ok()
            log.debug(
                "BTC weekly trend: open=%.0f close=%.0f → %s",
                open_price, close_price, trend,
            )
            return trend
        except Exception as exc:
            self._fail("fetch_btc_weekly_trend", exc)
            return None

    def fetch_indicators(self, symbol: str, interval: str = "60",
                         rsi_period: int = 14) -> Dict[str, Any]:
        """
        Fetch RSI and MACD at the current moment for a symbol.
        interval: "60" = 1h candles (default), "240" = 4h, "D" = daily
        Returns dict with keys: rsi, macd_hist, macd_hist_prev, macd_trend
        All values None if insufficient data.
        """
        out: Dict[str, Any] = {
            "rsi": None, "macd_hist": None,
            "macd_hist_prev": None, "macd_trend": None,
        }
        try:
            resp = self._session.get_kline(
                category="linear", symbol=symbol,
                interval=interval, limit=70,
            )
            raw = resp.get("result", {}).get("list", [])
            if not raw or len(raw) < rsi_period + 2:
                return out
            closes = [float(c[4]) for c in reversed(raw)]

            # ── RSI ──────────────────────────────────────────────────────────
            rc = closes[-(rsi_period + 1):]
            d  = [rc[i] - rc[i-1] for i in range(1, len(rc))]
            g  = [max(x, 0) for x in d]
            lo = [abs(min(x, 0)) for x in d]
            ag = sum(g[:rsi_period]) / rsi_period
            al = sum(lo[:rsi_period]) / rsi_period
            for i in range(rsi_period, len(d)):
                ag = (ag * (rsi_period - 1) + g[i]) / rsi_period
                al = (al * (rsi_period - 1) + lo[i]) / rsi_period
            out["rsi"] = round(100.0 if al == 0 else 100.0 - 100.0 / (1 + ag / al), 2)

            # ── MACD ─────────────────────────────────────────────────────────
            if len(closes) >= 35:
                def _ema(v, p):
                    if len(v) < p: return []
                    k = 2.0 / (p + 1)
                    r = [None] * (p - 1)
                    r.append(sum(v[:p]) / p)
                    for x in v[p:]:
                        r.append(x * k + r[-1] * (1 - k))
                    return r
                ef = _ema(closes, 12)
                es = _ema(closes, 26)
                ml = [f - s for f, s in zip(ef, es)
                      if f is not None and s is not None]
                if len(ml) >= 9:
                    se = _ema(ml, 9)
                    if len(se) >= 2 and se[-1] is not None:
                        out["macd_hist"]      = ml[-1] - se[-1]
                        out["macd_hist_prev"] = ml[-2] - (se[-2] if se[-2] is not None else se[-1])
                        out["macd_trend"]     = "bull" if ml[-1] > 0 else "bear"
            self._ok()
        except Exception as exc:
            self._fail(f"fetch_indicators {symbol}", exc)
        return out

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
