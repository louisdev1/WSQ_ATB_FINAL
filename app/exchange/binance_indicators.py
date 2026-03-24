"""
binance_indicators.py – Fetch RSI and MACD from Binance public API.

Uses Binance /api/v3/klines (no API key required).
Called by signal_filter.py at signal time to check RSI < 40.

Look-ahead safe:
  - For 1h/4h candles: drops the last (in-progress) candle, uses previous
    completed candle only — so the value matches what we backtested against.
  - endTime = now - 1ms ensures we never receive a partial candle.
"""

import logging
import time
from typing import Optional

import numpy as np
import requests

log = logging.getLogger(__name__)

BINANCE_BASE   = "https://api.binance.com"
CACHE: dict    = {}          # {cache_key: (timestamp, value)}
CACHE_TTL      = 300         # seconds — re-fetch every 5 min max


def _binance_symbol(symbol: str) -> str:
    """Convert bot symbol (HBAR) to Binance pair (HBARUSDT)."""
    s = symbol.upper().strip()
    if not s.endswith("USDT"):
        s += "USDT"
    return s


def _fetch_klines(symbol: str, interval: str, limit: int = 52) -> list:
    """
    Fetch the last `limit` closed candles for a symbol.
    endTime = now-1ms so we never get the current in-progress candle.
    For 1h/4h we also drop the last returned candle (may be in-progress
    depending on exact timing) — leaving 50 confirmed closed candles.
    """
    url    = f"{BINANCE_BASE}/api/v3/klines"
    end_ms = int(time.time() * 1000) - 1
    params = {
        "symbol":  symbol,
        "interval": interval,
        "endTime": end_ms,
        "limit":   limit,
    }
    try:
        r = requests.get(url, params=params, timeout=10)
        r.raise_for_status()
        candles = r.json()
        # Drop last candle for slow timeframes (may still be building)
        if interval in ("1h", "4h", "1d", "1w") and len(candles) >= 2:
            candles = candles[:-1]
        return candles
    except Exception as exc:
        log.warning("Binance klines error %s %s: %s", symbol, interval, exc)
        return []


def _compute_rsi(closes: list, period: int = 14) -> Optional[float]:
    if len(closes) < period + 1:
        return None
    c      = np.array(closes, dtype=float)
    deltas = np.diff(c)
    gains  = np.where(deltas > 0, deltas, 0.0)
    losses = np.where(deltas < 0, -deltas, 0.0)
    avg_g  = gains[:period].mean()
    avg_l  = losses[:period].mean()
    for i in range(period, len(deltas)):
        avg_g = (avg_g * (period - 1) + gains[i]) / period
        avg_l = (avg_l * (period - 1) + losses[i]) / period
    if avg_l == 0:
        return 100.0
    return round(100.0 - 100.0 / (1.0 + avg_g / avg_l), 2)


def _compute_macd(closes: list,
                  fast: int = 12, slow: int = 26, signal: int = 9
                  ) -> Optional[dict]:
    if len(closes) < slow + signal - 1:
        return None
    c = np.array(closes, dtype=float)

    def ema(arr, period):
        e   = np.full_like(arr, np.nan)
        e[period - 1] = arr[:period].mean()
        k = 2.0 / (period + 1)
        for i in range(period, len(arr)):
            e[i] = arr[i] * k + e[i - 1] * (1 - k)
        return e

    ema_f  = ema(c, fast)
    ema_s  = ema(c, slow)
    macd_l = ema_f - ema_s
    vs     = slow - 1
    sig    = np.full_like(macd_l, np.nan)
    sig[vs + signal - 1] = macd_l[vs:vs + signal].mean()
    k = 2.0 / (signal + 1)
    for i in range(vs + signal, len(macd_l)):
        sig[i] = macd_l[i] * k + sig[i - 1] * (1 - k)

    lm, ls = float(macd_l[-1]), float(sig[-1])
    if np.isnan(lm) or np.isnan(ls):
        return None
    return {
        "macd":   round(lm, 8),
        "signal": round(ls, 8),
        "hist":   round(lm - ls, 8),
    }


def fetch_indicators(symbol: str, interval: str = "1h") -> dict:
    """
    Fetch RSI(14) and MACD(12,26,9) for a symbol at a given timeframe.
    Results are cached for CACHE_TTL seconds.

    Returns:
        {
            "rsi":         float or None,
            "macd_hist":   float or None,   # positive = bullish
            "macd_line":   float or None,
            "macd_signal": float or None,
        }
    """
    b_sym     = _binance_symbol(symbol)
    cache_key = f"{b_sym}_{interval}"
    now       = time.time()

    if cache_key in CACHE:
        ts, val = CACHE[cache_key]
        if now - ts < CACHE_TTL:
            log.debug("Indicator cache hit: %s %s rsi=%.1f",
                      symbol, interval, val.get("rsi") or -1)
            return val

    candles = _fetch_klines(b_sym, interval, limit=52)
    if not candles:
        log.warning("No candles for %s %s — skipping RSI filter", symbol, interval)
        return {}

    closes = [float(c[4]) for c in candles]
    rsi    = _compute_rsi(closes)
    macd   = _compute_macd(closes)

    result = {
        "rsi":         rsi,
        "macd_hist":   macd["hist"]   if macd else None,
        "macd_line":   macd["macd"]   if macd else None,
        "macd_signal": macd["signal"] if macd else None,
    }

    CACHE[cache_key] = (now, result)
    log.info("Indicators %s %s: RSI=%.1f  MACD_hist=%s",
             symbol, interval,
             rsi or -1,
             f"{macd['hist']:+.6f}" if macd else "n/a")
    return result


def fetch_btc_weekly_direction() -> str | None:
    """
    Fetch the most recently COMPLETED BTC weekly candle direction.
    Returns "bull" if close > open, "bear" if close <= open, None on error.

    Uses the PREVIOUS completed week — never the current in-progress candle.
    Example: signal on Tuesday → uses last Monday-Sunday completed week.
    """
    cache_key = "BTCUSDT_1w_direction"
    now       = time.time()

    if cache_key in CACHE:
        ts, val = CACHE[cache_key]
        if now - ts < CACHE_TTL:
            return val

    # Fetch 3 weekly candles; the last one is in-progress, use the second-to-last
    candles = _fetch_klines("BTCUSDT", "1w", limit=3)
    if not candles:
        log.warning("Cannot fetch BTC weekly candles — BTC weekly filter skipped")
        return None

    # Candles are sorted oldest → newest.
    # _fetch_klines drops the last for 1w (in-progress), so use candles[-1]
    completed = candles[-1]
    o = float(completed[1])   # open
    c = float(completed[4])   # close
    direction = "bull" if c > o else "bear"

    CACHE[cache_key] = (now, direction)
    log.info("BTC weekly: open=%.0f close=%.0f direction=%s", o, c, direction)
    return direction
