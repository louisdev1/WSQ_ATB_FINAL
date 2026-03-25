"""
signal_filter.py – Pre-trade signal filter + RSI/BTC weekly check.

Evaluates each incoming signal against confirmed optimal thresholds.
Returns TAKE or SKIP with a reason string.

Confirmed optimal parameters (wsq_filter_optimizer.py, 317 trades):
  - Entry range width ≥ 2%   (zone too narrow = imprecise signal)
  - SL distance ≥ 3%         (too tight = wicked by noise)
  - TP1 R:R ≤ 1.1            (too far = first target unreachable)
  - Number of targets ≥ 6    (5T signals lose money on all strategies)
  - Block 8T LONG             (structurally weak bucket, 17.5% loss rate)
  - RSI 1h < 40               (Sharpe +0.550 vs RSI<45 Sharpe +0.131)
  - BTC weekly bull for LONGs (macro guard — LONGs blocked in bear weeks)
"""

import logging
import math
import time
from typing import Optional, Tuple

import requests

from app.config import config

log = logging.getLogger(__name__)

BINANCE = "https://api.binance.com"

# ── RSI cache (in-memory, per run) ────────────────────────────────────────────
_rsi_cache: dict = {}          # key: (symbol, interval) → (rsi_value, fetched_at_ts)
RSI_CACHE_TTL = 300            # seconds — re-fetch RSI after 5 minutes


# ── Binance helpers ───────────────────────────────────────────────────────────

def _fetch_rsi(symbol: str, interval: str = "1h", period: int = 14) -> Optional[float]:
    """
    Fetch RSI(14) for a symbol at the current time from Binance public API.
    No API key required. Uses in-memory cache with 5-minute TTL.
    """
    cache_key = (symbol, interval)
    now = time.time()
    cached = _rsi_cache.get(cache_key)
    if cached and (now - cached[1]) < RSI_CACHE_TTL:
        return cached[0]

    try:
        r = requests.get(
            f"{BINANCE}/api/v3/klines",
            params={"symbol": symbol, "interval": interval,
                    "limit": period + 2},
            timeout=8,
        )
        if r.status_code in (400, 404):
            log.debug("RSI fetch: symbol %s not found on Binance", symbol)
            return None
        r.raise_for_status()
        candles = r.json()
        closes  = [float(c[4]) for c in candles[:-1]]  # drop in-progress candle
        rsi_val = _compute_rsi(closes, period)
        if rsi_val is not None:
            _rsi_cache[cache_key] = (rsi_val, now)
        return rsi_val
    except Exception as exc:
        log.warning("RSI fetch failed for %s: %s", symbol, exc)
        return None


def _compute_rsi(closes: list, period: int = 14) -> Optional[float]:
    import numpy as np
    if len(closes) < period + 1:
        return None
    c      = np.array(closes, dtype=float)
    deltas = np.diff(c)
    gains  = np.where(deltas > 0, deltas, 0.0)
    losses = np.where(deltas < 0, -deltas, 0.0)
    ag     = gains[:period].mean()
    al     = losses[:period].mean()
    for i in range(period, len(deltas)):
        ag = (ag * (period - 1) + gains[i]) / period
        al = (al * (period - 1) + losses[i]) / period
    if al == 0:
        return 100.0
    return round(100.0 - 100.0 / (1.0 + ag / al), 2)


_btc_weekly_cache: dict = {}   # {"dir": "bull"/"bear", "ts": timestamp}
BTC_WEEKLY_TTL = 3600          # re-fetch BTC weekly direction every hour


def _fetch_btc_weekly_direction() -> Optional[str]:
    """Returns 'bull' or 'bear' based on the last completed BTC weekly candle."""
    now = time.time()
    cached = _btc_weekly_cache.get("dir")
    if cached and (now - _btc_weekly_cache.get("ts", 0)) < BTC_WEEKLY_TTL:
        return cached
    try:
        r = requests.get(
            f"{BINANCE}/api/v3/klines",
            params={"symbol": "BTCUSDT", "interval": "1w", "limit": 3},
            timeout=8,
        )
        r.raise_for_status()
        candles   = r.json()
        completed = candles[-2]           # second-to-last = last completed week
        o, c      = float(completed[1]), float(completed[4])
        direction = "bull" if c > o else "bear"
        _btc_weekly_cache["dir"] = direction
        _btc_weekly_cache["ts"]  = now
        log.debug("BTC weekly: %s (o=%.0f c=%.0f)", direction, o, c)
        return direction
    except Exception as exc:
        log.warning("BTC weekly fetch failed: %s", exc)
        return None


def _binance_symbol(symbol: str) -> str:
    """Map WSQ symbol names to Binance perpetual symbols."""
    mapping = {
        "PEPE":  "1000PEPEUSDT",
        "MATIC": "POLUSDT",
        "AGIX":  "FETUSDT",
    }
    s = symbol.upper().strip()
    return mapping.get(s, s if s.endswith("USDT") else s + "USDT")


# ── Main filter function ──────────────────────────────────────────────────────

def evaluate_signal(
    symbol:      str,
    direction:   str,
    entry_high:  float,
    entry_low:   float,
    stop_loss:   float,
    targets:     list,
) -> Tuple[str, str]:
    """
    Evaluate a parsed signal against all confirmed optimal filters.

    Returns:
        ("TAKE", reason)  – open the trade
        ("SKIP", reason)  – reject this signal

    All price calculations use abs() — works for LONG and SHORT equally.
    """
    if not getattr(config, "filter_enabled", True):
        return "TAKE", "Filter disabled"

    is_long = direction.upper() in ("LONG", "BUY")

    # ── Derived metrics ───────────────────────────────────────────────────────
    entry_mid = (entry_high + entry_low) / 2 if entry_high > 0 and entry_low > 0 \
                else max(entry_high, entry_low)
    fill      = entry_high if is_long else entry_low

    if entry_mid <= 0 or stop_loss <= 0:
        return "TAKE", "Cannot calculate metrics (missing prices)"

    entry_range_pct = (abs(entry_high - entry_low) / entry_high * 100
                       if entry_high > 0 and entry_low > 0 else 0.0)
    sl_pct          = abs(entry_mid - stop_loss) / entry_mid * 100
    sl_dist_fill    = abs(fill - stop_loss)
    tp1_rr          = (abs(targets[0] - fill) / sl_dist_fill
                       if targets and sl_dist_fill > 0 else 0.0)
    n_targets       = len(targets)

    # ── Filter 1: Zone width ──────────────────────────────────────────────────
    min_zone = getattr(config, "filter_min_entry_range_pct", 2.0)
    if min_zone > 0 and entry_range_pct < min_zone:
        return "SKIP", f"Zone too narrow ({entry_range_pct:.1f}% < {min_zone}%)"

    # ── Filter 2: SL distance ─────────────────────────────────────────────────
    min_sl = getattr(config, "filter_min_sl_pct", 3.0)
    if min_sl > 0 and sl_pct < min_sl:
        return "SKIP", f"SL too tight ({sl_pct:.1f}% < {min_sl}%)"

    # ── Filter 3: TP1 R:R ─────────────────────────────────────────────────────
    max_tp1 = getattr(config, "filter_max_tp1_rr", 1.1)
    if max_tp1 > 0 and tp1_rr > max_tp1:
        return "SKIP", f"TP1 R:R too high ({tp1_rr:.2f} > {max_tp1})"

    # ── Filter 4: Min targets ─────────────────────────────────────────────────
    min_targets = getattr(config, "filter_min_num_targets", 6)
    if n_targets < min_targets:
        return "SKIP", f"Too few targets ({n_targets} < {min_targets})"

    # ── Filter 5: Block 8T LONG ───────────────────────────────────────────────
    if getattr(config, "filter_block_8t_long", True):
        if n_targets == 8 and is_long:
            return "SKIP", "8T LONG blocked (weak bucket — 17.5% loss rate historically)"

    # ── Filter 6: RSI < 40 ────────────────────────────────────────────────────
    rsi_max = getattr(config, "filter_rsi_signal_max", 40.0)
    rsi_tf  = getattr(config, "filter_rsi_tf", "1h")
    b_sym   = _binance_symbol(symbol)
    rsi     = _fetch_rsi(b_sym, rsi_tf)

    if rsi is not None:
        if rsi >= rsi_max:
            return "SKIP", f"RSI {rsi_tf} too high ({rsi:.1f} ≥ {rsi_max})"
        log.debug("RSI %s %s = %.1f (< %.0f ✓)", symbol, rsi_tf, rsi, rsi_max)
    else:
        log.warning("RSI unavailable for %s — passing through (delisted on Binance?)", symbol)

    # ── Filter 7: BTC weekly (LONGs only) ────────────────────────────────────
    if getattr(config, "filter_btc_weekly_enabled", True) and is_long:
        btc_dir = _fetch_btc_weekly_direction()
        if btc_dir == "bear":
            return "SKIP", "BTC weekly is bearish — LONG blocked"
        log.debug("BTC weekly: %s — LONG allowed", btc_dir or "unknown")

    log.info(
        "FILTER TAKE %s %s | zone=%.1f%% sl=%.1f%% tp1rr=%.2f n=%d rsi=%s",
        symbol, direction, entry_range_pct, sl_pct, tp1_rr, n_targets,
        f"{rsi:.1f}" if rsi else "n/a",
    )
    return "TAKE", "All filters passed"
