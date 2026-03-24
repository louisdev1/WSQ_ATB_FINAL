"""
signal_filter.py – Pre-trade signal filter.

Optimal settings confirmed from full backtesting analysis
(Jan 2024 – May 2025, 324 signals, walk-forward validated):

  RSI 1h < 40       — core edge, Sharpe +0.622, WR 79.4%
  Min 6 targets     — 5T signals all strategies lose money
  SL >= 3%          — too-tight stops get wicked by noise
  TP1 R:R <= 1.1    — first target not absurdly far from entry
  Zone >= 2%        — signal quality floor
  BTC weekly filter — block LONGs when BTC weekly candle is bearish

RSI and BTC weekly fetched from Binance public API (no key needed).
If Binance is unreachable the filter passes through — never blocks on API error.
"""

import logging
from datetime import datetime
from typing import Optional, Tuple

from app.config import config
from app.exchange.binance_indicators import fetch_indicators, fetch_btc_weekly_direction

log = logging.getLogger(__name__)


def _is_long(direction: str) -> bool:
    return direction.lower() in ("long", "buy")


def evaluate_signal(
    symbol: str,
    entry_high: float,
    entry_low: float,
    stop_loss: float,
    targets: list,
    direction: str = "LONG",
    last_trade_result: Optional[str] = None,
    last_signal_time: Optional[datetime] = None,
) -> Tuple[str, str]:
    """
    Evaluate a parsed signal. Returns (decision, reason).

    decision:
      "TAKE" — place the trade
      "SKIP" — do not trade this signal

    All metric calculations use entry_mid (symmetric for LONG/SHORT).
    """
    if not getattr(config, "filter_enabled", True):
        return "TAKE", "Filter disabled"

    # ── Compute signal metrics ────────────────────────────────────────────────

    if entry_high <= 0 and entry_low <= 0:
        return "TAKE", "Missing entry prices"

    entry_mid = (entry_high + entry_low) / 2 if entry_high > 0 and entry_low > 0 \
                else max(entry_high, entry_low)

    if entry_mid <= 0 or stop_loss <= 0:
        return "TAKE", "Cannot compute metrics"

    zone_width_pct = (
        abs(entry_high - entry_low) / entry_mid * 100
        if entry_high > 0 and entry_low > 0 and entry_high != entry_low
        else 0.0
    )

    sl_dist_pct = abs(entry_mid - stop_loss) / entry_mid * 100

    risk   = abs(entry_mid - stop_loss)
    tp1_rr = abs(targets[0] - entry_mid) / risk if risk > 0 and targets else 0.0

    num_targets = len(targets)

    # ── Filter 1: Minimum targets ─────────────────────────────────────────────
    min_targets = getattr(config, "filter_min_num_targets", 6)
    if num_targets < min_targets:
        reason = f"Too few targets ({num_targets} < {min_targets})"
        log.info("FILTER SKIP %s: %s", symbol, reason)
        return "SKIP", reason

    # ── Filter 2: SL too tight ────────────────────────────────────────────────
    min_sl = getattr(config, "filter_min_sl_pct", 3.0)
    if min_sl > 0 and sl_dist_pct < min_sl:
        reason = f"SL too tight ({sl_dist_pct:.1f}% < {min_sl}%)"
        log.info("FILTER SKIP %s: %s", symbol, reason)
        return "SKIP", reason

    # ── Filter 3: SL too wide ─────────────────────────────────────────────────
    max_sl = getattr(config, "filter_max_sl_pct", 0.0)
    if max_sl > 0 and sl_dist_pct > max_sl:
        reason = f"SL too wide ({sl_dist_pct:.1f}% > {max_sl}%)"
        log.info("FILTER SKIP %s: %s", symbol, reason)
        return "SKIP", reason

    # ── Filter 4: TP1 R:R too high ────────────────────────────────────────────
    max_tp1_rr = getattr(config, "filter_max_tp1_rr", 1.1)
    if max_tp1_rr > 0 and tp1_rr > max_tp1_rr:
        reason = f"TP1 R:R too high ({tp1_rr:.2f} > {max_tp1_rr})"
        log.info("FILTER SKIP %s: %s", symbol, reason)
        return "SKIP", reason

    # ── Filter 5: Zone too narrow ─────────────────────────────────────────────
    min_zone = getattr(config, "filter_min_entry_range_pct", 2.0)
    if min_zone > 0 and zone_width_pct < min_zone:
        reason = f"Zone too narrow ({zone_width_pct:.1f}% < {min_zone}%)"
        log.info("FILTER SKIP %s: %s", symbol, reason)
        return "SKIP", reason

    # ── Filter 6: BTC weekly — block LONGs in bear weeks ─────────────────────
    btc_weekly_enabled = getattr(config, "filter_btc_weekly_enabled", True)
    if btc_weekly_enabled and _is_long(direction):
        try:
            btc_dir = fetch_btc_weekly_direction()
            if btc_dir == "bear":
                reason = "BTC weekly bearish — LONG signal blocked"
                log.info("FILTER SKIP %s: %s", symbol, reason)
                return "SKIP", reason
            elif btc_dir == "bull":
                log.info("BTC weekly bull — LONG allowed for %s", symbol)
            else:
                log.warning("BTC weekly unavailable — LONG passes through for %s", symbol)
        except Exception as exc:
            log.warning("BTC weekly check failed (%s) — passing through", exc)

    # ── Filter 7: RSI from Binance ────────────────────────────────────────────
    rsi_max = getattr(config, "filter_rsi_signal_max", 40)
    if rsi_max and rsi_max > 0:
        rsi_tf = getattr(config, "filter_rsi_tf", "1h")
        try:
            inds = fetch_indicators(symbol, interval=rsi_tf)
            rsi  = inds.get("rsi")
            if rsi is not None:
                if rsi >= rsi_max:
                    reason = (f"RSI {rsi_tf}={rsi:.1f} >= {rsi_max} "
                              f"— momentum unfavorable")
                    log.info("FILTER SKIP %s: %s", symbol, reason)
                    return "SKIP", reason
                log.info("RSI OK %s: %s=%.1f < %d", symbol, rsi_tf, rsi, rsi_max)
            else:
                log.warning("RSI unavailable for %s — passing through", symbol)
        except Exception as exc:
            log.warning("RSI fetch failed %s (%s) — passing through", symbol, exc)

    # ── All filters passed ────────────────────────────────────────────────────
    log.info(
        "FILTER TAKE %s %s: zone=%.1f%% SL=%.1f%% TP1rr=%.2f n=%d",
        symbol, direction, zone_width_pct, sl_dist_pct, tp1_rr, num_targets,
    )
    return "TAKE", "All filters passed"
