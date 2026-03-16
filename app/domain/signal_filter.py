"""
signal_filter.py – Pre-trade signal filter based on backtested optimal parameters.

Evaluates each incoming signal against data-driven thresholds.
Returns TAKE, HALF, or SKIP with a reason string.

Works identically for LONG and SHORT signals — all calculations use
absolute values so direction is irrelevant.

Optimal config (tested on 258,720 combinations):
  - SL distance must be > 3%
  - TP1 R:R must be < 1.0
  - Number of targets must be >= 5
  - Skip the next signal after a losing trade
"""

import logging
from datetime import datetime
from typing import Tuple, Optional

from app.config import config

log = logging.getLogger(__name__)


def evaluate_signal(
    symbol: str,
    entry_high: float,
    entry_low: float,
    stop_loss: float,
    targets: list,
    last_trade_result: Optional[str],
    last_signal_time: Optional[datetime],
) -> Tuple[str, str]:
    """
    Evaluate a parsed signal against optimal filter thresholds.

    Returns:
        ("TAKE", reason)  – open the trade at full size
        ("HALF", reason)  – open at half position size
        ("SKIP", reason)  – do not trade this signal

    All calculations use abs() — works for both LONG and SHORT.
    """
    if not getattr(config, "filter_enabled", True):
        return "TAKE", "Filter disabled"

    # ── Calculate signal metrics ──────────────────────────────────────────────

    entry_mid = (entry_high + entry_low) / 2 if entry_high > 0 and entry_low > 0 else max(entry_high, entry_low)
    if entry_mid <= 0 or stop_loss <= 0:
        return "TAKE", "Cannot calculate metrics (missing prices)"

    # SL distance as % of entry (abs = works for long and short)
    sl_distance_pct = abs(entry_mid - stop_loss) / entry_mid * 100

    # TP1 reward-to-risk ratio (abs = works for long and short)
    risk = abs(entry_mid - stop_loss)
    if risk > 0 and len(targets) > 0:
        tp1_reward = abs(targets[0] - entry_mid)
        tp1_rr = tp1_reward / risk
    else:
        tp1_rr = 0

    # Number of targets
    num_targets = len(targets)

    # ── Apply filters ─────────────────────────────────────────────────────────

    min_sl = getattr(config, "filter_min_sl_pct", 3.0)
    max_tp1_rr = getattr(config, "filter_max_tp1_rr", 1.0)
    min_targets = getattr(config, "filter_min_num_targets", 5)
    skip_after_loss = getattr(config, "filter_skip_after_loss", True)
    half_rapid_hours = getattr(config, "filter_half_rapid_hours", 0)

    # Filter 1: SL too tight
    if sl_distance_pct < min_sl:
        reason = f"SL too tight ({sl_distance_pct:.1f}% < {min_sl}%)"
        log.info("FILTER SKIP %s: %s", symbol, reason)
        return "SKIP", reason

    # Filter 2: TP1 R:R too high (first target too far relative to risk)
    if tp1_rr > max_tp1_rr:
        reason = f"TP1 R:R too high ({tp1_rr:.2f} > {max_tp1_rr})"
        log.info("FILTER SKIP %s: %s", symbol, reason)
        return "SKIP", reason

    # Filter 3: Not enough targets
    if num_targets < min_targets:
        reason = f"Too few targets ({num_targets} < {min_targets})"
        log.info("FILTER SKIP %s: %s", symbol, reason)
        return "SKIP", reason

    # Filter 4: Skip after a losing trade
    if skip_after_loss and last_trade_result == "LOSS":
        reason = "Previous trade was a loss (skip-after-loss)"
        log.info("FILTER SKIP %s: %s", symbol, reason)
        return "SKIP", reason

    # Filter 5: Half size if signal arrives rapidly after previous
    if half_rapid_hours > 0 and last_signal_time:
        now = datetime.utcnow()
        gap_hours = (now - last_signal_time).total_seconds() / 3600
        if 0 < gap_hours < half_rapid_hours:
            reason = f"Rapid signal ({gap_hours:.1f}h < {half_rapid_hours}h)"
            log.info("FILTER HALF %s: %s", symbol, reason)
            return "HALF", reason

    # All filters passed
    log.info(
        "FILTER TAKE %s: SL=%.1f%% TP1rr=%.2f NTP=%d LastResult=%s",
        symbol, sl_distance_pct, tp1_rr, num_targets, last_trade_result or "N/A",
    )
    return "TAKE", "All filters passed"
