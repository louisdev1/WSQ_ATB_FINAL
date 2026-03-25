"""
signal_quality.py  —  Signal Quality Scorer
============================================
Computes a quality score (0–6) for each WSQ signal at parse time.
No API calls needed. All inputs come directly from the signal itself.

The score drives position sizing in trade_manager.py:
    score ≥ 5  →  1.5× base risk   (HIGH — 92.7% WR historically)
    score 3–4  →  1.0× base risk   (MED  — 80.8% WR historically)
    score ≤ 2  →  0.7× base risk   (LOW  — 70.6% WR historically)

HOW THE SCORE WORKS
───────────────────
Three independent dimensions of signal geometry, each scored 0–2:

1. SL DISTANCE from entry midpoint
   5–7%   → +2  (only 9% historical loss rate in this band)
   3–10%  → +1  (broad acceptable range)
   else   → +0

2. ZONE WIDTH (entry_high – entry_low as % of entry_high)
   4–5%   → +2  (only 8% historical loss rate)
   3–7%   → +1  (acceptable width range)
   else   → +0

3. TP1 R:R RATIO (distance to TP1 / SL distance)
   0.7–0.9 → +2  (only 13% historical loss rate — closest TP is balanced)
   0.5–1.1 → +1  (acceptable R:R range)
   else    → +0

WHY IT WORKS
────────────
These aren't arbitrary — they come from the loss rate analysis on 324 real
WSQ signals (Jan 2024–May 2025):

    Dimension       Band        Loss rate    n
    SL distance     5–7%         9%          73
    SL distance     3–5%        26%         105
    Zone width      4–5%         8%          49
    Zone width      3–4%        15%          87
    TP1 R:R         0.7–0.9     13%         112
    TP1 R:R         0.9–1.1     22%          82

The sweet spots reflect well-structured signals where:
  • SL 5–7%: wide enough to not get wicked by noise, tight enough to
    maintain a favourable R:R on TP levels
  • Zone 4–5%: broad entry zone signals the analyst has high conviction
    on the range but isn't so wide it indicates uncertainty
  • TP1 0.7–0.9R: first target is reachable (under 1R away) but not
    trivially close (above 0.7R) — reflects a realistic near-term target

Combined score ≥5 → 92.7% WR (n=55, confirmed on val set)
Combined score ≤2 → 70.6% WR (n=34)

INTEGRATION
───────────
In trade_manager.py, after parsing the signal:

    from signal_quality import compute_quality_score, quality_risk_multiplier

    score      = compute_quality_score(signal)
    multiplier = quality_risk_multiplier(score)
    risk       = float(os.getenv("RISK_PER_TRADE", 0.10)) * multiplier
    # Pass risk into _calc_qty instead of the flat RISK_PER_TRADE
"""

from __future__ import annotations
import logging

log = logging.getLogger(__name__)


# ── Score thresholds ──────────────────────────────────────────────────────────

# SL distance sweet spot (from entry_mid)
SL_HIGH_LO  = 5.0   # %
SL_HIGH_HI  = 7.0   # %
SL_MID_LO   = 3.0   # %
SL_MID_HI   = 10.0  # %

# Zone width sweet spot
ZONE_HIGH_LO = 4.0   # %
ZONE_HIGH_HI = 5.0   # %
ZONE_MID_LO  = 3.0   # %
ZONE_MID_HI  = 7.0   # %

# TP1 R:R sweet spot
TP1_HIGH_LO  = 0.7
TP1_HIGH_HI  = 0.9
TP1_MID_LO   = 0.5
TP1_MID_HI   = 1.1

# Risk multipliers per quality tier
MULT_HIGH = 1.5   # score ≥ 5
MULT_MED  = 1.0   # score 3–4
MULT_LOW  = 0.7   # score ≤ 2

# Tier labels
TIER_HIGH = "HIGH"
TIER_MED  = "MED"
TIER_LOW  = "LOW"


# ── Core functions ────────────────────────────────────────────────────────────

def compute_sl_score(sl_pct: float) -> int:
    """
    Score SL distance from entry midpoint.
    sl_pct = abs(entry_mid - stop_loss) / entry_mid * 100
    """
    if SL_HIGH_LO <= sl_pct < SL_HIGH_HI:
        return 2
    if SL_MID_LO <= sl_pct < SL_MID_HI:
        return 1
    return 0


def compute_zone_score(zone_pct: float) -> int:
    """
    Score entry zone width.
    zone_pct = abs(entry_high - entry_low) / entry_high * 100
    """
    if ZONE_HIGH_LO <= zone_pct < ZONE_HIGH_HI:
        return 2
    if ZONE_MID_LO <= zone_pct < ZONE_MID_HI:
        return 1
    return 0


def compute_tp1_score(tp1_rr: float) -> int:
    """
    Score TP1 R:R ratio.
    tp1_rr = abs(tp1_price - fill_price) / abs(fill_price - stop_loss)
    """
    if TP1_HIGH_LO <= tp1_rr < TP1_HIGH_HI:
        return 2
    if TP1_MID_LO <= tp1_rr < TP1_MID_HI:
        return 1
    return 0


def compute_quality_score(signal: dict) -> int:
    """
    Compute the quality score (0–6) from a parsed signal dict.

    Expected signal keys (all available after signal parsing):
        entry_low       float
        entry_high      float
        stop_loss       float
        targets         list[float]   (first element = TP1)
        side            str           "LONG" or "SHORT"

    Returns:
        int score 0–6
    """
    try:
        entry_low  = float(signal["entry_low"])
        entry_high = float(signal["entry_high"])
        stop_loss  = float(signal["stop_loss"])
        targets    = signal.get("targets", [])
        side       = str(signal.get("side", signal.get("direction","LONG"))).upper()
        is_long    = side in ("LONG", "BUY")

        entry_mid  = (entry_low + entry_high) / 2
        fill_price = entry_high if is_long else entry_low

        # SL distance from entry_mid (%, to entry_mid not fill)
        sl_pct = abs(entry_mid - stop_loss) / entry_mid * 100 if entry_mid > 0 else 0

        # Zone width as % of entry_high
        zone_pct = abs(entry_high - entry_low) / entry_high * 100 if entry_high > 0 else 0

        # TP1 R:R  (TP1 vs fill, normalised by SL distance from fill)
        sl_dist = abs(fill_price - stop_loss)
        tp1_rr  = 0.0
        if targets and sl_dist > 0:
            tp1_rr = abs(targets[0] - fill_price) / sl_dist

        sl_score   = compute_sl_score(sl_pct)
        zone_score = compute_zone_score(zone_pct)
        tp1_score  = compute_tp1_score(tp1_rr)
        total      = sl_score + zone_score + tp1_score

        log.debug(
            "Quality score: %d  (SL=%+d zone=%+d TP1=%+d) | "
            "sl_pct=%.1f  zone_pct=%.1f  tp1_rr=%.3f",
            total, sl_score, zone_score, tp1_score,
            sl_pct, zone_pct, tp1_rr,
        )
        return total

    except Exception as exc:
        log.warning("compute_quality_score failed: %s — returning 3 (neutral)", exc)
        return 3   # neutral — use base risk


def quality_tier(score: int) -> str:
    """Return the tier label for a given score."""
    if score >= 5:
        return TIER_HIGH
    if score >= 3:
        return TIER_MED
    return TIER_LOW


def quality_risk_multiplier(score: int) -> float:
    """
    Return the risk multiplier for a given quality score.
    Multiply your base RISK_PER_TRADE by this value.

        HIGH (≥5): 1.5×   e.g. 10% base → 15% effective risk
        MED  (3-4): 1.0×  e.g. 10% base → 10% effective risk
        LOW  (≤2): 0.7×   e.g. 10% base → 7%  effective risk
    """
    tier = quality_tier(score)
    if tier == TIER_HIGH:
        return MULT_HIGH
    if tier == TIER_MED:
        return MULT_MED
    return MULT_LOW


def describe_score(signal: dict) -> str:
    """
    Human-readable quality breakdown for a signal.
    Used in wsq_signal_checker.py and logging.
    """
    try:
        entry_low  = float(signal["entry_low"])
        entry_high = float(signal["entry_high"])
        stop_loss  = float(signal["stop_loss"])
        targets    = signal.get("targets", [])
        side       = str(signal.get("side", "LONG")).upper()
        is_long    = side in ("LONG","BUY")

        entry_mid  = (entry_low + entry_high) / 2
        fill_price = entry_high if is_long else entry_low
        sl_pct     = abs(entry_mid - stop_loss) / entry_mid * 100 if entry_mid > 0 else 0
        zone_pct   = abs(entry_high - entry_low) / entry_high * 100 if entry_high > 0 else 0
        sl_dist    = abs(fill_price - stop_loss)
        tp1_rr     = abs(targets[0] - fill_price) / sl_dist if targets and sl_dist > 0 else 0

        sl_s    = compute_sl_score(sl_pct)
        zone_s  = compute_zone_score(zone_pct)
        tp1_s   = compute_tp1_score(tp1_rr)
        total   = sl_s + zone_s + tp1_s
        tier    = quality_tier(total)
        mult    = quality_risk_multiplier(total)

        lines = [
            f"Quality score: {total}/6  [{tier}]  → {mult:.1f}× risk multiplier",
            f"  SL distance : {sl_pct:.1f}%   score {sl_s}/2"
            + ("  ✓ sweet spot" if sl_s==2 else ""),
            f"  Zone width  : {zone_pct:.1f}%   score {zone_s}/2"
            + ("  ✓ sweet spot" if zone_s==2 else ""),
            f"  TP1 R:R     : {tp1_rr:.3f}   score {tp1_s}/2"
            + ("  ✓ sweet spot" if tp1_s==2 else ""),
        ]
        return "\n".join(lines)

    except Exception as exc:
        return f"Quality score: N/A ({exc})"


# ── Convenience: batch score a DataFrame ─────────────────────────────────────

def score_dataframe(df) -> "pd.Series":
    """
    Score a DataFrame of signals. Expects columns:
        entry_low, entry_high, stop_loss, targets, side (or direction)
    Returns a Series of int scores.
    """
    import json
    scores = []
    for _, row in df.iterrows():
        try:
            targets = row.get("targets", [])
            if isinstance(targets, str):
                targets = json.loads(targets)
            sig = {
                "entry_low":  row["entry_low"],
                "entry_high": row["entry_high"],
                "stop_loss":  row["stop_loss"],
                "targets":    [float(t) for t in targets if t],
                "side":       row.get("side", row.get("direction", "LONG")),
            }
            scores.append(compute_quality_score(sig))
        except Exception:
            scores.append(3)
    import pandas as pd
    return pd.Series(scores, index=df.index)
