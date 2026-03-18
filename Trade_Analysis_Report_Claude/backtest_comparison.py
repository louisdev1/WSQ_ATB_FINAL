"""
WSQ_ATB — Backtest: Raw Signals vs Filtered Model
====================================================
Simulates trading all signals vs applying filter rules,
then compares P&L, win rates, and drawdowns.

Uses the TP allocation from tp_allocation.py and the
filter rules derived from the ML analysis.

Usage:
  python backtest_comparison.py

Input:  trade_analysis_report.json  (or enriched_trades.json if available)
Output: backtest_results.json
        Printed comparison report
"""

import json
import os
import sys
import math
from datetime import datetime
from collections import defaultdict

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Try enriched first, fall back to raw
ENRICHED_PATH = os.path.join(BASE_DIR, "enriched_trades.json")
TRADES_PATH = os.path.join(BASE_DIR, "trade_analysis_report.json")
RESULTS_PATH = os.path.join(BASE_DIR, "backtest_results.json")

# ── TP Allocation table (from tp_allocation.py) ──
TP_ALLOCATION_PCT = {
    2:  [66.0, 34.0],
    3:  [47.8, 31.1, 21.1],
    4:  [38.6, 28.4, 18.3, 14.7],
    5:  [31.1, 24.1, 18.8, 14.2, 11.8],
    6:  [26.5, 21.1, 18.1, 13.8, 11.2, 9.3],
    7:  [24.0, 19.4, 16.0, 13.4, 10.5, 8.8, 7.9],
    8:  [22.1, 17.7, 14.4, 13.0, 10.6, 8.4, 7.1, 6.7],
    9:  [19.8, 16.0, 13.7, 11.8, 10.3, 8.9, 7.3, 6.1, 6.1],
    10: [17.9, 14.4, 12.4, 11.1, 10.6, 8.7, 7.3, 6.6, 5.5, 5.5],
    11: [16.2, 13.1, 12.3, 10.7, 9.7, 8.5, 7.3, 6.2, 6.0, 5.0, 5.0],
    12: [14.7, 12.6, 11.7, 10.1, 9.0, 8.6, 7.0, 6.6, 5.6, 5.3, 4.4, 4.4],
    13: [13.8, 11.9, 11.0, 9.5, 8.5, 8.1, 7.1, 6.6, 5.5, 5.0, 4.6, 4.2, 4.2],
    14: [12.3, 11.6, 10.0, 9.4, 8.2, 7.7, 7.4, 6.5, 5.6, 5.0, 4.5, 4.2, 3.8, 3.8],
}

# ── Large cap symbols (top coins by market cap) ──
LARGE_CAP_SYMBOLS = {"BTC", "ETH", "BNB", "SOL", "XRP", "ADA", "DOGE", "TON", "TRX", "DOT"}


# ═══════════════════════════════════════════════════
# FILTER RULES (from ML analysis)
# ═══════════════════════════════════════════════════

def apply_filters(trade):
    """
    Apply the predictive filter rules.
    Returns: (action, reason)
        action: "FULL", "HALF", or "SKIP"
        reason: explanation string
    """
    sl_pct = trade.get("stop_loss_pct")
    symbol = trade.get("symbol", "")
    num_targets = trade.get("num_targets", 0)
    rr = trade.get("rr_per_target", {})
    tp1_rr = rr.get("tp1_R")

    # Get market data if available
    cg = trade.get("cg_data") or {}
    mc = cg.get("market_cap")
    vol = cg.get("total_volume")
    vol_mcap = (vol / mc) if mc and vol and mc > 0 else None

    # Rule 1: SL too tight → SKIP
    if sl_pct is not None and sl_pct < 3.0:
        return "SKIP", f"SL too tight ({sl_pct:.1f}% < 3%)"

    # Rule 2: Very high vol/mcap → SKIP (panic/hype)
    if vol_mcap is not None and vol_mcap > 1.0:
        return "SKIP", f"Vol/MCap too high ({vol_mcap:.2f} > 1.0)"

    # Rule 3: TP1 R:R too low → SKIP
    if tp1_rr is not None and tp1_rr < 0.5:
        return "SKIP", f"TP1 R:R too low ({tp1_rr:.2f} < 0.5)"

    # Rule 4: Low liquidity → HALF size
    if vol_mcap is not None and vol_mcap < 0.1:
        return "HALF", f"Low liquidity (vol/mcap={vol_mcap:.3f} < 0.1)"

    # Rule 5: Large cap → HALF size
    if symbol in LARGE_CAP_SYMBOLS:
        return "HALF", f"Large cap ({symbol})"

    return "FULL", "All filters passed"


# ═══════════════════════════════════════════════════
# P&L SIMULATION
# ═══════════════════════════════════════════════════

def simulate_trade_pnl(trade, position_size=100.0):
    """
    Simulate P&L for a single trade using weighted TP allocation.

    Assumptions:
    - Entry at entry_mid
    - Each TP hit closes its allocated % of the position at that TP's distance
    - If SL hit (and no TP hit), full position lost at SL distance
    - If partial TP hit then trade assumed closed at last TP (conservative)
    - Position size in USDT

    Returns: realized P&L in USDT
    """
    entry = trade.get("entry_mid")
    sl = trade.get("stop_loss")
    sl_pct = trade.get("stop_loss_pct")
    targets = trade.get("targets", [])
    num_targets = trade.get("num_targets", 0)
    highest_hit = trade.get("highest_target_hit", 0)
    outcome = trade.get("outcome", "UNKNOWN")
    side = trade.get("side", "LONG")
    target_pcts = trade.get("target_distances_pct", [])

    if not entry or not sl_pct or num_targets == 0:
        return 0.0

    # Get TP allocation for this number of targets
    alloc = TP_ALLOCATION_PCT.get(num_targets)
    if not alloc:
        # Fallback: equal split
        alloc = [100.0 / num_targets] * num_targets

    # Cap target distances at 50% to filter parsing artifacts
    target_pcts = [min(tp, 50.0) for tp in target_pcts]

    pnl = 0.0

    if outcome == "LOSS" or (outcome == "NO_UPDATE" and highest_hit == 0):
        # Full SL hit — lose sl_pct on entire position
        pnl = -position_size * (sl_pct / 100.0)

    elif highest_hit > 0:
        # Partial or full TP hit
        # Profit from each TP level hit
        for i in range(min(highest_hit, len(target_pcts), len(alloc))):
            tp_distance_pct = target_pcts[i]
            tp_alloc_pct = alloc[i]
            tp_position = position_size * (tp_alloc_pct / 100.0)
            pnl += tp_position * (tp_distance_pct / 100.0)

        # Remaining position (not yet closed by TPs)
        closed_pct = sum(alloc[:min(highest_hit, len(alloc))])
        remaining_pct = 100.0 - closed_pct

        if remaining_pct > 0:
            remaining_position = position_size * (remaining_pct / 100.0)
            if trade.get("sl_hit"):
                # Remaining got stopped out
                pnl -= remaining_position * (sl_pct / 100.0)
            else:
                # Conservative: assume remaining closed at breakeven
                # (moved SL to entry after TPs hit)
                pass  # 0 P&L on remaining

    return round(pnl, 4)


def simulate_trade_pnl_equal_split(trade, position_size=100.0):
    """Same simulation but with equal TP split (no weighted allocation)."""
    entry = trade.get("entry_mid")
    sl_pct = trade.get("stop_loss_pct")
    targets = trade.get("targets", [])
    num_targets = trade.get("num_targets", 0)
    highest_hit = trade.get("highest_target_hit", 0)
    outcome = trade.get("outcome", "UNKNOWN")
    target_pcts = trade.get("target_distances_pct", [])

    if not entry or not sl_pct or num_targets == 0:
        return 0.0

    # Equal split
    alloc = [100.0 / num_targets] * num_targets

    # Cap target distances at 50% to filter parsing artifacts
    target_pcts = [min(tp, 50.0) for tp in target_pcts]

    pnl = 0.0

    if outcome == "LOSS" or (outcome == "NO_UPDATE" and highest_hit == 0):
        pnl = -position_size * (sl_pct / 100.0)

    elif highest_hit > 0:
        for i in range(min(highest_hit, len(target_pcts), len(alloc))):
            tp_distance_pct = target_pcts[i]
            tp_alloc_pct = alloc[i]
            tp_position = position_size * (tp_alloc_pct / 100.0)
            pnl += tp_position * (tp_distance_pct / 100.0)

        closed_pct = sum(alloc[:min(highest_hit, len(alloc))])
        remaining_pct = 100.0 - closed_pct
        if remaining_pct > 0:
            remaining_position = position_size * (remaining_pct / 100.0)
            if trade.get("sl_hit"):
                pnl -= remaining_position * (sl_pct / 100.0)

    return round(pnl, 4)


# ═══════════════════════════════════════════════════
# BACKTEST ENGINE
# ═══════════════════════════════════════════════════

def run_backtest(trades, strategy_name, filter_fn=None, use_weighted_tp=True):
    """
    Run a backtest over all trades.

    Args:
        trades: list of trade dicts
        strategy_name: label for this strategy
        filter_fn: function(trade) -> (action, reason) or None for no filter
        use_weighted_tp: use weighted TP allocation vs equal split
    """
    POSITION_SIZE = 100.0  # $100 per trade base
    results = []
    equity_curve = [0.0]
    total_pnl = 0.0
    trades_taken = 0
    trades_skipped = 0
    trades_half = 0
    wins = 0
    losses = 0
    total_win_pnl = 0.0
    total_loss_pnl = 0.0
    peak = 0.0
    max_drawdown = 0.0
    skip_reasons = defaultdict(int)

    monthly_pnl = defaultdict(float)
    yearly_pnl = defaultdict(float)

    for trade in trades:
        if trade["outcome"] == "UNKNOWN":
            continue

        # Apply filter
        action = "FULL"
        reason = ""
        if filter_fn:
            action, reason = filter_fn(trade)

        if action == "SKIP":
            trades_skipped += 1
            skip_reasons[reason.split("(")[0].strip()] += 1
            continue

        # Determine position size
        pos_size = POSITION_SIZE
        if action == "HALF":
            pos_size = POSITION_SIZE * 0.5
            trades_half += 1

        # Simulate P&L
        if use_weighted_tp:
            pnl = simulate_trade_pnl(trade, pos_size)
        else:
            pnl = simulate_trade_pnl_equal_split(trade, pos_size)

        total_pnl += pnl
        trades_taken += 1

        if pnl > 0:
            wins += 1
            total_win_pnl += pnl
        elif pnl < 0:
            losses += 1
            total_loss_pnl += pnl

        equity_curve.append(total_pnl)
        peak = max(peak, total_pnl)
        drawdown = peak - total_pnl
        max_drawdown = max(max_drawdown, drawdown)

        # Monthly/yearly tracking
        if trade.get("date"):
            ym = trade["date"][:7]
            yr = trade["date"][:4]
            monthly_pnl[ym] += pnl
            yearly_pnl[yr] += pnl

        results.append({
            "message_id": trade["message_id"],
            "date": trade.get("date", ""),
            "symbol": trade["symbol"],
            "action": action,
            "pnl": pnl,
            "cumulative_pnl": round(total_pnl, 2),
        })

    win_rate = wins / trades_taken * 100 if trades_taken > 0 else 0
    avg_win = total_win_pnl / wins if wins > 0 else 0
    avg_loss = total_loss_pnl / losses if losses > 0 else 0
    profit_factor = abs(total_win_pnl / total_loss_pnl) if total_loss_pnl != 0 else float("inf")
    expectancy = total_pnl / trades_taken if trades_taken > 0 else 0

    return {
        "strategy": strategy_name,
        "trades_taken": trades_taken,
        "trades_skipped": trades_skipped,
        "trades_half_size": trades_half,
        "wins": wins,
        "losses": losses,
        "win_rate": round(win_rate, 2),
        "total_pnl": round(total_pnl, 2),
        "avg_win": round(avg_win, 2),
        "avg_loss": round(avg_loss, 2),
        "profit_factor": round(profit_factor, 2),
        "expectancy_per_trade": round(expectancy, 2),
        "max_drawdown": round(max_drawdown, 2),
        "peak_equity": round(peak, 2),
        "skip_reasons": dict(skip_reasons),
        "yearly_pnl": {k: round(v, 2) for k, v in sorted(yearly_pnl.items())},
        "monthly_pnl": {k: round(v, 2) for k, v in sorted(monthly_pnl.items())},
        "equity_curve": equity_curve,
        "trades": results,
    }


# ═══════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════

def main():
    # Load trades
    if os.path.exists(ENRICHED_PATH):
        path = ENRICHED_PATH
    elif os.path.exists(TRADES_PATH):
        path = TRADES_PATH
    else:
        print(f"ERROR: No trade data found in {BASE_DIR}")
        sys.exit(1)

    with open(path, encoding="utf-8") as f:
        data = json.load(f)
    trades = data["trades"]
    print(f"Loaded {len(trades)} trades from {os.path.basename(path)}")

    has_market = sum(1 for t in trades if t.get("cg_data"))
    print(f"Trades with market data: {has_market}/{len(trades)}")

    # ── Strategy A: Raw signals, equal TP split (baseline) ──
    strat_a = run_backtest(
        trades,
        "A: Raw Signals + Equal TP",
        filter_fn=None,
        use_weighted_tp=False,
    )

    # ── Strategy B: Raw signals, weighted TP split ──
    strat_b = run_backtest(
        trades,
        "B: Raw Signals + Weighted TP",
        filter_fn=None,
        use_weighted_tp=True,
    )

    # ── Strategy C: Filtered + weighted TP ──
    strat_c = run_backtest(
        trades,
        "C: Filtered + Weighted TP",
        filter_fn=apply_filters,
        use_weighted_tp=True,
    )

    # ── Strategy D: Filtered + equal TP (isolate filter impact) ──
    strat_d = run_backtest(
        trades,
        "D: Filtered + Equal TP",
        filter_fn=apply_filters,
        use_weighted_tp=False,
    )

    strategies = [strat_a, strat_b, strat_c, strat_d]

    # ═══════════════════════════════════════════════
    # PRINT COMPARISON
    # ═══════════════════════════════════════════════

    print("\n" + "=" * 80)
    print("  BACKTEST COMPARISON — $100 per trade")
    print("=" * 80)

    header = f"{'Metric':<30}"
    for s in strategies:
        header += f"{'|':>2} {s['strategy']:<20}"
    print(header)
    print("-" * 80)

    metrics = [
        ("Trades taken", "trades_taken"),
        ("Trades skipped", "trades_skipped"),
        ("Trades half-sized", "trades_half_size"),
        ("Wins", "wins"),
        ("Losses", "losses"),
        ("Win rate %", "win_rate"),
        ("Total P&L ($)", "total_pnl"),
        ("Avg win ($)", "avg_win"),
        ("Avg loss ($)", "avg_loss"),
        ("Profit factor", "profit_factor"),
        ("Expectancy/trade ($)", "expectancy_per_trade"),
        ("Max drawdown ($)", "max_drawdown"),
        ("Peak equity ($)", "peak_equity"),
    ]

    for label, key in metrics:
        row = f"  {label:<28}"
        for s in strategies:
            val = s[key]
            if isinstance(val, float):
                row += f"{'|':>2} {val:>18.2f}  "
            else:
                row += f"{'|':>2} {val:>18}  "
        print(row)

    # ── Improvement summary ──
    print("\n" + "=" * 80)
    print("  IMPROVEMENT: Filtered+Weighted (C) vs Raw+Equal (A)")
    print("=" * 80)

    a, c = strat_a, strat_c
    pnl_diff = c["total_pnl"] - a["total_pnl"]
    wr_diff = c["win_rate"] - a["win_rate"]
    dd_diff = a["max_drawdown"] - c["max_drawdown"]
    exp_diff = c["expectancy_per_trade"] - a["expectancy_per_trade"]
    pf_diff = c["profit_factor"] - a["profit_factor"]

    print(f"  P&L improvement:        ${pnl_diff:>+10.2f}  ({pnl_diff/abs(a['total_pnl'])*100:>+.1f}%)" if a['total_pnl'] != 0 else f"  P&L improvement:        ${pnl_diff:>+10.2f}")
    print(f"  Win rate improvement:   {wr_diff:>+10.2f}pp")
    print(f"  Expectancy improvement: ${exp_diff:>+10.2f} per trade")
    print(f"  Drawdown reduction:     ${dd_diff:>+10.2f}")
    print(f"  Profit factor change:   {pf_diff:>+10.2f}")
    print(f"  Trades filtered out:    {c['trades_skipped']:>10}")

    # ── Filter breakdown ──
    if c["skip_reasons"]:
        print(f"\n  Filter breakdown (skipped trades):")
        for reason, count in sorted(c["skip_reasons"].items(), key=lambda x: -x[1]):
            print(f"    {reason:<35}: {count:>4} trades skipped")

    # ── Yearly comparison ──
    print("\n" + "=" * 80)
    print("  YEARLY P&L COMPARISON")
    print("=" * 80)
    all_years = sorted(set(list(a["yearly_pnl"].keys()) + list(c["yearly_pnl"].keys())))
    print(f"  {'Year':<8} {'Raw+Equal':>12} {'Raw+Weighted':>14} {'Filtered+Weighted':>18} {'Improvement':>14}")
    print(f"  {'-'*8} {'-'*12} {'-'*14} {'-'*18} {'-'*14}")
    for yr in all_years:
        a_pnl = a["yearly_pnl"].get(yr, 0)
        b_pnl = strat_b["yearly_pnl"].get(yr, 0)
        c_pnl = c["yearly_pnl"].get(yr, 0)
        diff = c_pnl - a_pnl
        print(f"  {yr:<8} ${a_pnl:>10.2f} ${b_pnl:>12.2f} ${c_pnl:>16.2f} ${diff:>+12.2f}")

    # ── Monthly P&L for strategy C ──
    print("\n" + "=" * 80)
    print("  MONTHLY P&L — Filtered + Weighted TP (Strategy C)")
    print("=" * 80)
    for ym, pnl in sorted(c["monthly_pnl"].items()):
        bar_len = int(abs(pnl) / 5)
        bar = ("█" * bar_len) if pnl >= 0 else ("░" * bar_len)
        sign = "+" if pnl >= 0 else ""
        print(f"  {ym}  ${sign}{pnl:>8.2f}  {bar}")

    # ── Save results ──
    # Remove equity curves and trade details from JSON (too large)
    export = []
    for s in strategies:
        e = dict(s)
        e.pop("equity_curve", None)
        e.pop("trades", None)
        export.append(e)

    with open(RESULTS_PATH, "w", encoding="utf-8") as f:
        json.dump(export, f, indent=2)
    print(f"\nResults saved to: {RESULTS_PATH}")


if __name__ == "__main__":
    main()
