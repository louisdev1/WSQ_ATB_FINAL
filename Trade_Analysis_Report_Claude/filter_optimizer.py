"""
WSQ_ATB — Brute Force Filter Optimizer
========================================
Tests every combination of filter thresholds across all discovered
variables to find the optimal filter set that maximizes risk-adjusted returns.

Usage:  python filter_optimizer.py

Input:  trade_analysis_report.json (or enriched_trades.json)
Output: Printed ranking of best filter combinations
        optimal_filters.json (best config for your bot)
"""

import json
import os
import sys
import math
from datetime import datetime
from collections import defaultdict
from itertools import product

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ENRICHED_PATH = os.path.join(BASE_DIR, "enriched_trades.json")
TRADES_PATH = os.path.join(BASE_DIR, "trade_analysis_report.json")
OUTPUT_PATH = os.path.join(BASE_DIR, "optimal_filters.json")

TP_ALLOC = {
    2: [66.0,34.0], 3: [47.8,31.1,21.1], 4: [38.6,28.4,18.3,14.7],
    5: [31.1,24.1,18.8,14.2,11.8], 6: [26.5,21.1,18.1,13.8,11.2,9.3],
    7: [24.0,19.4,16.0,13.4,10.5,8.8,7.9], 8: [22.1,17.7,14.4,13.0,10.6,8.4,7.1,6.7],
    9: [19.8,16.0,13.7,11.8,10.3,8.9,7.3,6.1,6.1],
    10: [17.9,14.4,12.4,11.1,10.6,8.7,7.3,6.6,5.5,5.5],
}

LARGE_CAP = {"BTC","ETH","BNB","SOL","XRP","ADA","DOGE","TON","TRX","DOT"}


def load_trades():
    for path in [ENRICHED_PATH, TRADES_PATH]:
        if os.path.exists(path):
            with open(path, encoding="utf-8") as f:
                data = json.load(f)
            return data["trades"]
    sys.exit("No trade data found")


def sim_pnl(trade, pos_size=100.0, use_weighted=False):
    sl_pct = trade.get("stop_loss_pct")
    num_tp = trade.get("num_targets", 0)
    highest = trade.get("highest_target_hit", 0)
    outcome = trade.get("outcome", "UNKNOWN")
    tp_pcts = trade.get("target_distances_pct", [])

    if not sl_pct or num_tp == 0:
        return 0.0

    tp_pcts = [min(tp, 50.0) for tp in tp_pcts]

    if use_weighted:
        alloc = TP_ALLOC.get(num_tp, [100.0 / num_tp] * num_tp)
    else:
        alloc = [100.0 / num_tp] * num_tp

    if outcome in ("LOSS", "NO_UPDATE") and highest == 0:
        return -pos_size * (sl_pct / 100.0)

    if highest > 0:
        pnl = 0.0
        for i in range(min(highest, len(tp_pcts), len(alloc))):
            pnl += pos_size * (alloc[i] / 100.0) * (tp_pcts[i] / 100.0)
        closed = sum(alloc[:min(highest, len(alloc))])
        remaining = 100.0 - closed
        if remaining > 0 and trade.get("sl_hit"):
            pnl -= pos_size * (remaining / 100.0) * (sl_pct / 100.0)
        return round(pnl, 4)

    return 0.0


def run_filtered(trades, filters, use_weighted=False):
    """
    Apply a filter config and return performance metrics.
    filters is a dict of threshold values.
    """
    sl_min = filters.get("sl_min", 0)
    sl_max = filters.get("sl_max", 999)
    tp1rr_max = filters.get("tp1rr_max", 999)
    entry_range_min = filters.get("entry_range_min", 0)
    num_targets_min = filters.get("num_targets_min", 0)
    skip_after_loss = filters.get("skip_after_loss", False)
    half_rapid = filters.get("half_rapid_hrs", 0)
    half_large_cap = filters.get("half_large_cap", False)

    POS = 100.0
    total_pnl = 0.0
    wins = 0
    losses = 0
    taken = 0
    skipped = 0
    peak = 0.0
    max_dd = 0.0
    win_pnl = 0.0
    loss_pnl = 0.0
    prev_result = None
    prev_dt = None

    sorted_trades = sorted(trades, key=lambda t: t.get("date", ""))

    for t in sorted_trades:
        if t["outcome"] == "UNKNOWN":
            continue

        sl_pct = t.get("stop_loss_pct")
        tp1_rr = t.get("rr_per_target", {}).get("tp1_R")
        entry_rng = t.get("entry_range_pct")
        n_targets = t.get("num_targets", 0)
        sym = t.get("symbol", "")

        dt = None
        if t.get("date"):
            try:
                dt = datetime.fromisoformat(t["date"])
            except:
                pass

        # Apply skip filters
        skip = False
        if sl_pct is not None and (sl_pct < sl_min or sl_pct > sl_max):
            skip = True
        if tp1_rr is not None and tp1_rr > tp1rr_max:
            skip = True
        if entry_rng is not None and entry_rng < entry_range_min:
            skip = True
        if n_targets < num_targets_min:
            skip = True
        if skip_after_loss and prev_result == "LOSS":
            skip = True

        if skip:
            skipped += 1
            # Still track result for consecutive analysis
            is_win = t["outcome"] in ("FULL_TP", "PARTIAL_TP", "PARTIAL_TP_THEN_SL")
            prev_result = "WIN" if is_win else "LOSS"
            if dt:
                prev_dt = dt
            continue

        # Apply half-size filters
        pos = POS
        if half_large_cap and sym in LARGE_CAP:
            pos = POS * 0.5
        if half_rapid > 0 and dt and prev_dt:
            gap_hrs = (dt - prev_dt).total_seconds() / 3600
            if 0 < gap_hrs < half_rapid:
                pos = POS * 0.5

        pnl = sim_pnl(t, pos, use_weighted)
        total_pnl += pnl
        taken += 1

        is_win = t["outcome"] in ("FULL_TP", "PARTIAL_TP", "PARTIAL_TP_THEN_SL")
        if pnl > 0:
            wins += 1
            win_pnl += pnl
        elif pnl < 0:
            losses += 1
            loss_pnl += pnl

        peak = max(peak, total_pnl)
        dd = peak - total_pnl
        max_dd = max(max_dd, dd)

        prev_result = "WIN" if is_win else "LOSS"
        if dt:
            prev_dt = dt

    if taken == 0:
        return None

    wr = wins / taken * 100
    pf = abs(win_pnl / loss_pnl) if loss_pnl != 0 else 999
    exp = total_pnl / taken
    ret_dd = total_pnl / max_dd if max_dd > 0 else 999

    return {
        "taken": taken,
        "skipped": skipped,
        "wins": wins,
        "losses": losses,
        "wr": round(wr, 2),
        "pnl": round(total_pnl, 2),
        "exp": round(exp, 2),
        "pf": round(pf, 2),
        "max_dd": round(max_dd, 2),
        "ret_dd": round(ret_dd, 2),
    }


def main():
    trades = load_trades()
    valid = [t for t in trades if t["outcome"] != "UNKNOWN"]
    print(f"Loaded {len(valid)} valid trades\n")

    # ── BASELINE ──
    baseline = run_filtered(valid, {})
    print(f"BASELINE (no filters): {baseline['taken']} trades | {baseline['wr']}% WR | ${baseline['pnl']} P&L | ${baseline['max_dd']} DD | {baseline['ret_dd']}x Ret/DD")

    # ═══════════════════════════════════════════════════
    # GRID SEARCH: test all threshold combinations
    # ═══════════════════════════════════════════════════

    # Define parameter grid
    sl_min_values = [0, 2, 2.5, 3, 3.5, 4, 5]
    tp1rr_max_values = [0.8, 1.0, 1.2, 1.5, 2.0, 999]
    entry_range_min_values = [0, 1, 1.5, 2, 2.5, 3]
    num_targets_min_values = [0, 5, 6, 7]
    skip_after_loss_values = [False, True]
    half_rapid_values = [0, 2, 4]
    half_large_cap_values = [False, True]
    use_weighted_values = [False, True]

    total_combos = (len(sl_min_values) * len(tp1rr_max_values) * len(entry_range_min_values)
                    * len(num_targets_min_values) * len(skip_after_loss_values)
                    * len(half_rapid_values) * len(half_large_cap_values) * len(use_weighted_values))

    print(f"\nGrid search: {total_combos:,} combinations to test...")
    print("Running...\n")

    results = []
    tested = 0

    for sl_min in sl_min_values:
        for tp1rr_max in tp1rr_max_values:
            for er_min in entry_range_min_values:
                for nt_min in num_targets_min_values:
                    for skip_loss in skip_after_loss_values:
                        for half_rapid in half_rapid_values:
                            for half_lc in half_large_cap_values:
                                for use_w in use_weighted_values:
                                    filters = {
                                        "sl_min": sl_min,
                                        "tp1rr_max": tp1rr_max,
                                        "entry_range_min": er_min,
                                        "num_targets_min": nt_min,
                                        "skip_after_loss": skip_loss,
                                        "half_rapid_hrs": half_rapid,
                                        "half_large_cap": half_lc,
                                    }
                                    r = run_filtered(valid, filters, use_weighted=use_w)
                                    if r and r["taken"] >= 300:  # Min 300 trades to be meaningful
                                        r["filters"] = filters
                                        r["use_weighted"] = use_w
                                        results.append(r)

                                    tested += 1
                                    if tested % 5000 == 0:
                                        print(f"  Tested {tested:,}/{total_combos:,}...")

    print(f"\nTested {tested:,} combinations, {len(results)} valid (300+ trades)\n")

    # ═══════════════════════════════════════════════════
    # RANK BY DIFFERENT OBJECTIVES
    # ═══════════════════════════════════════════════════

    def print_top(title, sort_key, reverse=True, n=10):
        print("=" * 100)
        print(f"  TOP {n}: {title}")
        print("=" * 100)
        ranked = sorted(results, key=lambda x: x[sort_key], reverse=reverse)[:n]
        for i, r in enumerate(ranked):
            f = r["filters"]
            w = "weighted" if r["use_weighted"] else "equal"
            desc = []
            if f["sl_min"] > 0: desc.append(f"SL>{f['sl_min']}%")
            if f["tp1rr_max"] < 999: desc.append(f"TP1rr<{f['tp1rr_max']}")
            if f["entry_range_min"] > 0: desc.append(f"ER>{f['entry_range_min']}%")
            if f["num_targets_min"] > 0: desc.append(f"NTP>={f['num_targets_min']}")
            if f["skip_after_loss"]: desc.append("skipAfterLoss")
            if f["half_rapid_hrs"] > 0: desc.append(f"halfRapid<{f['half_rapid_hrs']}h")
            if f["half_large_cap"]: desc.append("halfLargeCap")
            desc_str = " + ".join(desc) if desc else "no filters"

            print(f"  #{i+1:>2} | {r['taken']:>4}T {r['wr']:>5.1f}%WR ${r['pnl']:>8.0f} PnL ${r['max_dd']:>6.0f}DD {r['ret_dd']:>5.1f}x R/DD PF:{r['pf']:>5.1f} Exp:${r['exp']:>5.2f} | {w:>8} | {desc_str}")
        print()

    print_top("Best RETURN / DRAWDOWN (risk-adjusted)", "ret_dd")
    print_top("Best WIN RATE", "wr")
    print_top("Best TOTAL P&L", "pnl")
    print_top("Best EXPECTANCY per trade", "exp")
    print_top("Best PROFIT FACTOR", "pf")
    print_top("Lowest MAX DRAWDOWN", "max_dd", reverse=False)

    # ═══════════════════════════════════════════════════
    # COMPOSITE SCORE (balanced ranking)
    # ═══════════════════════════════════════════════════

    # Normalize each metric to 0-1 range, then weighted combine
    if results:
        max_retdd = max(r["ret_dd"] for r in results)
        max_wr = max(r["wr"] for r in results)
        max_pnl = max(r["pnl"] for r in results)
        min_dd = min(r["max_dd"] for r in results)
        max_dd = max(r["max_dd"] for r in results)
        max_exp = max(r["exp"] for r in results)
        max_pf = max(r["pf"] for r in results)

        for r in results:
            # Composite: 30% ret/dd + 20% WR + 20% PnL + 15% expectancy + 15% low DD
            r["composite"] = (
                0.30 * (r["ret_dd"] / max_retdd if max_retdd > 0 else 0) +
                0.20 * (r["wr"] / max_wr if max_wr > 0 else 0) +
                0.20 * (r["pnl"] / max_pnl if max_pnl > 0 else 0) +
                0.15 * (r["exp"] / max_exp if max_exp > 0 else 0) +
                0.15 * (1 - (r["max_dd"] - min_dd) / (max_dd - min_dd) if max_dd > min_dd else 1)
            )

        print_top("OVERALL BEST (composite score)", "composite")

        # Save the #1 overall config
        best = sorted(results, key=lambda x: x["composite"], reverse=True)[0]
        export = {
            "best_filters": best["filters"],
            "use_weighted_tp": best["use_weighted"],
            "performance": {
                "trades_taken": best["taken"],
                "trades_skipped": best["skipped"],
                "win_rate": best["wr"],
                "total_pnl": best["pnl"],
                "expectancy_per_trade": best["exp"],
                "profit_factor": best["pf"],
                "max_drawdown": best["max_dd"],
                "return_over_drawdown": best["ret_dd"],
            },
            "baseline": {
                "trades_taken": baseline["taken"],
                "win_rate": baseline["wr"],
                "total_pnl": baseline["pnl"],
                "max_drawdown": baseline["max_dd"],
                "return_over_drawdown": baseline["ret_dd"],
            },
            "improvement": {
                "wr_change_pp": round(best["wr"] - baseline["wr"], 2),
                "dd_reduction_pct": round((1 - best["max_dd"] / baseline["max_dd"]) * 100, 1) if baseline["max_dd"] > 0 else 0,
                "ret_dd_improvement_pct": round((best["ret_dd"] / baseline["ret_dd"] - 1) * 100, 1) if baseline["ret_dd"] > 0 else 0,
            }
        }

        with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
            json.dump(export, f, indent=2)

        print("=" * 100)
        print("  OPTIMAL CONFIGURATION SAVED")
        print("=" * 100)
        print(f"\n  Filters:")
        for k, v in best["filters"].items():
            if v and v != 0 and v != 999 and v is not False:
                print(f"    {k}: {v}")
        print(f"  TP split: {'weighted' if best['use_weighted'] else 'equal'}")
        print(f"\n  Performance vs baseline:")
        print(f"    WR:     {baseline['wr']}% -> {best['wr']}% ({best['wr']-baseline['wr']:+.1f}pp)")
        print(f"    P&L:    ${baseline['pnl']} -> ${best['pnl']} ({(best['pnl']-baseline['pnl'])/abs(baseline['pnl'])*100:+.1f}%)")
        print(f"    MaxDD:  ${baseline['max_dd']} -> ${best['max_dd']} ({(1-best['max_dd']/baseline['max_dd'])*100:+.1f}%)")
        print(f"    Ret/DD: {baseline['ret_dd']}x -> {best['ret_dd']}x ({(best['ret_dd']/baseline['ret_dd']-1)*100:+.1f}%)")
        print(f"\n  Saved to: {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
