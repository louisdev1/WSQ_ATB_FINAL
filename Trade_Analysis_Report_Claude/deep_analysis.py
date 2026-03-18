"""
WSQ_ATB — Deep Signal Pattern Analysis
========================================
Runs extensive analysis across every dimension of the signal data
to find hidden patterns that predict wins vs losses.

Usage:  python deep_analysis.py

Input:  trade_analysis_report.json (or enriched_trades.json)
Output: Printed analysis report with all discovered patterns
"""

import json
import os
import sys
import math
from datetime import datetime, timedelta
from collections import defaultdict, Counter
from itertools import combinations

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ENRICHED_PATH = os.path.join(BASE_DIR, "enriched_trades.json")
TRADES_PATH = os.path.join(BASE_DIR, "trade_analysis_report.json")


def load_trades():
    for path in [ENRICHED_PATH, TRADES_PATH]:
        if os.path.exists(path):
            with open(path, encoding="utf-8") as f:
                data = json.load(f)
            print(f"Loaded {len(data['trades'])} trades from {os.path.basename(path)}")
            return data["trades"]
    print("ERROR: No trade data found")
    sys.exit(1)


def classify(t):
    """Returns 'WIN', 'LOSS', or None for unknown."""
    if t["outcome"] in ("FULL_TP", "PARTIAL_TP", "PARTIAL_TP_THEN_SL"):
        return "WIN"
    elif t["outcome"] in ("LOSS", "NO_UPDATE"):
        return "LOSS"
    return None


def bucket_analysis(trades, feature_name, bucket_fn, min_bucket_size=15):
    """
    Generic bucketed win rate analysis.
    bucket_fn(trade) -> bucket_name or None
    """
    buckets = defaultdict(lambda: {"wins": 0, "losses": 0, "total": 0, "targets": []})
    for t in trades:
        result = classify(t)
        if result is None:
            continue
        bucket = bucket_fn(t)
        if bucket is None:
            continue
        buckets[bucket]["total"] += 1
        if result == "WIN":
            buckets[bucket]["wins"] += 1
            buckets[bucket]["targets"].append(t["highest_target_hit"])
        else:
            buckets[bucket]["losses"] += 1

    # Filter small buckets
    buckets = {k: v for k, v in buckets.items() if v["total"] >= min_bucket_size}
    return buckets


def print_bucket_results(title, buckets, sort_by="wr"):
    if not buckets:
        return
    print(f"\n  {title}")
    print(f"  {'-' * 75}")

    if sort_by == "wr":
        items = sorted(buckets.items(), key=lambda x: x[1]["wins"] / x[1]["total"] if x[1]["total"] > 0 else 0, reverse=True)
    else:
        items = sorted(buckets.items(), key=lambda x: x[0])

    for name, b in items:
        wr = b["wins"] / b["total"] * 100 if b["total"] > 0 else 0
        avg_tp = sum(b["targets"]) / len(b["targets"]) if b["targets"] else 0
        bar = "█" * int(wr / 2)
        print(f"    {str(name):<35} {b['total']:>4} trades  {wr:>5.1f}% WR  avg TP:{avg_tp:>4.1f}  {bar}")


def main():
    trades = load_trades()

    # Pre-parse dates
    for t in trades:
        t["_dt"] = None
        if t.get("date"):
            try:
                t["_dt"] = datetime.fromisoformat(t["date"])
            except:
                pass

    valid = [t for t in trades if classify(t) is not None]
    wins = [t for t in valid if classify(t) == "WIN"]
    losses = [t for t in valid if classify(t) == "LOSS"]
    total = len(valid)

    print(f"\nTotal valid trades: {total} ({len(wins)} wins, {len(losses)} losses)")
    print(f"Base win rate: {len(wins)/total*100:.1f}%\n")

    print("=" * 80)
    print("  1. STOP LOSS ANALYSIS (granular)")
    print("=" * 80)

    print_bucket_results("SL distance (1% buckets)", bucket_analysis(valid, "sl_pct",
        lambda t: f"{int(t.get('stop_loss_pct', 0))}%-{int(t.get('stop_loss_pct', 0))+1}%" if t.get("stop_loss_pct") else None,
        min_bucket_size=10), sort_by="key")

    print("\n" + "=" * 80)
    print("  2. R:R RATIO ANALYSIS")
    print("=" * 80)

    for tp_key, tp_label in [("tp1_R", "TP1 R:R"), ("tp2_R", "TP2 R:R"), ("tp3_R", "TP3 R:R")]:
        def make_rr_bucket(tp_k):
            def fn(t):
                rr = t.get("rr_per_target", {}).get(tp_k)
                if rr is None:
                    return None
                if rr < 0.3: return "< 0.3"
                if rr < 0.5: return "0.3 - 0.5"
                if rr < 0.8: return "0.5 - 0.8"
                if rr < 1.0: return "0.8 - 1.0"
                if rr < 1.5: return "1.0 - 1.5"
                if rr < 2.0: return "1.5 - 2.0"
                if rr < 3.0: return "2.0 - 3.0"
                return "> 3.0"
            return fn
        print_bucket_results(tp_label, bucket_analysis(valid, tp_key, make_rr_bucket(tp_key)), sort_by="key")

    print("\n" + "=" * 80)
    print("  3. NUMBER OF TARGETS")
    print("=" * 80)

    print_bucket_results("Targets in signal", bucket_analysis(valid, "num_targets",
        lambda t: f"{t['num_targets']} targets"), sort_by="key")

    print("\n" + "=" * 80)
    print("  4. ENTRY RANGE WIDTH")
    print("=" * 80)

    print_bucket_results("Entry range %", bucket_analysis(valid, "entry_range",
        lambda t: (
            "< 1%" if (t.get("entry_range_pct") or 0) < 1 else
            "1-2%" if (t.get("entry_range_pct") or 0) < 2 else
            "2-3%" if (t.get("entry_range_pct") or 0) < 3 else
            "3-5%" if (t.get("entry_range_pct") or 0) < 5 else
            "5-8%" if (t.get("entry_range_pct") or 0) < 8 else
            "8-12%" if (t.get("entry_range_pct") or 0) < 12 else
            "> 12%"
        ) if t.get("entry_range_pct") is not None else None), sort_by="key")

    print("\n" + "=" * 80)
    print("  5. SIDE (LONG vs SHORT)")
    print("=" * 80)

    print_bucket_results("Trade side", bucket_analysis(valid, "side",
        lambda t: t["side"]))

    print("\n" + "=" * 80)
    print("  6. TIME ANALYSIS")
    print("=" * 80)

    print_bucket_results("Hour of day (UTC)", bucket_analysis(valid, "hour",
        lambda t: f"{t['_dt'].hour:02d}:00" if t["_dt"] else None, min_bucket_size=10), sort_by="key")

    print_bucket_results("Day of week", bucket_analysis(valid, "dow",
        lambda t: t["_dt"].strftime("%A") if t["_dt"] else None), sort_by="key")

    print_bucket_results("Month", bucket_analysis(valid, "month",
        lambda t: t["_dt"].strftime("%B") if t["_dt"] else None, min_bucket_size=10), sort_by="key")

    # Time blocks (morning/afternoon/evening/night)
    print_bucket_results("Time block (UTC)", bucket_analysis(valid, "timeblock",
        lambda t: (
            "Night (0-6)" if t["_dt"].hour < 6 else
            "Morning (6-12)" if t["_dt"].hour < 12 else
            "Afternoon (12-18)" if t["_dt"].hour < 18 else
            "Evening (18-24)"
        ) if t["_dt"] else None), sort_by="key")

    print("\n" + "=" * 80)
    print("  7. SYMBOL FREQUENCY vs WIN RATE")
    print("=" * 80)

    sym_stats = bucket_analysis(valid, "symbol", lambda t: t["symbol"], min_bucket_size=5)
    # Sort by win rate
    print_bucket_results("Symbols (5+ trades, sorted by WR)", sym_stats, sort_by="wr")

    print("\n" + "=" * 80)
    print("  8. CONSECUTIVE SIGNAL ANALYSIS")
    print("=" * 80)

    # Does the result of the previous trade predict the next one?
    sorted_trades = sorted(valid, key=lambda t: t.get("date", ""))
    prev_result = None
    after_win = {"wins": 0, "losses": 0}
    after_loss = {"wins": 0, "losses": 0}
    after_2wins = {"wins": 0, "losses": 0}
    after_2losses = {"wins": 0, "losses": 0}
    prev_prev = None

    for t in sorted_trades:
        result = classify(t)
        if result and prev_result:
            if prev_result == "WIN":
                after_win["wins" if result == "WIN" else "losses"] += 1
            else:
                after_loss["wins" if result == "WIN" else "losses"] += 1

            if prev_prev:
                if prev_prev == "WIN" and prev_result == "WIN":
                    after_2wins["wins" if result == "WIN" else "losses"] += 1
                if prev_prev == "LOSS" and prev_result == "LOSS":
                    after_2losses["wins" if result == "WIN" else "losses"] += 1

        prev_prev = prev_result
        prev_result = result

    print(f"\n  After a WIN:")
    aw_total = after_win["wins"] + after_win["losses"]
    print(f"    {aw_total} trades | {after_win['wins']/aw_total*100:.1f}% win next")

    print(f"  After a LOSS:")
    al_total = after_loss["wins"] + after_loss["losses"]
    print(f"    {al_total} trades | {after_loss['wins']/al_total*100:.1f}% win next")

    if after_2wins["wins"] + after_2wins["losses"] > 10:
        a2w = after_2wins["wins"] + after_2wins["losses"]
        print(f"  After 2 consecutive WINs:")
        print(f"    {a2w} trades | {after_2wins['wins']/a2w*100:.1f}% win next")

    if after_2losses["wins"] + after_2losses["losses"] > 10:
        a2l = after_2losses["wins"] + after_2losses["losses"]
        print(f"  After 2 consecutive LOSSes:")
        print(f"    {a2l} trades | {after_2losses['wins']/a2l*100:.1f}% win next")

    print("\n" + "=" * 80)
    print("  9. SAME-SYMBOL REPEAT ANALYSIS")
    print("=" * 80)

    # If the same symbol was traded before, does the repeat perform differently?
    seen_symbols = {}
    first_time = {"wins": 0, "losses": 0}
    repeat = {"wins": 0, "losses": 0}
    repeat_after_win = {"wins": 0, "losses": 0}
    repeat_after_loss = {"wins": 0, "losses": 0}

    for t in sorted_trades:
        result = classify(t)
        if not result:
            continue
        sym = t["symbol"]
        if sym in seen_symbols:
            repeat["wins" if result == "WIN" else "losses"] += 1
            prev_sym_result = seen_symbols[sym]
            if prev_sym_result == "WIN":
                repeat_after_win["wins" if result == "WIN" else "losses"] += 1
            else:
                repeat_after_loss["wins" if result == "WIN" else "losses"] += 1
        else:
            first_time["wins" if result == "WIN" else "losses"] += 1
        seen_symbols[sym] = result

    ft = first_time["wins"] + first_time["losses"]
    rt = repeat["wins"] + repeat["losses"]
    print(f"\n  First time trading a symbol:")
    print(f"    {ft} trades | {first_time['wins']/ft*100:.1f}% WR")
    print(f"  Repeat trade on same symbol:")
    print(f"    {rt} trades | {repeat['wins']/rt*100:.1f}% WR")

    raw = repeat_after_win["wins"] + repeat_after_win["losses"]
    ral = repeat_after_loss["wins"] + repeat_after_loss["losses"]
    if raw > 10:
        print(f"  Repeat after previous WIN on that symbol:")
        print(f"    {raw} trades | {repeat_after_win['wins']/raw*100:.1f}% WR")
    if ral > 10:
        print(f"  Repeat after previous LOSS on that symbol:")
        print(f"    {ral} trades | {repeat_after_loss['wins']/ral*100:.1f}% WR")

    print("\n" + "=" * 80)
    print("  10. TIME BETWEEN SIGNALS")
    print("=" * 80)

    # Does the gap between signals matter?
    gaps = []
    for i in range(1, len(sorted_trades)):
        if sorted_trades[i]["_dt"] and sorted_trades[i-1]["_dt"]:
            gap_hours = (sorted_trades[i]["_dt"] - sorted_trades[i-1]["_dt"]).total_seconds() / 3600
            result = classify(sorted_trades[i])
            if result and 0 < gap_hours < 720:  # up to 30 days
                gaps.append((gap_hours, result))

    print_bucket_results("Gap since previous signal", bucket_analysis(valid, "gap",
        lambda t: None, min_bucket_size=0), sort_by="key")

    # Manual gap bucketing
    gap_buckets = defaultdict(lambda: {"wins": 0, "losses": 0, "total": 0, "targets": []})
    for gap_h, result in gaps:
        if gap_h < 2:
            bucket = "< 2 hours"
        elif gap_h < 6:
            bucket = "2-6 hours"
        elif gap_h < 12:
            bucket = "6-12 hours"
        elif gap_h < 24:
            bucket = "12-24 hours"
        elif gap_h < 48:
            bucket = "1-2 days"
        elif gap_h < 168:
            bucket = "2-7 days"
        else:
            bucket = "> 7 days"

        gap_buckets[bucket]["total"] += 1
        if result == "WIN":
            gap_buckets[bucket]["wins"] += 1
        else:
            gap_buckets[bucket]["losses"] += 1

    for name in ["< 2 hours", "2-6 hours", "6-12 hours", "12-24 hours", "1-2 days", "2-7 days", "> 7 days"]:
        b = gap_buckets.get(name)
        if b and b["total"] >= 10:
            wr = b["wins"] / b["total"] * 100
            bar = "█" * int(wr / 2)
            print(f"    {name:<35} {b['total']:>4} trades  {wr:>5.1f}% WR  {bar}")

    print("\n" + "=" * 80)
    print("  11. MULTI-SIGNAL DAYS")
    print("=" * 80)

    # Do days with many signals perform differently?
    daily_counts = defaultdict(list)
    for t in valid:
        if t["_dt"]:
            day = t["_dt"].strftime("%Y-%m-%d")
            daily_counts[day].append(t)

    multi_buckets = defaultdict(lambda: {"wins": 0, "losses": 0, "total": 0, "targets": []})
    for day, day_trades in daily_counts.items():
        n = len(day_trades)
        if n == 1:
            bucket = "1 signal/day"
        elif n == 2:
            bucket = "2 signals/day"
        elif n <= 3:
            bucket = "3 signals/day"
        elif n <= 5:
            bucket = "4-5 signals/day"
        else:
            bucket = "6+ signals/day"

        for t in day_trades:
            result = classify(t)
            if result:
                multi_buckets[bucket]["total"] += 1
                if result == "WIN":
                    multi_buckets[bucket]["wins"] += 1
                    multi_buckets[bucket]["targets"].append(t["highest_target_hit"])
                else:
                    multi_buckets[bucket]["losses"] += 1

    for name in ["1 signal/day", "2 signals/day", "3 signals/day", "4-5 signals/day", "6+ signals/day"]:
        b = multi_buckets.get(name)
        if b and b["total"] >= 10:
            wr = b["wins"] / b["total"] * 100
            avg_tp = sum(b["targets"]) / len(b["targets"]) if b["targets"] else 0
            bar = "█" * int(wr / 2)
            print(f"    {name:<35} {b['total']:>4} trades  {wr:>5.1f}% WR  avg TP:{avg_tp:>4.1f}  {bar}")

    print("\n" + "=" * 80)
    print("  12. COMBINED FILTERS (interaction effects)")
    print("=" * 80)

    # SL tight + LONG vs SL tight + SHORT
    combos = [
        ("LONG + SL < 3%", lambda t: t["side"] == "LONG" and (t.get("stop_loss_pct") or 99) < 3),
        ("SHORT + SL < 3%", lambda t: t["side"] == "SHORT" and (t.get("stop_loss_pct") or 99) < 3),
        ("LONG + SL 3-8%", lambda t: t["side"] == "LONG" and 3 <= (t.get("stop_loss_pct") or 0) < 8),
        ("SHORT + SL 3-8%", lambda t: t["side"] == "SHORT" and 3 <= (t.get("stop_loss_pct") or 0) < 8),
        ("LONG + SL > 8%", lambda t: t["side"] == "LONG" and (t.get("stop_loss_pct") or 0) >= 8),
        ("SHORT + SL > 8%", lambda t: t["side"] == "SHORT" and (t.get("stop_loss_pct") or 0) >= 8),
        ("LONG + few targets (<=5)", lambda t: t["side"] == "LONG" and t.get("num_targets", 0) <= 5),
        ("SHORT + few targets (<=5)", lambda t: t["side"] == "SHORT" and t.get("num_targets", 0) <= 5),
        ("LONG + many targets (>=8)", lambda t: t["side"] == "LONG" and t.get("num_targets", 0) >= 8),
        ("SHORT + many targets (>=8)", lambda t: t["side"] == "SHORT" and t.get("num_targets", 0) >= 8),
        ("Evening + LONG", lambda t: t["_dt"] and t["_dt"].hour >= 18 and t["side"] == "LONG"),
        ("Evening + SHORT", lambda t: t["_dt"] and t["_dt"].hour >= 18 and t["side"] == "SHORT"),
        ("Morning + LONG", lambda t: t["_dt"] and 6 <= t["_dt"].hour < 12 and t["side"] == "LONG"),
        ("Morning + SHORT", lambda t: t["_dt"] and 6 <= t["_dt"].hour < 12 and t["side"] == "SHORT"),
        ("Weekend + LONG", lambda t: t["_dt"] and t["_dt"].weekday() >= 5 and t["side"] == "LONG"),
        ("Weekend + SHORT", lambda t: t["_dt"] and t["_dt"].weekday() >= 5 and t["side"] == "SHORT"),
        ("Weekday + LONG", lambda t: t["_dt"] and t["_dt"].weekday() < 5 and t["side"] == "LONG"),
        ("Weekday + SHORT", lambda t: t["_dt"] and t["_dt"].weekday() < 5 and t["side"] == "SHORT"),
        ("TP1 RR > 1.0", lambda t: t.get("rr_per_target", {}).get("tp1_R", 0) > 1.0),
        ("TP1 RR < 0.5", lambda t: (t.get("rr_per_target", {}).get("tp1_R") or 99) < 0.5),
        ("Wide entry (>5%) + Wide SL (>8%)", lambda t: (t.get("entry_range_pct") or 0) > 5 and (t.get("stop_loss_pct") or 0) > 8),
        ("Tight entry (<2%) + Tight SL (<3%)", lambda t: (t.get("entry_range_pct") or 99) < 2 and (t.get("stop_loss_pct") or 99) < 3),
        ("Tight entry (<2%) + Normal SL (3-8%)", lambda t: (t.get("entry_range_pct") or 99) < 2 and 3 <= (t.get("stop_loss_pct") or 0) < 8),
        ("6+ targets + SL > 5%", lambda t: t.get("num_targets", 0) >= 6 and (t.get("stop_loss_pct") or 0) > 5),
        ("6+ targets + SL < 5%", lambda t: t.get("num_targets", 0) >= 6 and (t.get("stop_loss_pct") or 0) < 5),
    ]

    combo_results = []
    for name, fn in combos:
        matching = [t for t in valid if fn(t)]
        if len(matching) >= 15:
            w = sum(1 for t in matching if classify(t) == "WIN")
            wr = w / len(matching) * 100
            avg_tp = sum(t["highest_target_hit"] for t in matching if classify(t) == "WIN") / max(w, 1)
            combo_results.append((name, len(matching), wr, avg_tp))

    combo_results.sort(key=lambda x: x[2], reverse=True)
    for name, count, wr, avg_tp in combo_results:
        bar = "█" * int(wr / 2)
        diff = wr - len(wins) / total * 100
        print(f"    {name:<40} {count:>4} trades  {wr:>5.1f}% WR ({diff:>+5.1f}pp)  avg TP:{avg_tp:>4.1f}  {bar}")

    print("\n" + "=" * 80)
    print("  13. OUTCOME BY PATTERN TYPE (from signal text)")
    print("=" * 80)

    # Check if certain chart patterns mentioned in signals perform differently
    patterns = {
        "Triangle": ["triangle", "traingle"],
        "Wedge": ["wedge"],
        "Flag": ["flag"],
        "Channel": ["channel"],
        "Head & Shoulders": ["head and shoulder", "head & shoulder", "h&s"],
        "Cup & Handle": ["cup and handle", "cup & handle"],
        "Double Top/Bottom": ["double top", "double bottom"],
        "Support/Resistance": ["support", "resistance"],
    }

    pattern_results = []
    for pname, keywords in patterns.items():
        matching = []
        for t in valid:
            # Check raw text in linked updates or signal structure
            # We don't have raw text in the report, so check symbol patterns instead
            # Actually we don't have the raw text here — skip this one
            pass

    # Instead: leverage field
    print_bucket_results("Leverage mentioned", bucket_analysis(valid, "leverage",
        lambda t: t.get("leverage") or "Not specified"), sort_by="wr")

    # Timeframe
    print_bucket_results("Timeframe mentioned", bucket_analysis(valid, "timeframe",
        lambda t: t.get("timeframe") or "Not specified", min_bucket_size=10), sort_by="wr")

    print("\n" + "=" * 80)
    print("  14. FULL TP vs PARTIAL TP ANALYSIS")
    print("=" * 80)

    full_tp = [t for t in valid if t["outcome"] == "FULL_TP"]
    partial_tp = [t for t in valid if t["outcome"] == "PARTIAL_TP"]

    print(f"\n  Full TP trades: {len(full_tp)}")
    print(f"  Partial TP trades: {len(partial_tp)}")

    # What distinguishes full TP from partial?
    for label, fn in [
        ("Avg SL %", lambda ts: sum(t.get("stop_loss_pct", 0) for t in ts) / len(ts)),
        ("Avg entry range %", lambda ts: sum(t.get("entry_range_pct", 0) for t in ts if t.get("entry_range_pct")) / max(1, sum(1 for t in ts if t.get("entry_range_pct")))),
        ("Avg num targets", lambda ts: sum(t.get("num_targets", 0) for t in ts) / len(ts)),
        ("% LONG", lambda ts: sum(1 for t in ts if t["side"] == "LONG") / len(ts) * 100),
    ]:
        full_val = fn(full_tp) if full_tp else 0
        partial_val = fn(partial_tp) if partial_tp else 0
        print(f"    {label:<25}: Full TP = {full_val:>6.2f}  |  Partial TP = {partial_val:>6.2f}")

    print("\n" + "=" * 80)
    print("  15. WORST PERFORMING COMBINATIONS (loss traps)")
    print("=" * 80)

    # Find the combinations with the lowest win rates
    print("\n  Bottom 10 combinations (most likely to lose):")
    worst = sorted(combo_results, key=lambda x: x[2])[:10]
    for name, count, wr, avg_tp in worst:
        bar = "░" * int((100 - wr) / 2)
        print(f"    {name:<40} {count:>4} trades  {wr:>5.1f}% WR  {bar}")

    print("\n" + "=" * 80)
    print("  SUMMARY: NEW FILTER CANDIDATES")
    print("=" * 80)

    # Auto-detect strong patterns
    print("\n  Patterns with WR > 85% (potential 'always take' signals):")
    for name, count, wr, avg_tp in combo_results:
        if wr > 85 and count >= 20:
            print(f"    {name:<40} {count:>4} trades  {wr:>5.1f}% WR")

    print("\n  Patterns with WR < 65% (potential 'skip' signals):")
    for name, count, wr, avg_tp in combo_results:
        if wr < 65 and count >= 20:
            print(f"    {name:<40} {count:>4} trades  {wr:>5.1f}% WR")


if __name__ == "__main__":
    main()
