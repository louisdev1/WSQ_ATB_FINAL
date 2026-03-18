"""
ratchet_sl_by_tp_count.py

Same ratchet SL analysis as ratchet_sl_analysis.py but split by
number_of_targets — so a 5-TP signal and a 7-TP signal are analysed
separately.

For each (tp_count, transition) group:
  - How many trades stopped here
  - Break-even recovery % needed for no-ratchet to win
  - Estimated recovery % (upper bound from conditional probabilities)
  - R saved/cost in pessimistic and optimistic scenarios

Filters to 2024+ data only.

Output:
  output/ratchet_analysis/ratchet_by_tp_count.csv
  output/ratchet_analysis/ratchet_by_tp_count_summary.csv
"""

import pandas as pd
import numpy as np
from pathlib import Path

# ══════════════════════════════════════════════════════════════════════════════
INPUT_PATH = Path("output/trades_dataset.csv")
OUTPUT_DIR = Path("output/ratchet_analysis")
START_YEAR = 2024
MIN_TRADES = 5   # skip groups with fewer trades — too noisy
# ══════════════════════════════════════════════════════════════════════════════

if not INPUT_PATH.exists():
    raise FileNotFoundError(f"Not found: {INPUT_PATH}")

df = pd.read_csv(INPUT_PATH)
df["date"] = pd.to_datetime(df["date"], utc=True, errors="coerce")
df["year"] = df["date"].dt.year
df["highest_target_hit"] = pd.to_numeric(df["highest_target_hit"], errors="coerce").fillna(0).astype(int)
df["number_of_targets"]  = pd.to_numeric(df["number_of_targets"],  errors="coerce").fillna(0).astype(int)
df = df[df["year"] >= START_YEAR].copy()

print(f"Trades {START_YEAR}+: {len(df)}")
print(f"Years: {sorted(df['year'].unique().tolist())}")
print()

# Build R:R lookup from actual data (median per TP level across all trades)
rr_cols = sorted(
    [c for c in df.columns if c.startswith("tp") and c.endswith("_R")],
    key=lambda c: int(c.replace("tp","").replace("_R",""))
)
actual_rr = {}
for col in rr_cols:
    tp_num = int(col.replace("tp","").replace("_R",""))
    vals = pd.to_numeric(df[col], errors="coerce").dropna()
    if len(vals) >= 5:
        actual_rr[tp_num] = float(vals.median())

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# ══════════════════════════════════════════════════════════════════════════════
# MAIN LOOP — for each tp_count group
# ══════════════════════════════════════════════════════════════════════════════

all_rows    = []
summary_rows = []

tp_counts = sorted(df["number_of_targets"].unique())

for n_targets in tp_counts:
    grp = df[df["number_of_targets"] == n_targets]
    n_trades = len(grp)
    if n_trades < MIN_TRADES:
        continue

    # Reach and conditional probabilities within this tp_count group
    reach       = {}
    conditional = {}

    for tp in range(1, n_targets + 1):
        hits = int((grp["highest_target_hit"] >= tp).sum())
        reach[tp] = hits / n_trades

        if tp == 1:
            conditional[tp] = reach[tp]
        else:
            prev_hits = int((grp["highest_target_hit"] >= tp - 1).sum())
            conditional[tp] = hits / prev_hits if prev_hits > 0 else 0.0

    # Per-transition ratchet analysis
    group_total_pess = 0.0
    group_total_opt  = 0.0
    group_stops      = 0

    for tp in range(1, n_targets):
        ratchet_sl_rr = actual_rr.get(tp,     0.0)
        next_tp_rr    = actual_rr.get(tp + 1, 0.0)

        if ratchet_sl_rr <= 0 or next_tp_rr <= 0:
            continue

        reached_tp_n  = int((grp["highest_target_hit"] >= tp).sum())
        reached_tp_n1 = int((grp["highest_target_hit"] >= tp + 1).sum())
        stopped_here  = reached_tp_n - reached_tp_n1

        if reached_tp_n == 0 or stopped_here == 0:
            continue

        frac_stopped = stopped_here / reached_tp_n

        # Break-even recovery %
        p_be = (ratchet_sl_rr + 1.0) / (next_tp_rr + 1.0)

        # Estimated recovery (upper bound) = conditional prob of next TP
        p_recover_opt = conditional.get(tp + 1, 0.0)

        ev_with    = ratchet_sl_rr
        ev_no_pess = -1.0
        ev_no_opt  = p_recover_opt * next_tp_rr + (1 - p_recover_opt) * (-1.0)

        net_pess = ev_with - ev_no_pess
        net_opt  = ev_with - ev_no_opt

        total_r_pess = net_pess * stopped_here
        total_r_opt  = net_opt  * stopped_here

        # Verdict
        margin = p_be * 100 - p_recover_opt * 100   # positive = ratchet wins
        if p_be >= 1.0:
            verdict = "ALWAYS BETTER"
        elif p_recover_opt < p_be:
            verdict = "RATCHET BETTER"
        elif p_recover_opt - p_be < 0.03:
            verdict = "BORDERLINE"
        else:
            verdict = "MAY HURT"

        group_total_pess += total_r_pess
        group_total_opt  += total_r_opt
        group_stops      += stopped_here

        all_rows.append({
            "TP_Count":           n_targets,
            "Trades_in_group":    n_trades,
            "Transition":         f"TP{tp}→TP{tp+1}",
            "TP_level":           tp,
            "Ratchet_SL_RR":      round(ratchet_sl_rr,  3),
            "Next_TP_RR":         round(next_tp_rr,     3),
            "Trades_reached_TPn": reached_tp_n,
            "Trades_stopped":     stopped_here,
            "Pct_stopped":        round(frac_stopped * 100, 1),
            "BE_recovery_pct":    round(p_be * 100, 1),
            "Est_recovery_pct":   round(p_recover_opt * 100, 1),
            "Margin_pp":          round(margin, 1),
            "EV_with_ratchet":    round(ev_with,    4),
            "EV_no_ratchet_opt":  round(ev_no_opt,  4),
            "Net_per_trade_pess": round(net_pess, 4),
            "Net_per_trade_opt":  round(net_opt,  4),
            "Total_R_pess":       round(total_r_pess, 2),
            "Total_R_opt":        round(total_r_opt,  2),
            "Verdict":            verdict,
        })

    summary_rows.append({
        "TP_Count":          n_targets,
        "Trades_in_group":   n_trades,
        "Total_stops":       group_stops,
        "Total_R_pess":      round(group_total_pess, 2),
        "Total_R_opt":       round(group_total_opt,  2),
        "R_per_trade_pess":  round(group_total_pess / n_trades, 4),
        "R_per_trade_opt":   round(group_total_opt  / n_trades, 4),
    })

# ══════════════════════════════════════════════════════════════════════════════
# PRINT
# ══════════════════════════════════════════════════════════════════════════════

detail_df  = pd.DataFrame(all_rows)
summary_df = pd.DataFrame(summary_rows)

print("=" * 105)
print("RATCHET ANALYSIS BY TP COUNT  —  2024+")
print("=" * 105)
print(f"{'TPs':>4}  {'Trans':10}  {'N trades':>8}  {'Stopped':>9}  {'BE%':>6}  "
      f"{'Est%':>6}  {'Margin':>8}  {'R saved(worst)':>15}  {'R saved(best)':>14}  {'Verdict'}")
print("-" * 105)

for n_targets in sorted(detail_df["TP_Count"].unique()):
    sub = detail_df[detail_df["TP_Count"] == n_targets]
    n_trades = sub["Trades_in_group"].iloc[0]
    print(f"\n  ── {n_targets} targets  ({n_trades} trades) ──")
    for _, row in sub.iterrows():
        marker = "✓" if row["Verdict"] in ("RATCHET BETTER","ALWAYS BETTER") else \
                 "~" if row["Verdict"] == "BORDERLINE" else "⚠"
        print(f"  {marker} {row['Transition']:10}  {row['Trades_reached_TPn']:>8}  "
              f"{row['Trades_stopped']:>5}({row['Pct_stopped']:>4.1f}%)  "
              f"{row['BE_recovery_pct']:>5.1f}%  {row['Est_recovery_pct']:>5.1f}%  "
              f"{row['Margin_pp']:>+7.1f}pp  "
              f"{row['Total_R_pess']:>+14.2f}R  {row['Total_R_opt']:>+13.2f}R  "
              f"{row['Verdict']}")

print()
print("=" * 75)
print("SUMMARY PER TP COUNT")
print("=" * 75)
print(f"{'TPs':>4}  {'Trades':>7}  {'Stops':>7}  {'R saved(worst)':>15}  "
      f"{'R saved(best)':>14}  {'R/trade(worst)':>15}  {'R/trade(best)':>14}")
print("-" * 75)
for _, row in summary_df.iterrows():
    print(f"  {row['TP_Count']:>2}  {row['Trades_in_group']:>7}  {row['Total_stops']:>7}  "
          f"{row['Total_R_pess']:>+14.2f}R  {row['Total_R_opt']:>+13.2f}R  "
          f"{row['R_per_trade_pess']:>+14.4f}R  {row['R_per_trade_opt']:>+13.4f}R")

print()
total_pess = summary_df["Total_R_pess"].sum()
total_opt  = summary_df["Total_R_opt"].sum()
total_t    = summary_df["Trades_in_group"].sum()
print(f"  ALL  {total_t:>7}  {summary_df['Total_stops'].sum():>7}  "
      f"{total_pess:>+14.2f}R  {total_opt:>+13.2f}R  "
      f"{total_pess/total_t:>+14.4f}R  {total_opt/total_t:>+13.4f}R")

# Flag all MAY HURT transitions
flags = detail_df[detail_df["Verdict"].isin(["MAY HURT","BORDERLINE"])]
if not flags.empty:
    print()
    print("=" * 75)
    print("FLAGS — Transitions where ratchet may be suboptimal")
    print("=" * 75)
    for _, row in flags.iterrows():
        print(f"  {row['TP_Count']} TPs  {row['Transition']:10}  "
              f"BE={row['BE_recovery_pct']:.1f}%  Est={row['Est_recovery_pct']:.1f}%  "
              f"gap={row['Margin_pp']:+.1f}pp  [{row['Verdict']}]  "
              f"worst-case R={row['Total_R_pess']:+.2f}R")

# ══════════════════════════════════════════════════════════════════════════════
# SAVE
# ══════════════════════════════════════════════════════════════════════════════
detail_df.to_csv(OUTPUT_DIR  / "ratchet_by_tp_count.csv",         index=False)
summary_df.to_csv(OUTPUT_DIR / "ratchet_by_tp_count_summary.csv", index=False)
print(f"\nSaved to {OUTPUT_DIR}/")