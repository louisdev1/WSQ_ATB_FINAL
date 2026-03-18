"""
find_test_signals.py
Finds the best test candidates from 2025 data for ratchet mode comparison.
Run from your project root: python find_test_signals.py
"""
import pandas as pd
import ast
from pathlib import Path

df = pd.read_csv("output/trades_dataset.csv")
df["date"] = pd.to_datetime(df["date"], errors="coerce")
df["year"] = df["date"].dt.year
df = df[df["year"] == 2025].copy()
df["highest_target_hit"] = pd.to_numeric(df["highest_target_hit"], errors="coerce").fillna(0).astype(int)
df["number_of_targets"]  = pd.to_numeric(df["number_of_targets"],  errors="coerce").fillna(0).astype(int)

# Case types
# A = hit TP3+ then stopped mid-way  ← best for ratchet comparison
# B = hit all TPs                    ← good baseline
# C = SL before TP1                  ← all modes same, skip
# D = hit only TP1 or TP2            ← limited comparison
df["case_type"] = "C"
df.loc[(df["highest_target_hit"] >= 3) &
       (df["highest_target_hit"] < df["number_of_targets"]), "case_type"] = "A"
df.loc[df["highest_target_hit"] == df["number_of_targets"], "case_type"] = "B"
df.loc[df["highest_target_hit"].between(1, 2), "case_type"] = "D"

picks = []
seen = set()

# Type A first — hit mid-way (ratchet modes diverge most here)
for _, row in df[df["case_type"] == "A"].sort_values("highest_target_hit", ascending=False).iterrows():
    sym = str(row["symbol"])
    if sym not in seen:
        seen.add(sym)
        picks.append(row)
    if len(picks) >= 6:
        break

# Then Type B — all TPs hit
for _, row in df[df["case_type"] == "B"].sort_values("number_of_targets", ascending=False).iterrows():
    sym = str(row["symbol"])
    if sym not in seen:
        seen.add(sym)
        picks.append(row)
    if len(picks) >= 10:
        break

# Then Type D — hit TP1 or TP2 only (shows ratchet saved a winner)
for _, row in df[df["case_type"] == "D"].sort_values("highest_target_hit", ascending=False).iterrows():
    sym = str(row["symbol"])
    if sym not in seen:
        seen.add(sym)
        picks.append(row)
    if len(picks) >= 14:
        break

print("=" * 75)
print("RATCHET BACKTEST — Recommended test signals (2025)")
print("=" * 75)

for i, row in enumerate(picks, 1):
    try:
        targets = ast.literal_eval(str(row["targets"]))
    except Exception:
        targets = []

    date_dt  = pd.to_datetime(row["date"])
    date_str = date_dt.strftime("%Y-%m-%d")
    time_str = date_dt.strftime("%H:%M")
    sym      = str(row["symbol"])
    ticker   = f"BYBIT:{sym}USDT.P"
    side     = "Long" if str(row["side"]).lower() == "long" else "Short"
    n        = int(row["number_of_targets"])
    hit      = int(row["highest_target_hit"])
    ctype    = row["case_type"]

    type_label = {
        "A": "★ Stopped mid-way  ← best for ratchet comparison",
        "B": "✓ All TPs hit",
        "D": "○ TP1/2 only — shows ratchet saved winner",
    }.get(ctype, "")

    print(f"\n{'─'*75}")
    print(f"Test #{i:>2}  |  {ticker:<28}  |  {date_str} {time_str}")
    print(f"         {type_label}")
    print(f"         Direction: {side}   Entry: {row['entry_mid']}   SL: {row['stop_loss']}")
    print(f"         Result: TP{hit}/{n} hit   |   Signal #{int(row['message_id'])}")
    print()
    print(f"  TradingView settings:")
    print(f"    Chart:     {ticker}  (1H timeframe)")
    print(f"    Date:      {date_str}   Time: {time_str}")
    print(f"    Entry:     {row['entry_mid']}")
    print(f"    SL:        {row['stop_loss']}")
    print(f"    Direction: {side}")
    for j, t in enumerate(targets, 1):
        print(f"    TP{j:<2}:      {t}")

print(f"\n{'='*75}")
print(f"Total: {len(picks)} signals across {len(set(str(r['symbol']) for r in picks))} symbols")
print(f"Type A (best): {sum(1 for r in picks if r['case_type']=='A')}")
print(f"Type B (all TPs): {sum(1 for r in picks if r['case_type']=='B')}")
print(f"Type D (TP1/2): {sum(1 for r in picks if r['case_type']=='D')}")
