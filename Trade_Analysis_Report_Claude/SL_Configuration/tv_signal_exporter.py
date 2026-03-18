"""
tv_signal_exporter.py

Reads trades_dataset.csv, filters to 2025 data, and for each trade
prints the exact values to paste into the TradingView Pine Script inputs.

Usage:
    python tv_signal_exporter.py                    # all 2025 signals
    python tv_signal_exporter.py --symbol LTCUSDT   # one symbol only
    python tv_signal_exporter.py --symbol LTCUSDT --limit 5

Output:
  output/tv_signals/tv_signals_2025.csv   — machine-readable
  output/tv_signals/tv_signals_2025.txt   — human-readable paste guide
"""

import pandas as pd
import ast
import argparse
from pathlib import Path

INPUT_PATH  = Path("output/trades_dataset.csv")
OUTPUT_DIR  = Path("output/tv_signals")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

parser = argparse.ArgumentParser()
parser.add_argument("--symbol", default=None, help="Filter to one symbol e.g. LTCUSDT")
parser.add_argument("--limit",  default=None, type=int, help="Max signals to export")
args = parser.parse_args()

df = pd.read_csv(INPUT_PATH)
df["date"] = pd.to_datetime(df["date"], utc=False, errors="coerce")
df["year"] = df["date"].dt.year
df = df[df["year"] == 2025].copy()

if args.symbol:
    # Match symbol with or without USDT suffix
    sym = args.symbol.upper().replace("USDT","")
    df = df[df["symbol"].str.upper().str.replace("USDT","") == sym]
    print(f"Filtered to symbol: {args.symbol} → {len(df)} signals")

if args.limit:
    df = df.head(args.limit)

df = df.reset_index(drop=True)
print(f"Total signals to export: {len(df)}")
print()

rows = []
txt_lines = []

for _, row in df.iterrows():
    symbol = str(row["symbol"]).upper()
    pair   = str(row.get("pair","USDT")).upper()
    ticker = f"BYBIT:{symbol}{pair}.P"  # Bybit perpetual format

    date_str  = pd.to_datetime(row["date"]).strftime("%Y-%m-%d")
    entry_mid = float(row["entry_mid"])
    sl        = float(row["stop_loss"])
    direction = 1 if str(row["side"]).lower() == "long" else -1

    # Parse targets
    try:
        targets = ast.literal_eval(str(row["targets"]))
    except Exception:
        targets = []

    # Pad to 10 TPs
    tps = [float(t) for t in targets] + [0.0] * (10 - len(targets))
    tps = tps[:10]

    n_tps   = len(targets)
    highest = int(row.get("highest_target_hit", 0) or 0)

    # TradingView timestamp for Pine Script: Unix ms
    ts_ms = int(pd.to_datetime(row["date"]).timestamp() * 1000)

    record = {
        "signal_id":   int(row["message_id"]),
        "ticker":      ticker,
        "date":        date_str,
        "ts_ms":       ts_ms,
        "direction":   direction,
        "entry_mid":   entry_mid,
        "stop_loss":   sl,
        "n_tps":       n_tps,
        "highest_hit": highest,
        "tp1":  tps[0], "tp2":  tps[1], "tp3":  tps[2], "tp4":  tps[3], "tp5":  tps[4],
        "tp6":  tps[5], "tp7":  tps[6], "tp8":  tps[7], "tp9":  tps[8], "tp10": tps[9],
    }
    rows.append(record)

    # Human-readable block
    txt_lines.append("=" * 60)
    txt_lines.append(f"Signal #{record['signal_id']}  |  {ticker}  |  {date_str}")
    txt_lines.append(f"Direction: {'Long' if direction==1 else 'Short'}")
    txt_lines.append(f"Entry (mid): {entry_mid}")
    txt_lines.append(f"Stop Loss:   {sl}")
    txt_lines.append(f"TPs ({n_tps}): {[t for t in tps if t > 0]}")
    txt_lines.append(f"Already known: highest_hit=TP{highest}")
    txt_lines.append("")
    txt_lines.append("── Paste into TradingView Pine Script inputs: ──")
    txt_lines.append(f"  Signal Date:    {date_str}")
    txt_lines.append(f"  Entry Price:    {entry_mid}")
    txt_lines.append(f"  Stop Loss:      {sl}")
    txt_lines.append(f"  Direction:      {direction}")
    for i, tp in enumerate(tps, 1):
        if tp > 0:
            txt_lines.append(f"  TP{i}:           {tp}")
    txt_lines.append(f"  TradingView chart: {ticker}, 1H timeframe")
    txt_lines.append("")

out_csv = OUTPUT_DIR / "tv_signals_2025.csv"
out_txt = OUTPUT_DIR / "tv_signals_2025.txt"

pd.DataFrame(rows).to_csv(out_csv, index=False)
Path(out_txt).write_text("\n".join(txt_lines), encoding="utf-8")

print(f"Saved {len(rows)} signals →")
print(f"  {out_csv}")
print(f"  {out_txt}")
print()
print("Quick preview of first signal:")
print(txt_lines[0] if txt_lines else "(none)")
for line in txt_lines[1:20]:
    print(line)

