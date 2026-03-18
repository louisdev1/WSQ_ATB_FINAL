"""
ratchet_bybit_backtest.py
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Fetches 1H OHLC from Bybit for every trade in trades_dataset.csv and simulates
every ratchet SL variant candle-by-candle — identical logic to the Pine Script.

Requirements:  python-dotenv  (pip install python-dotenv)
               All other deps are stdlib (urllib, json, csv, etc.)

Usage:
    python ratchet_bybit_backtest.py
    python ratchet_bybit_backtest.py --csv path/to/trades_dataset.csv
    python ratchet_bybit_backtest.py --env path/to/.env
    python ratchet_bybit_backtest.py --out path/to/output_dir

Reads from .env:  BYBIT_API_KEY, BYBIT_API_SECRET
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""

import argparse
import ast
import csv
import json
import math
import os
import sys
import time
import urllib.parse
import urllib.request
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

try:
    from dotenv import load_dotenv
except ImportError:
    print("ERROR: python-dotenv not installed.  Run:  pip install python-dotenv")
    sys.exit(1)

# ── Constants ─────────────────────────────────────────────────────────────────
BYBIT_BASE      = "https://api.bybit.com"
MAX_HOLD_BARS   = 720          # 30 days on 1H — same as Pine Script default
CANDLE_INTERVAL = "60"         # 1H in Bybit's format
MAX_CANDLES_PER_REQUEST = 200  # Bybit limit
RATE_LIMIT_PAUSE = 0.12        # seconds between API calls (~8 req/s, well under limit)

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# RATCHET MODES
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#
# Each mode is a function:
#   ratchet_fn(tp_index, entry, original_sl, tp_prices) -> new_sl
#
# tp_index  : 0-based index of the TP that just filled (0 = TP1 just hit)
# entry     : entry price
# original_sl: the SL from the signal (never changes as input)
# tp_prices : list of all TP prices in order
#
# Returns the new SL price (or original_sl to leave it unchanged).
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def _prev_tp(i, tp_prices):
    """Price of the TP one step before index i (0-based). Returns None if i==0."""
    return tp_prices[i - 1] if i > 0 else None

def _tp_n_back(i, n, tp_prices):
    """TP price n steps before index i. Returns None if out of range."""
    idx = i - n
    return tp_prices[idx] if idx >= 0 else None


def make_ratchets(entry, original_sl, tp_prices):
    """
    Build a dict of {mode_name: ratchet_function} for one trade.
    All functions share the same closure over entry/original_sl/tp_prices.

    A ratchet function takes (tp_index) and returns the new SL price.
    tp_index is 0-based (0 = TP1 just triggered).
    """

    def no_ratchet(i):
        return original_sl

    def be_only(i):
        # TP1 hit → move to break-even (entry). Never moves again.
        return entry if i == 0 else None          # None = keep current SL

    def standard(i):
        # TP1 → entry (BE), TP2 → TP1, TP3 → TP2, ...
        return entry if i == 0 else tp_prices[i - 1]

    def skip_tp1_move(i):
        # Don't move at TP1. TP2 → BE, TP3 → TP2, TP4 → TP3, ...
        if i == 0:   return None
        if i == 1:   return entry
        return tp_prices[i - 1]

    def skip_tp2_move(i):
        # TP1 → BE. Don't move at TP2 (SL stays at BE). TP3 → TP2, ...
        if i == 0:   return entry
        if i == 1:   return None          # keep at BE
        return tp_prices[i - 1]

    def skip_tp3_move(i):
        # TP1 → BE. TP2 → TP1. Don't move at TP3. TP4 → TP3, ...
        if i == 0:   return entry
        if i == 1:   return tp_prices[0]
        if i == 2:   return None
        return tp_prices[i - 1]

    def lag_2(i):
        # Always move SL to 2 TPs behind the one just hit.
        # TP1 → original_sl, TP2 → original_sl, TP3 → TP1, TP4 → TP2, ...
        # (need to have hit at least TP3 before SL starts trailing at all)
        idx = i - 2
        if idx < 0:   return None
        if idx == 0:  return entry        # 2 behind TP3 = entry (BE)
        return tp_prices[idx - 1]

    def lag_3(i):
        idx = i - 3
        if idx < 0:   return None
        if idx == 0:  return entry
        return tp_prices[idx - 1]

    def every_2(i):
        # Move SL only on even-numbered TPs (TP2, TP4, TP6, ...)
        # TP2 → entry (BE), TP4 → TP2, TP6 → TP4, ...
        if (i + 1) % 2 != 0:   return None
        if i == 1:   return entry
        return tp_prices[i - 2]

    def every_3(i):
        # Move SL only on TP3, TP6, TP9, ...
        if (i + 1) % 3 != 0:   return None
        if i == 2:   return entry
        return tp_prices[i - 3]

    def every_2_from_tp1(i):
        # Move on TP1, TP3, TP5, ...  (odd-numbered TPs, 1-based)
        if (i + 1) % 2 == 0:   return None
        return entry if i == 0 else tp_prices[i - 1]

    def half_ratchet(i):
        # Move SL halfway between current SL and the previous TP.
        # TP1 → halfway between original_sl and entry (= 0.5R)
        # TP2 → halfway between entry and TP1
        # TP3 → halfway between TP1 and TP2  ...
        if i == 0:
            return (original_sl + entry) / 2
        return (tp_prices[i - 2] + tp_prices[i - 1]) / 2 if i >= 2 else (entry + tp_prices[0]) / 2

    def quarter_ratchet(i):
        # Move SL 25% of the way from the previous level toward the TP just hit.
        # TP1 → 25% from original_sl toward entry
        # TP2 → 25% from entry toward TP1  ...
        if i == 0:
            return original_sl + 0.25 * (entry - original_sl)
        prev = entry if i == 1 else tp_prices[i - 2]
        return prev + 0.25 * (tp_prices[i - 1] - prev)

    def three_quarter_ratchet(i):
        # Aggressive: SL moves 75% toward the just-hit TP.
        if i == 0:
            return original_sl + 0.75 * (entry - original_sl)
        prev = entry if i == 1 else tp_prices[i - 2]
        return prev + 0.75 * (tp_prices[i - 1] - prev)

    def be_then_lag2(i):
        # TP1 → BE. Then lag-2 standard ratchet (TP3 → TP1, TP4 → TP2, ...)
        if i == 0: return entry
        if i == 1: return None
        return tp_prices[i - 2]

    def be_then_lag3(i):
        if i == 0: return entry
        if i <= 2: return None
        return tp_prices[i - 3]

    def tp2_be_then_standard(i):
        # Don't move at TP1. TP2 → BE. Then standard from TP3 onward.
        if i == 0: return None
        if i == 1: return entry
        return tp_prices[i - 1]

    def tp3_be_then_standard(i):
        # Don't move until TP3. TP3 → BE. Then standard from TP4 onward.
        if i < 2:  return None
        if i == 2: return entry
        return tp_prices[i - 1]

    def skip_every_other_after_be(i):
        # TP1 → BE. Then move only on TP3, TP5, TP7, ... (every other, starting TP3)
        if i == 0: return entry
        if i == 1: return None
        return tp_prices[i - 1] if i % 2 == 0 else None

    def aggressive_plus1(i):
        # Like standard but SL moves to the TP that just hit (not 1 behind).
        # Maximum locking-in at cost of being stopped out more aggressively.
        return entry if i == 0 else tp_prices[i]  if i < len(tp_prices) - 1 else tp_prices[i - 1]

    def two_steps_forward(i):
        # TP1 → BE. TP2 → TP1. TP3 → TP1 still (no extra move). TP4 → TP2 ...
        # Effectively lag-2 but with BE lock on TP1.
        if i == 0: return entry
        if i == 1: return tp_prices[0]
        idx = i - 2
        return tp_prices[idx] if idx >= 0 else tp_prices[0]

    return {
        "no_ratchet":               no_ratchet,
        "be_only":                  be_only,
        "standard":                 standard,
        "skip_tp1_move":            skip_tp1_move,
        "skip_tp2_move":            skip_tp2_move,
        "skip_tp3_move":            skip_tp3_move,
        "lag_2":                    lag_2,
        "lag_3":                    lag_3,
        "every_2_tps":              every_2,
        "every_3_tps":              every_3,
        "every_2_from_tp1":         every_2_from_tp1,
        "half_ratchet":             half_ratchet,
        "quarter_ratchet":          quarter_ratchet,
        "three_quarter_ratchet":    three_quarter_ratchet,
        "be_then_lag2":             be_then_lag2,
        "be_then_lag3":             be_then_lag3,
        "tp2_be_then_standard":     tp2_be_then_standard,
        "tp3_be_then_standard":     tp3_be_then_standard,
        "skip_every_other_after_be":skip_every_other_after_be,
        "aggressive_plus1":         aggressive_plus1,
        "two_steps_forward":        two_steps_forward,
    }

ALL_MODES = list(make_ratchets(1.0, 0.9, [1.1, 1.2, 1.3]).keys())

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# TP ALLOCATION WEIGHTS  (must match bot's _TP_DIST exactly)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
_TP_DIST = {
    1:  [100],
    2:  [14, 86],
    3:  [41, 21, 65],
    4:  [14, 10, 16, 60],
    5:  [10, 12, 18, 25, 35],
    6:  [5, 7, 10, 15, 25, 38],
    7:  [4, 5, 7, 10, 15, 25, 34],
    8:  [3, 4, 5, 7, 10, 16, 25, 30],
    9:  [3, 3, 4, 6, 8, 12, 17, 22, 25],
    10: [3, 3, 4, 5, 7, 10, 13, 17, 20, 18],
    11: [3, 3, 3, 4, 6, 8, 11, 14, 17, 16, 15],
    12: [2, 3, 3, 4, 5, 7, 9, 12, 14, 15, 14, 12],
    13: [2, 2, 3, 4, 5, 6, 8, 10, 12, 13, 13, 12, 10],
    14: [2, 2, 3, 3, 4, 5, 7, 9, 11, 12, 13, 12, 10, 7],
    15: [2, 2, 2, 3, 4, 5, 6, 8, 10, 11, 12, 12, 10, 7, 6],
}

def tp_fractions(n):
    pcts = _TP_DIST.get(n, [100 / n] * n)
    total = sum(pcts)
    return [p / total for p in pcts]


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# BYBIT API — OHLC FETCHER
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def _bybit_get(endpoint, params):
    """Make a GET request to Bybit public market data (no auth needed for klines)."""
    url = BYBIT_BASE + endpoint + "?" + urllib.parse.urlencode(params)
    req = urllib.request.Request(url, headers={"User-Agent": "ratchet-backtest/1.0"})
    with urllib.request.urlopen(req, timeout=15) as resp:
        return json.loads(resp.read())


def fetch_ohlc(symbol, start_ts_ms, end_ts_ms):
    """
    Fetch 1H candles for symbol between start_ts_ms and end_ts_ms (Unix ms).
    Bybit returns: [startTime, open, high, low, close, volume, turnover]
    Returns list of dicts with keys: ts, open, high, low, close
    Paginates automatically.
    """
    candles = []
    end = end_ts_ms

    while True:
        time.sleep(RATE_LIMIT_PAUSE)
        data = _bybit_get("/v5/market/kline", {
            "category": "linear",
            "symbol":   symbol,
            "interval": CANDLE_INTERVAL,
            "start":    start_ts_ms,
            "end":      end,
            "limit":    MAX_CANDLES_PER_REQUEST,
        })

        if data.get("retCode") != 0:
            raise RuntimeError(f"Bybit API error: {data}")

        batch = data.get("result", {}).get("list", [])
        if not batch:
            break

        for c in batch:
            candles.append({
                "ts":    int(c[0]),
                "open":  float(c[1]),
                "high":  float(c[2]),
                "low":   float(c[3]),
                "close": float(c[4]),
            })

        # Bybit returns newest first — oldest candle in this batch sets the new end
        oldest_ts = min(int(c[0]) for c in batch)
        if oldest_ts <= start_ts_ms or len(batch) < MAX_CANDLES_PER_REQUEST:
            break
        end = oldest_ts - 1

    # Sort ascending by timestamp
    candles.sort(key=lambda c: c["ts"])
    # Filter strictly to window
    candles = [c for c in candles if start_ts_ms <= c["ts"] <= end_ts_ms]
    return candles


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# CANDLE-LEVEL SIMULATION
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def simulate_trade(trade, candles):
    """
    Walk candles and simulate all ratchet modes simultaneously.

    Returns dict of {mode_name: {"realised_R": float, "highest_tp": int, "close_reason": str}}
    """
    entry      = trade["entry_mid"]
    sl_orig    = trade["stop_loss"]
    tp_prices  = trade["targets"]
    is_long    = trade["side"] == "long"
    n_tps      = len(tp_prices)
    fracs      = tp_fractions(n_tps)
    risk       = abs(entry - sl_orig)

    if risk <= 0 or n_tps == 0 or not candles:
        return {m: {"realised_R": 0.0, "highest_tp": 0, "close_reason": "no_data"} for m in ALL_MODES}

    ratchet_fns = make_ratchets(entry, sl_orig, tp_prices)

    # Per-mode state
    states = {}
    for mode in ALL_MODES:
        states[mode] = {
            "sl":        sl_orig,
            "realised":  0.0,
            "remaining": 1.0,
            "tp_hit":    [False] * n_tps,
            "highest_tp":0,
            "done":      False,
            "reason":    "timeout",
        }

    for bar_i, candle in enumerate(candles):
        hi  = candle["high"]
        lo  = candle["low"]
        cls = candle["close"]

        for mode, st in states.items():
            if st["done"]:
                continue

            fn = ratchet_fns[mode]

            # ── Check TPs in order ──────────────────────────────────────────
            for i in range(n_tps):
                if st["tp_hit"][i]:
                    continue
                tp_px   = tp_prices[i]
                touched = (hi >= tp_px) if is_long else (lo <= tp_px)
                if touched:
                    st["tp_hit"][i]  = True
                    st["highest_tp"] = i + 1
                    alloc            = fracs[i]
                    tp_rr            = abs(tp_px - entry) / risk
                    st["realised"]  += alloc * tp_rr
                    st["remaining"] -= alloc

                    # Apply ratchet
                    new_sl = fn(i)
                    if new_sl is not None:
                        # For long: SL can only move up. For short: only move down.
                        if is_long:
                            st["sl"] = max(st["sl"], new_sl)
                        else:
                            st["sl"] = min(st["sl"], new_sl)

            # ── SL check ────────────────────────────────────────────────────
            sl_hit = (lo <= st["sl"]) if is_long else (hi >= st["sl"])
            if sl_hit:
                if st["highest_tp"] == 0:
                    st["realised"] = -1.0
                else:
                    sl_rr           = abs(st["sl"] - entry) / risk
                    st["realised"] += st["remaining"] * sl_rr
                st["done"]   = True
                st["reason"] = "sl"
                continue

            # ── All TPs hit ─────────────────────────────────────────────────
            if st["remaining"] <= 0.001:
                st["done"]   = True
                st["reason"] = "all_tps"

        # Early exit if all modes done
        if all(st["done"] for st in states.values()):
            break

    # Timeout: close remaining at last candle's close price
    for mode, st in states.items():
        if not st["done"]:
            last_close = candles[-1]["close"]
            to_rr      = (last_close - entry) * (1 if is_long else -1) / risk
            st["realised"] += st["remaining"] * to_rr
            st["reason"]    = "timeout"

    return {
        mode: {
            "realised_R":  round(st["realised"], 4),
            "highest_tp":  st["highest_tp"],
            "close_reason":st["reason"],
        }
        for mode, st in states.items()
    }


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# LOAD TRADES
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def load_trades(csv_path):
    trades = []
    with open(csv_path, newline="", encoding="utf-8-sig") as f:
        for row in csv.DictReader(f):
            try:
                targets = ast.literal_eval(row["targets"])
                entry   = float(row["entry_mid"])
                sl      = float(row["stop_loss"])
                n_tp    = int(row["number_of_targets"])
                side    = row["side"].lower()
                dt      = datetime.fromisoformat(row["date"])
                symbol  = row["symbol"] + row["pair"]
            except Exception:
                continue
            if n_tp == 0 or len(targets) == 0 or abs(entry - sl) == 0:
                continue
            trades.append({
                "message_id": row.get("message_id", ""),
                "date":       row.get("date", ""),
                "symbol":     symbol,
                "side":       side,
                "entry_mid":  entry,
                "stop_loss":  sl,
                "targets":    targets,
                "n_targets":  n_tp,
                "dt":         dt,
            })
    return trades


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# OUTPUT WRITERS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def write_per_trade_csv(results, out_path):
    cols = ["message_id", "date", "symbol", "side", "n_targets"]
    for m in ALL_MODES:
        cols += [f"{m}_R", f"{m}_tp", f"{m}_reason"]
    cols += ["best_mode", "best_R"]

    with open(out_path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        w.writerows(results)
    print(f"  → {out_path}")


def write_summary_csv(per_trade, out_path):
    # Group by n_targets
    by_n = defaultdict(lambda: defaultdict(list))
    overall = defaultdict(list)

    for row in per_trade:
        n = row["n_targets"]
        for m in ALL_MODES:
            r = row.get(f"{m}_R")
            if r is not None:
                by_n[n][m].append(r)
                overall[m].append(r)

    rows = []
    for n in sorted(by_n.keys()):
        d     = by_n[n]
        count = len(d[ALL_MODES[0]])
        avgs  = {m: sum(d[m]) / len(d[m]) for m in ALL_MODES if d[m]}
        best  = max(avgs, key=avgs.get)
        row   = {"n_targets": n, "trade_count": count, "best_mode": best}
        for m in ALL_MODES:
            row[f"{m}_avg_R"] = round(avgs.get(m, 0), 4)
        rows.append(row)

    cols = ["n_targets", "trade_count", "best_mode"] + [f"{m}_avg_R" for m in ALL_MODES]
    with open(out_path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        w.writerows(rows)
    print(f"  → {out_path}")
    return rows, overall


def write_report(summary_rows, overall, out_path, n_trades):
    overall_avgs = {m: sum(v) / len(v) for m, v in overall.items() if v}
    ranked = sorted(overall_avgs.items(), key=lambda x: x[1], reverse=True)

    lines = []
    W = 78
    lines.append("=" * W)
    lines.append("  RATCHET SL BACKTEST — BYBIT OHLC  |  CANDLE-ACCURATE SIMULATION")
    lines.append(f"  {n_trades} trades  |  {len(ALL_MODES)} ratchet modes  |  1H candles")
    lines.append("=" * W)

    lines.append("\nOVERALL AVERAGE R — ALL TRADES (ranked)")
    lines.append("-" * W)
    for rank, (m, avg) in enumerate(ranked, 1):
        bar    = "█" * max(0, int((avg + 1) * 12))
        marker = "  ← BEST" if rank == 1 else ("  ← 2nd" if rank == 2 else "")
        lines.append(f"  {rank:>2}. {m:<30} {avg:>+7.4f}R  {bar}{marker}")

    lines.append("\nOPTIMAL MODE BY NUMBER OF TARGETS")
    lines.append("-" * W)
    hdr = f"  {'NTPs':>5}  {'Trades':>6}  {'Best mode':<30}  {'Avg R':>8}  {'vs standard':>12}  {'vs no_ratchet':>14}"
    lines.append(hdr)
    lines.append("  " + "-" * (W - 2))

    for row in summary_rows:
        n     = row["n_targets"]
        cnt   = row["trade_count"]
        best  = row["best_mode"]
        b_avg = row[f"{best}_avg_R"]
        std   = row.get("standard_avg_R", 0)
        nor   = row.get("no_ratchet_avg_R", 0)
        lines.append(
            f"  {n:>5}  {cnt:>6}  {best:<30}  {b_avg:>+8.4f}R"
            f"  {b_avg - std:>+10.4f}R  {b_avg - nor:>+12.4f}R"
        )

    lines.append("\nFULL BREAKDOWN PER TP COUNT")
    lines.append("-" * W)
    for row in summary_rows:
        n   = row["n_targets"]
        cnt = row["trade_count"]
        lines.append(f"\n  ── {n} targets ({cnt} trades) ──")
        avgs = [(m, row[f"{m}_avg_R"]) for m in ALL_MODES]
        avgs.sort(key=lambda x: x[1], reverse=True)
        for rank, (m, avg) in enumerate(avgs, 1):
            bar    = "█" * max(0, int((avg + 1) * 10))
            marker = "  ← best" if rank == 1 else ""
            lines.append(f"    {rank:>2}. {m:<30} {avg:>+7.4f}R  {bar}{marker}")

    lines.append("\n" + "=" * W)
    lines.append("  RECOMMENDED RATCHET PER TP COUNT")
    lines.append("-" * W)
    for row in summary_rows:
        best = row["best_mode"]
        avg  = row[f"{best}_avg_R"]
        lines.append(f"    {row['n_targets']:>2} TPs  →  {best:<30}  ({avg:>+.4f}R avg)")
    lines.append(f"\n  Overall winner:  {ranked[0][0]}  ({ranked[0][1]:>+.4f}R avg)")
    lines.append("=" * W)

    text = "\n".join(lines)
    with open(out_path, "w") as f:
        f.write(text)
    print(f"  → {out_path}")
    print()
    print(text)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# MAIN
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--csv", default=r"C:\Users\louis\OneDrive\Louis\WSQ_ATB\output\trades_dataset.csv")
    parser.add_argument("--env", default=r"C:\Users\louis\OneDrive\Louis\WSQ_ATB\.env")
    parser.add_argument("--out", default=r"C:\Users\louis\OneDrive\Louis\WSQ_ATB\Trade_Analysis_Report_Claude\SL_Configuration")
    parser.add_argument("--resume", action="store_true",
                        help="Skip trades already in ratchet_results.csv")
    args = parser.parse_args()

    # Load .env
    env_path = Path(args.env)
    if not env_path.exists():
        print(f"ERROR: .env not found at {env_path}")
        sys.exit(1)
    load_dotenv(env_path)

    api_key    = os.getenv("BYBIT_API_KEY", "")
    api_secret = os.getenv("BYBIT_API_SECRET", "")
    if not api_key or not api_secret:
        print("ERROR: BYBIT_API_KEY / BYBIT_API_SECRET not set in .env")
        sys.exit(1)
    # Note: kline endpoint is public — keys loaded but not needed for OHLC.
    # Kept in case you extend this to private endpoints later.
    print(f"Loaded credentials from {env_path}  (key: {api_key[:6]}...)")

    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    results_path = out_dir / "ratchet_results.csv"
    summary_path = out_dir / "ratchet_summary.csv"
    report_path  = out_dir / "ratchet_report.txt"
    errors_path  = out_dir / "ratchet_errors.csv"

    trades = load_trades(args.csv)
    print(f"Loaded {len(trades)} trades from {args.csv}")
    print(f"Modes to test: {len(ALL_MODES)}")
    print(f"Output dir: {out_dir}")
    print()

    # Resume: load already-completed message_ids
    done_ids = set()
    existing_results = []
    if args.resume and results_path.exists():
        with open(results_path, newline="") as f:
            for row in csv.DictReader(f):
                done_ids.add(row["message_id"])
                existing_results.append(row)
        print(f"Resuming — {len(done_ids)} trades already done, skipping them.")

    errors = []
    all_results = list(existing_results)

    for i, trade in enumerate(trades):
        mid = trade["message_id"]
        if mid in done_ids:
            continue

        sym = trade["symbol"]
        dt  = trade["dt"]

        # Time window: signal bar to signal bar + MAX_HOLD_BARS hours + 1H buffer
        start_ms = int(dt.replace(tzinfo=timezone.utc).timestamp() * 1000)
        end_ms   = start_ms + (MAX_HOLD_BARS + 1) * 3600 * 1000

        progress = f"[{i+1}/{len(trades)}]"
        print(f"{progress} {sym:>15}  {dt.strftime('%Y-%m-%d %H:%M')}  ", end="", flush=True)

        try:
            candles = fetch_ohlc(sym, start_ms, end_ms)
            if not candles:
                raise ValueError("No candles returned")

            result = simulate_trade(trade, candles)

            best_mode = max(result, key=lambda m: result[m]["realised_R"])
            best_R    = result[best_mode]["realised_R"]

            row = {
                "message_id": mid,
                "date":       trade["date"],
                "symbol":     sym,
                "side":       trade["side"],
                "n_targets":  trade["n_targets"],
                "best_mode":  best_mode,
                "best_R":     best_R,
            }
            for m in ALL_MODES:
                r = result[m]
                row[f"{m}_R"]      = r["realised_R"]
                row[f"{m}_tp"]     = r["highest_tp"]
                row[f"{m}_reason"] = r["close_reason"]

            all_results.append(row)
            print(f"✓  {len(candles):>4} candles  best={best_mode} ({best_R:>+.3f}R)")

        except Exception as exc:
            print(f"✗  ERROR: {exc}")
            errors.append({"message_id": mid, "symbol": sym, "date": trade["date"], "error": str(exc)})

        # Write incrementally every 25 trades so progress is never lost
        if (i + 1) % 25 == 0:
            write_per_trade_csv(all_results, results_path)

    # Final write
    print(f"\nWriting outputs...")
    write_per_trade_csv(all_results, results_path)

    if errors:
        with open(errors_path, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=["message_id", "symbol", "date", "error"])
            w.writeheader()
            w.writerows(errors)
        print(f"  → {errors_path}  ({len(errors)} errors)")

    summary_rows, overall = write_summary_csv(all_results, summary_path)
    write_report(summary_rows, overall, report_path, len(all_results))

    print(f"\nDone.  {len(all_results)} trades simulated across {len(ALL_MODES)} modes.")
    if errors:
        print(f"  {len(errors)} trades failed (see ratchet_errors.csv) — re-run with --resume to retry.")


if __name__ == "__main__":
    main()
