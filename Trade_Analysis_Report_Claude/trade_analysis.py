"""
WSQ Signal Group — Comprehensive Trade Analysis
=================================================
Parses every trade signal from the Telegram export JSON,
links update messages (target hits, SL hits) back to signals,
and produces a full per-trade breakdown + aggregate stats.

Usage:  python trade_analysis.py
Input:  result.json from Trade_Analysis_Report_Claude folder
Output: trade_analysis_report.json in the same folder
"""

import json
import re
import sys
import os
from datetime import datetime
from collections import defaultdict, Counter

# ── Base directory: same folder as this script ──
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
INPUT_PATH = os.path.join(BASE_DIR, "result.json")
OUTPUT_PATH = os.path.join(BASE_DIR, "trade_analysis_report.json")

# ──────────────────────────────────────────────────────────
# 1. HELPERS
# ──────────────────────────────────────────────────────────

def extract_text(msg):
    """Flatten Telegram's mixed text format into a plain string."""
    raw = msg.get("text", "")
    if isinstance(raw, str):
        return raw
    parts = []
    for chunk in raw:
        if isinstance(chunk, str):
            parts.append(chunk)
        elif isinstance(chunk, dict):
            parts.append(chunk.get("text", ""))
    return "".join(parts)


def parse_numbers(s):
    """Extract all numeric values from a string (handles $ signs, commas, etc.)."""
    # Match numbers like 1.234, 0.00456, 12345, $1.23, etc.
    return [float(x) for x in re.findall(r'[\d]+\.?[\d]*', s)]


def parse_entry(text):
    """Extract entry range from signal text."""
    patterns = [
        r'(?:entry|buy)\s*(?:zone)?\s*[:=]\s*([\d\.\$\s,\-–]+)',
        r'(?:entry|buy)\s*[:=]\s*([\d\.\$\s,\-–]+?)(?:\$?\s*\(|$|\n)',
    ]
    for pat in patterns:
        m = re.search(pat, text, re.IGNORECASE)
        if m:
            nums = parse_numbers(m.group(1))
            if nums:
                return nums
    return []


def parse_targets(text):
    """Extract target prices from signal text."""
    patterns = [
        r'targets?\s*[:=]\s*([\d\.\$\s,\-–\+]+)',
    ]
    for pat in patterns:
        matches = re.findall(pat, text, re.IGNORECASE)
        for match in matches:
            nums = parse_numbers(match)
            if nums:
                return nums
    return []


def parse_stoploss(text):
    """Extract stop-loss from signal text."""
    patterns = [
        r'stop[\s\-]*loss\s*[:=]\s*([\d\.\$\s]+)',
        r'stoploss\s*[:=]\s*([\d\.\$\s]+)',
        r'sl\s*[:=]\s*([\d\.\$\s]+)',
    ]
    for pat in patterns:
        m = re.search(pat, text, re.IGNORECASE)
        if m:
            nums = parse_numbers(m.group(1))
            if nums:
                return nums[0]
    return None


def parse_symbol(text):
    """Extract trading symbol/pair."""
    # Try #SYMBOL/USDT or #SYMBOLUSDT or SYMBOL/USDT patterns
    patterns = [
        r'#?(\w+)[/\s]*USDT',
        r'#?(\w+)[/\s]*BTC',
        r'#?(\w+)[/\s]*BUSD',
        r'Coin\s*:\s*#?(\w+)',
    ]
    for pat in patterns:
        m = re.search(pat, text, re.IGNORECASE)
        if m:
            sym = m.group(1).upper().rstrip('/')
            # Clean up — remove USDT suffix if captured
            sym = re.sub(r'USDT$|BTC$|BUSD$', '', sym)
            if sym and len(sym) >= 2:
                return sym
    return None


def parse_pair(text):
    """Extract the full pair (e.g., MATIC/USDT)."""
    m = re.search(r'#?(\w+)\s*/\s*(USDT|BTC|BUSD)', text, re.IGNORECASE)
    if m:
        return f"{m.group(1).upper()}/{m.group(2).upper()}"
    # Try SYMBOLUSDT format (no slash)
    m = re.search(r'#?(\w{2,10})(USDT|BTC|BUSD)', text, re.IGNORECASE)
    if m:
        return f"{m.group(1).upper()}/{m.group(2).upper()}"
    return None


def parse_side(text):
    """Determine if the trade is LONG or SHORT."""
    text_lower = text.lower()
    # Explicit direction field
    m = re.search(r'direction\s*:\s*(long|short)', text_lower)
    if m:
        return m.group(1).upper()
    if re.search(r'short\s+set[\-\s]*up|short\s+entry|short\s+call', text_lower):
        return "SHORT"
    if re.search(r'long\s+set[\-\s]*up|long\s+entry|long\s+call|future\s+call|buy\s*:', text_lower):
        return "LONG"
    # If has "sell" entry keyword
    if re.search(r'sell\s*:', text_lower):
        return "SHORT"
    return "LONG"  # Default assumption


def parse_leverage(text):
    """Extract leverage recommendation."""
    m = re.search(r'(?:leverage|lev)\s*[:=]?\s*(\d+)\s*[-–]\s*(\d+)\s*x', text, re.IGNORECASE)
    if m:
        return f"{m.group(1)}-{m.group(2)}x"
    m = re.search(r'(\d+)\s*x\s*(?:leverage|lev)', text, re.IGNORECASE)
    if m:
        return f"{m.group(1)}x"
    return None


def parse_timeframe(text):
    """Extract mentioned timeframe (short/mid/long term)."""
    m = re.search(r'\((short[\s\-]*(?:mid|long)?[\s\-]*term)\)', text, re.IGNORECASE)
    if m:
        return m.group(1).strip().title()
    m = re.search(r'\((mid[\s\-]*(?:long)?[\s\-]*term)\)', text, re.IGNORECASE)
    if m:
        return m.group(1).strip().title()
    m = re.search(r'\((long[\s\-]*term)\)', text, re.IGNORECASE)
    if m:
        return m.group(1).strip().title()
    return None


def is_signal(text):
    """Determine if a message is a new trade signal."""
    t = text.lower()
    has_entry = bool(re.search(r'(buy\s*:|entry\s*:|entry\s+zone)', t))
    has_targets = bool(re.search(r'targets?\s*:', t))
    has_sl = bool(re.search(r'(stop[\s\-]*loss|stoploss|sl\s*:)', t))
    return has_entry and has_targets and has_sl


def is_update(text):
    """Determine if a message is a trade update / result notification."""
    t = text.lower()
    # Classic pattern: "SYMBOL UPDATE: ..."
    if 'update' in t and (
        'target' in t or 'sl' in t or 'stop' in t or 
        'done' in t or 'hit' in t or 'reached' in t or 'booked' in t
    ):
        return True
    # Result messages without "update" word — e.g. "Target 1, 2, 3 done nicely✅"
    if re.search(r'target\s*\d.*done|all\s*target.*done|target.*done.*nicely', t):
        return True
    # SL messages without "update" word — e.g. "SL hit❌" or "Stoploss hit"
    if re.search(r'sl\s*hit|sl\s*❌|sl\s*🛑|stoploss\s*hit|stopped\s*out', t):
        return True
    # "BOOM" style results — e.g. "#EOS/USDT BOOM💥 7 Targets done"
    if 'boom' in t and ('target' in t or 'done' in t):
        return True
    # Profit booking without update word — "So far X% profit"
    if re.search(r'so far.*profit|profit.*booked', t) and 'target' in t:
        return True
    return False


def parse_update_targets_hit(text):
    """From an update message, extract which target numbers were hit."""
    t = text.lower()
    hit = set()
    
    # "Target 1,2,3 done" / "Target 1, 2 done" / "Target 1,2,3,4,5 done"
    m = re.search(r'target\s*([\d\s,and&]+)\s*(?:done|also done|complete|hit|reached|nicely)', t)
    if m:
        nums = [int(x) for x in re.findall(r'\d+', m.group(1))]
        hit.update(nums)
    
    # "All targets done/hit"
    if re.search(r'all\s+targets?\s+(?:done|hit|reached|complete)', t):
        hit.add(-1)  # Sentinel for "all"
    
    # "first target done"
    if re.search(r'first\s+target\s+(?:done|hit|reached|complete)', t):
        hit.add(1)
    
    # "Target 3 also done"
    m = re.search(r'target\s+(\d+)\s+also\s+(?:done|hit|reached|complete)', t)
    if m:
        hit.add(int(m.group(1)))
    
    # "Target 1 done"
    m = re.search(r'target\s+(\d+)\s+(?:done|hit|reached|complete)', t)
    if m:
        hit.add(int(m.group(1)))
    
    return hit


def parse_update_sl_hit(text):
    """Check if update indicates stop-loss was hit."""
    t = text.lower()
    # Broad patterns for SL hit
    if re.search(r'sl\s*hit|sl\s*❌|sl\s*🛑', t):
        return True
    if re.search(r'stop[\s\-]*loss\s*hit|stoploss\s*hit', t):
        return True
    if re.search(r'stopped\s+out|sl\s+done|sl\s+triggered', t):
        return True
    # "SL❌" or "Stoploss hit❌" or "Stoploss Hit🛑"
    if re.search(r'stoploss\s*hit|stoploss\s*❌|stoploss\s*🛑', t):
        return True
    # "small loss booked" without target context
    if re.search(r'loss\s+booked', t) and 'target' not in t:
        return True
    return False


def parse_update_symbol(text):
    """Extract symbol from an update message."""
    # #SYMBOLUSDT UPDATE or SYMBOL/USDT UPDATE or SYMBOL UPDATE
    m = re.search(r'#?(\w+?)(?:USDT|/USDT)?\s+update', text, re.IGNORECASE)
    if m:
        sym = m.group(1).upper()
        sym = re.sub(r'USDT$|BTC$|BUSD$', '', sym)
        if sym and len(sym) >= 2:
            return sym
    # Fallback: #SYMBOL/USDT at start of message (no "update" word)
    m = re.search(r'^#?(\w+?)(?:USDT|/USDT)', text, re.IGNORECASE | re.MULTILINE)
    if m:
        sym = m.group(1).upper()
        sym = re.sub(r'USDT$|BTC$|BUSD$', '', sym)
        if sym and len(sym) >= 2:
            return sym
    return None


# ──────────────────────────────────────────────────────────
# 2. MAIN PIPELINE
# ──────────────────────────────────────────────────────────

def main(json_path):
    with open(json_path, encoding="utf-8") as f:
        data = json.load(f)

    messages = data["messages"]
    msg_map = {m["id"]: m for m in messages}

    # ── Pass 1: Identify signals & updates ──
    signals = []
    updates = []

    for m in messages:
        if m.get("type") != "message":
            continue
        text = extract_text(m)
        if not text.strip():
            continue

        if is_signal(text):
            signals.append({
                "msg_id": m["id"],
                "date": m.get("date", ""),
                "text": text,
            })
        elif is_update(text):
            updates.append({
                "msg_id": m["id"],
                "date": m.get("date", ""),
                "text": text,
                "reply_to": m.get("reply_to_message_id"),
            })

    print(f"Signals found: {len(signals)}")
    print(f"Update messages found: {len(updates)}")

    # ── Pass 2: Parse each signal ──
    trades = []
    signal_ids = set()

    for sig in signals:
        text = sig["text"]
        symbol = parse_symbol(text)
        pair = parse_pair(text)
        side = parse_side(text)
        entry_vals = parse_entry(text)
        targets = parse_targets(text)
        stoploss = parse_stoploss(text)
        leverage = parse_leverage(text)
        timeframe = parse_timeframe(text)

        if not symbol:
            continue

        entry_mid = sum(entry_vals) / len(entry_vals) if entry_vals else None

        # Compute R:R per target
        rr_per_target = {}
        if entry_mid and stoploss and stoploss != entry_mid:
            risk = abs(entry_mid - stoploss)
            for i, tp in enumerate(targets, 1):
                reward = abs(tp - entry_mid)
                rr_per_target[f"tp{i}_R"] = round(reward / risk, 3) if risk > 0 else None

        # SL distance %
        sl_pct = None
        if entry_mid and stoploss:
            sl_pct = round(abs(entry_mid - stoploss) / entry_mid * 100, 3)

        # Entry range %
        entry_range_pct = None
        if len(entry_vals) >= 2:
            entry_range_pct = round(abs(max(entry_vals) - min(entry_vals)) / max(entry_vals) * 100, 3)

        # Target distances %
        target_pcts = []
        if entry_mid:
            for tp in targets:
                target_pcts.append(round(abs(tp - entry_mid) / entry_mid * 100, 3))

        trade = {
            "message_id": sig["msg_id"],
            "date": sig["date"],
            "symbol": symbol,
            "pair": pair or f"{symbol}/USDT",
            "side": side,
            "leverage": leverage,
            "timeframe": timeframe,
            "entry_values": entry_vals,
            "entry_mid": round(entry_mid, 8) if entry_mid else None,
            "stop_loss": stoploss,
            "stop_loss_pct": sl_pct,
            "entry_range_pct": entry_range_pct,
            "targets": targets,
            "target_distances_pct": target_pcts,
            "num_targets": len(targets),
            "rr_per_target": rr_per_target,
            # Result fields — filled in Pass 3
            "highest_target_hit": 0,
            "sl_hit": False,
            "outcome": "UNKNOWN",
            "linked_updates": [],
        }
        trades.append(trade)
        signal_ids.add(sig["msg_id"])

    print(f"Parsed trades: {len(trades)}")

    # ── Pass 3: Link updates → signals ──
    # Build a lookup: symbol → list of trades (sorted by date)
    symbol_trades = defaultdict(list)
    id_to_trade = {}
    for t in trades:
        symbol_trades[t["symbol"]].append(t)
        id_to_trade[t["message_id"]] = t

    for upd in updates:
        text = upd["text"]
        linked_trade = None

        # Method 1: reply_to links directly to the signal
        if upd["reply_to"] and upd["reply_to"] in id_to_trade:
            linked_trade = id_to_trade[upd["reply_to"]]
        
        # Method 2: reply_to links to another update that links to the signal
        if not linked_trade and upd["reply_to"]:
            # Walk the reply chain
            seen = set()
            rid = upd["reply_to"]
            while rid and rid not in id_to_trade and rid not in seen:
                seen.add(rid)
                parent = msg_map.get(rid)
                if parent:
                    rid = parent.get("reply_to_message_id")
                else:
                    break
            if rid and rid in id_to_trade:
                linked_trade = id_to_trade[rid]

        # Method 3: Match by symbol — find the most recent open trade for this symbol
        if not linked_trade:
            upd_symbol = parse_update_symbol(text)
            if upd_symbol and upd_symbol in symbol_trades:
                upd_dt = upd["date"]
                candidates = [
                    t for t in symbol_trades[upd_symbol]
                    if t["date"] <= upd_dt
                ]
                if candidates:
                    linked_trade = candidates[-1]  # Most recent prior signal

        if not linked_trade:
            continue

        # Parse what the update says
        targets_hit = parse_update_targets_hit(text)
        sl_hit = parse_update_sl_hit(text)

        linked_trade["linked_updates"].append({
            "msg_id": upd["msg_id"],
            "date": upd["date"],
            "targets_hit": sorted(targets_hit) if targets_hit else [],
            "sl_hit": sl_hit,
            "text_snippet": text[:200],
        })

        # Update highest target
        for th in targets_hit:
            if th == -1:  # "all targets"
                linked_trade["highest_target_hit"] = linked_trade["num_targets"]
            elif th > linked_trade["highest_target_hit"]:
                linked_trade["highest_target_hit"] = th

        if sl_hit:
            linked_trade["sl_hit"] = True

    # ── Pass 4: Determine outcomes ──
    for t in trades:
        ht = t["highest_target_hit"]
        if t["sl_hit"] and ht == 0:
            t["outcome"] = "LOSS"
        elif ht >= t["num_targets"] and t["num_targets"] > 0:
            t["outcome"] = "FULL_TP"
        elif ht > 0:
            t["outcome"] = "PARTIAL_TP"
        elif t["sl_hit"] and ht > 0:
            t["outcome"] = "PARTIAL_TP_THEN_SL"
        elif len(t["linked_updates"]) == 0:
            t["outcome"] = "NO_UPDATE"
        else:
            t["outcome"] = "UNKNOWN"

    # ──────────────────────────────────────────────────────
    # 3. AGGREGATE STATISTICS
    # ──────────────────────────────────────────────────────

    total = len(trades)
    outcomes = Counter(t["outcome"] for t in trades)
    sides = Counter(t["side"] for t in trades)

    profitable = sum(1 for t in trades if t["outcome"] in ("FULL_TP", "PARTIAL_TP", "PARTIAL_TP_THEN_SL"))
    losses = outcomes.get("LOSS", 0)

    targets_hit_list = [t["highest_target_hit"] for t in trades if t["highest_target_hit"] > 0]
    avg_targets_hit = sum(targets_hit_list) / len(targets_hit_list) if targets_hit_list else 0
    median_targets_hit = sorted(targets_hit_list)[len(targets_hit_list)//2] if targets_hit_list else 0

    sl_pcts = [t["stop_loss_pct"] for t in trades if t["stop_loss_pct"] is not None]
    avg_sl_pct = sum(sl_pcts) / len(sl_pcts) if sl_pcts else 0

    num_targets_list = [t["num_targets"] for t in trades]
    avg_num_targets = sum(num_targets_list) / len(num_targets_list) if num_targets_list else 0

    # R:R analysis (cap at 50 to filter parsing artifacts)
    all_rrs = []
    for t in trades:
        if t["highest_target_hit"] > 0 and t["rr_per_target"]:
            key = f"tp{t['highest_target_hit']}_R"
            rr = t["rr_per_target"].get(key)
            if rr is not None and rr < 50:
                all_rrs.append(rr)

    avg_realized_rr = sum(all_rrs) / len(all_rrs) if all_rrs else 0

    # Per-symbol stats
    symbol_stats = defaultdict(lambda: {"total": 0, "wins": 0, "losses": 0, "avg_targets": []})
    for t in trades:
        s = t["symbol"]
        symbol_stats[s]["total"] += 1
        if t["outcome"] in ("FULL_TP", "PARTIAL_TP", "PARTIAL_TP_THEN_SL"):
            symbol_stats[s]["wins"] += 1
        elif t["outcome"] == "LOSS":
            symbol_stats[s]["losses"] += 1
        if t["highest_target_hit"] > 0:
            symbol_stats[s]["avg_targets"].append(t["highest_target_hit"])

    # Per-year stats
    year_stats = defaultdict(lambda: {"total": 0, "wins": 0, "losses": 0, "targets": []})
    for t in trades:
        yr = t["date"][:4] if t["date"] else "UNKNOWN"
        year_stats[yr]["total"] += 1
        if t["outcome"] in ("FULL_TP", "PARTIAL_TP", "PARTIAL_TP_THEN_SL"):
            year_stats[yr]["wins"] += 1
        elif t["outcome"] == "LOSS":
            year_stats[yr]["losses"] += 1
        if t["highest_target_hit"] > 0:
            year_stats[yr]["targets"].append(t["highest_target_hit"])

    # Per-side stats
    side_stats = {}
    for side_val in ["LONG", "SHORT"]:
        side_trades = [t for t in trades if t["side"] == side_val]
        if side_trades:
            sw = sum(1 for t in side_trades if t["outcome"] in ("FULL_TP", "PARTIAL_TP", "PARTIAL_TP_THEN_SL"))
            sl_count = sum(1 for t in side_trades if t["outcome"] == "LOSS")
            side_stats[side_val] = {
                "total": len(side_trades),
                "wins": sw,
                "losses": sl_count,
                "win_rate": round(sw / len(side_trades) * 100, 2) if side_trades else 0,
            }

    # Day-of-week stats
    dow_stats = defaultdict(lambda: {"total": 0, "wins": 0, "losses": 0})
    for t in trades:
        if t["date"]:
            try:
                dt = datetime.fromisoformat(t["date"])
                dow = dt.strftime("%A")
                dow_stats[dow]["total"] += 1
                if t["outcome"] in ("FULL_TP", "PARTIAL_TP", "PARTIAL_TP_THEN_SL"):
                    dow_stats[dow]["wins"] += 1
                elif t["outcome"] == "LOSS":
                    dow_stats[dow]["losses"] += 1
            except:
                pass

    # Hour-of-day stats
    hour_stats = defaultdict(lambda: {"total": 0, "wins": 0, "losses": 0})
    for t in trades:
        if t["date"]:
            try:
                dt = datetime.fromisoformat(t["date"])
                hour_stats[dt.hour]["total"] += 1
                if t["outcome"] in ("FULL_TP", "PARTIAL_TP", "PARTIAL_TP_THEN_SL"):
                    hour_stats[dt.hour]["wins"] += 1
                elif t["outcome"] == "LOSS":
                    hour_stats[dt.hour]["losses"] += 1
            except:
                pass

    # Monthly performance
    month_stats = defaultdict(lambda: {"total": 0, "wins": 0, "losses": 0})
    for t in trades:
        if t["date"]:
            ym = t["date"][:7]
            month_stats[ym]["total"] += 1
            if t["outcome"] in ("FULL_TP", "PARTIAL_TP", "PARTIAL_TP_THEN_SL"):
                month_stats[ym]["wins"] += 1
            elif t["outcome"] == "LOSS":
                month_stats[ym]["losses"] += 1

    # Top symbols by frequency
    top_symbols = sorted(symbol_stats.items(), key=lambda x: x[1]["total"], reverse=True)[:20]

    # Target distribution (how many trades hit exactly N targets)
    target_distribution = Counter(t["highest_target_hit"] for t in trades)

    # ──────────────────────────────────────────────────────
    # 4. PRINT REPORT
    # ──────────────────────────────────────────────────────

    print("\n" + "=" * 60)
    print("  WSQ SIGNAL GROUP — FULL TRADE ANALYSIS")
    print("=" * 60)

    print(f"\n{'OVERALL SUMMARY':=^60}")
    print(f"  Total trades parsed:       {total}")
    print(f"  Profitable trades:         {profitable}  ({profitable/total*100:.1f}%)")
    print(f"  Loss trades:               {losses}  ({losses/total*100:.1f}%)")
    print(f"  Full TP:                   {outcomes.get('FULL_TP', 0)}  ({outcomes.get('FULL_TP', 0)/total*100:.1f}%)")
    print(f"  Partial TP:                {outcomes.get('PARTIAL_TP', 0)}  ({outcomes.get('PARTIAL_TP', 0)/total*100:.1f}%)")
    print(f"  Partial TP then SL:        {outcomes.get('PARTIAL_TP_THEN_SL', 0)}")
    print(f"  No update found:           {outcomes.get('NO_UPDATE', 0)}")
    print(f"  Unknown outcome:           {outcomes.get('UNKNOWN', 0)}")

    print(f"\n{'TARGET STATS':=^60}")
    print(f"  Avg targets per signal:    {avg_num_targets:.1f}")
    print(f"  Avg targets hit (winners): {avg_targets_hit:.2f}")
    print(f"  Median targets hit:        {median_targets_hit}")
    print(f"  Avg SL distance:           {avg_sl_pct:.2f}%")
    print(f"  Avg realized R:R:          {avg_realized_rr:.2f}")

    print(f"\n{'TARGET HIT DISTRIBUTION':=^60}")
    for n in sorted(target_distribution.keys()):
        count = target_distribution[n]
        bar = "█" * (count // 5)
        label = f"  TP {n}" if n > 0 else "  No TP"
        print(f"{label:>12}: {count:>5}  {bar}")

    print(f"\n{'SIDE BREAKDOWN':=^60}")
    for side_val, ss in side_stats.items():
        print(f"  {side_val}: {ss['total']} trades | {ss['wins']} wins | {ss['losses']} losses | {ss['win_rate']}% win rate")

    print(f"\n{'YEARLY PERFORMANCE':=^60}")
    for yr in sorted(year_stats.keys()):
        ys = year_stats[yr]
        wr = round(ys["wins"] / ys["total"] * 100, 1) if ys["total"] > 0 else 0
        avg_t = sum(ys["targets"]) / len(ys["targets"]) if ys["targets"] else 0
        print(f"  {yr}: {ys['total']:>4} trades | {ys['wins']:>4} wins | {ys['losses']:>3} losses | {wr:>5.1f}% WR | avg targets hit: {avg_t:.1f}")

    print(f"\n{'MONTHLY PERFORMANCE':=^60}")
    for ym in sorted(month_stats.keys()):
        ms = month_stats[ym]
        wr = round(ms["wins"] / ms["total"] * 100, 1) if ms["total"] > 0 else 0
        bar = "█" * ms["total"]
        print(f"  {ym}: {ms['total']:>3} trades | {ms['wins']:>3}W {ms['losses']:>3}L | {wr:>5.1f}% WR  {bar}")

    print(f"\n{'DAY-OF-WEEK PERFORMANCE':=^60}")
    day_order = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    for dow in day_order:
        if dow in dow_stats:
            ds = dow_stats[dow]
            wr = round(ds["wins"] / ds["total"] * 100, 1) if ds["total"] > 0 else 0
            print(f"  {dow:<10}: {ds['total']:>4} trades | {ds['wins']:>4}W {ds['losses']:>3}L | {wr:>5.1f}% WR")

    print(f"\n{'HOUR-OF-DAY PERFORMANCE (UTC)':=^60}")
    for h in sorted(hour_stats.keys()):
        hs = hour_stats[h]
        wr = round(hs["wins"] / hs["total"] * 100, 1) if hs["total"] > 0 else 0
        bar = "█" * (hs["total"] // 2)
        print(f"  {h:>2}:00: {hs['total']:>4} trades | {hs['wins']:>3}W {hs['losses']:>3}L | {wr:>5.1f}% WR  {bar}")

    print(f"\n{'TOP 20 SYMBOLS':=^60}")
    for sym, ss in top_symbols:
        wr = round(ss["wins"] / ss["total"] * 100, 1) if ss["total"] > 0 else 0
        avg_t = sum(ss["avg_targets"]) / len(ss["avg_targets"]) if ss["avg_targets"] else 0
        print(f"  {sym:<12}: {ss['total']:>3} trades | {ss['wins']:>3}W {ss['losses']:>3}L | {wr:>5.1f}% WR | avg targets: {avg_t:.1f}")

    # ── Consecutive streaks ──
    print(f"\n{'STREAKS':=^60}")
    sorted_trades = sorted(trades, key=lambda t: t["date"])
    max_win_streak = 0
    max_loss_streak = 0
    cur_win = 0
    cur_loss = 0
    for t in sorted_trades:
        if t["outcome"] in ("FULL_TP", "PARTIAL_TP", "PARTIAL_TP_THEN_SL"):
            cur_win += 1
            cur_loss = 0
        elif t["outcome"] == "LOSS":
            cur_loss += 1
            cur_win = 0
        else:
            cur_win = 0
            cur_loss = 0
        max_win_streak = max(max_win_streak, cur_win)
        max_loss_streak = max(max_loss_streak, cur_loss)
    print(f"  Longest win streak:   {max_win_streak}")
    print(f"  Longest loss streak:  {max_loss_streak}")

    # ── Time to resolution ──
    resolution_hours = []
    for t in trades:
        if t["linked_updates"] and t["date"]:
            try:
                sig_dt = datetime.fromisoformat(t["date"])
                last_upd = max(t["linked_updates"], key=lambda u: u["date"])
                upd_dt = datetime.fromisoformat(last_upd["date"])
                delta_h = (upd_dt - sig_dt).total_seconds() / 3600
                if 0 < delta_h < 30 * 24:  # sanity: < 30 days
                    resolution_hours.append(delta_h)
            except:
                pass

    if resolution_hours:
        avg_res = sum(resolution_hours) / len(resolution_hours)
        med_res = sorted(resolution_hours)[len(resolution_hours) // 2]
        print(f"\n{'TIME TO RESOLUTION':=^60}")
        print(f"  Avg time to last update:    {avg_res:.1f} hours ({avg_res/24:.1f} days)")
        print(f"  Median time to last update: {med_res:.1f} hours ({med_res/24:.1f} days)")

    # ──────────────────────────────────────────────────────
    # 5. EXPORT
    # ──────────────────────────────────────────────────────

    # Clean up for JSON export (convert sets, etc.)
    export_trades = []
    for t in trades:
        et = dict(t)
        for upd in et["linked_updates"]:
            upd["targets_hit"] = list(upd["targets_hit"]) if isinstance(upd["targets_hit"], set) else upd["targets_hit"]
        export_trades.append(et)

    report = {
        "summary": {
            "total_trades": total,
            "profitable": profitable,
            "losses": losses,
            "win_rate_pct": round(profitable / total * 100, 2) if total > 0 else 0,
            "full_tp": outcomes.get("FULL_TP", 0),
            "partial_tp": outcomes.get("PARTIAL_TP", 0),
            "no_update": outcomes.get("NO_UPDATE", 0),
            "avg_targets_hit": round(avg_targets_hit, 2),
            "median_targets_hit": median_targets_hit,
            "avg_sl_distance_pct": round(avg_sl_pct, 2),
            "avg_realized_rr": round(avg_realized_rr, 2),
            "max_win_streak": max_win_streak,
            "max_loss_streak": max_loss_streak,
        },
        "yearly": {yr: {
            "total": ys["total"],
            "wins": ys["wins"],
            "losses": ys["losses"],
            "win_rate": round(ys["wins"] / ys["total"] * 100, 1) if ys["total"] > 0 else 0,
        } for yr, ys in sorted(year_stats.items())},
        "side_stats": side_stats,
        "top_symbols": {sym: {
            "total": ss["total"],
            "wins": ss["wins"],
            "losses": ss["losses"],
            "win_rate": round(ss["wins"] / ss["total"] * 100, 1) if ss["total"] > 0 else 0,
        } for sym, ss in top_symbols},
        "trades": export_trades,
    }

    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, default=str)
    print(f"\nDetailed report saved to: {OUTPUT_PATH}")
    print(f"({len(export_trades)} trades with full per-trade detail)")


if __name__ == "__main__":
    path = sys.argv[1] if len(sys.argv) > 1 else INPUT_PATH
    main(path)
