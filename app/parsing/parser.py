"""
parser.py – Classify and parse raw Telegram message text into structured objects.

Strategy:
  1. Clean/normalise text
  2. Run classifiers in priority order
  3. Return the first matched ParsedMessage subclass
"""

import re
import logging
from typing import Optional, List

from app.parsing.models import (
    ParsedMessage, MessageType, Direction,
    NewSignal, CloseAll, CloseSymbol, CancelRemainingEntries,
    MoveSLBreakEven, MoveSLPrice, UpdateTargets, AddEntries,
    MarketEntry, PartialClose, CancelSignal, Commentary, Ignore,
)

log = logging.getLogger(__name__)

# ── helpers ───────────────────────────────────────────────────────────────────

def _clean(text: str) -> str:
    """Normalise whitespace; keep newlines for multi-line parsing."""
    return re.sub(r"[ \t]+", " ", text).strip()


def _extract_symbol(text: str) -> str:
    """Pull a crypto symbol from text like #AXLUSDT, AXLUSDT, $AXLUSDT, #1000LUNCUSDT.
    Also handles slash format: #ETH/USDT → ETHUSDT, #HBAR/USDT → HBARUSDT.
    """
    upper = text.upper()
    # Handle slash format first: #ETH/USDT or ETH/USDT → ETHUSDT
    m = re.search(r"#?\$?([A-Z0-9]{2,15})/USDT", upper)
    if m:
        return m.group(1) + "USDT"
    # Standard concatenated format: BTCUSDT, #CFXUSDT, 1MBABYDOGEUSDT
    m = re.search(r"#?\$?([A-Z0-9]{2,20}USDT)", upper)
    if m:
        return m.group(1).lstrip("$#")
    # Fallback: any ALL-CAPS word 3-12 chars
    m = re.search(r"\b([A-Z]{3,12})\b", text)
    return m.group(1) if m else ""


def _normalise_number(s: str) -> float:
    """
    Convert a raw number string to float, correctly handling both:
      - Thousands separators:  "72,260"  → 72260.0
      - Decimal separators:    "0,0412"  → 0.0412   (European style)

    Rules (in order):
      1. If the integer part (before first comma) is 0, it's always a decimal.
         e.g. "0,295" → 0.295, "0,0412" → 0.0412
      2. If a comma is followed by exactly 3 digits with no further separator,
         it's a thousands separator. e.g. "72,260" → 72260, "1,000,000" → 1000000
      3. Otherwise treat comma as decimal separator (European style).
    """
    s = s.strip().lstrip("$")
    # Rule 1: integer part is 0 → must be decimal
    if re.match(r"^0,", s):
        return float(s.replace(",", "."))
    # Rule 2: thousands separator — comma(s) each followed by exactly 3 digits
    if re.search(r",\d{3}(?!\d|[.,])", s):
        s = s.replace(",", "")
        return float(s)
    # Rule 3: decimal separator
    return float(s.replace(",", "."))


def _extract_price(text: str) -> float:
    """Extract first price from text, correctly handling thousands separators."""
    m = re.search(r"\$?([\d][\d,\.]*)", text)
    if m:
        try:
            return _normalise_number(m.group(1))
        except ValueError:
            pass
    return 0.0


def _extract_prices(text: str) -> List[float]:
    """Extract all prices from a line, correctly handling thousands separators."""
    # Remove parenthetical notes like (Short term), (Enter partially)
    text = re.sub(r"\([^)]*\)", "", text)
    # Match numbers that may contain commas or dots (e.g. 72,260 or 0.00412)
    nums = re.findall(r"\$?([\d][\d,\.]*\d|\d)", text)
    result = []
    for n in nums:
        try:
            result.append(_normalise_number(n))
        except ValueError:
            pass
    return result


def _direction(text: str) -> Optional[Direction]:
    t = text.lower()
    if re.search(r"\b(long|buy|bullish)\b", t):
        return Direction.LONG
    if re.search(r"\b(short|sell|bearish)\b", t):
        return Direction.SHORT
    return None


# ── classifiers ───────────────────────────────────────────────────────────────

def _is_new_signal(text: str) -> bool:
    low = text.lower()
    has_coin      = bool(re.search(r"coin\s*:", low))
    has_direction = bool(re.search(r"direction\s*:", low))
    has_entry     = bool(re.search(r"entry\s*:", low))
    has_sl        = bool(re.search(r"stop.?loss\s*:|stop\s*:|\bsl\s*:", low))
    return (has_coin or has_direction) and has_entry and has_sl


def _is_close_all(text: str) -> bool:
    low = text.lower()
    return bool(re.search(r"\bclose\s+all\b|\bexit\s+all\b|\bemergency\s+close\b", low))


def _is_close_symbol(text: str) -> bool:
    low = text.lower()
    # "close AXLUSDT", "exit BTCUSDT", "#AXLUSDT close the position", "all targets done, close position for X"
    return bool(re.search(
        r"(close|exit)\s+#?[A-Z0-9]{2,20}USDT\b"
        r"|#?[A-Z0-9]{2,20}USDT.{0,20}(close|exit)\b"
        r"|close\s+position\s+for\s+#?[A-Z0-9]{2,20}USDT",
        low, re.IGNORECASE,
    ))


def _is_cancel_remaining(text: str) -> bool:
    low = text.lower()
    return bool(re.search(
        r"cancel\s+(remaining|open)\s+(entries|orders|buy|sell)",
        low,
    ))


def _is_move_sl_be(text: str) -> bool:
    low = text.lower()
    return bool(re.search(
        r"(move\s+sl|move\s+stop|put\s+stop|set\s+stop).{0,30}(entry|break.?even|\bbe\b)",
        low,
    ))


def _is_move_sl_price(text: str) -> bool:
    low = text.lower()
    return bool(re.search(
        r"(move\s+stop|new\s+stop|update\s+stop|stop.loss|move\s+sl|sl\s+to)\s*.{0,20}\d",
        low,
    ))


def _is_update_targets(text: str) -> bool:
    low = text.lower()
    return bool(re.search(
        r"(new\s+targets?|update\s+tp|remove\s+tp|update\s+targets?)",
        low,
    ))


def _is_add_entries(text: str) -> bool:
    low = text.lower()
    return bool(re.search(
        r"(new\s+entry|add\s+entr|average\s+in|additional\s+entry)",
        low,
    ))


def _is_market_entry(text: str) -> bool:
    low = text.lower()
    return bool(re.search(r"\b(buy|sell|enter)\s+now\b", low))


def _is_partial_close(text: str) -> bool:
    low = text.lower()
    return bool(re.search(
        r"(close\s+\d+\s*%|close\s+half|take\s+partial|partial\s+(close|profit)|secure\s+profit)",
        low,
    ))


def _is_cancel_signal(text: str) -> bool:
    low = text.lower()
    return bool(re.search(
        r"(ignore\s+previous|cancel\s+previous|setup\s+invalidated|signal\s+cancelled|disregard)",
        low,
    ))


def _is_update_commentary(text: str) -> bool:
    """Update messages and stop/target hit notifications — logged but not traded."""
    if re.search(r"#[A-Z0-9]+(?:/USDT)?\s+UPDATE\s*:", text, re.IGNORECASE):
        return True
    if re.search(
        r"#[A-Z0-9]+(?:/USDT)?\s+(Stop\s+Target\s+Hit|stop\s+hit|target\s+hit|all\s+targets)",
        text, re.IGNORECASE,
    ):
        return True
    return False


# ── individual parsers ────────────────────────────────────────────────────────

def _parse_new_signal(raw: str, msg_id: int) -> NewSignal:
    lines = [l.strip() for l in raw.splitlines() if l.strip()]
    sig   = NewSignal(raw_text=raw, message_type=MessageType.NEW_SIGNAL,
                      telegram_message_id=msg_id)

    for line in lines:
        low = line.lower()

        if re.match(r"coin\s*:", low):
            # Strip parenthetical labels like "(Futures)", "(Perp)" from coin line
            clean_line = re.sub(r"\([^)]*\)", "", line)
            sig.symbol = _extract_symbol(clean_line)

        elif re.match(r"direction\s*:", low):
            d = _direction(line)
            if d:
                sig.direction = d

        elif sig.direction is None and re.search(r"\b(long|short|buy|sell|bullish|bearish)\b", low):
            # Infer direction from body lines like "Long Set-Up" when no Direction: field
            d = _direction(line)
            if d:
                sig.direction = d

        elif re.match(r"leverage\s*:", low):
            nums = re.findall(r"\d+", line)
            if len(nums) >= 2:
                sig.leverage_min, sig.leverage_max = int(nums[0]), int(nums[1])
            elif len(nums) == 1:
                sig.leverage_min = sig.leverage_max = int(nums[0])

        elif re.match(r"entry\s*:", low):
            prices = _extract_prices(line)
            if len(prices) >= 2:
                sig.entry_low  = min(prices[:2])
                sig.entry_high = max(prices[:2])
            elif len(prices) == 1:
                sig.entry_low = sig.entry_high = prices[0]
            sig.enter_partially = bool(re.search(r"partial(ly)?|buy\s+partial", low))

        elif re.match(r"targets?\s*:|tp\s*\d*\s*:", low):
            # Targets on a single line: "Targets: 1.0 - 2.0 - 3.0"
            prices = _extract_prices(line)
            if prices:
                sig.targets = prices

        elif re.match(r"target\s*\d+\s*:", low):
            # "Target 1: 1780" — strip the label number, extract only the price after the colon
            after_colon = line.split(":", 1)[1] if ":" in line else line
            prices = _extract_prices(after_colon)
            if prices:
                sig.targets.append(prices[0])

        elif re.match(r"stop.?loss\s*:|stop\s*:|\bsl\s*:", low):
            prices = _extract_prices(line)
            if prices:
                sig.stop_loss = prices[0]

    return sig


def _parse_close_symbol(raw: str, msg_id: int) -> CloseSymbol:
    sym = _extract_symbol(raw)
    return CloseSymbol(raw_text=raw, message_type=MessageType.CLOSE_SYMBOL,
                       telegram_message_id=msg_id, symbol=sym)


def _parse_cancel_remaining(raw: str, msg_id: int) -> CancelRemainingEntries:
    sym = _extract_symbol(raw)
    return CancelRemainingEntries(raw_text=raw, message_type=MessageType.CANCEL_REMAINING_ENTRIES,
                                  telegram_message_id=msg_id, symbol=sym)


def _parse_move_sl_be(raw: str, msg_id: int) -> MoveSLBreakEven:
    sym = _extract_symbol(raw)
    return MoveSLBreakEven(raw_text=raw, message_type=MessageType.MOVE_SL_BREAK_EVEN,
                           telegram_message_id=msg_id, symbol=sym)


def _parse_move_sl_price(raw: str, msg_id: int) -> MoveSLPrice:
    sym   = _extract_symbol(raw)
    price = _extract_price(raw)
    return MoveSLPrice(raw_text=raw, message_type=MessageType.MOVE_SL_PRICE,
                       telegram_message_id=msg_id, symbol=sym, price=price)


def _parse_update_targets(raw: str, msg_id: int) -> UpdateTargets:
    sym     = _extract_symbol(raw)
    targets = _extract_prices(raw)
    return UpdateTargets(raw_text=raw, message_type=MessageType.UPDATE_TARGETS,
                         telegram_message_id=msg_id, symbol=sym, targets=targets)


def _parse_add_entries(raw: str, msg_id: int) -> AddEntries:
    sym    = _extract_symbol(raw)
    prices = _extract_prices(raw)
    lo  = min(prices[:2]) if len(prices) >= 2 else (prices[0] if prices else 0.0)
    hi  = max(prices[:2]) if len(prices) >= 2 else lo
    return AddEntries(raw_text=raw, message_type=MessageType.ADD_ENTRIES,
                      telegram_message_id=msg_id, symbol=sym, entry_low=lo, entry_high=hi)


def _parse_market_entry(raw: str, msg_id: int) -> MarketEntry:
    sym = _extract_symbol(raw)
    d   = _direction(raw)
    return MarketEntry(raw_text=raw, message_type=MessageType.MARKET_ENTRY,
                       telegram_message_id=msg_id, symbol=sym, direction=d)


def _parse_partial_close(raw: str, msg_id: int) -> PartialClose:
    sym = _extract_symbol(raw)
    m   = re.search(r"(\d+)\s*%", raw)
    pct = float(m.group(1)) if m else 50.0
    if re.search(r"half", raw.lower()):
        pct = 50.0
    return PartialClose(raw_text=raw, message_type=MessageType.PARTIAL_CLOSE,
                        telegram_message_id=msg_id, symbol=sym, percent=pct)


def _parse_cancel_signal(raw: str, msg_id: int) -> CancelSignal:
    sym = _extract_symbol(raw)
    return CancelSignal(raw_text=raw, message_type=MessageType.CANCEL_SIGNAL,
                        telegram_message_id=msg_id, symbol=sym)


# ── public entry point ────────────────────────────────────────────────────────

def parse_message(raw_text: str, telegram_message_id: int = 0) -> ParsedMessage:
    """
    Main parser entry point.
    Returns a typed ParsedMessage subclass.
    """
    text = _clean(raw_text)

    try:
        if _is_new_signal(text):
            return _parse_new_signal(text, telegram_message_id)

        if _is_close_all(text):
            return CloseAll(raw_text=text, message_type=MessageType.CLOSE_ALL,
                            telegram_message_id=telegram_message_id)

        if _is_cancel_signal(text):
            return _parse_cancel_signal(text, telegram_message_id)

        if _is_close_symbol(text):
            return _parse_close_symbol(text, telegram_message_id)

        if _is_cancel_remaining(text):
            return _parse_cancel_remaining(text, telegram_message_id)

        if _is_move_sl_be(text):
            return _parse_move_sl_be(text, telegram_message_id)

        if _is_move_sl_price(text):
            return _parse_move_sl_price(text, telegram_message_id)

        if _is_update_targets(text):
            return _parse_update_targets(text, telegram_message_id)

        if _is_add_entries(text):
            return _parse_add_entries(text, telegram_message_id)

        if _is_market_entry(text):
            return _parse_market_entry(text, telegram_message_id)

        if _is_partial_close(text):
            return _parse_partial_close(text, telegram_message_id)

        if _is_update_commentary(text):
            return Commentary(raw_text=text, message_type=MessageType.COMMENTARY,
                              telegram_message_id=telegram_message_id)

    except Exception as exc:
        log.warning("Parser error for msg_id=%s: %s", telegram_message_id, exc)

    return Ignore(raw_text=text, message_type=MessageType.IGNORE,
                  telegram_message_id=telegram_message_id)
