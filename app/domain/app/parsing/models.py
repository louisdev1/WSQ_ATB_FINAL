"""
models.py – Dataclasses for every parsed Telegram message type.
"""

from dataclasses import dataclass, field
from typing import List, Optional
from enum import Enum


class MessageType(str, Enum):
    NEW_SIGNAL = "new_signal"
    COMMENTARY = "commentary"
    CLOSE_ALL = "close_all"
    CLOSE_SYMBOL = "close_symbol"
    CANCEL_REMAINING_ENTRIES = "cancel_remaining_entries"
    MOVE_SL_BREAK_EVEN = "move_sl_break_even"
    MOVE_SL_PRICE = "move_sl_price"
    UPDATE_TARGETS = "update_targets"
    ADD_ENTRIES = "add_entries"
    MARKET_ENTRY = "market_entry"
    PARTIAL_CLOSE = "partial_close"
    CANCEL_SIGNAL = "cancel_signal"
    IGNORE = "ignore"


class Direction(str, Enum):
    LONG = "long"
    SHORT = "short"


@dataclass
class ParsedMessage:
    raw_text: str
    message_type: MessageType
    telegram_message_id: int = 0


@dataclass
class NewSignal(ParsedMessage):
    symbol: str = ""
    direction: Optional[Direction] = None
    leverage_min: int = 5
    leverage_max: int = 10
    entry_low: float = 0.0
    entry_high: float = 0.0
    targets: List[float] = field(default_factory=list)
    stop_loss: float = 0.0
    enter_partially: bool = True
    commentary: str = ""

    def __post_init__(self):
        self.message_type = MessageType.NEW_SIGNAL


@dataclass
class CloseAll(ParsedMessage):
    def __post_init__(self):
        self.message_type = MessageType.CLOSE_ALL


@dataclass
class CloseSymbol(ParsedMessage):
    symbol: str = ""

    def __post_init__(self):
        self.message_type = MessageType.CLOSE_SYMBOL


@dataclass
class CancelRemainingEntries(ParsedMessage):
    symbol: str = ""

    def __post_init__(self):
        self.message_type = MessageType.CANCEL_REMAINING_ENTRIES


@dataclass
class MoveSLBreakEven(ParsedMessage):
    symbol: str = ""

    def __post_init__(self):
        self.message_type = MessageType.MOVE_SL_BREAK_EVEN


@dataclass
class MoveSLPrice(ParsedMessage):
    symbol: str = ""
    price: float = 0.0

    def __post_init__(self):
        self.message_type = MessageType.MOVE_SL_PRICE


@dataclass
class UpdateTargets(ParsedMessage):
    symbol: str = ""
    targets: List[float] = field(default_factory=list)

    def __post_init__(self):
        self.message_type = MessageType.UPDATE_TARGETS


@dataclass
class AddEntries(ParsedMessage):
    symbol: str = ""
    entry_low: float = 0.0
    entry_high: float = 0.0

    def __post_init__(self):
        self.message_type = MessageType.ADD_ENTRIES


@dataclass
class MarketEntry(ParsedMessage):
    symbol: str = ""
    direction: Optional[Direction] = None

    def __post_init__(self):
        self.message_type = MessageType.MARKET_ENTRY


@dataclass
class PartialClose(ParsedMessage):
    symbol: str = ""
    percent: float = 50.0

    def __post_init__(self):
        self.message_type = MessageType.PARTIAL_CLOSE


@dataclass
class CancelSignal(ParsedMessage):
    symbol: str = ""

    def __post_init__(self):
        self.message_type = MessageType.CANCEL_SIGNAL


@dataclass
class Commentary(ParsedMessage):
    def __post_init__(self):
        self.message_type = MessageType.COMMENTARY


@dataclass
class Ignore(ParsedMessage):
    def __post_init__(self):
        self.message_type = MessageType.IGNORE
