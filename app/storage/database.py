"""
database.py – Async SQLite storage layer using aiosqlite.

All state is persisted here. The bot can restart without losing context.
"""

import json
import logging
import aiosqlite
from pathlib import Path
from typing import Optional, Dict, Any, List

log = logging.getLogger(__name__)

_CREATE_TABLES = """
PRAGMA journal_mode=WAL;

CREATE TABLE IF NOT EXISTS raw_messages (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    telegram_id     INTEGER UNIQUE,
    raw_text        TEXT,
    message_type    TEXT,
    received_at     TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS trades (
    id                      INTEGER PRIMARY KEY AUTOINCREMENT,
    signal_telegram_id      INTEGER UNIQUE,
    symbol                  TEXT NOT NULL,
    direction               TEXT NOT NULL,
    leverage                INTEGER,
    entry_low               REAL,
    entry_high              REAL,
    stop_loss               REAL,
    targets_json            TEXT,
    filled_size             REAL DEFAULT 0,
    avg_entry_price         REAL DEFAULT 0,
    state                   TEXT DEFAULT 'pending',
    break_even_activated    INTEGER DEFAULT 0,
    entries_cancelled       INTEGER DEFAULT 0,
    highest_tp_hit          INTEGER DEFAULT 0,
    -- Filter/tier metadata stored for logging and analysis
    tier                    TEXT DEFAULT 'BASE',
    size_mult               REAL DEFAULT 1.0,
    rsi_at_signal           REAL,
    mid_rsi_risk            INTEGER DEFAULT 0,
    created_at              TEXT DEFAULT (datetime('now')),
    updated_at              TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS orders (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    trade_id        INTEGER REFERENCES trades(id),
    bybit_order_id  TEXT UNIQUE,
    symbol          TEXT,
    order_type      TEXT,   -- entry | tp | sl | close
    side            TEXT,
    price           REAL,
    qty             REAL,
    status          TEXT DEFAULT 'open',
    created_at      TEXT DEFAULT (datetime('now')),
    updated_at      TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS alerts (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    alert_type  TEXT,
    message     TEXT,
    sent_at     TEXT DEFAULT (datetime('now'))
);
"""


class Database:
    def __init__(self, path: Path):
        self._path = path
        self._db: Optional[aiosqlite.Connection] = None

    async def connect(self):
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._db = await aiosqlite.connect(str(self._path))
        self._db.row_factory = aiosqlite.Row
        await self._db.executescript(_CREATE_TABLES)
        await self._migrate()
        log.info("Database connected: %s", self._path)

    async def _migrate(self):
        """Add new columns to existing databases without breaking them."""
        migrations = [
            "ALTER TABLE trades ADD COLUMN highest_tp_hit INTEGER DEFAULT 0",
            "ALTER TABLE trades ADD COLUMN tier TEXT DEFAULT 'BASE'",
            "ALTER TABLE trades ADD COLUMN size_mult REAL DEFAULT 1.0",
            "ALTER TABLE trades ADD COLUMN rsi_at_signal REAL",
            "ALTER TABLE trades ADD COLUMN mid_rsi_risk INTEGER DEFAULT 0",
        ]
        for sql in migrations:
            try:
                await self._db.execute(sql)
                await self._db.commit()
                col = sql.split("ADD COLUMN")[1].strip().split()[0]
                log.info("DB migration: added column %s", col)
            except Exception:
                pass  # Column already exists — fine

    async def close(self):
        if self._db:
            await self._db.close()

    # ── raw messages ──────────────────────────────────────────────────────────

    async def save_raw_message(self, telegram_id: int, raw_text: str, message_type: str) -> bool:
        """Returns False if duplicate (already processed)."""
        try:
            await self._db.execute(
                "INSERT OR IGNORE INTO raw_messages (telegram_id, raw_text, message_type) VALUES (?,?,?)",
                (telegram_id, raw_text, message_type),
            )
            await self._db.commit()
            # Check if it was a duplicate
            async with self._db.execute(
                "SELECT id FROM raw_messages WHERE telegram_id=?", (telegram_id,)
            ) as cur:
                row = await cur.fetchone()
            return row is not None
        except Exception as exc:
            log.error("save_raw_message error: %s", exc)
            return False

    async def is_duplicate_message(self, telegram_id: int) -> bool:
        async with self._db.execute(
            "SELECT id FROM raw_messages WHERE telegram_id=?", (telegram_id,)
        ) as cur:
            return await cur.fetchone() is not None

    # ── trades ────────────────────────────────────────────────────────────────

    async def upsert_trade(self, data: Dict[str, Any]) -> int:
        targets_json = json.dumps(data.get("targets", []))
        await self._db.execute(
            """INSERT INTO trades
               (signal_telegram_id, symbol, direction, leverage,
                entry_low, entry_high, stop_loss, targets_json, state,
                tier, size_mult, rsi_at_signal, mid_rsi_risk)
               VALUES
               (:signal_telegram_id,:symbol,:direction,:leverage,
                :entry_low,:entry_high,:stop_loss,:targets_json,:state,
                :tier,:size_mult,:rsi_at_signal,:mid_rsi_risk)
               ON CONFLICT(signal_telegram_id) DO UPDATE SET
                   state=excluded.state,
                   updated_at=datetime('now')
            """,
            {
                **data,
                "targets_json":  targets_json,
                "state":         data.get("state", "pending"),
                "tier":          data.get("tier", "BASE"),
                "size_mult":     data.get("size_mult", 1.0),
                "rsi_at_signal": data.get("rsi_at_signal"),
                "mid_rsi_risk":  data.get("mid_rsi_risk", 0),
            },
        )
        await self._db.commit()
        async with self._db.execute(
            "SELECT id FROM trades WHERE signal_telegram_id=?", (data["signal_telegram_id"],)
        ) as cur:
            row = await cur.fetchone()
        return row["id"]

    async def get_trade_by_symbol(self, symbol: str) -> Optional[Dict]:
        async with self._db.execute(
            "SELECT * FROM trades WHERE symbol=? AND state NOT IN ('closed','cancelled','sl_hit') ORDER BY id DESC LIMIT 1",
            (symbol,),
        ) as cur:
            row = await cur.fetchone()
        if row:
            d = dict(row)
            d["targets"] = json.loads(d.get("targets_json") or "[]")
            return d
        return None

    async def get_active_trades(self) -> List[Dict]:
        async with self._db.execute(
            "SELECT * FROM trades WHERE state NOT IN ('closed','cancelled','sl_hit')"
        ) as cur:
            rows = await cur.fetchall()
        result = []
        for row in rows:
            d = dict(row)
            d["targets"] = json.loads(d.get("targets_json") or "[]")
            result.append(d)
        return result

    async def update_trade(self, trade_id: int, **kwargs):
        if "targets" in kwargs:
            kwargs["targets_json"] = json.dumps(kwargs.pop("targets"))
        sets = ", ".join(f"{k}=?" for k in kwargs)
        vals = list(kwargs.values()) + [trade_id]
        await self._db.execute(
            f"UPDATE trades SET {sets}, updated_at=datetime('now') WHERE id=?", vals
        )
        await self._db.commit()

    async def update_trade_state(self, trade_id: int, state: str):
        await self.update_trade(trade_id, state=state)

    # ── orders ────────────────────────────────────────────────────────────────

    async def save_order(self, trade_id: int, bybit_order_id: str, symbol: str,
                         order_type: str, side: str, price: float, qty: float):
        await self._db.execute(
            """INSERT OR IGNORE INTO orders
               (trade_id, bybit_order_id, symbol, order_type, side, price, qty)
               VALUES (?,?,?,?,?,?,?)""",
            (trade_id, bybit_order_id, symbol, order_type, side, price, qty),
        )
        await self._db.commit()

    async def get_open_orders_for_trade(self, trade_id: int) -> List[Dict]:
        async with self._db.execute(
            "SELECT * FROM orders WHERE trade_id=? AND status='open'", (trade_id,)
        ) as cur:
            return [dict(r) for r in await cur.fetchall()]

    async def mark_order_status(self, bybit_order_id: str, status: str):
        await self._db.execute(
            "UPDATE orders SET status=?, updated_at=datetime('now') WHERE bybit_order_id=?",
            (status, bybit_order_id),
        )
        await self._db.commit()

    async def get_order_by_bybit_id(self, bybit_order_id: str) -> Optional[Dict]:
        async with self._db.execute(
            "SELECT * FROM orders WHERE bybit_order_id=?", (bybit_order_id,)
        ) as cur:
            row = await cur.fetchone()
        return dict(row) if row else None

    async def get_trade_by_id(self, trade_id: int) -> Optional[Dict]:
        async with self._db.execute(
            "SELECT * FROM trades WHERE id=?", (trade_id,)
        ) as cur:
            row = await cur.fetchone()
        if row:
            d = dict(row)
            d["targets"] = json.loads(d.get("targets_json") or "[]")
            return d
        return None

    # ── alerts ────────────────────────────────────────────────────────────────

    async def save_alert(self, alert_type: str, message: str):
        await self._db.execute(
            "INSERT INTO alerts (alert_type, message) VALUES (?,?)",
            (alert_type, message),
        )
        await self._db.commit()

    async def get_last_alert_time(self, alert_type: str) -> Optional[str]:
        async with self._db.execute(
            "SELECT sent_at FROM alerts WHERE alert_type=? ORDER BY id DESC LIMIT 1",
            (alert_type,),
        ) as cur:
            row = await cur.fetchone()
        return row["sent_at"] if row else None
