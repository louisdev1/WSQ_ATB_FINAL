"""
seed_trades.py – One-time script to register existing Bybit positions into the bot DB.

Run ONCE with the bot STOPPED, then restart the bot.
The bot will pick up these trades on startup and resume managing them.

Usage:
    python seed_trades.py
"""

import asyncio
import json
import sys
from pathlib import Path

# Make sure app imports work when run from project root
sys.path.insert(0, str(Path(__file__).resolve().parent))

from app.config import config
from app.storage.database import Database


TRADES = [
    {
        "symbol":          "LTCUSDT",
        "direction":       "long",
        "leverage":        10,
        "entry_low":       57.6,
        "entry_high":      57.6,
        "avg_entry_price": 57.6,
        "filled_size":     1.4,
        "stop_loss":       54.6,
        "targets":         [58.8, 60.0, 61.2, 62.6, 66.0],
        "state":           "active",
    },
    {
        "symbol":          "XTZUSDT",
        "direction":       "long",
        "leverage":        10,
        "entry_low":       0.375,
        "entry_high":      0.387,
        "avg_entry_price": 0.385,
        "filled_size":     139.3,
        "stop_loss":       0.366,
        "targets":         [0.396, 0.406, 0.416, 0.425, 0.45],
        "state":           "active",
    },
    {
        "symbol":          "QNTUSDT",
        "direction":       "long",
        "leverage":        10,
        "entry_low":       64.0,
        "entry_high":      66.5,
        "avg_entry_price": 66.5,
        "filled_size":     0.57,
        "stop_loss":       62.0,
        "targets":         [68.5, 70.0, 71.5, 73.0, 76.0, 80.0, 84.0],
        "state":           "active",
    },
]


async def main():
    db = Database(config.db_path)
    await db.connect()

    # Use negative IDs so they never clash with real Telegram message IDs
    fake_telegram_id = -1000

    inserted = 0
    skipped  = 0

    for trade in TRADES:
        symbol = trade["symbol"]

        # Skip if already tracked
        existing = await db.get_trade_by_symbol(symbol)
        if existing:
            print(f"SKIP  {symbol} — already active in DB (state: {existing['state']})")
            skipped += 1
            continue

        trade_id = await db.upsert_trade({
            "signal_telegram_id": fake_telegram_id,
            "symbol":    trade["symbol"],
            "direction": trade["direction"],
            "leverage":  trade["leverage"],
            "entry_low":  trade["entry_low"],
            "entry_high": trade["entry_high"],
            "stop_loss":  trade["stop_loss"],
            "targets":    trade["targets"],
            "state":      "pending",   # upsert starts as pending, then we update
        })

        # Update with fill data and set to active
        await db.update_trade(
            trade_id,
            filled_size=trade["filled_size"],
            avg_entry_price=trade["avg_entry_price"],
            entries_cancelled=1,   # no pending entries — position is already live
            state="active",
        )

        print(
            f"OK    {symbol} id={trade_id} "
            f"size={trade['filled_size']} entry={trade['avg_entry_price']} "
            f"sl={trade['stop_loss']} targets={trade['targets']}"
        )
        inserted += 1
        fake_telegram_id -= 1

    await db.close()
    print(f"\nDone — {inserted} inserted, {skipped} skipped.")
    print("Now restart the bot. It will detect the positions and place TP orders automatically.")


if __name__ == "__main__":
    asyncio.run(main())
