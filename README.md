# Telegram → Bybit Auto Trading Bot

A fully modular Python trading bot that listens to a Telegram signal group, parses trading signals, and executes orders on Bybit perpetual futures.

---

## Quick Setup

### 1. Clone / copy the project folder

```
trading_strategy/
├── app/
├── data/         ← auto-created
├── logs/         ← auto-created
├── sessions/     ← auto-created
├── .env
├── requirements.txt
└── README.md
```

### 2. Create your `.env`

```bash
cp .env.example .env
```

Edit `.env`:
- Set `PATH` to the **absolute path** of this folder on your machine
- Fill in `TELEGRAM_API_ID`, `TELEGRAM_API_HASH`, `TELEGRAM_PHONE`
- Fill in `BYBIT_API_KEY`, `BYBIT_API_SECRET`
- Fill in `ALERT_BOT_TOKEN`, `ALERT_CHAT_ID` (optional but recommended)
- Set `DRY_RUN=true` for safe testing first

### 3. Install dependencies

```bash
pip install -r requirements.txt
```

### 4. Run the bot

```bash
python -m app.main
```

The **first time** Telethon runs it will ask for your phone number verification code. After that the session is saved and the bot reconnects automatically.

---

## Moving to Raspberry Pi

1. Copy the entire project folder to your Pi
2. Edit `.env`: change `PATH` to the Pi path, e.g. `/home/pi/trading_strategy`
3. Change any Pi-specific credentials if needed
4. Run the same command: `python -m app.main`

No code changes required.

---

## Project Structure

```
app/
├── config.py              ← Loads .env, builds all paths
├── logger.py              ← Rotating log setup
├── main.py                ← Entry point, wires everything together
│
├── intake/
│   └── telegram_listener.py   ← Telethon group listener
│
├── parsing/
│   ├── models.py              ← Typed dataclasses for every message type
│   └── parser.py              ← Classifier + parser
│
├── domain/
│   └── trade_manager.py       ← All lifecycle logic (entry, TP, SL, BE, close)
│
├── exchange/
│   └── bybit_client.py        ← Pure Bybit API wrapper (no strategy logic)
│
├── storage/
│   └── database.py            ← Async SQLite persistence
│
└── monitoring/
    ├── alerter.py             ← Telegram alert sender with cooldown
    └── watchdog.py            ← Background health checker
```

---

## Supported Telegram Message Types

| Type | Example |
|------|---------|
| `new_signal` | `Coin: #AXLUSDT / Direction: Short / Entry: ... / SL: ...` |
| `close_all` | `close all trades` |
| `close_symbol` | `close AXLUSDT` |
| `cancel_remaining_entries` | `cancel remaining entries for AXLUSDT` |
| `move_sl_break_even` | `move SL to breakeven` |
| `move_sl_price` | `new stop loss: 0.0310` |
| `update_targets` | `new targets for AXLUSDT ...` |
| `add_entries` | `add entries between X and Y` |
| `market_entry` | `buy now` |
| `partial_close` | `close 50%` / `close half` |
| `cancel_signal` | `ignore previous signal` |
| `commentary` | `#AXLUSDT UPDATE: ...` (logged, not traded) |

---

## Key Trade Rules

- Position size = `(balance × RISK_PER_TRADE) / distance_to_SL`
- Entry orders split across `entry_low` and `entry_high`
- TP orders split equally across all target levels
- When break-even is activated → remaining entry orders auto-cancelled
- Duplicate Telegram messages are ignored by `telegram_message_id`
- State persists in SQLite – bot survives restarts

---

## Alerts

Alerts fire via a **separate bot** (ALERT_BOT_TOKEN) to ALERT_CHAT_ID.

Alerts fire only for:
- Telegram down > ALERT_TELEGRAM_SECONDS
- Bybit API down > ALERT_BYBIT_SECONDS
- Position without SL > ALERT_SL_SECONDS
- Fatal crash / traceback in logs

Each alert type has a cooldown of ALERT_COOLDOWN_SECONDS to avoid spam.

---

## Environment Variables Reference

| Variable | Description |
|----------|-------------|
| `PATH` | Absolute project root path |
| `TELEGRAM_API_ID` | From my.telegram.org |
| `TELEGRAM_API_HASH` | From my.telegram.org |
| `TELEGRAM_PHONE` | Your phone number (international format) |
| `TELEGRAM_SESSION_NAME` | Session file name (default: telegram_session) |
| `TELEGRAM_GROUP_NAME` | Exact group name to listen to |
| `BYBIT_API_KEY` | Bybit API key |
| `BYBIT_API_SECRET` | Bybit API secret |
| `BYBIT_TESTNET` | true/false |
| `DEFAULT_LEVERAGE` | Default leverage if signal doesn't specify |
| `MAX_LEVERAGE` | Hard cap on leverage |
| `RISK_PER_TRADE` | Fraction of balance to risk (e.g. 0.03 = 3%) |
| `DRY_RUN` | true = parse everything, never send orders |
| `ALERT_BOT_TOKEN` | Bot token for alert messages |
| `ALERT_CHAT_ID` | Your Telegram user/chat ID for alerts |
| `BOT_LOG_FILE` | Log file path (auto-derived from PATH) |
| `DB_PATH` | SQLite DB path (auto-derived from PATH) |
| `SESSION_DIR` | Telethon session directory |
| `ALERT_*_SECONDS` | Alert thresholds per issue type |
| `ALERT_COOLDOWN_SECONDS` | Minimum seconds between repeated alerts |
