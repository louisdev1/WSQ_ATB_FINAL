"""
main.py – Bot entry point.

Run with:
    python -m app.main

Architecture:
  - Telegram listener  → parse signals → TradeManager
  - Bybit WebSocket    → real-time fills → TradeManager (TP ratchet, SL enforcement)
  - Watchdog           → health checks, REST fallback fill sync (every 30s)
"""

import asyncio
import logging
import sys
import traceback

from app.config import config
from app.logger import setup_logging
from app.storage.database import Database
from app.exchange.bybit_client import BybitClient
from app.exchange.ws_stream import BybitStream
from app.parsing.parser import parse_message
from app.parsing.models import MessageType
from app.domain.trade_manager import TradeManager
from app.intake.telegram_listener import TelegramListener
from app.monitoring.watchdog import watchdog_loop, report_bybit_ok, report_bybit_fail
from app.monitoring.alerter import send_alert

log = logging.getLogger(__name__)

_TELEGRAM_RECONNECT_DELAY = 15

# ANSI colours – degrade gracefully on terminals that don't support them
_G   = "\033[92m"
_Y   = "\033[93m"
_R   = "\033[91m"
_C   = "\033[96m"
_W   = "\033[97m"
_DIM = "\033[2m"
_RST = "\033[0m"


def _col(text: str, colour: str) -> str:
    return f"{colour}{text}{_RST}"


async def _print_startup_summary(db: Database, bybit: BybitClient):
    """
    Print a human-readable dashboard to stdout right after startup sync.
    Only active trades (confirmed live on Bybit) are shown as active.
    Stale trades were already closed by startup_position_sync before this runs.
    """
    W = 62
    # get_active_trades only returns non-closed/cancelled/sl_hit states
    trades = await db.get_active_trades()

    lines = []
    lines.append(_col("=" * W, _C))
    lines.append(_col("  WSQ_ATB  \u2014  startup summary", _W))
    lines.append(_col(
        f"  mode: {'DRY RUN' if config.dry_run else 'LIVE'}  |  "
        f"testnet: {config.bybit_testnet}  |  "
        f"risk/trade: {config.risk_per_trade * 100:.1f}%",
        _DIM,
    ))
    lines.append(_col("-" * W, _C))

    balance = bybit.fetch_wallet_balance()
    bal_col = _G if balance > 0 else _Y
    lines.append(f"  {'Balance':<20} {_col(f'{balance:.2f} USDT', bal_col)}")
    lines.append(_col("-" * W, _C))

    if not trades:
        lines.append(f"  {_col('No active trades', _DIM)}")
    else:
        lines.append(f"  {_col(f'{len(trades)} active trade(s)', _W)}")
        lines.append("")

        for t in trades:
            sym       = t["symbol"]
            direction = t["direction"].upper()
            state     = t["state"]
            avg_entry = t.get("avg_entry_price", 0) or 0.0
            sl        = t.get("stop_loss", 0) or 0.0
            targets   = t.get("targets", [])
            tp_hit    = t.get("highest_tp_hit", 0) or 0

            # Source of truth: Bybit live position
            pos        = bybit.fetch_position(sym)
            live_size  = float(pos.get("size", 0))       if pos else 0.0
            mark_price = float(pos.get("markPrice", 0))  if pos else 0.0
            upnl       = float(pos.get("unrealisedPnl", 0)) if pos else 0.0
            live_sl    = float(pos.get("stopLoss") or 0) if pos else 0.0
            live_entry = float(pos.get("avgPrice", 0))   if pos else avg_entry

            dir_col = _G if direction == "LONG" else _R
            seeded  = bool(t.get("entries_cancelled")) and live_entry > 0 and not targets

            header = (f"  {_col(sym, _W)}  {_col(direction, dir_col)}  "
                      f"{_col(f'[{state}]', _DIM)}")
            if seeded:
                header += _col("  \u26a0 no targets set", _Y)
            lines.append(header)

            if pos and live_size > 0:
                pnl_col = _G if upnl >= 0 else _R
                lines.append(f"    {'Size (Bybit)':<20} {live_size}")
                lines.append(f"    {'Avg entry (Bybit)':<20} {live_entry:.6f}")
                lines.append(f"    {'Mark price':<20} {mark_price:.6f}")
                lines.append(f"    {'Unrealised PnL':<20} {_col(f'{upnl:+.4f} USDT', pnl_col)}")
                if live_sl:
                    lines.append(f"    {'Stop-loss (Bybit)':<20} {_col(f'{live_sl:.6f}', _R)}")
                else:
                    lines.append(f"    {'Stop-loss':<20} {_col('NOT SET  \u26a0', _R)}")
            else:
                # Should rarely appear here (startup_position_sync closes these),
                # but guard just in case race condition on very fresh entry orders.
                lines.append(f"    {_col('No live position yet (entry orders pending?)', _Y)}")
                if sl:
                    lines.append(f"    {'Stop-loss (DB)':<20} {_col(f'{sl:.6f}', _R)}")

            # Open entry orders from Bybit REST (source of truth)
            open_orders = bybit.fetch_open_orders(sym)
            entry_orders = [o for o in open_orders if o.get("orderType") == "Limit"
                            and o.get("side") in ("Buy", "Sell")
                            and str(o.get("reduceOnly", "false")).lower() != "true"]
            if entry_orders:
                lines.append(f"    {'Open entries':<20} {len(entry_orders)} limit order(s)")
                for o in entry_orders:
                    lines.append(
                        f"      \u25aa  {float(o.get('price', 0)):.6f}  "
                        f"qty {float(o.get('qty', 0))}"
                    )

            if targets:
                remaining = len(targets) - tp_hit
                lines.append(
                    f"    {'Targets':<20} {len(targets)} total  |  "
                    f"hit: {tp_hit}  |  remaining: {remaining}"
                )
                for i, tp in enumerate(targets):
                    marker = _col("\u2713", _G) if i < tp_hit else _col("\u25cb", _DIM)
                    lines.append(f"      {marker}  TP{i + 1}: {tp:.6f}")
            else:
                lines.append(
                    f"    {'Targets':<20} "
                    + _col(f"none \u2014 send \u00abnew targets for {sym}\u00bb", _Y)
                )

            lines.append("")

    lines.append(_col("=" * W, _C))
    print("\n" + "\n".join(lines) + "\n", flush=True)


async def _on_telegram_message(db: Database, trade_manager: TradeManager,
                                raw_text: str, msg_id: int):
    if await db.is_duplicate_message(msg_id):
        log.debug("Duplicate message %d – skipped", msg_id)
        return

    parsed = parse_message(raw_text, msg_id)
    log.info("MSG[%d] type=%s symbol=%s",
             msg_id, parsed.message_type.value,
             getattr(parsed, "symbol", "-") or "-")

    await db.save_raw_message(msg_id, raw_text, parsed.message_type.value)

    if parsed.message_type in (MessageType.IGNORE, MessageType.COMMENTARY):
        return

    try:
        await trade_manager.handle(parsed)
    except Exception as exc:
        log.error("TradeManager.handle error: %s\n%s", exc, traceback.format_exc())


async def _telegram_loop(db: Database, trade_manager: TradeManager):
    """Keeps the Telegram listener alive with auto-reconnect."""
    while True:
        async def message_handler(raw_text: str, msg_id: int):
            await _on_telegram_message(db, trade_manager, raw_text, msg_id)

        listener = TelegramListener(on_message=message_handler)
        try:
            await listener.start()
            log.warning("Telegram listener stopped. Reconnecting in %ss…", _TELEGRAM_RECONNECT_DELAY)
        except Exception as exc:
            log.error("Telegram listener crashed: %s", exc)
        await asyncio.sleep(_TELEGRAM_RECONNECT_DELAY)


async def main():
    # ── setup ─────────────────────────────────────────────────────────────────
    config.ensure_dirs()
    setup_logging(config.log_file)
    log.info("=" * 60)
    log.info("Trading Bot starting (dry_run=%s, testnet=%s)",
             config.dry_run, config.bybit_testnet)

    # ── storage ───────────────────────────────────────────────────────────────
    db = Database(config.db_path)
    await db.connect()

    # ── exchange (REST) ───────────────────────────────────────────────────────
    bybit = BybitClient(
        api_key=config.bybit_api_key,
        api_secret=config.bybit_api_secret,
        testnet=config.bybit_testnet,
    )
    bybit._dry_run = config.dry_run
    bybit.set_health_callbacks(on_ok=report_bybit_ok, on_fail=report_bybit_fail)

    # ── domain ────────────────────────────────────────────────────────────────
    trade_manager = TradeManager(db=db, bybit=bybit)

    # ── exchange (WebSocket) ──────────────────────────────────────────────────
    ws = BybitStream(
        api_key=config.bybit_api_key,
        api_secret=config.bybit_api_secret,
        testnet=config.bybit_testnet,
        on_execution=trade_manager.on_ws_execution,
        on_order=trade_manager.on_ws_order,
        dry_run=config.dry_run,
    )

    # ── startup position sync ─────────────────────────────────────────────────
    log.info("Running startup position sync…")
    await trade_manager.startup_position_sync()
    await _print_startup_summary(db, bybit)

    # ── run all tasks concurrently ────────────────────────────────────────────
    try:
        await asyncio.gather(
            _telegram_loop(db, trade_manager),
            ws.start(),
            watchdog_loop(db, bybit, trade_manager),
        )
    except KeyboardInterrupt:
        log.info("Shutting down…")
    except Exception as exc:
        tb = traceback.format_exc()
        log.critical("Fatal crash: %s\n%s", exc, tb)
        await send_alert("fatal_crash", f"Bot crashed:\n```{tb[-800:]}```", db)
        sys.exit(1)
    finally:
        ws.stop()
        await db.close()
        log.info("Bot stopped")


if __name__ == "__main__":
    asyncio.run(main())
