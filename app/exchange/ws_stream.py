"""
ws_stream.py – Bybit WebSocket stream for real-time fill and order events.

Subscribes to:
  - execution stream  → individual fills (entry fills, TP fills)
  - order stream      → order status changes (SL hit detection)

On each fill event the registered callback is fired immediately.
The watchdog still runs every 30s as a REST fallback and health checker.

Reconnection: checks is_connected() every 15s. Reconnects if socket dropped.
"""

import asyncio
import logging
from typing import Callable, Optional

from pybit.unified_trading import WebSocket

from app.monitoring.watchdog import report_bybit_ok, report_bybit_fail

log = logging.getLogger(__name__)

# How often to check that the socket is still alive (seconds)
_HEALTH_CHECK_INTERVAL = 15


class BybitStream:
    """
    Wraps pybit WebSocket and exposes async callbacks for the trade manager.

    Usage:
        stream = BybitStream(api_key, api_secret, on_execution=handler)
        await stream.start()   # runs forever, call from asyncio.gather()
    """

    def __init__(
        self,
        api_key: str,
        api_secret: str,
        testnet: bool = False,
        on_execution: Optional[Callable] = None,
        on_order: Optional[Callable] = None,
        dry_run: bool = False,
    ):
        self._api_key     = api_key
        self._api_secret  = api_secret
        self._testnet     = testnet
        self._on_execution = on_execution
        self._on_order     = on_order
        self._dry_run      = dry_run
        self._ws: Optional[WebSocket] = None
        self._loop        = None
        self._running     = False

    def _build_ws(self) -> WebSocket:
        ws = WebSocket(
            channel_type="private",
            testnet=self._testnet,
            api_key=self._api_key,
            api_secret=self._api_secret,
        )
        if self._on_execution:
            ws.execution_stream(callback=self._execution_cb)
        if self._on_order:
            ws.order_stream(callback=self._order_cb)
        return ws

    # ── raw callbacks (called from pybit's internal thread) ───────────────────

    def _execution_cb(self, msg: dict):
        report_bybit_ok()
        if self._on_execution and self._loop:
            asyncio.run_coroutine_threadsafe(
                self._on_execution(msg), self._loop
            )

    def _order_cb(self, msg: dict):
        report_bybit_ok()
        if self._on_order and self._loop:
            asyncio.run_coroutine_threadsafe(
                self._on_order(msg), self._loop
            )

    # ── lifecycle ─────────────────────────────────────────────────────────────

    async def start(self):
        """
        Start the WebSocket and monitor it forever.
        Uses is_connected() to detect drops rather than message timestamps,
        since pybit heartbeats don't trigger user callbacks.
        """
        if self._dry_run:
            log.info("[DRY] BybitStream: skipping WebSocket (dry_run=True)")
            while True:
                await asyncio.sleep(60)

        self._loop    = asyncio.get_running_loop()
        self._running = True
        log.info("BybitStream starting (testnet=%s)", self._testnet)

        while self._running:
            try:
                self._ws = self._build_ws()
                log.info("BybitStream connected")
                report_bybit_ok()

                # Poll is_connected() every _HEALTH_CHECK_INTERVAL seconds
                while self._running:
                    await asyncio.sleep(_HEALTH_CHECK_INTERVAL)
                    if not self._ws.is_connected():
                        log.warning("BybitStream: socket dropped — reconnecting")
                        report_bybit_fail()
                        break  # exit inner loop → reconnect

            except Exception as exc:
                log.error("BybitStream error: %s — reconnecting in 15s", exc)
                report_bybit_fail()

            # Brief pause before reconnecting
            await asyncio.sleep(15)

        log.info("BybitStream stopped")

    def stop(self):
        self._running = False
        if self._ws:
            try:
                self._ws.exit()
            except Exception:
                pass
