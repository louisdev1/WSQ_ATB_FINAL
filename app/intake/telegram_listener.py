"""
telegram_listener.py – Listens to a Telegram group and dispatches messages.

Uses Telethon (user account, not bot) so it can read group messages.
Session is stored at SESSION_DIR/TELEGRAM_SESSION_NAME.session
"""

import asyncio
import logging
from pathlib import Path
from typing import Callable, Awaitable

from telethon import TelegramClient, events
from telethon.errors import FloodWaitError, RPCError

from app.config import config
from app.monitoring.watchdog import report_telegram_ok, report_telegram_fail

log = logging.getLogger(__name__)

MessageHandler = Callable[[str, int], Awaitable[None]]


class TelegramListener:
    def __init__(self, on_message: MessageHandler):
        session_path = str(config.session_dir / config.telegram_session_name)
        self._client = TelegramClient(
            session_path,
            config.telegram_api_id,
            config.telegram_api_hash,
        )
        self._on_message = on_message
        self._group_name = config.telegram_group_name

    async def start(self):
        """Connect, register handler, and run until disconnected."""
        log.info("Connecting to Telegram…")
        await self._client.start(phone=config.telegram_phone)
        report_telegram_ok()
        log.info("Telegram connected")

        # Resolve group entity once
        # Support numeric chat IDs (e.g. -1001234567890) as well as usernames
        try:
            group_ref = int(self._group_name)
        except ValueError:
            group_ref = self._group_name

        try:
            group = await self._client.get_entity(group_ref)
        except Exception as exc:
            log.error("Cannot resolve Telegram group '%s': %s", self._group_name, exc)
            report_telegram_fail()
            return

        @self._client.on(events.NewMessage(chats=group))
        async def _handler(event):
            try:
                text = event.raw_text or ""
                msg_id = event.message.id
                if text.strip():
                    await self._on_message(text, msg_id)
                report_telegram_ok()
            except FloodWaitError as fwe:
                log.warning("FloodWait: sleeping %ss", fwe.seconds)
                await asyncio.sleep(fwe.seconds)
            except Exception as exc:
                log.error("Telegram message handler error: %s", exc)
                report_telegram_fail()

        log.info("Listening to group: %s", self._group_name)
        await self._client.run_until_disconnected()
        report_telegram_fail()
        log.warning("Telegram client disconnected")

    async def disconnect(self):
        await self._client.disconnect()
