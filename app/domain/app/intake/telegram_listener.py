"""
telegram_listener.py – Listens to a Telegram group and dispatches messages.

Uses Telethon (user account, not bot) so it can read group messages.
Session is stored at SESSION_DIR/TELEGRAM_SESSION_NAME.session

Reconnect fix: a single TelegramClient is created once and reused across
reconnects.  On each reconnect only the event handler is re-registered,
avoiding abandoned socket handles and potential session-file locking issues
that occurred when a new TelegramClient was instantiated on every loop.
"""

import asyncio
import logging
from typing import Callable, Awaitable

from telethon import TelegramClient, events
from telethon.errors import FloodWaitError

from app.config import config
from app.monitoring.watchdog import report_telegram_ok, report_telegram_fail

log = logging.getLogger(__name__)

MessageHandler = Callable[[str, int], Awaitable[None]]


class TelegramListener:
    def __init__(self, on_message: MessageHandler):
        config.session_dir.mkdir(parents=True, exist_ok=True)
        session_path = str(config.session_dir / config.telegram_session_name)
        # Create the client once — reused across reconnects
        self._client = TelegramClient(
            session_path,
            config.telegram_api_id,
            config.telegram_api_hash,
        )
        self._on_message  = on_message
        self._group_name  = config.telegram_group_name
        self._group_entity = None   # resolved once, cached permanently

    async def start(self):
        """
        Connect, register the message handler, and run until disconnected.

        On reconnect (called again from _telegram_loop) the existing client
        is re-used: we call connect() instead of start() so Telethon does not
        prompt for a phone code again when the session is already authorised.
        """
        log.info("Connecting to Telegram…")

        if not self._client.is_connected():
            await self._client.connect()

        if not await self._client.is_user_authorized():
            # First-ever run or session expired — full interactive login
            log.info("Telegram session not authorised — starting interactive login")
            await self._client.start(phone=config.telegram_phone)

        report_telegram_ok()
        log.info("Telegram connected (session: %s)", config.telegram_session_name)

        # Resolve the group entity once; cache it for subsequent reconnects
        if self._group_entity is None:
            try:
                group_ref = int(self._group_name)
            except ValueError:
                group_ref = self._group_name

            try:
                self._group_entity = await self._client.get_entity(group_ref)
                log.info("Telegram group resolved: %s", self._group_name)
            except Exception as exc:
                log.error("Cannot resolve Telegram group '%s': %s", self._group_name, exc)
                report_telegram_fail()
                return

        # Remove any previous handler registrations to avoid duplicates on reconnect
        self._client.remove_event_handler(self._message_handler)

        # Register the handler
        self._client.add_event_handler(
            self._message_handler,
            events.NewMessage(chats=self._group_entity),
        )

        log.info("Listening to group: %s", self._group_name)
        await self._client.run_until_disconnected()
        report_telegram_fail()
        log.warning("Telegram client disconnected")

    async def _message_handler(self, event):
        """Handles a single incoming message event."""
        try:
            text   = event.raw_text or ""
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

    async def disconnect(self):
        """Gracefully disconnect the client."""
        try:
            await self._client.disconnect()
        except Exception as exc:
            log.debug("Telegram disconnect error (ignored): %s", exc)
