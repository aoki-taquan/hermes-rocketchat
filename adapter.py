"""Rocket.Chat gateway adapter for Hermes Agent.

Connects to a self-hosted (or cloud) Rocket.Chat workspace and relays
messages between Rocket.Chat channels / DMs and the Hermes agent.

Two transports are used, both over connections Hermes already depends on
(``aiohttp`` — no Rocket.Chat SDK required):

* **Receiving** — the Realtime API (Meteor DDP over a WebSocket at
  ``<server>/websocket``).  The adapter logs in with a resume token and
  subscribes to ``stream-room-messages`` for ``__my_messages__`` so it
  sees every message in every room the bot account is a member of.
* **Sending** — the REST API (``/api/v1/``) via ``chat.sendMessage`` and
  ``rooms.upload`` (file attachments).  REST is also used out-of-process
  by cron delivery through :func:`_standalone_send`.

Authentication (set in ``~/.hermes/.env``):

    ROCKETCHAT_URL              Server URL (e.g. https://chat.example.com)
    ROCKETCHAT_USER_ID          Bot user id (pairs with a Personal Access Token)
    ROCKETCHAT_TOKEN            Personal Access Token  (recommended)
        — or —
    ROCKETCHAT_USERNAME         Bot username  (password login fallback)
    ROCKETCHAT_PASSWORD         Bot password

Optional:

    ROCKETCHAT_ALLOWED_USERS         Comma-separated Rocket.Chat *user ids* allowed to
                                     talk to the bot (usernames do NOT match)
    ROCKETCHAT_ALLOW_ALL_USERS       Allow any user (dev only)
    ROCKETCHAT_HOME_CHANNEL          Room id for cron / notification delivery
    ROCKETCHAT_REPLY_MODE            'thread' (nested) or 'off' (flat, default)
    ROCKETCHAT_REQUIRE_MENTION       Require @bot mention in channels (default true)
    ROCKETCHAT_FREE_RESPONSE_CHANNELS  Room ids where @mention is not required
    ROCKETCHAT_ALLOWED_CHANNELS      If set, bot only responds in these room ids
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urljoin, urlparse

from gateway.config import Platform, PlatformConfig
from gateway.platforms.helpers import MessageDeduplicator
from gateway.platforms.base import (
    BasePlatformAdapter,
    MessageEvent,
    MessageType,
    SendResult,
)

logger = logging.getLogger(__name__)

# Identifier used in config.yaml (gateway.platforms.rocketchat) and by
# ``Platform("rocketchat")``.  Registered at import-time via register().
PLATFORM_NAME = "rocketchat"

# Rocket.Chat has no hard message ceiling, but very long single messages
# render badly and can trip server-side message size limits. 4000 mirrors
# the readable threshold the Mattermost adapter uses.
MAX_MESSAGE_LENGTH = 4000

# Rocket.Chat room "type" codes (the ``t`` field on a room object).
_ROOM_TYPE_MAP = {
    "d": "dm",       # direct message
    "c": "channel",  # public channel
    "p": "group",    # private group
    "l": "channel",  # livechat
}

# Reconnect parameters (exponential backoff with jitter).
_RECONNECT_BASE_DELAY = 2.0
_RECONNECT_MAX_DELAY = 60.0
_RECONNECT_JITTER = 0.2

# If the DDP login+subscribe handshake doesn't complete within this window,
# the socket is torn down and reconnected (avoids a live-but-deaf connection).
_HANDSHAKE_TIMEOUT = 30.0

# Ceiling on a single inbound attachment. The body is read into memory before
# being cached, so an unbounded read is a trivial DoS.
_MAX_ATTACHMENT_BYTES = 32 * 1024 * 1024

# Rocket.Chat expires a typing indicator after ~15s; its own client refreshes
# roughly every 10s. Anything much faster is pointless DDP traffic.
_TYPING_REFRESH_INTERVAL = 8.0

# Backstop so a missed stop_typing can never leave a task spinning forever.
_TYPING_MAX_DURATION = 300.0


def check_rocketchat_requirements() -> bool:
    """Return True if the Rocket.Chat adapter can be used."""
    url = os.getenv("ROCKETCHAT_URL", "")
    token = os.getenv("ROCKETCHAT_TOKEN", "")
    user_id = os.getenv("ROCKETCHAT_USER_ID", "")
    username = os.getenv("ROCKETCHAT_USERNAME", "")
    password = os.getenv("ROCKETCHAT_PASSWORD", "")
    if not url:
        logger.debug("Rocket.Chat: ROCKETCHAT_URL not set")
        return False
    has_token = bool(token and user_id)
    has_login = bool(username and password)
    if not has_token and not has_login:
        logger.debug(
            "Rocket.Chat: need ROCKETCHAT_USER_ID+ROCKETCHAT_TOKEN or "
            "ROCKETCHAT_USERNAME+ROCKETCHAT_PASSWORD"
        )
        return False
    try:
        import aiohttp  # noqa: F401
        return True
    except ImportError:
        logger.warning("Rocket.Chat: aiohttp not installed")
        return False


class RocketChatAdapter(BasePlatformAdapter):
    """Gateway adapter for Rocket.Chat (self-hosted or cloud)."""

    def __init__(self, config: PlatformConfig):
        super().__init__(config, Platform(PLATFORM_NAME))

        self._base_url: str = (
            (config.extra.get("url") if config.extra else "")
            or os.getenv("ROCKETCHAT_URL", "")
        ).rstrip("/")

        # Personal Access Token auth (preferred).
        self._user_id: str = (
            (config.extra.get("user_id") if config.extra else "")
            or os.getenv("ROCKETCHAT_USER_ID", "")
        )
        self._token: str = config.token or os.getenv("ROCKETCHAT_TOKEN", "")

        # Password login fallback.
        self._username: str = os.getenv("ROCKETCHAT_USERNAME", "")
        self._password: str = os.getenv("ROCKETCHAT_PASSWORD", "")

        self._bot_user_id: str = ""
        self._bot_username: str = ""

        self._session: Any = None  # aiohttp.ClientSession
        self._ws: Any = None  # aiohttp.ClientWebSocketResponse
        # chat_id -> typing refresh task (the indicator expires server-side)
        self._typing_tasks: Dict[str, asyncio.Task] = {}
        self._ws_task: Optional[asyncio.Task] = None
        self._closing = False
        self._ddp_seq = 0

        # Reply mode: "thread" nests replies under the triggering message,
        # "off" posts flat into the room.
        self._reply_mode: str = (
            (config.extra.get("reply_mode") if config.extra else "")
            or os.getenv("ROCKETCHAT_REPLY_MODE", "off")
        ).lower()

        # Cache of room id -> Hermes chat_type ("dm"/"channel"/"group") so
        # mention-gating doesn't re-hit the REST API for every message.
        self._room_type_cache: Dict[str, str] = {}

        self._dedup = MessageDeduplicator()

    # ------------------------------------------------------------------
    # HTTP helpers
    # ------------------------------------------------------------------

    def _headers(self) -> Dict[str, str]:
        return {
            "X-Auth-Token": self._token,
            "X-User-Id": self._user_id,
            "Content-Type": "application/json",
        }

    def _auth_headers(self) -> Dict[str, str]:
        """Auth-only headers (for multipart uploads — no Content-Type)."""
        return {
            "X-Auth-Token": self._token,
            "X-User-Id": self._user_id,
        }

    async def _api_get(self, path: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        import aiohttp
        url = f"{self._base_url}/api/v1/{path.lstrip('/')}"
        try:
            async with self._session.get(
                url, headers=self._headers(), params=params,
                timeout=aiohttp.ClientTimeout(total=30),
            ) as resp:
                if resp.status >= 400:
                    body = await resp.text()
                    logger.error("RC API GET %s -> %s: %s", path, resp.status, body[:200])
                    return {}
                return await resp.json()
        except aiohttp.ClientError as exc:
            logger.error("RC API GET %s network error: %s", path, exc)
            return {}

    async def _api_post(self, path: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        import aiohttp
        url = f"{self._base_url}/api/v1/{path.lstrip('/')}"
        try:
            async with self._session.post(
                url, headers=self._headers(), json=payload,
                timeout=aiohttp.ClientTimeout(total=30),
            ) as resp:
                if resp.status >= 400:
                    body = await resp.text()
                    logger.error("RC API POST %s -> %s: %s", path, resp.status, body[:200])
                    return {}
                return await resp.json()
        except aiohttp.ClientError as exc:
            logger.error("RC API POST %s network error: %s", path, exc)
            return {}

    async def _upload_file(
        self,
        rid: str,
        file_data: bytes,
        filename: str,
        content_type: str,
        msg: str = "",
        tmid: Optional[str] = None,
    ) -> Optional[str]:
        """Upload a file to a room via ``rooms.upload``. Returns message id."""
        import aiohttp
        url = f"{self._base_url}/api/v1/rooms.upload/{rid}"
        form = aiohttp.FormData()
        form.add_field("file", file_data, filename=filename, content_type=content_type)
        if msg:
            form.add_field("msg", msg)
        if tmid:
            form.add_field("tmid", tmid)
        try:
            async with self._session.post(
                url, headers=self._auth_headers(), data=form,
                timeout=aiohttp.ClientTimeout(total=60),
            ) as resp:
                if resp.status >= 400:
                    body = await resp.text()
                    logger.error("RC file upload -> %s: %s", resp.status, body[:200])
                    return None
                data = await resp.json()
                return (data.get("message") or {}).get("_id")
        except aiohttp.ClientError as exc:
            logger.error("RC file upload network error: %s", exc)
            return None

    # ------------------------------------------------------------------
    # Required overrides
    # ------------------------------------------------------------------

    async def connect(self, *, is_reconnect: bool = False) -> bool:
        """Connect to Rocket.Chat.

        ``is_reconnect`` is passed by the Hermes core (``gateway/run.py``) when
        it re-establishes a dropped platform. The core calls
        ``adapter.connect(is_reconnect=...)`` unconditionally, so **this
        keyword must stay even though the body only uses it for logging** --
        removing it makes the plugin fail to load with a TypeError.
        """
        import aiohttp

        if not self._base_url:
            logger.error("Rocket.Chat: ROCKETCHAT_URL not configured")
            return False

        # The core may reconnect without calling disconnect() first. Without
        # this teardown the previous ClientSession leaks and the old _ws_loop
        # keeps running, so two loops end up fighting over self._ws and the
        # room subscription is duplicated.
        if self._session is not None or self._ws_task is not None:
            logger.info(
                "Rocket.Chat: %s - tearing down the previous connection",
                "reconnecting" if is_reconnect else "connect() called while connected",
            )
            await self.disconnect()

        self._session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30))
        # Must follow disconnect(), which sets _closing = True.
        self._closing = False

        # If we only have username/password, log in via REST to obtain the
        # userId + authToken used for both REST and the DDP resume login.
        if not (self._token and self._user_id):
            if not (self._username and self._password):
                logger.error(
                    "Rocket.Chat: provide ROCKETCHAT_USER_ID+ROCKETCHAT_TOKEN "
                    "or ROCKETCHAT_USERNAME+ROCKETCHAT_PASSWORD"
                )
                await self._session.close()
                return False
            if not await self._rest_login():
                await self._session.close()
                return False

        # Verify credentials + fetch bot identity.
        me = await self._api_get("me")
        if not me or not me.get("success", True) or "_id" not in me:
            logger.error(
                "Rocket.Chat: failed to authenticate — check ROCKETCHAT_URL, "
                "ROCKETCHAT_USER_ID and ROCKETCHAT_TOKEN"
            )
            await self._session.close()
            return False

        self._bot_user_id = me["_id"]
        self._bot_username = me.get("username", "")
        logger.info(
            "Rocket.Chat: authenticated as @%s (%s) on %s",
            self._bot_username, self._bot_user_id, self._base_url,
        )

        self._ws_task = asyncio.create_task(self._ws_loop())
        self._mark_connected()
        return True

    async def _rest_login(self) -> bool:
        """Authenticate with username/password to populate token + user id."""
        import aiohttp
        url = f"{self._base_url}/api/v1/login"
        try:
            async with self._session.post(
                url, json={"user": self._username, "password": self._password},
                timeout=aiohttp.ClientTimeout(total=30),
            ) as resp:
                data = await resp.json()
                if resp.status >= 400 or data.get("status") != "success":
                    logger.error(
                        "Rocket.Chat: password login failed (%s): %s",
                        resp.status, str(data.get("message") or data.get("error"))[:200],
                    )
                    return False
                self._token = data["data"]["authToken"]
                self._user_id = data["data"]["userId"]
                return True
        except (aiohttp.ClientError, KeyError) as exc:
            logger.error("Rocket.Chat: password login error: %s", exc)
            return False

    async def _notify_typing(self, chat_id: str, is_typing: bool) -> None:
        """Tell Rocket.Chat whether the bot is composing a message.

        Two events are emitted because the wire format changed: Rocket.Chat 4.x
        moved to the ``UserAction`` module and modern clients render
        ``<rid>/user-activity``, while older ones only understand
        ``<rid>/typing``. Both are cheap, so send both rather than probing the
        server version.
        """
        ws = self._ws
        if ws is None or getattr(ws, "closed", True):
            return
        name = self._bot_username or self._username
        if not name:
            logger.debug("Rocket.Chat: no bot username resolved, skipping typing notify")
            return
        activities = ["user-typing"] if is_typing else []
        try:
            await ws.send_json({
                "msg": "method",
                "method": "stream-notify-room",
                "id": self._next_seq(),
                "params": [f"{chat_id}/user-activity", name, activities, {}],
            })
            await ws.send_json({
                "msg": "method",
                "method": "stream-notify-room",
                "id": self._next_seq(),
                "params": [f"{chat_id}/typing", name, bool(is_typing)],
            })
        except Exception as exc:
            # The indicator is cosmetic; never let it break message handling.
            # Logged because a silent failure here is indistinguishable from
            # "the server ignored us".
            logger.debug("Rocket.Chat: typing notify failed for %s: %s", chat_id, exc)

    async def _typing_loop(self, chat_id: str) -> None:
        """Keep the indicator alive; Rocket.Chat expires it after a few seconds.

        Bounded by ``_TYPING_MAX_DURATION`` so that a missed ``stop_typing``
        (an exception in the core between start and stop, say) cannot leave a
        task spinning and the room stuck on "typing" forever.
        """
        loop = asyncio.get_running_loop()
        deadline = loop.time() + _TYPING_MAX_DURATION
        try:
            while loop.time() < deadline:
                await self._notify_typing(chat_id, True)
                await asyncio.sleep(_TYPING_REFRESH_INTERVAL)
            logger.warning(
                "Rocket.Chat: typing indicator for %s expired after %.0fs "
                "(stop_typing was never called)", chat_id, _TYPING_MAX_DURATION,
            )
            await self._notify_typing(chat_id, False)
        finally:
            # Deregister, but only if we are still the registered task -- a
            # replacement must not be evicted by its predecessor.
            if self._typing_tasks.get(chat_id) is asyncio.current_task():
                self._typing_tasks.pop(chat_id, None)

    async def send_typing(
        self, chat_id: str, metadata: Optional[Dict[str, Any]] = None
    ) -> None:
        task = self._typing_tasks.get(chat_id)
        if task and not task.done():
            return
        # Register synchronously: an await between the check and the assignment
        # would let two concurrent callers both start a loop, and the loser
        # would be unreachable to stop_typing/disconnect. The loop sends the
        # first notification itself.
        self._typing_tasks[chat_id] = asyncio.create_task(
            self._typing_loop(chat_id), name=f"rc-typing-{chat_id}",
        )

    async def stop_typing(self, chat_id: str) -> None:
        task = self._typing_tasks.pop(chat_id, None)
        if task and not task.done():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                # Only swallow the cancellation we caused; if this coroutine is
                # itself being cancelled, that must propagate.
                if not task.cancelled():
                    raise
            except Exception as exc:
                logger.debug("Rocket.Chat: typing loop ended with error: %s", exc)
        await self._notify_typing(chat_id, False)

    async def disconnect(self) -> None:
        self._closing = True
        # Best-effort: clear the indicator while the socket is still open.
        for chat_id in list(self._typing_tasks):
            await self._notify_typing(chat_id, False)
        typing_tasks = [t for t in self._typing_tasks.values() if t and not t.done()]
        self._typing_tasks.clear()
        for task in typing_tasks:
            task.cancel()
        if typing_tasks:
            # Awaiting matters: cancel() only schedules the cancellation, and
            # closing the loop with it still pending raises
            # "Task was destroyed but it is pending!".
            await asyncio.gather(*typing_tasks, return_exceptions=True)
        if self._ws_task and not self._ws_task.done():
            self._ws_task.cancel()
            try:
                await self._ws_task
            except (asyncio.CancelledError, Exception):
                pass
        if self._ws:
            try:
                await self._ws.close()
            except Exception:
                pass
            self._ws = None
        if self._session and not self._session.closed:
            await self._session.close()
        logger.info("Rocket.Chat: disconnected")

    async def _resolve_thread_root(self, message_id: str) -> str:
        """Return the thread-root id for ``message_id``.

        If the message is itself a thread reply (has ``tmid``), Rocket.Chat
        expects the *root* id for further replies — use that.  Otherwise the
        message id starts a new thread.
        """
        if not message_id:
            return message_id
        data = await self._api_get("chat.getMessage", {"msgId": message_id})
        msg = data.get("message") if isinstance(data, dict) else None
        if isinstance(msg, dict) and msg.get("tmid"):
            return msg["tmid"]
        return message_id

    async def send(
        self,
        chat_id: str,
        content: str,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        if not content:
            return SendResult(success=True)

        formatted = self.format_message(content)
        chunks = self.truncate_message(formatted, MAX_MESSAGE_LENGTH)

        tmid: Optional[str] = None
        if reply_to and self._reply_mode == "thread":
            tmid = await self._resolve_thread_root(reply_to)

        last_id = None
        for chunk in chunks:
            message: Dict[str, Any] = {"rid": chat_id, "msg": chunk}
            if tmid:
                message["tmid"] = tmid
            data = await self._api_post("chat.sendMessage", {"message": message})
            if not data or not data.get("success", bool(data.get("message"))):
                return SendResult(success=False, error="Failed to send message")
            last_id = (data.get("message") or {}).get("_id")

        return SendResult(success=True, message_id=last_id)

    async def edit_message(
        self,
        chat_id: str,
        message_id: str,
        content: str,
        *,
        finalize: bool = False,
    ) -> SendResult:
        """Edit a message in place via ``chat.update``.

        This is what unlocks live progress. The core checks whether an adapter
        overrides ``edit_message`` at all (``gateway/run.py``) and, when it does
        not, drops streaming/tool-progress updates entirely rather than posting
        a new message per step. Without this the room only ever sees the typing
        indicator and then the final answer.

        ``finalize`` is a no-op here: Rocket.Chat has no separate "in progress"
        message state, so an edit is just an edit.
        """
        if not message_id:
            return SendResult(success=False, error="edit_message requires a message id")

        formatted = self.format_message(content)
        # An edit cannot be split across messages; keep it within one.
        if len(formatted) > MAX_MESSAGE_LENGTH:
            formatted = formatted[: MAX_MESSAGE_LENGTH - 1] + "…"

        data = await self._api_post(
            "chat.update", {"roomId": chat_id, "msgId": message_id, "text": formatted}
        )
        if not data or not data.get("success", bool(data.get("message"))):
            # Callers fall back to sending a fresh message on failure.
            return SendResult(success=False, error="Failed to edit message")
        return SendResult(success=True, message_id=message_id)

    async def delete_message(self, chat_id: str, message_id: str) -> bool:
        """Delete a message via ``chat.delete``.

        Needed for ``display.cleanup_progress``: the core collects the ids of
        the tool-progress messages it posted and deletes them once the answer
        is in. Without this the room keeps every "🐍 Running code …" line
        forever, since the fallback is to leave them in place.
        """
        if not message_id:
            return False
        data = await self._api_post(
            "chat.delete", {"roomId": chat_id, "msgId": message_id, "asUser": True}
        )
        return bool(data and data.get("success"))

    async def get_chat_info(self, chat_id: str) -> Dict[str, Any]:
        data = await self._api_get("rooms.info", {"roomId": chat_id})
        room = data.get("room") if isinstance(data, dict) else None
        if not isinstance(room, dict):
            return {"name": chat_id, "type": "channel", "chat_id": chat_id}
        rtype = _ROOM_TYPE_MAP.get(room.get("t", "c"), "channel")
        name = room.get("fname") or room.get("name") or chat_id
        return {"name": name, "type": rtype, "chat_id": chat_id}

    # ------------------------------------------------------------------
    # Optional overrides
    # ------------------------------------------------------------------

    async def send_image(
        self,
        chat_id: str,
        image_url: str,
        caption: Optional[str] = None,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        return await self._send_url_as_file(chat_id, image_url, caption, reply_to, "image")

    async def send_image_file(
        self,
        chat_id: str,
        image_path: str,
        caption: Optional[str] = None,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        return await self._send_local_file(chat_id, image_path, caption, reply_to)

    async def send_document(
        self,
        chat_id: str,
        file_path: str,
        caption: Optional[str] = None,
        file_name: Optional[str] = None,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        return await self._send_local_file(chat_id, file_path, caption, reply_to, file_name)

    async def send_voice(
        self,
        chat_id: str,
        audio_path: str,
        caption: Optional[str] = None,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        return await self._send_local_file(chat_id, audio_path, caption, reply_to)

    async def send_video(
        self,
        chat_id: str,
        video_path: str,
        caption: Optional[str] = None,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        return await self._send_local_file(chat_id, video_path, caption, reply_to)

    def format_message(self, content: str) -> str:
        """Rocket.Chat renders standard Markdown.

        Strip image markdown into bare URLs — files are uploaded separately
        and Rocket.Chat previews bare image links inline.
        """
        return re.sub(r"!\[([^\]]*)\]\(([^)]+)\)", r"\2", content)

    # ------------------------------------------------------------------
    # File helpers
    # ------------------------------------------------------------------

    async def _send_url_as_file(
        self,
        chat_id: str,
        url: str,
        caption: Optional[str],
        reply_to: Optional[str],
        kind: str = "file",
    ) -> SendResult:
        from tools.url_safety import is_safe_url
        if not is_safe_url(url):
            logger.warning("Rocket.Chat: blocked unsafe URL (SSRF protection)")
            return await self.send(chat_id, f"{caption or ''}\n{url}".strip(), reply_to)

        import aiohttp
        file_data = None
        ct = "application/octet-stream"
        fname = url.rsplit("/", 1)[-1].split("?")[0] or f"{kind}.png"

        for attempt in range(3):
            try:
                async with self._session.get(
                    url, timeout=aiohttp.ClientTimeout(total=30),
                ) as resp:
                    if resp.status >= 500 or resp.status == 429:
                        if attempt < 2:
                            await asyncio.sleep(1.5 * (attempt + 1))
                            continue
                    if resp.status >= 400:
                        return await self.send(chat_id, f"{caption or ''}\n{url}".strip(), reply_to)
                    file_data = await resp.read()
                    ct = resp.content_type or "application/octet-stream"
                    break
            except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
                if attempt < 2:
                    await asyncio.sleep(1.5 * (attempt + 1))
                    continue
                logger.warning("Rocket.Chat: failed to download %s: %s", url[:80], exc)
                return await self.send(chat_id, f"{caption or ''}\n{url}".strip(), reply_to)

        if file_data is None:
            return await self.send(chat_id, f"{caption or ''}\n{url}".strip(), reply_to)

        tmid = await self._resolve_thread_root(reply_to) if (reply_to and self._reply_mode == "thread") else None
        mid = await self._upload_file(chat_id, file_data, fname, ct, caption or "", tmid)
        if not mid:
            return await self.send(chat_id, f"{caption or ''}\n{url}".strip(), reply_to)
        return SendResult(success=True, message_id=mid)

    async def _send_local_file(
        self,
        chat_id: str,
        file_path: str,
        caption: Optional[str],
        reply_to: Optional[str],
        file_name: Optional[str] = None,
    ) -> SendResult:
        import mimetypes
        p = Path(file_path)
        if not p.exists():
            logger.warning("Rocket.Chat: local file not found, skipping: %s", file_path)
            return SendResult(success=True, message_id=None)
        fname = file_name or p.name
        ct = mimetypes.guess_type(fname)[0] or "application/octet-stream"
        tmid = await self._resolve_thread_root(reply_to) if (reply_to and self._reply_mode == "thread") else None
        mid = await self._upload_file(chat_id, p.read_bytes(), fname, ct, caption or "", tmid)
        if not mid:
            return SendResult(success=False, error="File upload failed")
        return SendResult(success=True, message_id=mid)

    # ------------------------------------------------------------------
    # Realtime API (DDP over WebSocket)
    # ------------------------------------------------------------------

    def _next_seq(self) -> str:
        self._ddp_seq += 1
        return str(self._ddp_seq)

    async def _ws_loop(self) -> None:
        delay = _RECONNECT_BASE_DELAY
        while not self._closing:
            try:
                await self._ws_connect_and_listen()
                delay = _RECONNECT_BASE_DELAY
            except asyncio.CancelledError:
                return
            except Exception as exc:
                if self._closing:
                    return
                err = str(exc).lower()
                if "unauthorized" in err or "403" in err or "401" in err:
                    logger.error("Rocket.Chat WS permanent error: %s — stopping reconnect", exc)
                    return
                logger.warning("Rocket.Chat WS error: %s — reconnecting in %.0fs", exc, delay)

            if self._closing:
                return
            import random
            jitter = delay * _RECONNECT_JITTER * random.random()
            await asyncio.sleep(delay + jitter)
            delay = min(delay * 2, _RECONNECT_MAX_DELAY)

    async def _ws_connect_and_listen(self) -> None:
        import aiohttp
        ws_url = re.sub(r"^http", "ws", self._base_url) + "/websocket"
        logger.info("Rocket.Chat: connecting realtime WebSocket to %s", ws_url)
        self._ws = await self._session.ws_connect(ws_url, heartbeat=25.0)

        # DDP handshake.
        await self._ws.send_json({"msg": "connect", "version": "1", "support": ["1"]})

        login_id: Optional[str] = None
        subscribed = {"v": False}

        # Watchdog: if the login+subscribe handshake hasn't completed within
        # the window, tear the socket down so _ws_loop reconnects. Without
        # this, a stalled handshake would leave a live-but-deaf connection.
        async def _watchdog() -> None:
            try:
                await asyncio.sleep(_HANDSHAKE_TIMEOUT)
                if not subscribed["v"] and not self._closing:
                    logger.warning(
                        "Rocket.Chat: DDP handshake did not complete in %.0fs — reconnecting",
                        _HANDSHAKE_TIMEOUT,
                    )
                    await self._ws.close()
            except asyncio.CancelledError:
                pass

        watchdog = asyncio.create_task(_watchdog())
        try:
            async for raw in self._ws:
                if self._closing:
                    return
                if raw.type in {raw.type.TEXT, raw.type.BINARY}:
                    try:
                        event = json.loads(raw.data)
                    except (json.JSONDecodeError, TypeError):
                        continue

                    msg = event.get("msg")
                    if msg == "ping":
                        pong: Dict[str, Any] = {"msg": "pong"}
                        if "id" in event:
                            pong["id"] = event["id"]
                        await self._ws.send_json(pong)
                        continue
                    if msg == "connected":
                        login_id = self._next_seq()
                        await self._ws.send_json({
                            "msg": "method", "method": "login", "id": login_id,
                            "params": [{"resume": self._token}],
                        })
                        continue
                    if msg == "result" and event.get("id") == login_id:
                        # Login failed (bad/expired resume token). Raise so
                        # _ws_loop backs off and reconnects instead of sitting
                        # connected-but-unsubscribed forever.
                        if event.get("error"):
                            err = event["error"]
                            raise RuntimeError(
                                "DDP login failed: "
                                + str(err.get("message") or err.get("reason") or err)[:200]
                            )
                        await self._ddp_subscribe()
                        continue
                    if msg == "ready":
                        subscribed["v"] = True
                        continue
                    if msg == "changed":
                        await self._handle_ddp_changed(event)
                        continue
                elif raw.type in {raw.type.ERROR, raw.type.CLOSE, raw.type.CLOSING, raw.type.CLOSED}:
                    logger.info("Rocket.Chat: WebSocket closed (%s)", raw.type)
                    break
        finally:
            watchdog.cancel()

    async def _ddp_subscribe(self) -> None:
        """Subscribe to every message the bot account can see."""
        await self._ws.send_json({
            "msg": "sub",
            "id": self._next_seq(),
            "name": "stream-room-messages",
            "params": ["__my_messages__", False],
        })
        logger.info("Rocket.Chat: subscribed to stream-room-messages")

    async def _get_room_type(self, rid: str) -> str:
        """Resolve a room's Hermes chat_type, caching only positive results.

        A transient ``rooms.info`` failure (network blip, 429, 5xx, a momentary
        permission gap) must NOT be cached: doing so would permanently
        misclassify a DM as a channel and then silently drop every later DM
        message via mention-gating.  On failure we return ``"channel"`` for
        just this message without caching, so the next message retries.
        """
        cached = self._room_type_cache.get(rid)
        if cached is not None:
            return cached
        data = await self._api_get("rooms.info", {"roomId": rid})
        room = data.get("room") if isinstance(data, dict) else None
        if isinstance(room, dict):
            rtype = _ROOM_TYPE_MAP.get(room.get("t", "c"), "channel")
            self._room_type_cache[rid] = rtype
            return rtype
        logger.debug("Rocket.Chat: could not resolve room type for %s; not caching", rid)
        return "channel"

    @staticmethod
    def _parse_ts(ts: Any) -> Optional[float]:
        """DDP EJSON dates arrive as ``{"$date": <ms>}``."""
        if isinstance(ts, dict) and "$date" in ts:
            try:
                return float(ts["$date"]) / 1000.0
            except (TypeError, ValueError):
                return None
        if isinstance(ts, (int, float)):
            return float(ts) / 1000.0
        return None

    async def _handle_ddp_changed(self, event: Dict[str, Any]) -> None:
        if event.get("collection") != "stream-room-messages":
            return
        fields = event.get("fields") or {}
        args = fields.get("args") or []
        if not args:
            return
        post = args[0]
        if not isinstance(post, dict):
            return

        # System messages carry a ``t`` type code (room name change, join, …).
        if post.get("t"):
            return

        sender = post.get("u") or {}
        sender_id = sender.get("_id", "")
        # Ignore our own messages (prevents reply loops).
        if sender_id == self._bot_user_id:
            return

        message_id = post.get("_id", "")
        # Dedup also suppresses edit re-deliveries (same _id).
        if self._dedup.is_duplicate(message_id):
            return

        rid = post.get("rid", "")
        chat_type = await self._get_room_type(rid)
        text = post.get("msg", "") or ""
        sender_name = sender.get("username", "") or sender_id

        # Mention-gating for non-DM rooms.
        if chat_type != "dm":
            extra = self.config.extra or {}

            allowed_raw = extra.get("allowed_channels")
            if allowed_raw is None:
                allowed_raw = os.getenv("ROCKETCHAT_ALLOWED_CHANNELS", "")
            allowed = _split_csv(allowed_raw)
            if allowed and rid not in allowed:
                return

            require_mention = os.getenv("ROCKETCHAT_REQUIRE_MENTION", "true").lower() not in {"false", "0", "no"}
            free_channels = _split_csv(os.getenv("ROCKETCHAT_FREE_RESPONSE_CHANNELS", ""))
            is_free = rid in free_channels

            mentions = post.get("mentions") or []
            has_mention = any(
                (m.get("_id") == self._bot_user_id) or (m.get("username") == self._bot_username)
                for m in mentions if isinstance(m, dict)
            )
            if not has_mention and self._bot_username:
                has_mention = f"@{self._bot_username}".lower() in text.lower()

            if require_mention and not is_free and not has_mention:
                return

            # Strip the @mention so the agent sees clean input.
            if has_mention and self._bot_username:
                text = re.sub(re.escape(f"@{self._bot_username}"), "", text, flags=re.IGNORECASE).strip()

        msg_type = MessageType.COMMAND if text.startswith("/") else MessageType.TEXT

        # Download attachments to local cache so downstream tools (vision,
        # transcription) can read them without auth headers.
        media_urls: List[str] = []
        media_types: List[str] = []
        for att in (post.get("attachments") or []):
            await self._cache_attachment(att, media_urls, media_types)

        if media_types and msg_type == MessageType.TEXT:
            if any(m.startswith("image/") for m in media_types):
                msg_type = MessageType.PHOTO
            elif any(m.startswith("audio/") for m in media_types):
                msg_type = MessageType.VOICE
            else:
                msg_type = MessageType.DOCUMENT

        thread_id = post.get("tmid") or None

        source = self.build_source(
            chat_id=rid,
            chat_type=chat_type,
            user_id=sender_id,
            user_name=sender_name,
            thread_id=thread_id,
        )

        from gateway.platforms.base import resolve_channel_prompt
        channel_prompt = resolve_channel_prompt(self.config.extra, rid, None)

        await self.handle_message(MessageEvent(
            text=text,
            message_type=msg_type,
            source=source,
            raw_message=post,
            message_id=message_id,
            media_urls=media_urls or None,
            media_types=media_types or None,
            channel_prompt=channel_prompt,
        ))

    def _resolve_attachment_url(self, rel: str) -> Tuple[Optional[str], bool]:
        """Resolve an attachment reference to a URL we are willing to fetch.

        Returns ``(url, same_origin)``; ``(None, False)`` means "do not fetch".
        Relative references resolve against our own server. Absolute ones are
        only allowed when they point at that same origin, or -- for genuinely
        external links -- when they pass Hermes' SSRF filter, in which case they
        are fetched without credentials.
        """
        rel = str(rel)
        if not rel.startswith(("http://", "https://")):
            # Relative to our own server. Guard against "//evil.example/x",
            # which urljoin would happily treat as a protocol-relative URL.
            if rel.startswith("//"):
                logger.warning("Rocket.Chat: refused protocol-relative attachment URL")
                return None, False
            return urljoin(self._base_url + "/", rel.lstrip("/")), True

        try:
            target = urlparse(rel)
            ours = urlparse(self._base_url)
        except ValueError:
            logger.warning("Rocket.Chat: refused malformed attachment URL")
            return None, False

        if (target.scheme, target.netloc) == (ours.scheme, ours.netloc):
            return rel, True

        # Someone else's host: never send credentials, and only proceed if the
        # target is not an internal address (link previews legitimately point
        # outward, so this is not an error).
        try:
            from tools.url_safety import is_safe_url
        except ImportError:
            logger.warning("Rocket.Chat: url_safety unavailable, skipping external attachment")
            return None, False
        if not is_safe_url(rel):
            logger.warning("Rocket.Chat: blocked unsafe attachment URL (SSRF protection)")
            return None, False
        return rel, False

    async def _cache_attachment(
        self, att: Dict[str, Any], media_urls: List[str], media_types: List[str],
    ) -> None:
        """Download a single Rocket.Chat attachment into the local cache.

        SECURITY: ``att`` comes straight off the wire and every field in it is
        attacker-controlled. Rocket.Chat's own link preview fills ``title_link``
        / ``image_url`` with arbitrary external URLs the moment anyone posts a
        link, so this runs on untrusted input by design. Two rules follow:

        1. The bot's Personal Access Token goes out **only** to our own server.
           Sending it anywhere else hands over the bot account.
        2. Only same-origin URLs are fetched at all, and redirects are refused,
           so this cannot be turned into an SSRF probe against the host network.

        Note this runs *before* the core authorizes the sender
        (``ROCKETCHAT_ALLOWED_USERS``), so the allowlist is not a mitigation.
        """
        import aiohttp
        rel = att.get("image_url") or att.get("audio_url") or att.get("video_url") or att.get("title_link")
        if not rel:
            return

        dl_url, same_origin = self._resolve_attachment_url(rel)
        if dl_url is None:
            return

        try:
            async with self._session.get(
                dl_url,
                # Credentials only ever leave for our own origin.
                headers=self._auth_headers() if same_origin else None,
                # A redirect would carry the headers to whatever host it names.
                allow_redirects=False,
                timeout=aiohttp.ClientTimeout(total=30),
            ) as resp:
                if resp.status in (301, 302, 303, 307, 308):
                    logger.warning(
                        "Rocket.Chat: attachment download refused (redirect to another host)"
                    )
                    return
                if resp.status >= 400:
                    logger.warning("Rocket.Chat: attachment download failed (HTTP %s)", resp.status)
                    return
                declared = resp.headers.get("Content-Length")
                if declared and declared.isdigit() and int(declared) > _MAX_ATTACHMENT_BYTES:
                    logger.warning(
                        "Rocket.Chat: attachment too large (%s bytes, limit %s)",
                        declared, _MAX_ATTACHMENT_BYTES,
                    )
                    return
                # Content-Length can lie or be absent; cap the actual read too.
                data = await resp.content.read(_MAX_ATTACHMENT_BYTES + 1)
                if len(data) > _MAX_ATTACHMENT_BYTES:
                    logger.warning(
                        "Rocket.Chat: attachment exceeded %s bytes, discarded",
                        _MAX_ATTACHMENT_BYTES,
                    )
                    return
                mime = resp.content_type or att.get("image_type") or "application/octet-stream"
        except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
            logger.warning("Rocket.Chat: attachment download error: %s", exc)
            return

        from gateway.platforms.base import (
            cache_image_from_bytes, cache_audio_from_bytes, cache_document_from_bytes,
        )
        raw_name = att.get("title") or rel.rsplit("/", 1)[-1].split("?")[0] or "file"
        # Strip any directory component -- the name reaches a file write downstream.
        fname = Path(str(raw_name)).name or "file"
        ext = Path(fname).suffix
        # Caching can raise (e.g. cache_image_from_bytes on non-image bytes such
        # as an HTML error page served with an image/* content-type). Don't let
        # one bad attachment abort handling of the whole message.
        try:
            if mime.startswith("image/"):
                media_urls.append(cache_image_from_bytes(data, ext or ".png"))
                media_types.append(mime)
            elif mime.startswith("audio/"):
                media_urls.append(cache_audio_from_bytes(data, ext or ".ogg"))
                media_types.append(mime)
            else:
                media_urls.append(cache_document_from_bytes(data, fname))
                media_types.append(mime)
        except Exception as exc:
            logger.warning("Rocket.Chat: failed to cache attachment %s: %s", fname, exc)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _split_csv(value: Any) -> set:
    """Normalise a list or comma-separated string into a set of trimmed values."""
    if isinstance(value, list):
        return {str(v).strip() for v in value if str(v).strip()}
    return {v.strip() for v in str(value or "").split(",") if v.strip()}


# ---------------------------------------------------------------------------
# Out-of-process cron delivery (standalone REST send)
# ---------------------------------------------------------------------------


async def _standalone_send(
    pconfig,
    chat_id: str,
    message: str,
    *,
    thread_id: Optional[str] = None,
    media_files: Optional[list] = None,
    force_document: bool = False,
) -> Dict[str, Any]:
    """Send via the Rocket.Chat REST API without a live gateway adapter.

    Used by ``tools/send_message_tool`` for cron jobs that run separately
    from the gateway process.  Reads ``ROCKETCHAT_TOKEN`` /
    ``ROCKETCHAT_USER_ID`` from ``pconfig`` (set by the config loader from
    env) with an env-var fallback; the server URL comes from
    ``pconfig.extra['url']`` or ``ROCKETCHAT_URL``.
    """
    try:
        import aiohttp
    except ImportError:
        return {"error": "aiohttp not installed. Run: pip install aiohttp"}

    extra = getattr(pconfig, "extra", {}) or {}
    base_url = (extra.get("url") or os.getenv("ROCKETCHAT_URL", "")).rstrip("/")
    token = (getattr(pconfig, "token", None) or os.getenv("ROCKETCHAT_TOKEN", "")).strip()
    user_id = (extra.get("user_id") or os.getenv("ROCKETCHAT_USER_ID", "")).strip()
    if not base_url or not token or not user_id:
        return {"error": "Rocket.Chat standalone send: ROCKETCHAT_URL, ROCKETCHAT_USER_ID and ROCKETCHAT_TOKEN must be set"}

    headers = {"X-Auth-Token": token, "X-User-Id": user_id, "Content-Type": "application/json"}
    upload_headers = {"X-Auth-Token": token, "X-User-Id": user_id}
    media_files = media_files or []

    try:
        from gateway.platforms.base import resolve_proxy_url, proxy_kwargs_for_aiohttp
        proxy = resolve_proxy_url(platform_env_var="ROCKETCHAT_PROXY")
        sess_kw, req_kw = proxy_kwargs_for_aiohttp(proxy)

        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=60), **sess_kw) as session:
            # Upload any media first (each as its own message in the room).
            last_mid = None
            for media in media_files:
                file_path = media.get("path") if isinstance(media, dict) else media
                if not file_path or not os.path.exists(file_path):
                    continue
                form = aiohttp.FormData()
                with open(file_path, "rb") as fh:
                    form.add_field("file", fh.read(), filename=os.path.basename(file_path))
                if thread_id:
                    form.add_field("tmid", thread_id)
                async with session.post(
                    f"{base_url}/api/v1/rooms.upload/{chat_id}",
                    data=form, headers=upload_headers, **req_kw,
                ) as up:
                    if up.status not in {200, 201}:
                        body = await up.text()
                        return {"error": f"Rocket.Chat upload failed ({up.status}): {body[:400]}"}
                    last_mid = ((await up.json()).get("message") or {}).get("_id")

            # Nothing to post (no text): treat as a no-op like the in-process
            # send() does, instead of POSTing an empty msg that Rocket.Chat
            # rejects with a 400.
            if not message:
                return {"success": True, "platform": "rocketchat", "chat_id": chat_id, "message_id": last_mid}

            payload: Dict[str, Any] = {"message": {"rid": chat_id, "msg": message}}
            if thread_id:
                payload["message"]["tmid"] = thread_id
            async with session.post(
                f"{base_url}/api/v1/chat.sendMessage",
                headers=headers, json=payload, **req_kw,
            ) as resp:
                if resp.status not in {200, 201}:
                    body = await resp.text()
                    return {"error": f"Rocket.Chat API error ({resp.status}): {body[:400]}"}
                data = await resp.json()
            return {
                "success": True,
                "platform": "rocketchat",
                "chat_id": chat_id,
                "message_id": (data.get("message") or {}).get("_id"),
            }
    except aiohttp.ClientError as exc:
        return {"error": f"Rocket.Chat send failed (network): {exc}"}
    except Exception as exc:  # noqa: BLE001
        return {"error": f"Rocket.Chat send failed: {exc}"}


# ---------------------------------------------------------------------------
# Interactive setup wizard
# ---------------------------------------------------------------------------


def interactive_setup() -> None:
    """Guide the user through Rocket.Chat bot setup."""
    from hermes_cli.config import get_env_value, save_env_value
    from hermes_cli.cli_output import (
        prompt, prompt_yes_no, print_header, print_info, print_success,
    )

    print_header("Rocket.Chat")
    if get_env_value("ROCKETCHAT_TOKEN") and get_env_value("ROCKETCHAT_USER_ID"):
        print_info("Rocket.Chat: already configured")
        if not prompt_yes_no("Reconfigure Rocket.Chat?", False):
            return

    print_info("Works with any self-hosted or cloud Rocket.Chat workspace.")
    print_info("   1. Log in as the bot account")
    print_info("   2. Avatar → My Account → Personal Access Tokens → Add")
    print_info("   3. Tick 'Ignore Two Factor Authentication', then copy the token AND user id")
    print()
    url = prompt("Rocket.Chat server URL (e.g. https://chat.example.com)")
    if url:
        save_env_value("ROCKETCHAT_URL", url.rstrip("/"))
    user_id = prompt("Bot user id")
    if user_id:
        save_env_value("ROCKETCHAT_USER_ID", user_id)
    token = prompt("Personal Access Token", password=True)
    if not token:
        return
    save_env_value("ROCKETCHAT_TOKEN", token)
    print_success("Rocket.Chat token saved")

    print()
    print_info("🔒 Security: restrict who can use your bot")
    allowed = prompt("Allowed usernames/ids (comma-separated, empty for open access)")
    if allowed:
        save_env_value("ROCKETCHAT_ALLOWED_USERS", allowed.replace(" ", ""))
        print_success("Rocket.Chat allowlist configured")
    else:
        print_info("⚠️  No allowlist set — anyone who can message the bot can use it!")

    print()
    print_info("📬 Home channel: where Hermes delivers cron results / notifications.")
    print_info("   Room id is the rid — open the room, it's in the admin info or the URL.")
    home = prompt("Home room id (leave empty to set later with /set-home)")
    if home:
        save_env_value("ROCKETCHAT_HOME_CHANNEL", home)


# ---------------------------------------------------------------------------
# YAML -> env config bridge
# ---------------------------------------------------------------------------


def _apply_yaml_config(yaml_cfg: dict, rc_cfg: dict) -> dict | None:
    """Translate ``config.yaml`` ``rocketchat:`` keys into env + extras.

    ``url`` / ``user_id`` are seeded into ``PlatformConfig.extra`` (returned
    dict).  Behavioural keys are bridged to the ``ROCKETCHAT_*`` env vars the
    adapter reads via ``os.getenv()``.  Env vars win — each assignment is
    guarded by ``not os.getenv(...)``.
    """
    extras: Dict[str, Any] = {}
    if rc_cfg.get("url") and not os.getenv("ROCKETCHAT_URL"):
        os.environ["ROCKETCHAT_URL"] = str(rc_cfg["url"]).rstrip("/")
    if rc_cfg.get("url"):
        extras["url"] = str(rc_cfg["url"]).rstrip("/")
    if rc_cfg.get("user_id"):
        extras["user_id"] = str(rc_cfg["user_id"])
        if not os.getenv("ROCKETCHAT_USER_ID"):
            os.environ["ROCKETCHAT_USER_ID"] = str(rc_cfg["user_id"])
    if "reply_mode" in rc_cfg:
        extras["reply_mode"] = str(rc_cfg["reply_mode"]).lower()
    if "require_mention" in rc_cfg and not os.getenv("ROCKETCHAT_REQUIRE_MENTION"):
        os.environ["ROCKETCHAT_REQUIRE_MENTION"] = str(rc_cfg["require_mention"]).lower()
    for key, env in (
        ("free_response_channels", "ROCKETCHAT_FREE_RESPONSE_CHANNELS"),
        ("allowed_channels", "ROCKETCHAT_ALLOWED_CHANNELS"),
    ):
        val = rc_cfg.get(key)
        if val is not None and not os.getenv(env):
            if isinstance(val, list):
                val = ",".join(str(v) for v in val)
            os.environ[env] = str(val)
    return extras or None


def _env_enablement() -> Optional[dict]:
    """Seed ``PlatformConfig.extra`` from env so env-only setups show up in
    ``hermes gateway status`` before the adapter is instantiated."""
    url = os.getenv("ROCKETCHAT_URL", "")
    user_id = os.getenv("ROCKETCHAT_USER_ID", "")
    if not url:
        return None
    extras: Dict[str, Any] = {"url": url.rstrip("/")}
    if user_id:
        extras["user_id"] = user_id
    return extras


def _is_connected(config) -> bool:
    """Connected when a URL and a working credential pair are present."""
    import hermes_cli.gateway as gateway_mod
    url = (gateway_mod.get_env_value("ROCKETCHAT_URL") or "").strip()
    token = (gateway_mod.get_env_value("ROCKETCHAT_TOKEN") or "").strip()
    user_id = (gateway_mod.get_env_value("ROCKETCHAT_USER_ID") or "").strip()
    username = (gateway_mod.get_env_value("ROCKETCHAT_USERNAME") or "").strip()
    password = (gateway_mod.get_env_value("ROCKETCHAT_PASSWORD") or "").strip()
    return bool(url and ((token and user_id) or (username and password)))


# ---------------------------------------------------------------------------
# Plugin registration entry point
# ---------------------------------------------------------------------------


def _build_adapter(config):
    return RocketChatAdapter(config)


def register(ctx) -> None:
    """Plugin entry point — called by the Hermes plugin system."""
    ctx.register_platform(
        name=PLATFORM_NAME,
        label="Rocket.Chat",
        adapter_factory=_build_adapter,
        check_fn=check_rocketchat_requirements,
        is_connected=_is_connected,
        required_env=["ROCKETCHAT_URL", "ROCKETCHAT_USER_ID", "ROCKETCHAT_TOKEN"],
        install_hint="pip install aiohttp",
        setup_fn=interactive_setup,
        apply_yaml_config_fn=_apply_yaml_config,
        env_enablement_fn=_env_enablement,
        allowed_users_env="ROCKETCHAT_ALLOWED_USERS",
        allow_all_env="ROCKETCHAT_ALLOW_ALL_USERS",
        cron_deliver_env_var="ROCKETCHAT_HOME_CHANNEL",
        standalone_sender_fn=_standalone_send,
        max_message_length=MAX_MESSAGE_LENGTH,
        emoji="🚀",
        platform_hint=(
            "You are on Rocket.Chat. Use standard Markdown for formatting "
            "(bold **text**, italics, `code`, ```code blocks```, lists, links). "
            "Replies are sent into the room; threads are used only when the "
            "user wrote in a thread."
        ),
        allow_update_command=True,
    )
