"""Regression tests for the changes this fork exists to carry.

Upstream (wachtelhund/hermes-rocketchat-gateway) stopped being updated on
2026-06-15. These cover the fixes layered on top, so a future refactor cannot
quietly reintroduce them:

* ``connect(is_reconnect=...)`` — without it the plugin does not load at all
* ``edit_message`` — without it the core drops all live progress updates
* typing indicator — sent over the modern ``user-activity`` event
* attachment fetching — must not leak the bot's token or become an SSRF probe

Transport is mocked; no live server is involved. Async methods are driven with
``asyncio.run`` to match the rest of the suite (no pytest-asyncio).
"""
import asyncio
import inspect
from unittest.mock import AsyncMock, MagicMock

import pytest


def run(coro):
    return asyncio.run(coro)


class FakeWS:
    """Minimal stand-in for aiohttp's ClientWebSocketResponse."""

    def __init__(self, closed=False):
        self.closed = closed
        self.sent = []

    async def send_json(self, payload):
        self.sent.append(payload)


# ---------------------------------------------------------------------------
# connect(is_reconnect=...)
# ---------------------------------------------------------------------------

class TestConnectSignature:
    def test_accepts_is_reconnect_keyword(self, rc_module):
        """The core calls connect(is_reconnect=...) unconditionally.

        Upstream's `async def connect(self)` raises TypeError here, which is
        what makes the plugin unusable on current Hermes.
        """
        sig = inspect.signature(rc_module.RocketChatAdapter.connect)
        assert "is_reconnect" in sig.parameters
        param = sig.parameters["is_reconnect"]
        assert param.default is False
        assert param.kind is inspect.Parameter.KEYWORD_ONLY

    def test_matches_base_class_signature(self, rc_module):
        from gateway.platforms.base import BasePlatformAdapter

        ours = inspect.signature(rc_module.RocketChatAdapter.connect)
        base = inspect.signature(BasePlatformAdapter.connect)
        assert ours.parameters.keys() == base.parameters.keys()

    def test_refuses_without_url(self, make_adapter):
        """Guard clause still fires, and passing the keyword is accepted."""
        adapter = make_adapter()
        adapter._base_url = ""
        assert run(adapter.connect(is_reconnect=True)) is False


# ---------------------------------------------------------------------------
# Typing indicator
# ---------------------------------------------------------------------------

class TestTypingIndicator:
    def test_signatures_match_base_class(self, rc_module):
        from gateway.platforms.base import BasePlatformAdapter

        for name in ("send_typing", "stop_typing"):
            ours = inspect.signature(getattr(rc_module.RocketChatAdapter, name))
            base = inspect.signature(getattr(BasePlatformAdapter, name))
            assert ours.parameters.keys() == base.parameters.keys(), name

    def test_emits_modern_user_activity_event(self, make_adapter):
        """Rocket.Chat 4.x+ clients render `<rid>/user-activity`.

        The legacy `<rid>/typing` event is still sent for old clients, but on
        its own nothing is displayed — that is the whole point of this fix.
        """
        adapter = make_adapter()
        ws = FakeWS()
        adapter._ws = ws

        run(adapter._notify_typing("room1", True))

        streams = [p["params"][0] for p in ws.sent]
        assert "room1/user-activity" in streams
        assert "room1/typing" in streams

        modern = next(p for p in ws.sent if p["params"][0] == "room1/user-activity")
        assert modern["method"] == "stream-notify-room"
        assert modern["params"][1] == "hermes"
        assert modern["params"][2] == ["user-typing"]
        assert "id" in modern  # DDP method calls require an id

    def test_stop_clears_activity_list(self, make_adapter):
        adapter = make_adapter()
        ws = FakeWS()
        adapter._ws = ws

        run(adapter._notify_typing("room1", False))

        modern = next(p for p in ws.sent if p["params"][0] == "room1/user-activity")
        assert modern["params"][2] == []
        legacy = next(p for p in ws.sent if p["params"][0] == "room1/typing")
        assert legacy["params"][2] is False

    def test_noop_when_socket_closed(self, make_adapter):
        adapter = make_adapter()
        adapter._ws = FakeWS(closed=True)
        run(adapter._notify_typing("room1", True))
        assert adapter._ws.sent == []

    def test_send_typing_registers_one_task(self, make_adapter):
        adapter = make_adapter()
        adapter._ws = FakeWS()

        async def scenario():
            await adapter.send_typing("room1")
            first = adapter._typing_tasks["room1"]
            # A second call while the first is live must not start another
            # loop; the loser would be unreachable to stop_typing.
            await adapter.send_typing("room1")
            assert adapter._typing_tasks["room1"] is first
            await adapter.stop_typing("room1")
            return first

        task = run(scenario())
        assert task.done()
        assert "room1" not in adapter._typing_tasks

    def test_concurrent_send_typing_does_not_orphan(self, make_adapter):
        """Regression: an await between the check and the assignment let two
        concurrent callers both spawn a loop, orphaning the first."""
        adapter = make_adapter()
        adapter._ws = FakeWS()

        async def scenario():
            await asyncio.gather(*(adapter.send_typing("room1") for _ in range(5)))
            tasks = [t for t in asyncio.all_tasks() if t.get_name().startswith("rc-typing-")]
            assert len(tasks) == 1
            await adapter.stop_typing("room1")
            return tasks[0]

        task = run(scenario())
        assert task.done()

    def test_stop_typing_sends_false(self, make_adapter):
        adapter = make_adapter()
        ws = FakeWS()
        adapter._ws = ws

        async def scenario():
            await adapter.send_typing("room1")
            ws.sent.clear()
            await adapter.stop_typing("room1")

        run(scenario())
        legacy = [p for p in ws.sent if p["params"][0] == "room1/typing"]
        assert legacy and legacy[-1]["params"][2] is False

    def test_disconnect_finishes_typing_tasks(self, make_adapter):
        """cancel() only schedules cancellation; disconnect must await it or
        the loop dies with 'Task was destroyed but it is pending!'."""
        adapter = make_adapter()
        adapter._ws = FakeWS()
        adapter._session = MagicMock(closed=True)

        async def scenario():
            await adapter.send_typing("room1")
            await adapter.send_typing("room2")
            tasks = list(adapter._typing_tasks.values())
            await adapter.disconnect()
            return tasks

        tasks = run(scenario())
        assert all(t.done() for t in tasks)
        assert adapter._typing_tasks == {}

    def test_loop_is_bounded(self, rc_module):
        """A missed stop_typing must not leave a task spinning forever."""
        assert rc_module._TYPING_MAX_DURATION > 0
        assert rc_module._TYPING_REFRESH_INTERVAL > 0
        # Refresh must be well inside Rocket.Chat's ~15s expiry.
        assert rc_module._TYPING_REFRESH_INTERVAL < 15


# ---------------------------------------------------------------------------
# Attachment fetching (credential leak / SSRF)
# ---------------------------------------------------------------------------

class TestAttachmentUrlResolution:
    def test_relative_url_resolves_to_our_server(self, make_adapter):
        adapter = make_adapter()
        url, same_origin = adapter._resolve_attachment_url("/file-upload/abc/x.png")
        assert url == "https://chat.example.com/file-upload/abc/x.png"
        assert same_origin is True

    def test_same_origin_absolute_keeps_credentials(self, make_adapter):
        adapter = make_adapter()
        url, same_origin = adapter._resolve_attachment_url(
            "https://chat.example.com/file-upload/abc/x.png"
        )
        assert same_origin is True

    def test_foreign_host_never_gets_credentials(self, make_adapter, monkeypatch):
        """The bot's Personal Access Token must not leave our origin.

        Rocket.Chat's link preview puts arbitrary external URLs in title_link
        whenever anyone posts a link, so this path runs on attacker-controlled
        input by design — and it runs before the core checks the allowlist.
        """
        import sys, types

        stub = types.ModuleType("tools.url_safety")
        stub.is_safe_url = lambda _u: True
        monkeypatch.setitem(sys.modules, "tools.url_safety", stub)

        adapter = make_adapter()
        url, same_origin = adapter._resolve_attachment_url("https://evil.example/x.png")
        assert url == "https://evil.example/x.png"
        assert same_origin is False, "credentials would be sent to a foreign host"

    def test_unsafe_url_is_refused(self, make_adapter, monkeypatch):
        import sys, types

        stub = types.ModuleType("tools.url_safety")
        stub.is_safe_url = lambda _u: False
        monkeypatch.setitem(sys.modules, "tools.url_safety", stub)

        adapter = make_adapter()
        url, _ = adapter._resolve_attachment_url("http://169.254.169.254/latest/meta-data/")
        assert url is None

    def test_protocol_relative_url_is_refused(self, make_adapter):
        """`//evil.example/x` would otherwise be treated as a foreign origin
        by urljoin while looking like a relative path."""
        adapter = make_adapter()
        url, _ = adapter._resolve_attachment_url("//evil.example/x.png")
        assert url is None

    def test_size_limit_is_configured(self, rc_module):
        assert rc_module._MAX_ATTACHMENT_BYTES > 0


# ---------------------------------------------------------------------------
# Live progress (message editing)
# ---------------------------------------------------------------------------

class TestEditMessage:
    def test_overrides_base_implementation(self, rc_module):
        """The core drops tool-progress/streaming updates entirely when an
        adapter leaves edit_message at the base implementation
        (gateway/run.py checks identity against BasePlatformAdapter)."""
        from gateway.platforms.base import BasePlatformAdapter

        assert (
            rc_module.RocketChatAdapter.edit_message
            is not BasePlatformAdapter.edit_message
        )

    def test_signature_matches_base_class(self, rc_module):
        from gateway.platforms.base import BasePlatformAdapter

        ours = inspect.signature(rc_module.RocketChatAdapter.edit_message)
        base = inspect.signature(BasePlatformAdapter.edit_message)
        assert ours.parameters.keys() == base.parameters.keys()

    def test_calls_chat_update(self, make_adapter):
        adapter = make_adapter()
        adapter._api_post = AsyncMock(return_value={"success": True})

        result = run(adapter.edit_message("room1", "msg1", "updated"))

        assert result.success is True
        assert result.message_id == "msg1"
        endpoint, payload = adapter._api_post.await_args.args
        assert endpoint == "chat.update"
        assert payload["roomId"] == "room1"
        assert payload["msgId"] == "msg1"
        assert payload["text"] == "updated"

    def test_requires_message_id(self, make_adapter):
        adapter = make_adapter()
        adapter._api_post = AsyncMock()
        result = run(adapter.edit_message("room1", "", "x"))
        assert result.success is False
        adapter._api_post.assert_not_awaited()

    def test_failure_is_reported_so_caller_can_fall_back(self, make_adapter):
        adapter = make_adapter()
        adapter._api_post = AsyncMock(return_value={"success": False})
        result = run(adapter.edit_message("room1", "msg1", "x"))
        assert result.success is False

    def test_oversized_content_is_truncated(self, make_adapter, rc_module):
        """An edit cannot be split across messages the way send() chunks."""
        adapter = make_adapter()
        adapter._api_post = AsyncMock(return_value={"success": True})

        run(adapter.edit_message("room1", "msg1", "x" * (rc_module.MAX_MESSAGE_LENGTH + 500)))

        _, payload = adapter._api_post.await_args.args
        assert len(payload["text"]) <= rc_module.MAX_MESSAGE_LENGTH


class TestDeleteMessage:
    def test_overrides_base_implementation(self, rc_module):
        """Required for display.cleanup_progress -- without an override the
        core cannot remove the tool-progress messages it posted."""
        from gateway.platforms.base import BasePlatformAdapter

        assert (
            rc_module.RocketChatAdapter.delete_message
            is not BasePlatformAdapter.delete_message
        )

    def test_signature_matches_base_class(self, rc_module):
        from gateway.platforms.base import BasePlatformAdapter

        ours = inspect.signature(rc_module.RocketChatAdapter.delete_message)
        base = inspect.signature(BasePlatformAdapter.delete_message)
        assert ours.parameters.keys() == base.parameters.keys()

    def test_calls_chat_delete(self, make_adapter):
        adapter = make_adapter()
        adapter._api_post = AsyncMock(return_value={"success": True})

        assert run(adapter.delete_message("room1", "msg1")) is True

        endpoint, payload = adapter._api_post.await_args.args
        assert endpoint == "chat.delete"
        assert payload["roomId"] == "room1"
        assert payload["msgId"] == "msg1"

    def test_requires_message_id(self, make_adapter):
        adapter = make_adapter()
        adapter._api_post = AsyncMock()
        assert run(adapter.delete_message("room1", "")) is False
        adapter._api_post.assert_not_awaited()

    def test_failure_returns_false(self, make_adapter):
        adapter = make_adapter()
        adapter._api_post = AsyncMock(return_value={"success": False})
        assert run(adapter.delete_message("room1", "msg1")) is False


# ---------------------------------------------------------------------------
# Capability flags read off the class with getattr()
# ---------------------------------------------------------------------------

class TestCapabilityFlags:
    def test_declares_it_splits_long_messages(self, rc_module):
        """send() chunks via truncate_message. Without the flag the delivery
        router replaces >4000 char output with a file reference instead."""
        assert rc_module.RocketChatAdapter.splits_long_messages is True

    def test_declares_code_block_support(self, rc_module):
        """Rocket.Chat renders Markdown; without this tool progress falls back
        to a short inline preview instead of a fenced block."""
        assert rc_module.RocketChatAdapter.supports_code_blocks is True

    def test_max_message_length_is_a_class_attribute(self, rc_module):
        """The core reads it with getattr(self, "MAX_MESSAGE_LENGTH", 4096).
        As a module-level constant only, it would size progress bubbles to
        4096 while our edit path trims at 4000, dropping the tail."""
        assert (
            getattr(rc_module.RocketChatAdapter, "MAX_MESSAGE_LENGTH", None)
            == rc_module.MAX_MESSAGE_LENGTH
        )


# ---------------------------------------------------------------------------
# send(): threading and multi-chunk ids
# ---------------------------------------------------------------------------

class TestSendThreadingAndIds:
    def test_metadata_thread_id_is_honoured(self, make_adapter):
        """Inbound messages carry their thread as source.thread_id. Ignoring it
        made progress and answers land in the channel instead of the thread."""
        adapter = make_adapter()
        adapter._api_post = AsyncMock(return_value={"success": True, "message": {"_id": "m1"}})

        run(adapter.send("room1", "hi", metadata={"thread_id": "root1"}))

        _, payload = adapter._api_post.await_args.args
        assert payload["message"]["tmid"] == "root1"

    def test_no_thread_when_not_requested(self, make_adapter):
        adapter = make_adapter()
        adapter._api_post = AsyncMock(return_value={"success": True, "message": {"_id": "m1"}})

        run(adapter.send("room1", "hi"))

        _, payload = adapter._api_post.await_args.args
        assert "tmid" not in payload["message"]

    def test_split_message_exposes_every_id(self, make_adapter, rc_module):
        """Cleanup reads the extras from raw_response["message_ids"]; returning
        only the final chunk's id left earlier chunks undeletable."""
        adapter = make_adapter()
        ids = iter(["m1", "m2", "m3"])
        adapter._api_post = AsyncMock(
            side_effect=lambda *a, **k: {"success": True, "message": {"_id": next(ids)}}
        )

        result = run(adapter.send("room1", "x" * (rc_module.MAX_MESSAGE_LENGTH * 2 + 10)))

        assert result.success is True
        assert adapter._api_post.await_count >= 2
        tracked = (result.raw_response or {}).get("message_ids")
        assert tracked and len(tracked) == adapter._api_post.await_count

    def test_single_chunk_has_no_continuation(self, make_adapter):
        adapter = make_adapter()
        adapter._api_post = AsyncMock(return_value={"success": True, "message": {"_id": "m1"}})
        result = run(adapter.send("room1", "short"))
        assert result.message_id == "m1"
        assert result.raw_response is None


# ---------------------------------------------------------------------------
# Single-instance guard / typing pause
# ---------------------------------------------------------------------------

class TestPlatformLock:
    def test_connect_refuses_when_lock_is_held(self, make_adapter):
        """Two gateways on one bot account both receive every message over DDP
        and both answer, so the room sees duplicate replies."""
        adapter = make_adapter()
        adapter._acquire_platform_lock = MagicMock(return_value=False)
        assert run(adapter.connect()) is False
        adapter._acquire_platform_lock.assert_called_once()

    def test_lock_backend_failure_is_non_fatal(self, make_adapter):
        """A lock backend problem must not stop a legitimate single instance."""
        adapter = make_adapter()
        adapter._acquire_platform_lock = MagicMock(side_effect=RuntimeError("boom"))
        adapter._base_url = ""  # stop right after the lock step
        assert run(adapter.connect()) is False  # refused for the URL, not the lock


class TestTypingPause:
    def test_loop_stops_emitting_while_paused(self, make_adapter):
        """The core parks typing during approval waits via
        pause_typing_for_chat(); ignoring it leaves 'typing' up the whole time."""
        adapter = make_adapter()
        ws = FakeWS()
        adapter._ws = ws

        async def scenario():
            adapter.pause_typing_for_chat("room1")
            await adapter.send_typing("room1")
            await asyncio.sleep(0.05)
            sent_while_paused = [
                p for p in ws.sent
                if p["params"][0] == "room1/user-activity" and p["params"][2] == ["user-typing"]
            ]
            await adapter.stop_typing("room1")
            return sent_while_paused

        assert run(scenario()) == []
