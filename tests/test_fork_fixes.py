"""Regression tests for the changes this fork exists to carry.

Upstream (wachtelhund/hermes-rocketchat-gateway) stopped being updated on
2026-06-15. These cover the fixes layered on top, so a future refactor cannot
quietly reintroduce them:

* ``connect(is_reconnect=...)`` — without it the plugin does not load at all
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
