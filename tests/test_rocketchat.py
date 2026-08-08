"""Unit tests for the Rocket.Chat gateway adapter.

Transport (aiohttp / DDP WebSocket) is mocked throughout — these tests cover
parsing, gating, chunking and config logic without a live server. An optional
live end-to-end test lives in ``tests/e2e/``.

Async adapter methods are driven with ``asyncio.run`` so the suite needs no
``pytest-asyncio`` plugin.
"""
import asyncio
from unittest.mock import AsyncMock

import pytest


def run(coro):
    return asyncio.run(coro)


# ---------------------------------------------------------------------------
# Registration / platform resolution
# ---------------------------------------------------------------------------

class TestRegistration:
    def test_platform_resolves(self, rc_module):
        from gateway.config import Platform
        assert Platform("rocketchat").value == "rocketchat"

    def test_registered_in_registry(self, rc_module):
        from gateway.platform_registry import platform_registry
        assert platform_registry.is_registered("rocketchat")
        entry = platform_registry.get("rocketchat")
        assert entry.label == "Rocket.Chat"
        assert entry.standalone_sender_fn is rc_module._standalone_send
        assert entry.max_message_length == rc_module.MAX_MESSAGE_LENGTH
        assert entry.allowed_users_env == "ROCKETCHAT_ALLOWED_USERS"
        assert entry.cron_deliver_env_var == "ROCKETCHAT_HOME_CHANNEL"

    def test_adapter_constructs(self, make_adapter):
        from gateway.config import Platform
        a = make_adapter()
        assert a.platform == Platform("rocketchat")
        assert a._base_url == "https://chat.example.com"


# ---------------------------------------------------------------------------
# Requirements check
# ---------------------------------------------------------------------------

class TestRequirements:
    def test_ok_with_token(self, rc_module, monkeypatch):
        monkeypatch.setenv("ROCKETCHAT_URL", "https://chat.example.com")
        monkeypatch.setenv("ROCKETCHAT_USER_ID", "u")
        monkeypatch.setenv("ROCKETCHAT_TOKEN", "t")
        assert rc_module.check_rocketchat_requirements() is True

    def test_ok_with_password(self, rc_module, monkeypatch):
        monkeypatch.setenv("ROCKETCHAT_URL", "https://chat.example.com")
        monkeypatch.delenv("ROCKETCHAT_TOKEN", raising=False)
        monkeypatch.delenv("ROCKETCHAT_USER_ID", raising=False)
        monkeypatch.setenv("ROCKETCHAT_USERNAME", "bot")
        monkeypatch.setenv("ROCKETCHAT_PASSWORD", "pw")
        assert rc_module.check_rocketchat_requirements() is True

    def test_missing_url(self, rc_module, monkeypatch):
        monkeypatch.delenv("ROCKETCHAT_URL", raising=False)
        monkeypatch.setenv("ROCKETCHAT_TOKEN", "t")
        monkeypatch.setenv("ROCKETCHAT_USER_ID", "u")
        assert rc_module.check_rocketchat_requirements() is False

    def test_missing_creds(self, rc_module, monkeypatch):
        monkeypatch.setenv("ROCKETCHAT_URL", "https://chat.example.com")
        monkeypatch.delenv("ROCKETCHAT_TOKEN", raising=False)
        monkeypatch.delenv("ROCKETCHAT_USERNAME", raising=False)
        monkeypatch.delenv("ROCKETCHAT_PASSWORD", raising=False)
        assert rc_module.check_rocketchat_requirements() is False


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

class TestHelpers:
    def test_split_csv_string(self, rc_module):
        assert rc_module._split_csv("a, b ,c") == {"a", "b", "c"}

    def test_split_csv_list(self, rc_module):
        assert rc_module._split_csv(["x", " y "]) == {"x", "y"}

    def test_split_csv_empty(self, rc_module):
        assert rc_module._split_csv("") == set()
        assert rc_module._split_csv(None) == set()

    def test_parse_ts_ejson(self, rc_module):
        assert rc_module.RocketChatAdapter._parse_ts({"$date": 1700000000000}) == 1700000000.0

    def test_parse_ts_number(self, rc_module):
        assert rc_module.RocketChatAdapter._parse_ts(1700000000000) == 1700000000.0

    def test_parse_ts_bad(self, rc_module):
        assert rc_module.RocketChatAdapter._parse_ts("nope") is None


# ---------------------------------------------------------------------------
# format_message
# ---------------------------------------------------------------------------

class TestFormatMessage:
    def test_strips_image_markdown_to_url(self, make_adapter):
        a = make_adapter()
        assert a.format_message("![cat](https://x/cat.png)") == "https://x/cat.png"

    def test_preserves_regular_markdown(self, make_adapter):
        a = make_adapter()
        content = "**bold** `code` [link](https://x)"
        assert a.format_message(content) == content


# ---------------------------------------------------------------------------
# send
# ---------------------------------------------------------------------------

class TestSend:
    def test_send_simple(self, make_adapter):
        a = make_adapter()
        a._api_post = AsyncMock(return_value={"success": True, "message": {"_id": "m1"}})
        res = run(a.send("room1", "hello"))
        assert res.success and res.message_id == "m1"
        a._api_post.assert_awaited_once()
        path, payload = a._api_post.await_args.args
        assert path == "chat.sendMessage"
        assert payload["message"] == {"rid": "room1", "msg": "hello"}

    def test_send_empty_is_noop(self, make_adapter):
        a = make_adapter()
        a._api_post = AsyncMock()
        res = run(a.send("room1", ""))
        assert res.success
        a._api_post.assert_not_awaited()

    def test_send_chunks_long_message(self, make_adapter, rc_module):
        a = make_adapter()
        a._api_post = AsyncMock(return_value={"success": True, "message": {"_id": "m"}})
        long = "x" * (rc_module.MAX_MESSAGE_LENGTH * 2 + 10)
        run(a.send("room1", long))
        assert a._api_post.await_count >= 2

    def test_send_thread_resolves_root(self, make_adapter):
        a = make_adapter(extra={"reply_mode": "thread"})
        a._api_post = AsyncMock(return_value={"success": True, "message": {"_id": "m1"}})
        a._resolve_thread_root = AsyncMock(return_value="root99")
        run(a.send("room1", "hi", reply_to="msg42"))
        a._resolve_thread_root.assert_awaited_once_with("msg42")
        _, payload = a._api_post.await_args.args
        assert payload["message"]["tmid"] == "root99"

    def test_send_flat_mode_no_thread(self, make_adapter):
        a = make_adapter(extra={"reply_mode": "off"})
        a._api_post = AsyncMock(return_value={"success": True, "message": {"_id": "m1"}})
        run(a.send("room1", "hi", reply_to="msg42"))
        _, payload = a._api_post.await_args.args
        assert "tmid" not in payload["message"]

    def test_send_failure(self, make_adapter):
        a = make_adapter()
        a._api_post = AsyncMock(return_value={})
        res = run(a.send("room1", "hi"))
        assert not res.success


# ---------------------------------------------------------------------------
# get_chat_info / room types
# ---------------------------------------------------------------------------

class TestChatInfo:
    @pytest.mark.parametrize("t,expected", [("d", "dm"), ("c", "channel"), ("p", "group"), ("l", "channel")])
    def test_room_type_mapping(self, make_adapter, t, expected):
        a = make_adapter()
        a._api_get = AsyncMock(return_value={"room": {"t": t, "name": "x", "fname": "X"}})
        info = run(a.get_chat_info("r1"))
        assert info["type"] == expected
        assert info["name"] == "X"

    def test_unknown_room_defaults(self, make_adapter):
        a = make_adapter()
        a._api_get = AsyncMock(return_value={})
        info = run(a.get_chat_info("r1"))
        assert info == {"name": "r1", "type": "channel", "chat_id": "r1"}


class TestRoomTypeCache:
    def test_caches_positive_result(self, make_adapter):
        a = make_adapter()
        a._api_get = AsyncMock(return_value={"room": {"_id": "dm1", "t": "d"}})
        assert run(a._get_room_type("dm1")) == "dm"
        # second call uses cache (no new API hit)
        a._api_get.reset_mock()
        assert run(a._get_room_type("dm1")) == "dm"
        a._api_get.assert_not_awaited()

    def test_does_not_cache_transient_failure(self, make_adapter):
        """A failed rooms.info lookup must not be cached — else a DM gets
        permanently misclassified as a channel and silently dropped."""
        a = make_adapter()
        a._api_get = AsyncMock(return_value={})  # transient failure -> {}
        assert run(a._get_room_type("dm1")) == "channel"
        assert "dm1" not in a._room_type_cache  # not cached
        # Recovery: next call resolves correctly.
        a._api_get = AsyncMock(return_value={"room": {"_id": "dm1", "t": "d"}})
        assert run(a._get_room_type("dm1")) == "dm"


# ---------------------------------------------------------------------------
# DDP changed-event handling
# ---------------------------------------------------------------------------

def _changed(message: dict) -> dict:
    return {
        "msg": "changed",
        "collection": "stream-room-messages",
        "fields": {"eventName": message.get("rid", "r"), "args": [message]},
    }


def _msg(**over):
    base = {
        "_id": "m1", "rid": "r1", "msg": "hello",
        "u": {"_id": "user1", "username": "alice"},
        "ts": {"$date": 1700000000000},
    }
    base.update(over)
    return base


class TestDDPHandling:
    def _prep(self, adapter, room_type="channel"):
        adapter.handle_message = AsyncMock()
        adapter._get_room_type = AsyncMock(return_value=room_type)
        return adapter

    def test_ignores_own_message(self, make_adapter):
        a = self._prep(make_adapter())
        run(a._handle_ddp_changed(_changed(_msg(u={"_id": "botid", "username": "hermes"}))))
        a.handle_message.assert_not_awaited()

    def test_ignores_system_message(self, make_adapter):
        a = self._prep(make_adapter())
        run(a._handle_ddp_changed(_changed(_msg(t="uj"))))  # user joined
        a.handle_message.assert_not_awaited()

    def test_ignores_wrong_collection(self, make_adapter):
        a = self._prep(make_adapter())
        ev = _changed(_msg())
        ev["collection"] = "something-else"
        run(a._handle_ddp_changed(ev))
        a.handle_message.assert_not_awaited()

    def test_dedup(self, make_adapter):
        a = self._prep(make_adapter(), room_type="dm")
        run(a._handle_ddp_changed(_changed(_msg())))
        run(a._handle_ddp_changed(_changed(_msg())))  # same _id
        assert a.handle_message.await_count == 1

    def test_dm_always_delivered(self, make_adapter):
        a = self._prep(make_adapter(), room_type="dm")
        run(a._handle_ddp_changed(_changed(_msg(msg="no mention here"))))
        a.handle_message.assert_awaited_once()
        ev = a.handle_message.await_args.args[0]
        assert ev.text == "no mention here"
        assert ev.source.chat_type == "dm"

    def test_channel_requires_mention(self, make_adapter, monkeypatch):
        monkeypatch.delenv("ROCKETCHAT_REQUIRE_MENTION", raising=False)
        a = self._prep(make_adapter(), room_type="channel")
        run(a._handle_ddp_changed(_changed(_msg(msg="just chatting"))))
        a.handle_message.assert_not_awaited()

    def test_channel_mention_by_array(self, make_adapter):
        a = self._prep(make_adapter(), room_type="channel")
        m = _msg(msg="@hermes hi there", mentions=[{"_id": "botid", "username": "hermes"}])
        run(a._handle_ddp_changed(_changed(m)))
        a.handle_message.assert_awaited_once()
        ev = a.handle_message.await_args.args[0]
        assert ev.text == "hi there"  # @mention stripped

    def test_channel_mention_by_text(self, make_adapter):
        a = self._prep(make_adapter(), room_type="channel")
        run(a._handle_ddp_changed(_changed(_msg(msg="hey @hermes help"))))
        a.handle_message.assert_awaited_once()
        ev = a.handle_message.await_args.args[0]
        assert "@hermes" not in ev.text

    def test_free_response_channel(self, make_adapter, monkeypatch):
        monkeypatch.setenv("ROCKETCHAT_FREE_RESPONSE_CHANNELS", "r1")
        a = self._prep(make_adapter(), room_type="channel")
        run(a._handle_ddp_changed(_changed(_msg(msg="no mention"))))
        a.handle_message.assert_awaited_once()

    def test_allowed_channels_whitelist_blocks(self, make_adapter, monkeypatch):
        monkeypatch.setenv("ROCKETCHAT_ALLOWED_CHANNELS", "other")
        a = self._prep(make_adapter(), room_type="channel")
        m = _msg(msg="@hermes hi", mentions=[{"_id": "botid"}])
        run(a._handle_ddp_changed(_changed(m)))
        a.handle_message.assert_not_awaited()

    def test_require_mention_false_env(self, make_adapter, monkeypatch):
        monkeypatch.setenv("ROCKETCHAT_REQUIRE_MENTION", "false")
        a = self._prep(make_adapter(), room_type="channel")
        run(a._handle_ddp_changed(_changed(_msg(msg="hello room"))))
        a.handle_message.assert_awaited_once()

    def test_command_message_type(self, make_adapter):
        a = self._prep(make_adapter(), room_type="dm")
        run(a._handle_ddp_changed(_changed(_msg(msg="/reset"))))
        from gateway.platforms.base import MessageType
        ev = a.handle_message.await_args.args[0]
        assert ev.message_type == MessageType.COMMAND

    def test_thread_id_passed_through(self, make_adapter):
        a = self._prep(make_adapter(), room_type="dm")
        run(a._handle_ddp_changed(_changed(_msg(tmid="root1"))))
        ev = a.handle_message.await_args.args[0]
        assert ev.source.thread_id == "root1"


# ---------------------------------------------------------------------------
# Connected / config bridges
# ---------------------------------------------------------------------------

class TestConfigBridges:
    def test_is_connected_with_token(self, rc_module, monkeypatch):
        import hermes_cli.gateway as gw
        monkeypatch.setattr(gw, "get_env_value", lambda k: {
            "ROCKETCHAT_URL": "https://x", "ROCKETCHAT_USER_ID": "u", "ROCKETCHAT_TOKEN": "t",
        }.get(k, ""))
        assert rc_module._is_connected(None) is True

    def test_is_connected_missing(self, rc_module, monkeypatch):
        import hermes_cli.gateway as gw
        monkeypatch.setattr(gw, "get_env_value", lambda k: "")
        assert rc_module._is_connected(None) is False

    def test_env_enablement(self, rc_module, monkeypatch):
        monkeypatch.setenv("ROCKETCHAT_URL", "https://chat.example.com/")
        monkeypatch.setenv("ROCKETCHAT_USER_ID", "bot")
        extras = rc_module._env_enablement()
        assert extras == {"url": "https://chat.example.com", "user_id": "bot"}

    def test_env_enablement_no_url(self, rc_module, monkeypatch):
        monkeypatch.delenv("ROCKETCHAT_URL", raising=False)
        assert rc_module._env_enablement() is None

    def test_apply_yaml_config(self, rc_module, monkeypatch):
        for k in ("ROCKETCHAT_URL", "ROCKETCHAT_USER_ID", "ROCKETCHAT_REQUIRE_MENTION",
                  "ROCKETCHAT_FREE_RESPONSE_CHANNELS", "ROCKETCHAT_ALLOWED_CHANNELS"):
            monkeypatch.delenv(k, raising=False)
        extras = rc_module._apply_yaml_config({}, {
            "url": "https://chat.example.com/",
            "user_id": "bot",
            "reply_mode": "Thread",
            "require_mention": False,
            "free_response_channels": ["a", "b"],
            "allowed_channels": "c,d",
        })
        assert extras["url"] == "https://chat.example.com"
        assert extras["user_id"] == "bot"
        assert extras["reply_mode"] == "thread"
        import os
        assert os.environ["ROCKETCHAT_REQUIRE_MENTION"] == "false"
        assert os.environ["ROCKETCHAT_FREE_RESPONSE_CHANNELS"] == "a,b"
        assert os.environ["ROCKETCHAT_ALLOWED_CHANNELS"] == "c,d"


# ---------------------------------------------------------------------------
# Standalone cron sender
# ---------------------------------------------------------------------------

class TestStandaloneSend:
    def test_missing_config_errors(self, rc_module, monkeypatch):
        for k in ("ROCKETCHAT_URL", "ROCKETCHAT_TOKEN", "ROCKETCHAT_USER_ID"):
            monkeypatch.delenv(k, raising=False)

        class _PC:
            token = ""
            extra = {}

        res = run(rc_module._standalone_send(_PC(), "room1", "hi"))
        assert "error" in res

    def test_empty_message_is_noop(self, rc_module, monkeypatch):
        """Empty message + no media must be a clean no-op, not an empty-msg
        POST that Rocket.Chat rejects with a 400."""
        monkeypatch.setenv("ROCKETCHAT_URL", "https://chat.example.com")
        monkeypatch.setenv("ROCKETCHAT_USER_ID", "u")
        monkeypatch.setenv("ROCKETCHAT_TOKEN", "t")

        class _PC:
            token = "t"
            extra = {"url": "https://chat.example.com", "user_id": "u"}

        res = run(rc_module._standalone_send(_PC(), "room1", ""))
        assert res.get("success") is True
        assert res.get("message_id") is None
