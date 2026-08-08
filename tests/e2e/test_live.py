#!/usr/bin/env python3
"""Live end-to-end test for the Rocket.Chat gateway against a real server.

Brings the adapter up against the Docker stack in this directory and drives a
full round-trip: a second user posts a channel @mention and a DM, the adapter
must receive both via the DDP realtime stream, and the adapter must be able to
send a reply and deliver via the standalone (cron) path.

Usage:
    docker compose -f tests/e2e/docker-compose.yml up -d
    HERMES_AGENT=~/.hermes/hermes-agent PYTHONPATH=$HERMES_AGENT \
        "$HERMES_AGENT/venv/bin/python" tests/e2e/test_live.py
    docker compose -f tests/e2e/docker-compose.yml down -v

Exits non-zero on any failed assertion.
"""
from __future__ import annotations

import asyncio
import importlib.util
import json
import os
import sys
import time
from pathlib import Path

import aiohttp

RC_URL = os.environ.get("RC_URL", "http://localhost:13000")
ADMIN_USER = os.environ.get("RC_ADMIN_USER", "admin")
ADMIN_PASS = os.environ.get("RC_ADMIN_PASS", "admin1234")

_REPO_ROOT = Path(__file__).resolve().parents[2]


# --- load Hermes + the adapter --------------------------------------------

def _bootstrap():
    hermes = Path(os.environ.get("HERMES_AGENT", Path.home() / ".hermes" / "hermes-agent"))
    if str(hermes) not in sys.path:
        sys.path.insert(0, str(hermes))
    spec = importlib.util.spec_from_file_location("rc_e2e_adapter", _REPO_ROOT / "adapter.py")
    module = importlib.util.module_from_spec(spec)
    sys.modules["rc_e2e_adapter"] = module
    spec.loader.exec_module(module)
    from gateway.platform_registry import platform_registry
    if not platform_registry.is_registered(module.PLATFORM_NAME):
        from hermes_cli.plugins import PluginContext, PluginManager, PluginManifest
        ctx = PluginContext(
            PluginManifest(name="rocketchat-platform", kind="platform", source="user", path=str(_REPO_ROOT)),
            PluginManager(),
        )
        module.register(ctx)
    return module


PASS, FAIL = 0, 0


def check(label, cond):
    global PASS, FAIL
    if cond:
        PASS += 1
        print(f"  \033[0;32mPASS\033[0m {label}")
    else:
        FAIL += 1
        print(f"  \033[0;31mFAIL\033[0m {label}")


async def _login(session, user, password):
    async with session.post(f"{RC_URL}/api/v1/login", json={"user": user, "password": password}) as r:
        data = await r.json()
        if data.get("status") != "success":
            raise RuntimeError(f"login failed for {user}: {data}")
        return data["data"]["userId"], data["data"]["authToken"]


def _hdr(uid, tok):
    return {"X-User-Id": uid, "X-Auth-Token": tok, "Content-Type": "application/json"}


async def _wait_ready(timeout=360):
    deadline = time.time() + timeout
    async with aiohttp.ClientSession() as s:
        while time.time() < deadline:
            try:
                async with s.get(f"{RC_URL}/api/v1/login", timeout=aiohttp.ClientTimeout(total=10)) as r:
                    if r.status in (200, 401, 400):  # endpoint is answering
                        # confirm a real login works (admin provisioned)
                        try:
                            await _login(s, ADMIN_USER, ADMIN_PASS)
                            return True
                        except Exception:
                            pass
            except Exception:
                pass
            print("  …waiting for Rocket.Chat to finish booting")
            await asyncio.sleep(8)
    return False


async def main():
    module = _bootstrap()
    from gateway.config import PlatformConfig

    print(f"▸ Waiting for Rocket.Chat at {RC_URL} …")
    if not await asyncio.wait_for(_wait_ready(), timeout=420):
        print("\033[0;31mRocket.Chat did not become ready in time\033[0m")
        return 2
    print("▸ Server is up.")

    async with aiohttp.ClientSession() as s:
        admin_id, admin_tok = await _login(s, ADMIN_USER, ADMIN_PASS)

        # Create a personal access token for the bot (admin) via DDP-capable token.
        # The session token already works for REST + DDP resume; use it directly.
        bot_id, bot_tok = admin_id, admin_tok

        # Create a second user who will message the bot.
        async with s.post(f"{RC_URL}/api/v1/users.create", headers=_hdr(admin_id, admin_tok),
                          data=json.dumps({
                              "name": "Tester", "email": "tester@example.com",
                              "password": "tester1234", "username": "tester",
                              "roles": ["user"], "joinDefaultChannels": False,
                              "verified": True,
                          })) as r:
            await r.json()  # ignore "already exists" on re-run
        tester_id, tester_tok = await _login(s, "tester", "tester1234")

        # Create a test channel with both members.
        async with s.post(f"{RC_URL}/api/v1/channels.create", headers=_hdr(admin_id, admin_tok),
                          data=json.dumps({"name": "hermes-e2e", "members": ["admin", "tester"]})) as r:
            ch = await r.json()
        rid = (ch.get("channel") or {}).get("_id")
        if not rid:
            # channel may already exist from a prior run — look it up
            async with s.get(f"{RC_URL}/api/v1/channels.info?roomName=hermes-e2e",
                             headers=_hdr(admin_id, admin_tok)) as r:
                rid = ((await r.json()).get("channel") or {}).get("_id")
        check("test channel exists", bool(rid))

    # Bring up the adapter as the bot (admin).
    received = []
    config = PlatformConfig(enabled=True, token=bot_tok, extra={"url": RC_URL, "user_id": bot_id})
    adapter = module.RocketChatAdapter(config)

    async def _capture(event):
        received.append(event)
    adapter.handle_message = _capture

    print("▸ Connecting adapter (DDP realtime) …")
    connected = await adapter.connect()
    check("adapter.connect() succeeded", connected)
    check("bot identity resolved", adapter._bot_user_id == bot_id and adapter._bot_username == "admin")
    await asyncio.sleep(3)  # let the subscription settle

    # As tester, post a channel @mention and a DM.
    async with aiohttp.ClientSession() as s:
        await s.post(f"{RC_URL}/api/v1/chat.postMessage", headers=_hdr(tester_id, tester_tok),
                     data=json.dumps({"roomId": rid, "text": "@admin ping from channel"}))
        # DM: create IM then post
        async with s.post(f"{RC_URL}/api/v1/im.create", headers=_hdr(tester_id, tester_tok),
                          data=json.dumps({"username": "admin"})) as r:
            im = await r.json()
        dm_rid = (im.get("room") or {}).get("_id") or (im.get("room") or {}).get("rid")
        await s.post(f"{RC_URL}/api/v1/chat.postMessage", headers=_hdr(tester_id, tester_tok),
                     data=json.dumps({"roomId": dm_rid, "text": "direct hello bot"}))

    # Wait for the adapter to receive both via DDP.
    deadline = time.time() + 30
    while time.time() < deadline and len(received) < 2:
        await asyncio.sleep(0.5)

    texts = [e.text for e in received]
    types = {e.source.chat_type for e in received}
    check(f"received channel @mention (got {texts!r})",
          any("ping from channel" in t for t in texts))
    check("channel mention stripped of @admin",
          all("@admin" not in t for t in texts if "ping" in t))
    check("received DM", any("direct hello bot" in t for t in texts))
    check("DM classified as dm", "dm" in types)

    # Adapter can send a reply into the channel.
    res = await adapter.send(rid, "**reply** from the bot")
    check("adapter.send() succeeded", res.success and res.message_id)

    # Standalone (cron) send path.
    std = await module._standalone_send(config, rid, "cron delivery line")
    check("standalone_send() succeeded", std.get("success") is True)

    await adapter.disconnect()
    print(f"\n▸ Result: {PASS} passed, {FAIL} failed")
    return 0 if FAIL == 0 else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
