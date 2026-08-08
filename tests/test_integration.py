"""Integration test: drive the adapter against a local fake Rocket.Chat server.

Stands up a real aiohttp server that speaks just enough of the Rocket.Chat
REST + DDP (Meteor) WebSocket protocol to exercise the adapter end-to-end:
connect -> DDP login -> subscribe -> receive a pushed channel message ->
reply via REST. No Docker required, fully deterministic.

This covers the protocol-risky realtime path that unit tests mock out.
"""
import asyncio
import json

import pytest
from aiohttp import web


def run(coro):
    return asyncio.run(coro)


class FakeRocketChat:
    """Minimal Rocket.Chat REST + DDP server for one adapter session."""

    def __init__(self):
        self.app = web.Application()
        self.app.add_routes([
            web.get("/api/v1/me", self.me),
            web.get("/api/v1/rooms.info", self.rooms_info),
            web.post("/api/v1/chat.sendMessage", self.send_message),
            web.get("/api/v1/chat.getMessage", self.get_message),
            web.get("/websocket", self.websocket),
        ])
        self.sent = []           # captured chat.sendMessage payloads
        self.ws_clients = []     # live DDP sockets
        self.logged_in = False
        self.subscribed = False
        self.login_attempts = 0
        self.fail_login_times = 0  # return a DDP login error this many times first
        self.runner = None
        self.port = None

    async def start(self):
        self.runner = web.AppRunner(self.app)
        await self.runner.setup()
        site = web.TCPSite(self.runner, "127.0.0.1", 0)
        await site.start()
        self.port = list(self.runner.addresses)[0][1]
        return f"http://127.0.0.1:{self.port}"

    async def stop(self):
        for ws in list(self.ws_clients):
            await ws.close()
        await self.runner.cleanup()

    # --- REST ---
    async def me(self, request):
        return web.json_response({"_id": "botid", "username": "hermes", "success": True})

    async def rooms_info(self, request):
        rid = request.query.get("roomId", "")
        # "dm1" is a direct message; everything else is a public channel.
        t = "d" if rid.startswith("dm") else "c"
        return web.json_response({"room": {"_id": rid, "t": t, "name": rid, "fname": rid}, "success": True})

    async def send_message(self, request):
        body = await request.json()
        self.sent.append(body)
        msg = body.get("message", {})
        return web.json_response({"success": True, "message": {"_id": "sent1", **msg}})

    async def get_message(self, request):
        return web.json_response({"success": True, "message": {"_id": request.query.get("msgId")}})

    # --- DDP WebSocket ---
    async def websocket(self, request):
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        self.ws_clients.append(ws)
        async for msg in ws:
            if msg.type != web.WSMsgType.TEXT:
                continue
            data = json.loads(msg.data)
            m = data.get("msg")
            if m == "connect":
                await ws.send_json({"msg": "connected", "session": "sess1"})
            elif m == "method" and data.get("method") == "login":
                self.login_attempts += 1
                if self.fail_login_times > 0:
                    self.fail_login_times -= 1
                    await ws.send_json({
                        "msg": "result", "id": data.get("id"),
                        "error": {"error": "error-invalid-token", "message": "invalid token"},
                    })
                    continue
                self.logged_in = True
                await ws.send_json({
                    "msg": "result", "id": data.get("id"),
                    "result": {"id": "botid", "token": "tok"},
                })
            elif m == "sub" and data.get("name") == "stream-room-messages":
                self.subscribed = True
                await ws.send_json({"msg": "ready", "subs": [data.get("id")]})
            elif m == "pong":
                pass
        return ws

    async def push_message(self, rid, text, sender_id="user1", username="alice", mentions=None, t=None):
        """Push a stream-room-messages 'changed' event to all DDP clients."""
        message = {
            "_id": f"m-{len(self.sent)}-{rid}-{text[:4]}",
            "rid": rid, "msg": text,
            "u": {"_id": sender_id, "username": username},
            "ts": {"$date": 1700000000000},
        }
        if mentions is not None:
            message["mentions"] = mentions
        if t is not None:
            message["t"] = t
        evt = {
            "msg": "changed", "collection": "stream-room-messages",
            "fields": {"eventName": rid, "args": [message]},
        }
        for ws in list(self.ws_clients):
            await ws.send_json(evt)

    async def ping_clients(self):
        for ws in list(self.ws_clients):
            await ws.send_json({"msg": "ping"})


async def _drive(rc_module):
    from gateway.config import PlatformConfig

    server = FakeRocketChat()
    base = await server.start()
    try:
        config = PlatformConfig(enabled=True, token="tok", extra={"url": base, "user_id": "botid"})
        adapter = rc_module.RocketChatAdapter(config)

        received = []
        ev_signal = asyncio.Event()

        async def _capture(event):
            received.append(event)
            ev_signal.set()
        adapter.handle_message = _capture

        ok = await adapter.connect()
        assert ok, "connect() failed"

        # Wait for DDP login + subscribe handshake to complete.
        for _ in range(50):
            if server.logged_in and server.subscribed:
                break
            await asyncio.sleep(0.05)
        assert server.logged_in, "DDP login never happened"
        assert server.subscribed, "subscription never happened"

        # Server pings; adapter must pong (no crash).
        await server.ping_clients()

        # 1) Channel message WITHOUT mention -> ignored.
        await server.push_message("chan1", "no mention here")
        await asyncio.sleep(0.3)
        assert received == [], "channel message without mention should be ignored"

        # 2) Channel message WITH @mention -> delivered, mention stripped.
        ev_signal.clear()
        await server.push_message("chan1", "@hermes do a thing",
                                  mentions=[{"_id": "botid", "username": "hermes"}])
        await asyncio.wait_for(ev_signal.wait(), timeout=5)
        assert len(received) == 1
        assert received[0].text == "do a thing"
        assert received[0].source.chat_type == "channel"

        # 3) System message -> ignored.
        await server.push_message("chan1", "room renamed", t="r",
                                  mentions=[{"_id": "botid"}])
        await asyncio.sleep(0.2)
        assert len(received) == 1, "system message should be ignored"

        # 4) DM -> delivered without mention.
        ev_signal.clear()
        await server.push_message("dm1", "hello in dm")
        await asyncio.wait_for(ev_signal.wait(), timeout=5)
        assert len(received) == 2
        assert received[1].source.chat_type == "dm"

        # 5) Own message -> ignored.
        await server.push_message("chan1", "@hermes echo", sender_id="botid", username="hermes",
                                  mentions=[{"_id": "botid"}])
        await asyncio.sleep(0.2)
        assert len(received) == 2, "own message should be ignored"

        # 6) Adapter can send a reply over REST.
        res = await adapter.send("chan1", "here is the reply")
        assert res.success and res.message_id == "sent1"
        assert server.sent[-1]["message"] == {"rid": "chan1", "msg": "here is the reply"}

        await adapter.disconnect()
    finally:
        await server.stop()


def test_rocketchat_end_to_end(rc_module):
    run(_drive(rc_module))


async def _drive_login_recovery(rc_module):
    """A failed DDP login must trigger a reconnect, not a deaf socket."""
    from gateway.config import PlatformConfig

    server = FakeRocketChat()
    server.fail_login_times = 1  # first login attempt fails, retry succeeds
    base = await server.start()
    try:
        config = PlatformConfig(enabled=True, token="tok", extra={"url": base, "user_id": "botid"})
        adapter = rc_module.RocketChatAdapter(config)
        adapter.handle_message = lambda e: asyncio.sleep(0)
        assert await adapter.connect()

        # Reconnect backoff is ~2s; allow margin for the retry to subscribe.
        for _ in range(120):
            if server.subscribed and server.login_attempts >= 2:
                break
            await asyncio.sleep(0.1)
        assert server.login_attempts >= 2, "adapter did not retry after login failure"
        assert server.subscribed, "adapter did not subscribe after recovering"

        await adapter.disconnect()
    finally:
        await server.stop()


def test_rocketchat_recovers_from_login_failure(rc_module):
    run(_drive_login_recovery(rc_module))
