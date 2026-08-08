# Hermes Rocket.Chat Gateway

A [Hermes Agent](https://github.com/NousResearch/hermes-agent) **platform plugin**
that connects your agent to a [Rocket.Chat](https://rocket.chat) workspace.
Message your Hermes agent from any Rocket.Chat channel or DM, and let it deliver
cron results and notifications back to a home channel.

It registers a `rocketchat` platform through the public plugin API, exactly like
the bundled Mattermost, IRC and Teams adapters — no changes to Hermes core.

| | |
|---|---|
| **Receiving** | Realtime API — Meteor DDP over a WebSocket (`<server>/websocket`), subscribed to `stream-room-messages` for `__my_messages__` |
| **Sending** | REST API (`/api/v1/chat.sendMessage`, `/api/v1/rooms.upload`) |
| **Typing** | DDP `stream-notify-room` (`<rid>/typing`), refreshed while the agent works |
| **Dependencies** | `aiohttp` only (already a Hermes dependency) — no Rocket.Chat SDK |
| **Auth** | Personal Access Token (recommended) or username/password |

## Fork notice

This is a fork of [`wachtelhund/hermes-rocketchat-gateway`](https://github.com/wachtelhund/hermes-rocketchat-gateway)
(MIT). The upstream has not been updated since 2026-06-15 and no longer loads
against current Hermes releases. Two changes:

**1. `connect(is_reconnect=...)` compatibility (required — the plugin does not load without it)**

Hermes calls `adapter.connect(is_reconnect=is_reconnect)` unconditionally
(`gateway/run.py`), with no fallback for adapters lacking the parameter.
Upstream's `async def connect(self)` therefore raises:

```
RocketChatAdapter.connect() got an unexpected keyword argument 'is_reconnect'
```

**2. Typing indicator (new)**

Hermes calls `adapter.send_typing()` / `stop_typing()` while generating a reply,
but the base class implements them as `pass` — platforms are expected to
override. Upstream did not, so Rocket.Chat showed no activity at all while the
agent was working. Implemented over DDP:

```python
{"msg": "method", "method": "stream-notify-room",
 "params": [f"{chat_id}/typing", username, True]}
```

Rocket.Chat expires the indicator after a few seconds, so it is refreshed on a
background task until `stop_typing` cancels it. Failures are swallowed — the
indicator is cosmetic and must never break message handling.

## Features

- 💬 Two-way messaging in channels, private groups and DMs
- ⌨️ **Typing indicator while the agent is working**
- 🧵 Optional **thread-mode** replies (`ROCKETCHAT_REPLY_MODE=thread`)
- 🔔 `@mention`-gating in channels, with free-response and channel allowlists
- 📎 Native **file uploads** (images, audio, documents) in both directions
- 🖼️ Inbound attachments are cached locally so vision/transcription tools can read them
- 📬 **Home-channel cron delivery**, including out-of-process cron jobs
- 🔒 Per-user allowlists (`ROCKETCHAT_ALLOWED_USERS`)
- ♻️ Auto-reconnect with exponential backoff + jitter

## Install

```sh
hermes plugins install aoki-taquan/hermes-rocketchat
hermes plugins enable rocketchat-platform
```

## Configure

Create a **bot account** in Rocket.Chat, then mint a **Personal Access Token**
(Avatar → *My Account* → *Personal Access Tokens* → *Add*, tick
*Ignore Two Factor Authentication*). Copy both the **token** and the **user id**.

> The bot account needs the `user` role in addition to `bot`. Rocket.Chat grants
> `create-personal-access-tokens` to `admin` and `user` only, so a `bot`-only
> account cannot mint a token for itself.

Add to `~/.hermes/.env` (environment variables take precedence):

```sh
ROCKETCHAT_URL=https://chat.example.com
ROCKETCHAT_USER_ID=abcdef0123456789
ROCKETCHAT_TOKEN=your-personal-access-token

# Recommended
ROCKETCHAT_ALLOWED_USERS=<user id>   # who may talk to the bot
ROCKETCHAT_HOME_CHANNEL=GENERAL      # room id for cron delivery
```

> `ROCKETCHAT_ALLOWED_USERS` is matched against Rocket.Chat **user ids**, not
> usernames — Hermes compares the entries to `source.user_id`
> (`gateway/authz_mixin.py`). Upstream's docs said "usernames/ids"; a username
> silently never matches and every message is rejected as `Unauthorized user`.

Then start the gateway:

```sh
hermes gateway restart
```

## Environment variables

| Variable | Required | Description |
|---|---|---|
| `ROCKETCHAT_URL` | ✅ | Server URL |
| `ROCKETCHAT_USER_ID` | ✅¹ | Bot user id |
| `ROCKETCHAT_TOKEN` | ✅¹ | Bot Personal Access Token |
| `ROCKETCHAT_USERNAME` / `ROCKETCHAT_PASSWORD` | ¹ | Password-login fallback |
| `ROCKETCHAT_ALLOWED_USERS` | | Comma-separated **user ids** allowed to use the bot |
| `ROCKETCHAT_ALLOW_ALL_USERS` | | Allow any user (dev only) |
| `ROCKETCHAT_HOME_CHANNEL` | | Room id for cron / notification delivery |
| `ROCKETCHAT_REPLY_MODE` | | `thread` or `off` (default) |
| `ROCKETCHAT_REQUIRE_MENTION` | | Require `@bot` in channels (default `true`) |
| `ROCKETCHAT_FREE_RESPONSE_CHANNELS` | | Room ids where mention is not required |
| `ROCKETCHAT_ALLOWED_CHANNELS` | | Restrict the bot to these room ids |

¹ Provide either the token pair or the username/password pair.

## License

MIT — see [LICENSE](LICENSE). Original work © 2026 wachtelhund.
