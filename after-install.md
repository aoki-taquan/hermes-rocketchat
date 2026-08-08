# Rocket.Chat gateway installed 🚀

Set these in `~/.hermes/.env`, then run `hermes gateway restart`:

```
ROCKETCHAT_URL=https://chat.example.com
ROCKETCHAT_USER_ID=<bot user id>
ROCKETCHAT_TOKEN=<personal access token>
ROCKETCHAT_ALLOWED_USERS=<your user id>    # recommended (user id, NOT username)
ROCKETCHAT_HOME_CHANNEL=<room id>          # optional, for cron delivery
```

Create the token in Rocket.Chat: Avatar → My Account → Personal Access Tokens →
Add (tick "Ignore Two Factor Authentication"). Then invite the bot to a channel
or DM it.

Full docs: https://github.com/aoki-taquan/hermes-rocketchat
