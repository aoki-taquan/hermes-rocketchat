#!/usr/bin/env sh
#
# Hermes Rocket.Chat gateway — installer
# ======================================
# Installs the Rocket.Chat platform plugin into your local Hermes Agent and
# enables it, so `hermes gateway` can connect to your Rocket.Chat workspace.
#
# Usage:
#   curl -fsSL https://raw.githubusercontent.com/aoki-taquan/hermes-rocketchat/main/install.sh | sh
#
# Honours $HERMES_HOME (defaults to ~/.hermes). Re-running upgrades in place.
#
set -eu

REPO_URL="https://github.com/aoki-taquan/hermes-rocketchat.git"
PLUGIN_NAME="rocketchat-platform"   # must match `name:` in plugin.yaml
PLATFORM="rocketchat"

HERMES_HOME="${HERMES_HOME:-$HOME/.hermes}"
PLUGINS_DIR="$HERMES_HOME/plugins"
TARGET_DIR="$PLUGINS_DIR/$PLUGIN_NAME"
CONFIG="$HERMES_HOME/config.yaml"

say()  { printf '\033[0;36m▸\033[0m %s\n' "$1"; }
ok()   { printf '\033[0;32m✓\033[0m %s\n' "$1"; }
warn() { printf '\033[0;33m!\033[0m %s\n' "$1"; }
die()  { printf '\033[0;31m✗ %s\033[0m\n' "$1" >&2; exit 1; }

command -v git >/dev/null 2>&1 || die "git is required but not found on PATH."

if [ ! -d "$HERMES_HOME" ]; then
  die "Hermes home not found at $HERMES_HOME. Install Hermes first, or set HERMES_HOME."
fi

# --- Locate a Python that can edit config.yaml (PyYAML) -------------------
PYTHON=""
for cand in "$HERMES_HOME/hermes-agent/venv/bin/python" \
            "$HERMES_HOME/hermes-agent/.venv/bin/python" \
            python3 python; do
  if command -v "$cand" >/dev/null 2>&1 && "$cand" -c "import yaml" >/dev/null 2>&1; then
    PYTHON="$cand"; break
  fi
done

# --- Clone + install ------------------------------------------------------
say "Installing Rocket.Chat gateway into $TARGET_DIR"
mkdir -p "$PLUGINS_DIR"
TMP_DIR="$(mktemp -d)"
trap 'rm -rf "$TMP_DIR"' EXIT
git clone --depth 1 "$REPO_URL" "$TMP_DIR/plugin" >/dev/null 2>&1 \
  || die "git clone failed: $REPO_URL"
[ -f "$TMP_DIR/plugin/plugin.yaml" ] || die "cloned repo has no plugin.yaml — aborting."

rm -rf "$TARGET_DIR"
# Keep the .git checkout so `hermes plugins update rocketchat-platform` works
# (the native installer keeps it too; without it updates are rejected).
mv "$TMP_DIR/plugin" "$TARGET_DIR"
ok "Plugin files installed."

# --- Enable the plugin in config.yaml ------------------------------------
if [ -n "$PYTHON" ]; then
  "$PYTHON" - "$CONFIG" "$PLUGIN_NAME" <<'PYEOF'
import sys
import yaml

cfg_path, name = sys.argv[1], sys.argv[2]
try:
    with open(cfg_path) as fh:
        data = yaml.safe_load(fh) or {}
except FileNotFoundError:
    data = {}
if not isinstance(data, dict):
    data = {}

plugins = data.get("plugins")
if not isinstance(plugins, dict):
    plugins = {}
    data["plugins"] = plugins
enabled = plugins.get("enabled")
if not isinstance(enabled, list):
    enabled = []
if name not in enabled:
    enabled.append(name)
plugins["enabled"] = sorted(enabled)

with open(cfg_path, "w") as fh:
    yaml.safe_dump(data, fh, default_flow_style=False, sort_keys=False)
print("enabled")
PYEOF
  ok "Plugin enabled in $CONFIG (plugins.enabled)."
else
  warn "Could not find a Python with PyYAML to edit config.yaml automatically."
  warn "Enable the plugin manually with:  hermes plugins enable $PLUGIN_NAME"
fi

# --- Next steps -----------------------------------------------------------
cat <<EOF

$(ok "Rocket.Chat gateway installed.")

Next steps:
  1. Create a bot account in Rocket.Chat and a Personal Access Token
     (Avatar → My Account → Personal Access Tokens → Add, tick
      "Ignore Two Factor Authentication").
  2. Add credentials to $HERMES_HOME/.env :

       ROCKETCHAT_URL=https://chat.example.com
       ROCKETCHAT_USER_ID=<bot user id>
       ROCKETCHAT_TOKEN=<personal access token>
       # optional, recommended:
       ROCKETCHAT_ALLOWED_USERS=<your username>
       ROCKETCHAT_HOME_CHANNEL=<room id for cron delivery>

  3. Invite the bot to a channel (or DM it), then start the gateway:

       hermes gateway restart

Docs:  https://github.com/aoki-taquan/hermes-rocketchat
EOF
