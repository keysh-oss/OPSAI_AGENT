#!/usr/bin/env bash
set -euo pipefail

# Install the LaunchAgent plist into the current user's LaunchAgents and load it.

BASE_DIR="$(cd "$(dirname "$0")/.." && pwd)"
PLIST_SRC="$BASE_DIR/deploy/com.opsai.mcp.plist"
PLIST_DEST="$HOME/Library/LaunchAgents/com.opsai.mcp.plist"

if [ ! -f "$PLIST_SRC" ]; then
  echo "Plist source not found: $PLIST_SRC" >&2
  exit 1
fi

mkdir -p "$HOME/Library/LaunchAgents"
cp "$PLIST_SRC" "$PLIST_DEST"
chmod 644 "$PLIST_DEST"

# Unload if already loaded
if launchctl list | grep -q com.opsai.mcp; then
  launchctl unload "$PLIST_DEST" || true
fi

launchctl load "$PLIST_DEST"
echo "Loaded com.opsai.mcp. Logs: /tmp/opsai_mcp.log /tmp/opsai_mcp.err.log"
