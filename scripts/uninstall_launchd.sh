#!/usr/bin/env bash
set -euo pipefail

PLIST_DEST="$HOME/Library/LaunchAgents/com.opsai.mcp.plist"

if [ -f "$PLIST_DEST" ]; then
  launchctl unload "$PLIST_DEST" || true
  rm -f "$PLIST_DEST"
  echo "Unloaded and removed $PLIST_DEST"
else
  echo "No plist found at $PLIST_DEST"
fi
