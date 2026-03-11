#!/usr/bin/env bash
set -euo pipefail
# Wrapper to load .env, pick venv python, and run the MCP server module.

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
ENV_FILE="$REPO_DIR/.env"

if [ -f "$ENV_FILE" ]; then
  # export env vars from .env (simple KEY=VALUE parser)
  set -o allexport
  # shellcheck disable=SC1090
  source "$ENV_FILE"
  set +o allexport
fi

# Prefer venv python if present
VENV_PY="$REPO_DIR/.venv/bin/python"
if [ -x "$VENV_PY" ]; then
  PY="$VENV_PY"
else
  PY="$(command -v python3 || command -v python)"
fi

cd "$REPO_DIR"
exec "$PY" -u -m opsai_agent.scripts.run_mcp
