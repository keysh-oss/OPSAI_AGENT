"""Simple SQLite-backed state store for channel cursors/last timestamps.

Provides a tiny API to get/set the last processed ts per Slack channel.
"""
import os
import sqlite3
from typing import Optional

DB_PATH = os.getenv("OPSAI_STATE_DB", os.path.join(os.path.dirname(__file__), "state.db"))


def _get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS channel_state (
            channel_id TEXT PRIMARY KEY,
            last_ts TEXT
        )
        """
    )
    conn.commit()
    return conn


def get_last_ts(channel_id: str) -> Optional[str]:
    conn = _get_conn()
    cur = conn.execute("SELECT last_ts FROM channel_state WHERE channel_id = ?", (channel_id,))
    row = cur.fetchone()
    conn.close()
    if row:
        return row[0]
    return None


def set_last_ts(channel_id: str, last_ts: str) -> None:
    conn = _get_conn()
    conn.execute(
        "INSERT INTO channel_state(channel_id, last_ts) VALUES (?, ?) ON CONFLICT(channel_id) DO UPDATE SET last_ts=excluded.last_ts",
        (channel_id, last_ts),
    )
    conn.commit()
    conn.close()
