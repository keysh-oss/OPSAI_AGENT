"""Slack connector skeleton with sample/full and incremental sync methods.

This is cloud-ready but the smoke-run uses the sample data to remain offline-safe.
"""
from typing import List, Dict, Optional
import os


class SlackConnector:
    def __init__(self, token: Optional[str] = None):
        # token would be used with slack_sdk in a full implementation
        self.token = token or os.getenv("SLACK_BOT_TOKEN")

    def full_sync(self) -> List[Dict]:
        """Return a sample list of messages (simulate a full export).

        In production this would page over conversations.list and conversations.history.
        """
        return [
            {
                "id": "slack_msg:C123:1620000000",
                "user": {"id": "slack_user:U111", "name": "alice", "email": "alice@example.com"},
                "channel": {"id": "C123", "name": "#general"},
                "text": "Hello team, see JIRA-123",
                "ts": "1620000000",
                "raw": "<raw payload>",
                "source": "slack",
            }
        ]

    def incremental_sync(self, since_ts: str) -> List[Dict]:
        """Return messages newer than since_ts (simulated).

        In production this would use cursor-based pagination or events.
        """
        # For the starter, just return same sample if newer
        return self.full_sync()
