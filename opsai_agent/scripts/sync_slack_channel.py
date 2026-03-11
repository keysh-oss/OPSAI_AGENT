"""Fetch recent messages from a Slack channel and ingest into Neo4j.

This script looks up channel `dummy-incident-war-room` (or uses SLACK_CHANNEL_NAME
env var if set), fetches recent messages, and ingests them into Neo4j using the
existing loader and processor. It will perform real writes when NEO4J_URI is set
in the environment.
"""
import os
from dotenv import load_dotenv
from typing import List

load_dotenv()

from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

from opsai_agent.ingest.slack_events import process_slack_event
from opsai_agent.state.state_store import get_last_ts, set_last_ts
from opsai_agent.loader.neo4j_loader import Neo4jLoader


def find_channel_id(client: WebClient, channel_name: str) -> str:
    cursor = None
    while True:
        res = client.conversations_list(limit=1000, cursor=cursor)
        for ch in res.get("channels", []):
            if ch.get("name") == channel_name:
                return ch.get("id")
        cursor = res.get("response_metadata", {}).get("next_cursor")
        if not cursor:
            break
    raise RuntimeError(f"Channel named '{channel_name}' not found")


def fetch_messages(client: WebClient, channel_id: str, limit: int = 200, oldest: str | None = None) -> List[dict]:
    all_msgs = []
    cursor = None
    while len(all_msgs) < limit:
        kwargs = {"channel": channel_id, "limit": min(200, limit - len(all_msgs))}
        if cursor:
            kwargs["cursor"] = cursor
        if oldest:
            kwargs["oldest"] = oldest
        res = client.conversations_history(**kwargs)
        msgs = res.get("messages", [])
        all_msgs.extend(msgs)
        cursor = res.get("response_metadata", {}).get("next_cursor")
        if not cursor:
            break
    return all_msgs


def main():
    token = os.getenv("SLACK_BOT_TOKEN")
    if not token:
        print("SLACK_BOT_TOKEN not set in environment. Aborting.")
        return 2

    channel_name = os.getenv("SLACK_CHANNEL_NAME", "dummy-incident-war-room")
    client = WebClient(token=token)

    try:
        channel_id = find_channel_id(client, channel_name)
    except SlackApiError as e:
        print("Slack API error while listing channels:", e)
        return 3
    except Exception as e:
        print(e)
        return 4

    print(f"Found channel {channel_name} -> {channel_id}")

    last_ts = get_last_ts(channel_id)
    if last_ts:
        print(f"Resuming from last_ts={last_ts}")
    msgs = fetch_messages(client, channel_id, limit=200, oldest=last_ts)
    print(f"Fetched {len(msgs)} messages from channel {channel_name}")

    # Prepare loader
    loader = Neo4jLoader()
    env_has_neo4j = bool(os.getenv("NEO4J_URI"))

    processed = 0
    latest_processed_ts = None
    for m in reversed(msgs):  # process oldest -> newest
        # skip messages without 'user' (e.g., bot messages) or with subtype edits
        if m.get("subtype") is not None:
            continue
        if not m.get("user"):
            continue

        payload = {"type": "event_callback", "event": {"type": "message", "channel": channel_id, "ts": m.get("ts"), "user": m.get("user"), "text": m.get("text")}}

        res = process_slack_event(payload, loader=loader, dry_run=not env_has_neo4j, slack_client=client)
        if res.get("ok"):
            processed += 1
            latest_processed_ts = m.get("ts")

    if latest_processed_ts:
        set_last_ts(channel_id, latest_processed_ts)
        print(f"Updated last_ts for {channel_name} to {latest_processed_ts}")

    print(f"Processed {processed} messages into Neo4j (dry_run={not env_has_neo4j})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
