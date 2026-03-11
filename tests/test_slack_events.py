from opsai_agent.ingest.slack_events import process_slack_event
from opsai_agent.loader.neo4j_loader import Neo4jLoader


def test_process_slack_url_verification():
    payload = {"type": "url_verification", "challenge": "xyz"}
    res = process_slack_event(payload, loader=Neo4jLoader(), dry_run=True)
    assert res.get("challenge") == "xyz"


def test_process_slack_message_event_dry_run():
    payload = {
        "type": "event_callback",
        "event": {"type": "message", "channel": "C1", "ts": "12345.6789", "user": "U1", "text": "hello"},
    }
    res = process_slack_event(payload, loader=Neo4jLoader(), dry_run=True)
    assert res.get("ok") is True
    assert res.get("processed").startswith("slack_msg:")
