from opsai_agent.transformers import mapper


def test_map_user():
    p = {"id": "u1", "name": "alice", "email": "a@example.com", "source": "slack"}
    out = mapper.map_user(p)
    assert out["id"] == "u1"
    assert out["email"] == "a@example.com"


def test_map_message():
    p = {"id": "m1", "text": "hi", "ts": "100", "channel": {"id": "C1"}, "user": {"id": "u1"}, "source": "slack"}
    out = mapper.map_message(p)
    assert out["id"] == "m1"
    assert out["channel"] == "C1"
