import json
from opsai_agent.loader.neo4j_loader import _extract_text_from_atlassian, _sanitize_for_neo4j


def test_extract_text_simple():
    node = {"type": "text", "text": "hello world"}
    assert _extract_text_from_atlassian(node) == "hello world"


def test_extract_text_nested():
    node = {
        "type": "doc",
        "content": [
            {"type": "paragraph", "content": [{"type": "text", "text": "first"}]},
            {"type": "paragraph", "content": [{"type": "text", "text": "second"}]},
        ],
    }
    out = _extract_text_from_atlassian(node)
    assert "first" in out and "second" in out


def test_sanitize_dict_without_text_returns_json_string():
    d = {"id": 1, "nested": {"a": 1}}
    s = _sanitize_for_neo4j(d)
    # Should be a JSON string because no 'text' fields
    assert isinstance(s, str)
    loaded = json.loads(s)
    assert loaded["nested"]["a"] == 1


def test_sanitize_list_and_primitives():
    val = ["a", {"type": "text", "text": "b"}, 3]
    out = _sanitize_for_neo4j(val)
    assert isinstance(out, list)
    assert "b" in out
    assert 3 in out
