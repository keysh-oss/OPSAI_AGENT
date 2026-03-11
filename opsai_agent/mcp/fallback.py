"""NL -> Cypher fallback heuristics for simple intents.

This module implements very small, high-precision rules to translate a few common
natural language queries into safe, read-only Cypher queries. It's used as a
fallback when the LLM-generated Cypher is unsafe or can't be repaired.
"""
import re
from typing import Optional


def _quote(s: str) -> str:
    return s.replace("'", "\\'")


def fallback_nl_to_cypher(nl: str) -> Optional[str]:
    q = nl.strip().lower()

    # 1) messages with word/phrase X
    m = re.search(r"(?:word|phrase|containing|contains|contain|with)\s+'?(?P<t>[^']+)'?", q)
    if not m:
        m = re.search(r"messages?\s+(?:that\s+)?(?:mention|mentioning|with|containing)\s+'?(?P<t>[^']+)'?", q)
    if m:
        token = m.group("t").strip()
        token_escaped = _quote(token)
        cy = f"MATCH (m:Message) WHERE toLower(m.text) CONTAINS '{token_escaped}' RETURN m LIMIT 200"
        return cy

    # 2) messages mentioning a jira key-like token (e.g., PROJ-123)
    m = re.search(r"\b([A-Z][A-Z0-9]+-\d+)\b", nl)
    if m:
        key = m.group(1)
        key_esc = _quote(key)
        cy = f"MATCH (m:Message) WHERE toLower(m.text) CONTAINS '{key_esc.lower()}' OR toLower(m.text) CONTAINS '{key_esc}' RETURN m LIMIT 200"
        return cy

    # 3) messages by user <name or email>
    m = re.search(r"messages?\s+(?:by|from)\s+(?P<user>\S+)", q)
    if m:
        user = m.group("user").strip()
        user_esc = _quote(user)
        # try matching by email or name (case-insensitive)
        cy = (
            "MATCH (u:User)-[:POSTED]->(m:Message) "
            f"WHERE toLower(u.email) CONTAINS '{user_esc.lower()}' OR toLower(u.name) CONTAINS '{user_esc.lower()}' RETURN m, u LIMIT 200"
        )
        return cy

    # 4) show recent messages
    if re.search(r"recent messages|latest messages|show latest messages|most recent messages", q):
        return "MATCH (m:Message) RETURN m ORDER BY m.ts DESC LIMIT 100"

    return None
