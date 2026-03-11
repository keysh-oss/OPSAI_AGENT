"""Neo4j loader that supports dry-run and real MERGE-based writes.

This implementation performs idempotent MERGE operations for users and messages
using UNWIND in transactions. For safety the default is dry_run=True so CI and
local runs won't accidentally write to a real database unless NEO4J_URI and
credentials are provided and dry_run=False.
"""
from typing import List, Dict, Optional
import os
from math import ceil
import json


def _extract_text_from_atlassian(node):
    """Walk Atlassian document model and concatenate any 'text' fields found."""
    texts = []

    def _walk(n):
        if isinstance(n, dict):
            # direct text node
            if "text" in n and isinstance(n.get("text"), str):
                texts.append(n.get("text"))
            for v in n.values():
                _walk(v)
        elif isinstance(n, list):
            for itm in n:
                _walk(itm)
        # primitives ignored

    _walk(node)
    return " ".join(t for t in texts if t)


def _sanitize_for_neo4j(value):
    """Convert dict/list values to primitives acceptable by Neo4j.

    - dict: try to extract textual content, otherwise JSON-serialize.
    - list: sanitize each element.
    - primitives: left as-is.
    """
    if isinstance(value, dict):
        extracted = _extract_text_from_atlassian(value)
        if extracted:
            return extracted
        try:
            return json.dumps(value, ensure_ascii=False)
        except Exception:
            return str(value)
    if isinstance(value, list):
        out = []
        for v in value:
            sv = _sanitize_for_neo4j(v)
            # Only include primitives or strings in arrays
            out.append(sv)
        return out
    return value

try:
    from neo4j import GraphDatabase
except Exception:  # pragma: no cover - neo4j may not be installed in some environments
    GraphDatabase = None


def _chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i : i + n]


class Neo4jLoader:
    def __init__(self, uri: Optional[str] = None, user: Optional[str] = None, password: Optional[str] = None):
        self.uri = uri or os.getenv("NEO4J_URI")
        self.user = user or os.getenv("NEO4J_USER")
        self.password = password or os.getenv("NEO4J_PASSWORD")
        self._driver = None

    def _ensure_driver(self):
        if self._driver is None:
            if not self.uri:
                raise RuntimeError("NEO4J_URI not set and driver requested")
            if GraphDatabase is None:
                raise RuntimeError("neo4j driver not available; install 'neo4j' package")
            self._driver = GraphDatabase.driver(self.uri, auth=(self.user, self.password))

    def close(self):
        if self._driver:
            self._driver.close()

    def write_users(self, users: List[Dict], dry_run: bool = True, batch_size: int = 100):
        if not users:
            return
        if dry_run:
            print("[neo4j_loader] Dry run — users to write:")
            for u in users:
                print(f" - {u.get('id')} ({u.get('email')})")
            return

        self._ensure_driver()

        def _write(tx, batch):
            tx.run(
                """
                UNWIND $batch as u
                MERGE (usr:User {id: u.id})
                // Only set non-unique properties to avoid constraint conflicts
                SET usr.email = u.email, usr.source = u.source
                """,
                batch=batch,
            )

        with self._driver.session() as session:
            for chunk in _chunks(users, batch_size):
                session.execute_write(_write, chunk)

    def write_messages(self, messages: List[Dict], dry_run: bool = True, batch_size: int = 100):
        if not messages:
            return
        if dry_run:
            print("[neo4j_loader] Dry run enabled — would write the following messages:")
            for m in messages:
                print(f" - {m.get('id')}: {m.get('text')}")
            return

        self._ensure_driver()

        def _write(tx, batch):
            tx.run(
                """
                UNWIND $batch as m
                MERGE (msg:Message {id: m.id})
                // Do not store raw map payload to avoid complex type errors; store primitives only
                SET msg.text = m.text, msg.ts = m.ts, msg.source = m.source

                MERGE (ch:Channel {id: m.channel})
                SET ch.name = coalesce(m.channel_name, ch.name)

                // If m.user is present (a map), create/merge user and relationship
                FOREACH (u IN CASE WHEN m.user IS NULL THEN [] ELSE [m.user] END |
                    MERGE (usr:User {id: u.id})
                    // Avoid updating potentially-unique 'name' property to prevent constraint conflicts
                    SET usr.email = u.email, usr.source = u.source
                    MERGE (usr)-[:POSTED]->(msg)
                )

                // create attachments if present
                FOREACH (a IN CASE WHEN m.attachments IS NULL THEN [] ELSE m.attachments END |
                    MERGE (att:Attachment {id: a.id})
                    SET att.filename = a.filename, att.url = a.url, att.size = a.size
                    MERGE (att)-[:ATTACHED_TO]->(msg)
                )

                MERGE (msg)-[:IN_CHANNEL]->(ch)

                // thread replies: if thread_ts present and different create REPLIED_TO
                FOREACH (t IN CASE WHEN m.thread_ts IS NULL OR m.thread_ts = m.ts THEN [] ELSE [m.thread_ts] END |
                    MERGE (parent:Message {id: 'slack_msg:' + m.channel + ':' + t})
                    MERGE (msg)-[:REPLIED_TO]->(parent)
                )
                """,
                batch=batch,
            )

        with self._driver.session() as session:
            for chunk in _chunks(messages, batch_size):
                session.execute_write(_write, chunk)

    def write_issues(self, issues: List[Dict], dry_run: bool = True, batch_size: int = 100):
        """Write Jira Issue objects into Neo4j.

        Each issue dict should contain keys like id, key, summary, status, reporter, assignee, project, created, updated, url.
        """
        if not issues:
            return
        if dry_run:
            print("[neo4j_loader] Dry run enabled — would write the following issues:")
            for i in issues:
                print(f" - {i.get('id')}: {i.get('summary')} ({i.get('status')})")
            return

        self._ensure_driver()

        def _write(tx, batch):
            # Targeted sanitization: preserve maps for project/reporter/assignee/comments
            # but sanitize potentially-rich fields like comment.body or description.
            sanitized = []
            for item in batch:
                s = dict(item)  # shallow copy

                # sanitize simple text fields that may be rich objects
                if s.get("summary") is not None:
                    s["summary"] = _sanitize_for_neo4j(s.get("summary"))
                if s.get("description") is not None:
                    s["description"] = _sanitize_for_neo4j(s.get("description"))

                # project/report/assignee should remain maps with primitive values
                if s.get("project") and isinstance(s.get("project"), dict):
                    for pk, pv in list(s["project"].items()):
                        s["project"][pk] = _sanitize_for_neo4j(pv)

                if s.get("reporter") and isinstance(s.get("reporter"), dict):
                    for rk, rv in list(s["reporter"].items()):
                        s["reporter"][rk] = _sanitize_for_neo4j(rv)

                if s.get("assignee") and isinstance(s.get("assignee"), dict):
                    for ak, av in list(s["assignee"].items()):
                        s["assignee"][ak] = _sanitize_for_neo4j(av)

                # comments: sanitize comment bodies and comment authors' primitive fields
                comments = s.get("comments")
                if comments and isinstance(comments, list):
                    new_comments = []
                    for c in comments:
                        nc = dict(c)
                        # body may be rich object
                        if nc.get("body") is not None:
                            nc["body"] = _sanitize_for_neo4j(nc.get("body"))
                        if nc.get("author") and isinstance(nc.get("author"), dict):
                            for ak, av in list(nc["author"].items()):
                                nc["author"][ak] = _sanitize_for_neo4j(av)
                        new_comments.append(nc)
                    s["comments"] = new_comments

                sanitized.append(s)

            tx.run(
                """
                UNWIND $batch as i
                MERGE (iss:Issue {id: i.id})
                SET iss.key = i.key, iss.summary = i.summary, iss.status = i.status, iss.created = i.created, iss.updated = i.updated, iss.source = i.source, iss.url = i.url

                // Project
                FOREACH (p IN CASE WHEN i.project IS NULL THEN [] ELSE [i.project] END |
                    MERGE (proj:Project {id: p.id})
                    SET proj.key = p.key, proj.name = p.name
                    MERGE (iss)-[:IN_PROJECT]->(proj)
                )

                // Reporter
                FOREACH (r IN CASE WHEN i.reporter IS NULL THEN [] ELSE [i.reporter] END |
                    MERGE (usr:User {id: r.id})
                    // avoid setting potentially-unique 'name' property to prevent constraint conflicts
                    SET usr.email = r.email, usr.source = 'jira'
                    MERGE (usr)-[:REPORTED]->(iss)
                )

                // Assignee
                FOREACH (a IN CASE WHEN i.assignee IS NULL THEN [] ELSE [i.assignee] END |
                    MERGE (asg:User {id: a.id})
                    // avoid setting potentially-unique 'name' property to prevent constraint conflicts
                    SET asg.email = a.email, asg.source = 'jira'
                    MERGE (asg)-[:ASSIGNED_TO]->(iss)
                )
                
                // Comments
                FOREACH (c IN CASE WHEN i.comments IS NULL THEN [] ELSE i.comments END |
                    MERGE (comm:Comment {id: c.id})
                    SET comm.body = c.body, comm.created = c.created, comm.updated = c.updated
                    MERGE (comm)-[:ON_ISSUE]->(iss)

                    FOREACH (ca IN CASE WHEN c.author IS NULL THEN [] ELSE [c.author] END |
                        MERGE (auth:User {id: ca.id})
                        // avoid setting potentially-unique 'name' property to prevent constraint conflicts
                        SET auth.email = ca.email, auth.source = 'jira'
                        MERGE (auth)-[:WROTE_COMMENT]->(comm)
                    )
                )
                """,
                batch=sanitized,
            )

        with self._driver.session() as session:
            for chunk in _chunks(issues, batch_size):
                session.execute_write(_write, chunk)

    def write_pages(self, pages: List[Dict], dry_run: bool = True, batch_size: int = 100):
        """Write Confluence Page objects into Neo4j.

        Each page dict should contain id, pid, title, space, author, created, updated, body, url.
        """
        if not pages:
            return
        if dry_run:
            print("[neo4j_loader] Dry run enabled — would write the following pages:")
            for p in pages:
                print(f" - {p.get('id')}: {p.get('title')}")
            return

        self._ensure_driver()

        def _write(tx, batch):
            # sanitize page bodies and nested maps
            sanitized = []
            for item in batch:
                s = dict(item)
                if s.get("title") is not None:
                    s["title"] = _sanitize_for_neo4j(s.get("title"))
                if s.get("body") is not None:
                    s["body"] = _sanitize_for_neo4j(s.get("body"))
                if s.get("space") and isinstance(s.get("space"), dict):
                    for k, v in list(s["space"].items()):
                        s["space"][k] = _sanitize_for_neo4j(v)
                if s.get("author") and isinstance(s.get("author"), dict):
                    for k, v in list(s["author"].items()):
                        s["author"][k] = _sanitize_for_neo4j(v)
                sanitized.append(s)

            tx.run(
                """
                UNWIND $batch as p
                MERGE (pg:Page {id: p.id})
                SET pg.pid = p.pid, pg.title = p.title, pg.created = p.created, pg.updated = p.updated, pg.body = p.body, pg.source = p.source, pg.url = p.url

                // Space
                FOREACH (sp IN CASE WHEN p.space IS NULL THEN [] ELSE [p.space] END |
                    MERGE (s:Space {key: sp.key})
                    SET s.name = sp.name
                    MERGE (pg)-[:IN_SPACE]->(s)
                )

                // Author
                FOREACH (a IN CASE WHEN p.author IS NULL THEN [] ELSE [p.author] END |
                    MERGE (u:User {id: a.id})
                    // avoid setting potentially-unique 'name' property to prevent constraint conflicts
                    SET u.email = a.email, u.source = 'confluence'
                    MERGE (u)-[:AUTHORED]->(pg)
                )
                """,
                batch=sanitized,
            )

        with self._driver.session() as session:
            for chunk in _chunks(pages, batch_size):
                session.execute_write(_write, chunk)

