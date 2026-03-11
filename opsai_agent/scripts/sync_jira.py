"""Incremental Jira -> Neo4j sync script.

Usage:
  # ensure .env has JIRA_BASE_URL, JIRA_USER, JIRA_API_TOKEN and Neo4j creds
  python -m opsai_agent.scripts.sync_jira

This script reads the last sync timestamp from the SQLite state store key 'jira_last_sync'
and updates it after a successful run.
"""
import os
from datetime import datetime, timedelta

from opsai_agent.connectors.jira_connector import JiraConnector
from opsai_agent.loader.neo4j_loader import Neo4jLoader
from opsai_agent.state.state_store import get_last_ts, set_last_ts


STATE_KEY = "jira_last_sync"


def _default_since():
    # default to 7 days ago to avoid fetching too much data on first run
    return (datetime.utcnow() - timedelta(days=7)).strftime("%Y-%m-%d %H:%M")


def run(dry_run: bool = True):
    since = get_last_ts(STATE_KEY) or _default_since()
    print(f"[sync_jira] last sync since: {since}")

    jc = JiraConnector()
    try:
        issues = jc.incremental_sync(since)
    except Exception as e:
        print(f"[sync_jira] Jira fetch error: {e}")
        return

    if not issues:
        print("[sync_jira] no issues to sync")
        # still update timestamp to now to avoid repeated queries
        set_last_ts(STATE_KEY, datetime.utcnow().strftime("%Y-%m-%d %H:%M"))
        return

    # prepare users and issues
    users = []
    for it in issues:
        r = it.get("reporter")
        a = it.get("assignee")
        if r:
            users.append({"id": r.get("id"), "name": r.get("name"), "email": r.get("email"), "source": "jira"})
        if a:
            users.append({"id": a.get("id"), "name": a.get("name"), "email": a.get("email"), "source": "jira"})
        # include comment authors as users so they are created before comments are linked
        for c in it.get("comments", []):
            ca = c.get("author")
            if ca:
                users.append({"id": ca.get("id"), "name": ca.get("name"), "email": ca.get("email"), "source": "jira"})

    # deduplicate users by id
    seen = set()
    deduped_users = []
    for u in users:
        uid = u.get("id")
        if not uid or uid in seen:
            continue
        seen.add(uid)
        deduped_users.append(u)

    loader = Neo4jLoader()
    # write users first to ensure relationships refer to existing nodes
    loader.write_users(deduped_users, dry_run=dry_run)

    # write issues individually so we can log per-issue successes/failures
    successes = []
    failures = []
    for it in issues:
        iid = it.get("id")
        try:
            loader.write_issues([it], dry_run=dry_run)
            print(f"[sync_jira] wrote issue: {iid}")
            successes.append(it)
        except Exception as e:
            print(f"[sync_jira] FAILED to write issue {iid}: {e}")
            failures.append({"id": iid, "error": str(e)})

    # update last sync to the newest updated/created value from successful issues only
    if not dry_run:
        try:
            ts_candidates = []
            # Use created timestamps so the cursor advances based on new issues created
            # after the last sync. This ensures subsequent runs only pick up newly
            # created issues.
            for it in successes:
                if it.get("created"):
                    ts_candidates.append(it.get("created"))
            if ts_candidates:
                latest = max(ts_candidates)
            else:
                latest = datetime.utcnow().strftime("%Y-%m-%d %H:%M")
            set_last_ts(STATE_KEY, latest)
            print(f"[sync_jira] updated last sync to: {latest}")
        except Exception as e:
            print(f"[sync_jira] failed to update last sync timestamp: {e}")


if __name__ == "__main__":
    dry = os.getenv("DRY_RUN", "1") not in ("0", "false", "False")
    run(dry_run=dry)
