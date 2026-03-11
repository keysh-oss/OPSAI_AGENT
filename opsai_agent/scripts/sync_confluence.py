"""Incremental Confluence -> Neo4j sync script.

Usage:
  # ensure .env has CONFLUENCE_BASE_URL, CONFLUENCE_EMAIL, CONFLUENCE_API_TOKEN and Neo4j creds
  python -m opsai_agent.scripts.sync_confluence [--since YYYY-MM-DD] [--write]

By default this runs as a dry-run. Use --write to actually persist pages and advance the cursor.
"""
from __future__ import annotations

import argparse
import os
from datetime import datetime, timedelta, timezone
from typing import Optional

from opsai_agent.connectors.confluence_connector import ConfluenceConnector
from opsai_agent.loader.neo4j_loader import Neo4jLoader
from opsai_agent.state.state_store import get_last_ts, set_last_ts

STATE_KEY = "confluence_last_sync"


def _default_since() -> str:
    # default to 7 days ago in YYYY-MM-DD format
    return (datetime.now(timezone.utc) - timedelta(days=7)).strftime("%Y-%m-%d")


def run(since: Optional[str] = None, dry_run: bool = True) -> None:
    since = since or get_last_ts(STATE_KEY) or _default_since()
    print(f"[sync_confluence] using since: {since} (dry_run={dry_run})")

    cc = ConfluenceConnector()
    try:
        pages = cc.incremental_sync(since)
    except Exception as e:
        print(f"[sync_confluence] Confluence fetch error: {e}")
        return

    if not pages:
        print("[sync_confluence] no pages to sync")
        if not dry_run:
            set_last_ts(STATE_KEY, datetime.now(timezone.utc).isoformat())
        return

    # prepare users and pages
    users = []
    for p in pages:
        a = p.get("author")
        if a:
            users.append({"id": a.get("id"), "name": a.get("name"), "email": a.get("email"), "source": "confluence"})

    # deduplicate users
    seen = set()
    deduped_users = []
    for u in users:
        uid = u.get("id")
        if not uid or uid in seen:
            continue
        seen.add(uid)
        deduped_users.append(u)

    loader = Neo4jLoader()
    loader.write_users(deduped_users, dry_run=dry_run)

    successes = []
    failures = []
    for p in pages:
        pid = p.get("id")
        try:
            loader.write_pages([p], dry_run=dry_run)
            print(f"[sync_confluence] wrote page: {pid}")
            successes.append(p)
        except Exception as e:
            print(f"[sync_confluence] FAILED to write page {pid}: {e}")
            failures.append({"id": pid, "error": str(e)})

    # update last sync using created timestamps of successful pages
    if not dry_run:
        try:
            if not successes:
                print("[sync_confluence] no successful writes; not updating cursor to avoid skipping pages")
            else:
                ts_candidates = []
                for p in successes:
                    c = p.get("created")
                    if c:
                        # created likely in ISO8601; keep as-is
                        ts_candidates.append(c)

                if ts_candidates:
                    latest = max(ts_candidates)
                else:
                    latest = datetime.now(timezone.utc).isoformat()

                set_last_ts(STATE_KEY, latest)
                print(f"[sync_confluence] updated last sync to: {latest}")
        except Exception as e:
            print(f"[sync_confluence] failed to update last sync timestamp: {e}")


def _parse_args():
    p = argparse.ArgumentParser(description="Incremental Confluence -> Neo4j sync")
    p.add_argument("--since", type=str, help="Since date (YYYY-MM-DD or ISO8601). Overrides stored cursor.")
    p.add_argument("--write", action="store_true", help="Perform writes and advance cursor (default: dry-run)")
    return p.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    # Respect env override for dry run as well, but CLI has precedence
    env_dry = os.getenv("DRY_RUN")
    if args.write:
        dry = False
    else:
        dry = True if env_dry is None else os.getenv("DRY_RUN", "1") not in ("0", "false", "False")

    run(since=args.since, dry_run=dry)
