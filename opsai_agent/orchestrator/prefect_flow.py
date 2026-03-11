"""Orchestrator skeleton. If Prefect is installed the Prefect flow can be used,
otherwise a simple runner function is provided for the smoke test.
"""
from typing import List

try:
    from prefect import flow, task
    PREFECT_AVAILABLE = True
except Exception:
    PREFECT_AVAILABLE = False


def run_simple_pipeline(connectors: List, mapper_module, loader, dry_run: bool = True):
    """Run connectors sequentially and push to loader in dry-run mode."""
    all_messages = []
    all_users = []

    for c in connectors:
        items = c.full_sync()
        for it in items:
            # best-effort mapping for known types
            stype = it.get("source")
            if stype == "slack":
                m = mapper_module.map_message(it)
                all_messages.append(m)
                if it.get("user"):
                    all_users.append(mapper_module.map_user(it.get("user")))
            elif stype == "jira":
                all_users.append(mapper_module.map_user(it.get("reporter")))
            elif stype == "confluence":
                all_users.append(mapper_module.map_user(it.get("author")))

    # deduplicate users by id
    users_by_id = {u["id"]: u for u in all_users if u.get("id")}

    loader.write_users(list(users_by_id.values()), dry_run=dry_run)
    loader.write_messages(all_messages, dry_run=dry_run)
