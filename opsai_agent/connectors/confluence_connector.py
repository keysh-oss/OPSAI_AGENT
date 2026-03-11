"""Confluence connector (Cloud-ready).

This connector uses Confluence Cloud's search API to find pages created since
the provided timestamp. It falls back to a small sample page when credentials
are not available in the environment.
"""
from typing import List, Dict, Optional
import os
import requests
from urllib.parse import quote_plus


class ConfluenceConnector:
    def __init__(self, base_url: Optional[str] = None, auth: Optional[Dict] = None):
        self.base_url = base_url or os.getenv("CONFLUENCE_BASE_URL")
        # Accept CONFLUENCE_EMAIL / CONFLUENCE_API_TOKEN in .env
        env_user = os.getenv("CONFLUENCE_EMAIL")
        self.user = (auth.get("user") if auth else env_user)
        self.api_token = (auth.get("api_token") if auth else os.getenv("CONFLUENCE_API_TOKEN"))

    def _has_creds(self) -> bool:
        return bool(self.base_url and self.user and self.api_token)

    def full_sync(self) -> List[Dict]:
        # sample fallback
        if not self._has_creds():
            return [
                {
                    "id": "confluence_page:456",
                    "title": "Architecture Notes",
                    "space": {"key": "DOCS", "name": "Documentation"},
                    "author": {"id": "confluence_user:U300", "name": "carol", "email": "carol@example.com"},
                    "created": "2023-01-01T12:00:00Z",
                    "updated": "2023-01-01T12:00:00Z",
                    "source": "confluence",
                }
            ]

        return self.incremental_sync("1970-01-01 00:00")

    def incremental_sync(self, since: str, limit: int = 50) -> List[Dict]:
        """Return pages created since the provided timestamp.

        Uses GET /rest/api/content/search?cql=... with pagination.
        """
        if not self._has_creds():
            return self.full_sync()

        results: List[Dict] = []
        auth = (self.user, self.api_token)
        headers = {"Accept": "application/json"}

        # Confluence CQL: type=page AND created >= "<since>"
        cql = f'type=page AND created >= "{since}" ORDER BY created ASC'
        start = 0

        while True:
            url = self.base_url.rstrip("/") + "/rest/api/content/search"
            params = {"cql": cql, "start": start, "limit": limit, "expand": "space,body.storage,version,history"}
            resp = requests.get(url, auth=auth, headers=headers, params=params, timeout=30)
            if resp.status_code != 200:
                raise RuntimeError(f"Confluence API returned {resp.status_code}: {resp.text}")

            data = resp.json()
            pages = data.get("results", [])

            for p in pages:
                pid = p.get("id")
                title = p.get("title")
                space = p.get("space") or {}
                history = p.get("history", {})
                created = history.get("createdDate") or p.get("_expandable", {}).get("history")
                # prefer body.storage if present
                body = None
                body_obj = p.get("body", {}).get("storage") if p.get("body") else None
                if body_obj:
                    body = body_obj.get("value")

                author = None
                created_by = history.get("createdBy") if history else None
                if created_by:
                    author = {"id": f"confluence_user:{created_by.get('accountId') or created_by.get('username')}", "name": created_by.get("displayName"), "email": created_by.get("email") if created_by.get("email") else None}

                item: Dict = {
                    "id": f"confluence_page:{pid}",
                    "pid": pid,
                    "title": title,
                    "space": {"key": space.get("key"), "name": space.get("name")},
                    "author": author,
                    "created": p.get("createdDate") or history.get("createdDate") if history else p.get("createdDate"),
                    "updated": p.get("version", {}).get("when") if p.get("version") else p.get("lastModified"),
                    "body": body,
                    "source": "confluence",
                    "url": self.base_url.rstrip("/") + f"/wiki/spaces/{quote_plus(space.get('key',''))}/pages/{pid}",
                }

                results.append(item)

            size = len(pages)
            if size < limit:
                break
            start += size

        return results
