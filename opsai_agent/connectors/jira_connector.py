"""Jira connector (Cloud-ready).

Uses the Jira REST API when `JIRA_BASE_URL` and credentials are present.
Falls back to sample data for offline/dry runs. This implementation uses the
GET /rest/api/3/search/jql endpoint (query string) to fetch issues and supports
pagination via `startAt`/`maxResults`.
"""
from typing import List, Dict, Optional
import os
import requests


class JiraConnector:
    def __init__(self, base_url: Optional[str] = None, auth: Optional[Dict] = None):
        self.base_url = base_url or os.getenv("JIRA_BASE_URL")
        # Accept JIRA_USER or JIRA_EMAIL for username, and JIRA_API_TOKEN for token
        env_user = os.getenv("JIRA_USER") or os.getenv("JIRA_EMAIL")
        self.user = (auth.get("user") if auth else env_user)
        self.api_token = auth.get("api_token") if auth else os.getenv("JIRA_API_TOKEN")

    def _has_creds(self) -> bool:
        return bool(self.base_url and self.user and self.api_token)

    def full_sync(self) -> List[Dict]:
        # conservative sample when no creds available
        if not self._has_creds():
            return [
                {
                    "id": "jira_issue:PROJ-123",
                    "key": "PROJ-123",
                    "summary": "Sample issue",
                    "status": "Open",
                    "reporter": {"id": "jira_user:U200", "name": "bob", "email": "bob@example.com"},
                    "source": "jira",
                }
            ]

        # when creds exist, fetch recent issues (no since filter)
        return self.incremental_sync("1970-01-01 00:00")

    def incremental_sync(self, since: str, max_results: int = 50) -> List[Dict]:
        """Return issues created since the provided timestamp.

        Uses GET /rest/api/3/search/jql?jql=...&startAt=...&maxResults=...&fields=...
        and pages through results when `total` is provided in the response.
        """
        if not self._has_creds():
            return self.full_sync()

        results: List[Dict] = []
        url = self.base_url.rstrip("/") + "/rest/api/3/search/jql"
        auth = (self.user, self.api_token)
        headers = {"Accept": "application/json"}

        # Use created >= since to return only newly created issues after the cursor.
        # This intentionally ignores updates to older issues — the sync is for new issues
        # created since the last cursor timestamp.
        jql = f'created >= "{since}" ORDER BY created ASC'
        start_at = 0

        fields = [
            "summary",
            "status",
            "reporter",
            "assignee",
            "created",
            "updated",
            "project",
            "issuetype",
            "description",
            "comment",
        ]
        fields_csv = ",".join(fields)

        while True:
            params = {"jql": jql, "startAt": start_at, "maxResults": max_results, "fields": fields_csv}
            resp = requests.get(url, auth=auth, headers=headers, params=params, timeout=30)
            if resp.status_code != 200:
                raise RuntimeError(f"Jira API returned {resp.status_code}: {resp.text}")

            data = resp.json()
            issues = data.get("issues", [])

            for it in issues:
                key = it.get("key")
                fid = it.get("id")
                fields_map = it.get("fields", {})
                reporter = fields_map.get("reporter")
                assignee = fields_map.get("assignee")
                project = fields_map.get("project") or {}

                item: Dict = {
                    "id": f"jira_issue:{key}",
                    "key": key,
                    "fid": fid,
                    "summary": fields_map.get("summary"),
                    "status": fields_map.get("status", {}).get("name") if fields_map.get("status") else None,
                    "reporter": (
                        {
                            "id": f"jira_user:{reporter.get('accountId')}" if reporter and reporter.get("accountId") else None,
                            "name": reporter.get("displayName") if reporter else None,
                            "email": reporter.get("emailAddress") if reporter else None,
                        }
                    )
                    if reporter
                    else None,
                    "assignee": (
                        {
                            "id": f"jira_user:{assignee.get('accountId')}" if assignee and assignee.get("accountId") else None,
                            "name": assignee.get("displayName") if assignee else None,
                            "email": assignee.get("emailAddress") if assignee else None,
                        }
                    )
                    if assignee
                    else None,
                    "project": {"id": project.get("id"), "key": project.get("key"), "name": project.get("name")},
                    "created": fields_map.get("created"),
                    "updated": fields_map.get("updated"),
                    "issuetype": fields_map.get("issuetype", {}).get("name") if fields_map.get("issuetype") else None,
                    "source": "jira",
                    "url": self.base_url.rstrip("/") + f"/browse/{key}",
                }

                # normalize comments if present
                comments_raw = fields_map.get("comment", {}).get("comments", []) if fields_map.get("comment") else []

                # Only fetch full comments via the issue GET endpoint when the search result
                # indicates there are comments (reduces API calls). Some search responses
                # include comment summaries but not full bodies; when `total` > 0 we fetch.
                comment_total = fields_map.get("comment", {}).get("total") if fields_map.get("comment") else 0
                if comment_total and int(comment_total) > 0:
                    try:
                        issue_url_full = self.base_url.rstrip("/") + f"/rest/api/3/issue/{key}"
                        resp2 = requests.get(issue_url_full, auth=auth, headers=headers, params={"fields": "comment"}, timeout=30)
                        if resp2.status_code == 200:
                            fields_full = resp2.json().get("fields", {})
                            comments_full = fields_full.get("comment", {}).get("comments", [])
                            if comments_full:
                                comments_raw = comments_full
                    except Exception:
                        # ignore comment fetch errors and continue with whatever comments we have
                        pass
                comments: List[Dict] = []
                for c in comments_raw:
                    author = c.get("author") or {}
                    comments.append(
                        {
                            "id": f"jira_comment:{key}:{c.get('id')}",
                            "cid": c.get("id"),
                            "body": c.get("body"),
                            "created": c.get("created"),
                            "updated": c.get("updated"),
                            "author": (
                                {
                                    "id": f"jira_user:{author.get('accountId')}" if author and author.get("accountId") else None,
                                    "name": author.get("displayName") if author else None,
                                    "email": author.get("emailAddress") if author else None,
                                }
                            )
                            if author
                            else None,
                        }
                    )

                if comments:
                    item["comments"] = comments

                results.append(item)

            total = data.get("total")
            # paginate using startAt/total when available
            if total is not None:
                start_at += len(issues)
                if start_at >= total or len(issues) == 0:
                    break
            else:
                # no total provided; stop after one page to avoid infinite loops
                break

        return results
