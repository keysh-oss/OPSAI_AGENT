"""Simple transformer/mapper for source payloads -> canonical graph shapes.

These functions are intentionally small and deterministic for testing/dry-runs.
"""
from typing import Dict, Any


def map_user(payload: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "id": payload.get("id"),
        "name": payload.get("name"),
        "email": payload.get("email"),
        "source": payload.get("source", "unknown"),
    }


def map_message(payload: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "id": payload.get("id"),
        "text": payload.get("text"),
        "ts": payload.get("ts"),
        "channel": payload.get("channel", {}).get("id") if payload.get("channel") else None,
        "user": payload.get("user"),
        "raw": payload.get("raw"),
        "source": payload.get("source", "slack"),
    }


def map_issue(payload: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "id": payload.get("id"),
        "key": payload.get("key"),
        "summary": payload.get("summary"),
        "status": payload.get("status"),
        "reporter": payload.get("reporter"),
        "source": payload.get("source", "jira"),
    }


def map_page(payload: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "id": payload.get("id"),
        "title": payload.get("title"),
        "space": payload.get("space"),
        "author": payload.get("author"),
        "lastModified": payload.get("lastModified"),
        "source": payload.get("source", "confluence"),
    }
