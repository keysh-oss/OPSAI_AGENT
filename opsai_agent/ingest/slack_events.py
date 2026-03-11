"""Slack Events API webhook handler and processor.

This module exposes a small Flask app for Slack Events API compatibility and a
pure function `process_slack_event` that can be tested without running a server.

Security: If SLACK_SIGNING_SECRET is set in the environment, incoming requests
will be verified using Slack's signing scheme. If not set, verification is
skipped to allow local testing.
"""
import os
import time
import hmac
import hashlib
from typing import Dict, Any, Optional

from dotenv import load_dotenv

# Load environment variables from .env if present
load_dotenv()

try:
    from flask import Flask, request, jsonify
    FLASK_AVAILABLE = True
except Exception:  # pragma: no cover - tests may run without Flask installed
    Flask = None
    request = None
    jsonify = None
    FLASK_AVAILABLE = False

try:
    from slack_sdk import WebClient
    SLACK_SDK_AVAILABLE = True
except Exception:  # pragma: no cover - slack_sdk may not be installed in test env
    WebClient = None
    SLACK_SDK_AVAILABLE = False

from opsai_agent.transformers import mapper
from opsai_agent.loader.neo4j_loader import Neo4jLoader


if FLASK_AVAILABLE:
    app = Flask(__name__)
else:
    app = None


def _verify_slack_request(signing_secret: str, body: bytes, timestamp: str, slack_signature: str) -> bool:
    # Protect against replay
    try:
        req_ts = int(timestamp)
    except Exception:
        return False
    if abs(time.time() - req_ts) > 60 * 5:
        return False

    basestring = f"v0:{timestamp}:".encode("utf-8") + body
    my_sig = "v0=" + hmac.new(signing_secret.encode(), basestring, hashlib.sha256).hexdigest()
    return hmac.compare_digest(my_sig, slack_signature)


def _fetch_slack_user_profile(client: Optional[Any], user_id: str) -> Dict[str, Optional[str]]:
    """Fetch user profile from Slack if client available; return dict with id/name/email/source."""
    if not client or not SLACK_SDK_AVAILABLE:
        return {"id": f"slack_user:{user_id}", "name": None, "email": None, "source": "slack"}

    try:
        res = client.users_info(user=user_id)
        user = res.get("user", {})
        profile = user.get("profile", {})
        return {"id": f"slack_user:{user_id}", "name": profile.get("real_name") or user.get("name"), "email": profile.get("email"), "source": "slack"}
    except Exception:
        return {"id": f"slack_user:{user_id}", "name": None, "email": None, "source": "slack"}


def process_slack_event(payload: Dict[str, Any], loader: Optional[Neo4jLoader] = None, dry_run: bool = True, slack_client: Optional[Any] = None) -> Dict[str, Any]:
    """Process a Slack Events API payload.

    - Handles URL verification challenge
    - For message events, maps and writes to Neo4j (or dry-run prints)

    Returns a small response dict indicating action taken.
    """
    # Handle url verification
    if payload.get("type") == "url_verification":
        return {"challenge": payload.get("challenge")}

    if payload.get("type") == "event_callback":
        event = payload.get("event", {})
        # Only handle message events (not edits/subtypes here)
        if event.get("type") == "message" and event.get("subtype") is None:
            target_channel = os.getenv("SLACK_CHANNEL_ID")
            # If a target channel is configured, ignore other channels
            if target_channel and event.get("channel") != target_channel:
                return {"ok": False, "reason": "channel_mismatch"}

            user_id = event.get("user")
            # Optional: use provided slack_client or create one from env
            token = os.getenv("SLACK_BOT_TOKEN")
            client = slack_client
            if client is None and SLACK_SDK_AVAILABLE and token:
                client = WebClient(token=token)

            # Enrich user profile when possible
            user_obj = None
            if user_id:
                user_obj = _fetch_slack_user_profile(client, user_id)

            # map message
            msg = {
                "id": f"slack_msg:{event.get('channel')}:{event.get('ts')}",
                "text": event.get("text"),
                "ts": event.get("ts"),
                "channel": event.get("channel"),
                "channel_name": None,
                "user": user_obj,
                "source": "slack",
            }

            # thread replies: include parent thread timestamp if present and different
            if event.get("thread_ts") and event.get("thread_ts") != event.get("ts"):
                msg["thread_ts"] = event.get("thread_ts")

            # attachments/files: map to a small list of primitives
            files = event.get("files") or []
            attachments = []
            for f in files:
                attachments.append({
                    "id": f.get("id") or f.get("file_id") or f.get("ts"),
                    "filename": f.get("name") or f.get("title"),
                    "url": f.get("url_private") or f.get("url_download") or f.get("permalink"),
                    "size": f.get("size"),
                })
            if attachments:
                msg["attachments"] = attachments

            # prepare users list for loader
            users = []
            if user_obj:
                users.append(user_obj)

            if loader is None:
                loader = Neo4jLoader()

            # If NEO4J_URI is set and dry_run param wasn't forced True, enable real writes
            env_has_neo4j = bool(os.getenv("NEO4J_URI"))
            use_dry = dry_run or not env_has_neo4j

            # write using loader (loader will perform MERGE and relationships)
            loader.write_users(users, dry_run=use_dry)
            loader.write_messages([msg], dry_run=use_dry)
            return {"ok": True, "processed": msg.get("id"), "dry_run": use_dry}

    return {"ok": False, "reason": "ignored"}


if FLASK_AVAILABLE:
    @app.route("/slack/events", methods=["POST"])
    def slack_events_endpoint():
        signing_secret = os.getenv("SLACK_SIGNING_SECRET")
        body = request.get_data()
        timestamp = request.headers.get("X-Slack-Request-Timestamp", "0")
        signature = request.headers.get("X-Slack-Signature", "")

        if signing_secret:
            if not _verify_slack_request(signing_secret, body, timestamp, signature):
                return jsonify({"error": "invalid_signature"}), 401

        payload = request.get_json(force=True)

        # Use dry-run if no Neo4j URI provided
        dry_run = not bool(os.getenv("NEO4J_URI"))
        loader = Neo4jLoader()
        result = process_slack_event(payload, loader=loader, dry_run=dry_run)
        return jsonify(result)


def create_app():
    if not FLASK_AVAILABLE:
        raise RuntimeError("Flask is not installed; cannot create app. Install Flask to run the webhook server.")
    return app
