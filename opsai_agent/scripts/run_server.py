"""Run the Slack Events Flask app locally.

Usage (example):

# .env should contain SLACK_SIGNING_SECRET and SLACK_BOT_TOKEN and optionally NEO4J_URI credentials.
python -m opsai_agent.scripts.run_server

This will start a Flask development server on 0.0.0.0:5000 by default.
"""
import os
from dotenv import load_dotenv

load_dotenv()

try:
    from opsai_agent.ingest.slack_events import create_app
except Exception as exc:
    raise RuntimeError("Failed to import Slack app; ensure dependencies like Flask are installed") from exc


def main():
    app = create_app()
    host = os.getenv("HOST", "0.0.0.0")
    port = int(os.getenv("PORT", "5000"))
    debug = os.getenv("FLASK_DEBUG", "0") == "1"
    app.run(host=host, port=port, debug=debug)


if __name__ == "__main__":
    main()
