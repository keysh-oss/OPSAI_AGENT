# OPSAI_AGENT — Knowledge Graph Pipeline (starter)

This repository contains a starter scaffold for ingesting Slack, Jira, and Confluence (Cloud)
into a Neo4j knowledge graph with a near-real-time cadence.

What is included
- Minimal connector skeletons for Slack, Jira, and Confluence (cloud-ready, but the smoke run is offline)
- A simple transformer/mapper
- A Neo4j loader that supports dry-run mode
- An orchestrator skeleton (Prefect-friendly) and a smoke runner that performs a dry run
- A small unit test for the mapper

Quick dry-run smoke test (no credentials required)

1. Ensure you have Python 3.10+ installed.
2. From the workspace root run:

```bash
python3 -u opsai_agent/scripts/run_smoke.py
```

This will run the connectors in dry-run mode and exercise the mapping/loader pathways without
talking to external APIs or Neo4j.

Next steps
- Provide credentials/environment variables to enable real API calls and Neo4j writes.
- I can add Prefect flows, state storage, and real webhook handlers next.

Running the Chat UI (OpenAI + Neo4j)

1. Ensure `.env` contains OPENAI_API_KEY and Neo4j credentials (NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD).
2. Install dependencies in your venv:

```bash
source .venv/bin/activate
pip install -r requirements.txt
```

3. Start the Flask web app:

```bash
python -m opsai_agent.webapp.app
```

4. Open http://localhost:8080 and enter a natural language query. The app will call OpenAI to translate to Cypher, run the query against Neo4j, and render returned nodes/relationships.

Security note: The app asks the LLM to create read-only Cypher, but LLM outputs can be unsafe. Review generated Cypher before enabling in production.

Docker Compose (optional)

You can bring up a local Neo4j with docker-compose (adjust credentials in `.env` before using):

```bash
docker compose up -d
```

./scripts/install_launchd.sh

./scripts/uninstall_launchd.sh


PYTHONPATH=/Users/keysh/OPSAI_AGENT/OPSAI_AGENT /Users/keysh/OPSAI_AGENT/OPSAI_AGENT/.venv/bin/python -m opsai_agent.scripts.run_mcp



python -m opsai_agent.webapp.app

to run the Jira Sync:

set -a && source .env && set +a && DRY_RUN=0 python -m opsai_agent.scripts.sync_jira

To run the slack sync

set -a && source .env && set +a && SLACK_CHANNEL_NAME=my-channel-name python -m opsai_agent.scripts.sync_slack_channel