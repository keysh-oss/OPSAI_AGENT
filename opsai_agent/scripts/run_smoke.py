"""Run a dry-run smoke test of the starter pipeline.

This script deliberately avoids contacting external systems. It runs the connector
sample exports, maps them, and calls the loader in dry-run mode.
"""
from opsai_agent.connectors.slack_connector import SlackConnector
from opsai_agent.connectors.jira_connector import JiraConnector
from opsai_agent.connectors.confluence_connector import ConfluenceConnector
from opsai_agent.transformers import mapper
from opsai_agent.loader.neo4j_loader import Neo4jLoader
from opsai_agent.orchestrator.prefect_flow import run_simple_pipeline


def main():
    print("OPSAI_AGENT smoke run — dry-run mode")

    slack = SlackConnector()
    jira = JiraConnector()
    conf = ConfluenceConnector()

    loader = Neo4jLoader()

    run_simple_pipeline([slack, jira, conf], mapper, loader, dry_run=True)


if __name__ == "__main__":
    main()
