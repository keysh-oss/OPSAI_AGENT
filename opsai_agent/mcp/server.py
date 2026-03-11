"""MCP server: accepts natural language, converts to Cypher (via LLM), validates,
executes against Neo4j, and returns structured graph results.
"""
import os
from flask import Flask, request, jsonify
from dotenv import load_dotenv

load_dotenv()

from opsai_agent.webapp.app import openai_to_cypher, run_cypher
from opsai_agent.mcp.fallback import fallback_nl_to_cypher
from opsai_agent.webapp.app import rewrite_cypher_to_read_only
try:
    from neo4j import GraphDatabase
except Exception:
    GraphDatabase = None

app = Flask(__name__)


FORBIDDEN = ["CREATE", "MERGE", "DELETE", "SET", "REMOVE", "DROP", "CALL", "LOAD CSV", "USING PERIODIC COMMIT"]


def is_read_only(cypher: str) -> bool:
    cu = cypher.upper()
    for kw in FORBIDDEN:
        if kw in cu:
            return False
    return True


@app.route("/mcp/query", methods=["POST"])
def mcp_query():
    data = request.get_json() or {}
    nl = data.get("query")
    if not nl:
        return jsonify({"error": "no query provided"}), 400

    try:
        cypher = openai_to_cypher(nl)
    except Exception as e:
        return jsonify({"error": f"LLM translation failed: {e}"}), 500

    # Safety check: try to repair if not read-only
    if not is_read_only(cypher):
        # attempt to repair with the LLM
        attempts = []
        for i in range(2):
            try:
                candidate = rewrite_cypher_to_read_only(cypher)
            except Exception as e:
                attempts.append({"error": str(e)})
                candidate = ""
            attempts.append({"candidate": candidate})
            if candidate and is_read_only(candidate):
                cypher = candidate
                break
        else:
            return jsonify({"error": "Generated Cypher contains write/unsafe operations and auto-repair failed.", "cypher": cypher, "attempts": attempts}), 400

    # Try an EXPLAIN first to surface syntax errors early
    uri = os.getenv("NEO4J_URI")
    user = os.getenv("NEO4J_USER")
    pwd = os.getenv("NEO4J_PASSWORD")
    if GraphDatabase is None or not uri or not user or not pwd:
        return jsonify({"error": "Neo4j driver/config missing on MCP server"}), 500

    try:
        driver = GraphDatabase.driver(uri, auth=(user, pwd))
        with driver.session() as session:
            # Use EXPLAIN to validate the query without executing writes
            session.run("EXPLAIN " + cypher).consume()
        driver.close()
    except Exception as e:
        # If EXPLAIN fails on the LLM-generated query, try a fallback heuristic translation of NL -> Cypher
        fallback = fallback_nl_to_cypher(nl)
        if fallback:
            try:
                driver = GraphDatabase.driver(uri, auth=(user, pwd))
                with driver.session() as session:
                    session.run("EXPLAIN " + fallback).consume()
                driver.close()
                cypher = fallback
            except Exception as e2:
                return jsonify({"error": f"Cypher validation (EXPLAIN) failed for both LLM and fallback: {e}; fallback error: {e2}", "cypher": cypher, "fallback": fallback}), 400
        else:
            return jsonify({"error": f"Cypher validation (EXPLAIN) failed: {e}", "cypher": cypher}), 400

    try:
        results = run_cypher(cypher)
    except Exception as e:
        return jsonify({"error": f"Cypher execution failed: {e}", "cypher": cypher}), 500

    return jsonify({"cypher": cypher, "results": results})


def main():
    host = os.getenv("MCP_HOST", "127.0.0.1")
    port = int(os.getenv("MCP_PORT", "5001"))
    app.run(host=host, port=port)


if __name__ == "__main__":
    main()
