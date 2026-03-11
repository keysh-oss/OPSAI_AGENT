"""Simple Flask web app: chat UI that translates NL -> Cypher via OpenAI and queries Neo4j.

Usage:
  export OPENAI_API_KEY=... (or put in .env)
  export NEO4J_URI=bolt://host:7687
  export NEO4J_USER=neo4j
  export NEO4J_PASSWORD=...
  python -m opsai_agent.webapp.app
"""
import os
import json
from flask import Flask, request, render_template, jsonify
from flask_cors import CORS
from dotenv import load_dotenv

load_dotenv()

try:
    import openai
    # detect new OpenAI client (openai>=1.0.0) which exposes OpenAI class
    try:
        from openai import OpenAI as OpenAIClient  # type: ignore
    except Exception:
        OpenAIClient = None
except Exception:
    openai = None
    OpenAIClient = None

try:
    from neo4j import GraphDatabase
except Exception:
    GraphDatabase = None

app = Flask(__name__, template_folder=os.path.join(os.path.dirname(__file__), "templates"), static_folder=os.path.join(os.path.dirname(__file__), "static"))
CORS(app)


def openai_to_cypher(nl: str, schema_hint: str = "Users, Messages, Channels, Attachments") -> str:
    """Call OpenAI to convert natural language to a Cypher query.

    The system prompt asks for a single Cypher query. This is a thin wrapper and
    must be used with caution. Review produced queries before enabling writes.
    """
    if (openai is None) and (OpenAIClient is None):
        raise RuntimeError("openai package not installed")
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY not set")

    system = (
        "You are a translator that converts plain English into a single read-only Cypher query. "
        "Return ONLY the Cypher query and nothing else. The target database has nodes: User (id,name,email), "
        "Message (id,text,ts,channel), Channel (id,name), Attachment (id,filename,url). "
        "Prefer MATCH and RETURN statements; do not modify the database. If the user asks for relationships, "
        "return a Cypher that yields nodes and relationships (e.g., MATCH (u:User)-[r:POSTED]->(m:Message) RETURN u,m,r LIMIT 100)."
    )

    prompt = [
        {"role": "system", "content": system},
        {"role": "user", "content": f"Translate to Cypher (read-only). Schema hint: {schema_hint}. Query: {nl}"},
    ]

    model = os.getenv("OPENAI_MODEL", "gpt-3.5-turbo")

    # Support both the new openai>=1.0.0 client and the older openai.<1.0 interface.
    if OpenAIClient is not None:
        client = OpenAIClient()
        resp = client.chat.completions.create(model=model, messages=prompt, max_tokens=256, temperature=0)
        # response may be an object; try several extraction strategies to get the assistant content
        text = None
        try:
            # object-like access
            if hasattr(resp, "choices") and len(resp.choices) > 0:
                first = resp.choices[0]
                # first may have .message
                if hasattr(first, "message"):
                    msg = first.message
                    if hasattr(msg, "content") and msg.content:
                        text = msg.content
                    else:
                        # fallback to dict-like
                        try:
                            text = first["message"]["content"]
                        except Exception:
                            pass
                else:
                    try:
                        text = first["message"]["content"]
                    except Exception:
                        pass
        except Exception:
            text = None
        if not text:
            # try dict-like access
            try:
                d = dict(resp)
                text = d.get("choices", [])[0].get("message", {}).get("content")
            except Exception:
                text = None
        if not text:
            # last resort: stringify and try to extract code block or MATCH...RETURN
            s = str(resp)
            # try to find triple-backtick block
            import re

            m = re.search(r"```(?:cypher)?\n([\s\S]*?)```", s, re.IGNORECASE)
            if m:
                text = m.group(1).strip()
            else:
                # try to find MATCH..RETURN substring
                m2 = re.search(r"(MATCH[\s\S]*RETURN[\s\S]*?)(?:\n|$)", s, re.IGNORECASE)
                if m2:
                    text = m2.group(1).strip()
        if isinstance(text, str):
            text = text.strip()
    else:
        # Old client interface
        openai.api_key = api_key
        resp = openai.ChatCompletion.create(model=model, messages=prompt, max_tokens=256, temperature=0)
        text = resp["choices"][0]["message"]["content"].strip()

    # crude: if the assistant wrapped code fences, strip them
    if text.startswith("```"):
        # remove ```cypher or ```
        parts = text.split("\n")
        if parts[0].startswith("```"):
            parts = parts[1:]
        if parts and parts[-1].strip().endswith("```"):
            parts = parts[:-1]
        text = "\n".join(parts).strip()

    return text


def rewrite_cypher_to_read_only(cypher: str) -> str:
    """Ask the LLM to rewrite an existing Cypher query into a read-only equivalent.

    Returns the rewritten Cypher string (may still need validation).
    """
    if (openai is None) and (OpenAIClient is None):
        raise RuntimeError("openai package not installed")
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY not set")

    prompt_system = (
        "You are a Cypher specialist. Rewrite user-supplied Cypher into an equivalent READ-ONLY Cypher query. "
        "The rewritten query must not contain any data-modification or administration keywords such as CREATE, MERGE, DELETE, SET, REMOVE, DROP, CALL, LOAD CSV, or USING PERIODIC COMMIT. "
        "Return ONLY the rewritten Cypher query and nothing else. If no read-only equivalent exists, return an empty string."
    )

    prompt_user = f"Rewrite the following Cypher into a read-only Cypher using MATCH/OPTIONAL MATCH/WHERE/RETURN/LIMIT only.\n\nOriginal Cypher:\n```\n{cypher}\n```\n"

    messages = [
        {"role": "system", "content": prompt_system},
        {"role": "user", "content": prompt_user},
    ]

    model = os.getenv("OPENAI_MODEL", "gpt-3.5-turbo")

    if OpenAIClient is not None:
        client = OpenAIClient()
        resp = client.chat.completions.create(model=model, messages=messages, max_tokens=256, temperature=0)
        out = None
        try:
            if hasattr(resp, "choices") and len(resp.choices) > 0:
                first = resp.choices[0]
                if hasattr(first, "message"):
                    msg = first.message
                    out = getattr(msg, "content", None)
                else:
                    try:
                        out = first["message"]["content"]
                    except Exception:
                        out = None
        except Exception:
            out = None
        if not out:
            try:
                d = dict(resp)
                out = d.get("choices", [])[0].get("message", {}).get("content")
            except Exception:
                out = None
        if not out:
            s = str(resp)
            import re

            m = re.search(r"```(?:cypher)?\n([\s\S]*?)```", s, re.IGNORECASE)
            if m:
                out = m.group(1).strip()
            else:
                m2 = re.search(r"(MATCH[\s\S]*RETURN[\s\S]*?)(?:\n|$)", s, re.IGNORECASE)
                if m2:
                    out = m2.group(1).strip()
        if isinstance(out, str):
            out = out.strip()
    else:
        openai.api_key = api_key
        resp = openai.ChatCompletion.create(model=model, messages=messages, max_tokens=256, temperature=0)
        out = resp["choices"][0]["message"]["content"].strip()

    # strip code fences if present
    if out.startswith("```"):
        parts = out.split("\n")
        if parts[0].startswith("```"):
            parts = parts[1:]
        if parts and parts[-1].strip().endswith("```"):
            parts = parts[:-1]
        out = "\n".join(parts).strip()

    return out


def run_cypher(query: str):
    uri = os.getenv("NEO4J_URI")
    user = os.getenv("NEO4J_USER")
    pwd = os.getenv("NEO4J_PASSWORD")
    if GraphDatabase is None:
        raise RuntimeError("neo4j driver not installed")
    if not uri or not user or not pwd:
        raise RuntimeError("NEO4J_URI/NEO4J_USER/NEO4J_PASSWORD must be set in env")

    driver = GraphDatabase.driver(uri, auth=(user, pwd))
    results = {"records": [], "nodes": [], "relationships": []}
    try:
        with driver.session() as session:
            res = session.run(query)

            # temporary maps to deduplicate nodes/relationships by id
            nodes_by_id = {}
            rels_by_id = {}

            for record in res:
                # convert record values
                row = {}
                for k, v in record.items():
                    pyval = _neo4j_value_to_python(v)
                    row[k] = pyval

                    # collect nodes/relationships from the converted value
                    def _collect(val):
                        if isinstance(val, dict):
                            t = val.get("type")
                            if t == "node":
                                nid = val.get("id")
                                if nid is not None and nid not in nodes_by_id:
                                    nodes_by_id[nid] = val
                            elif t == "relationship":
                                rid = val.get("id")
                                if rid is not None and rid not in rels_by_id:
                                    rels_by_id[rid] = val
                            elif t == "path":
                                for n in val.get("nodes", []):
                                    nid = n.get("id")
                                    if nid is not None and nid not in nodes_by_id:
                                        nodes_by_id[nid] = n
                                for r in val.get("relationships", []):
                                    rid = r.get("id")
                                    if rid is not None and rid not in rels_by_id:
                                        rels_by_id[rid] = r
                        elif isinstance(val, list):
                            for item in val:
                                _collect(item)

                    _collect(pyval)

                results["records"].append(row)

            # populate top-level nodes/relationships lists
            results["nodes"] = list(nodes_by_id.values())
            results["relationships"] = list(rels_by_id.values())
    finally:
        driver.close()
    return results


def _neo4j_value_to_python(v):
    # neo4j graph types have 'id', 'labels', 'properties' or are primitives
    try:
        from neo4j.graph import Node, Relationship
    except Exception:
        Node = Relationship = None

    if Node is not None and isinstance(v, Node):
        return {"type": "node", "id": v.id, "labels": list(v.labels), "properties": dict(v)}
    if Relationship is not None and isinstance(v, Relationship):
        return {"type": "relationship", "id": v.id, "type_name": v.type, "start_node_id": v.start_node.id, "end_node_id": v.end_node.id, "properties": dict(v)}
    # fallback for paths
    try:
        # Path objects: iterate nodes and relationships
        from neo4j.graph import Path
        if isinstance(v, Path):
            nodes = [ {"type": "node", "id": n.id, "labels": list(n.labels), "properties": dict(n)} for n in v.nodes ]
            rels = []
            for r in v.relationships:
                rels.append({"type": "relationship", "id": r.id, "type_name": r.type, "start_node_id": r.start_node.id, "end_node_id": r.end_node.id, "properties": dict(r)})
            return {"type": "path", "nodes": nodes, "relationships": rels}
    except Exception:
        pass

    # primitives or lists
    if isinstance(v, (list, tuple)):
        return [_neo4j_value_to_python(x) for x in v]
    if hasattr(v, "items"):
        try:
            return dict(v)
        except Exception:
            pass
    return v


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/query", methods=["POST"])
def api_query():
    data = request.json or {}
    nl = data.get("query")
    if not nl:
        return jsonify({"error": "no query provided"}), 400
    # If MCP_URL is set, forward the query to the MCP server which will handle LLM->Cypher->Neo4j
    mcp_url = os.getenv("MCP_URL")
    if mcp_url:
        import requests

        try:
            resp = requests.post(mcp_url.rstrip("/") + "/mcp/query", json={"query": nl}, timeout=30)
            return (resp.content, resp.status_code, {"Content-Type": resp.headers.get("Content-Type", "application/json")})
        except Exception as e:
            return jsonify({"error": f"Failed to contact MCP server: {e}"}), 500

    try:
        cypher = openai_to_cypher(nl)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

    try:
        results = run_cypher(cypher)
    except Exception as e:
        # return cypher and error so user can inspect
        return jsonify({"cypher": cypher, "error": str(e)}), 500

    return jsonify({"cypher": cypher, "results": results})


def main():
    host = os.getenv("HOST", "0.0.0.0")
    port = int(os.getenv("PORT", "8080"))
    app.run(host=host, port=port, debug=os.getenv("FLASK_DEBUG", "0") == "1")


if __name__ == "__main__":
    main()
