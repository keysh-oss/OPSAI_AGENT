"""Integration test: write a test user and message to Neo4j, verify, then clean up.

This script uses environment variables NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD from
`.env` (loaded by python-dotenv). It performs idempotent writes via the existing
Neo4jLoader, verifies via direct Cypher queries, and deletes the test nodes.

Use cautiously — it will delete the test nodes it creates only.
"""
import os
import time
import uuid
from dotenv import load_dotenv

load_dotenv()

from neo4j import GraphDatabase
from opsai_agent.loader.neo4j_loader import Neo4jLoader


def run_integration():
    uri = os.getenv("NEO4J_URI")
    user = os.getenv("NEO4J_USER")
    password = os.getenv("NEO4J_PASSWORD")

    if not uri or not user or not password:
        print("NEO4J_URI/NEO4J_USER/NEO4J_PASSWORD not set in environment. Aborting.")
        return 2

    # Unique ids for this run
    run_id = uuid.uuid4().hex[:8]
    test_user_id = f"integ_user:{run_id}"
    test_msg_id = f"integ_msg:{run_id}"

    loader = Neo4jLoader(uri=uri, user=user, password=password)

    user_payload = [{"id": test_user_id, "name": "integ-test", "email": f"integ+{run_id}@example.com", "source": "integ"}]
    msg_payload = [
        {
            "id": test_msg_id,
            "text": "integration test message",
            "ts": str(int(time.time())),
            "channel": f"integ_chan:{run_id}",
            "channel_name": "integ-channel",
            "user": {"id": test_user_id, "name": "integ-test", "email": f"integ+{run_id}@example.com", "source": "integ"},
            "raw": {},
            "source": "integ",
        }
    ]

    print("[integration] Writing test user and message to Neo4j...")
    loader.write_users(user_payload, dry_run=False)
    loader.write_messages(msg_payload, dry_run=False)

    # Verify using direct driver
    driver = GraphDatabase.driver(uri, auth=(user, password))
    with driver.session() as session:
        user_count = session.run("MATCH (u:User {id:$id}) RETURN count(u) AS c", id=test_user_id).single().get("c")
        msg_count = session.run("MATCH (m:Message {id:$id}) RETURN count(m) AS c", id=test_msg_id).single().get("c")
        rel_count = session.run(
            "MATCH (u:User {id:$uid})-[:POSTED]->(m:Message {id:$mid}) RETURN count(*) AS c",
            uid=test_user_id,
            mid=test_msg_id,
        ).single().get("c")

    print(f"[integration] verification: users={user_count}, messages={msg_count}, posted_rel={rel_count}")

    success = user_count >= 1 and msg_count >= 1 and rel_count >= 1

    # Cleanup
    print("[integration] Cleaning up test nodes...")
    with driver.session() as session:
        session.run("MATCH (u:User {id:$id}) DETACH DELETE u", id=test_user_id)
        session.run("MATCH (m:Message {id:$id}) DETACH DELETE m", id=test_msg_id)
        # remove channel if no other relationships exist with that id
        session.run("MATCH (c:Channel {id:$id}) WHERE NOT (c)--() DELETE c", id=f"integ_chan:{run_id}")

    driver.close()

    if success:
        print("[integration] SUCCESS: integration test passed and cleaned up.")
        return 0
    else:
        print("[integration] FAILURE: verification failed (see counts above).")
        return 3


if __name__ == "__main__":
    raise SystemExit(run_integration())
