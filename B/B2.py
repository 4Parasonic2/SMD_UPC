"""
B2 — Lab part B, exercise 2
===========================
Execute the Cypher query for exercise B2 (read-only).
"""

from __future__ import annotations

import getpass
import os

from neo4j import GraphDatabase

NEO4J_URI = os.environ.get("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.environ.get("NEO4J_USERNAME", "neo4j")

QUERY = """
// TODO: exercise B2
MATCH (n) RETURN labels(n)[0] AS label, count(*) AS c LIMIT 10
"""


def main() -> None:
    pw = os.environ.get("NEO4J_PASSWORD", "").strip() or getpass.getpass("Neo4j password: ")
    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, pw), connection_timeout=120)
    with driver.session() as session:
        for record in session.run(QUERY):
            print(dict(record))
    driver.close()


if __name__ == "__main__":
    main()
