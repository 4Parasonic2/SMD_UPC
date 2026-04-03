"""
B1 — Lab part B, exercise 1
===========================
Execute the Cypher query required for this exercise (read-only).

Set ``QUERY`` below to your solution. Connection uses env vars
``NEO4J_URI``, ``NEO4J_USERNAME``, ``NEO4J_PASSWORD`` or prompts for password.
"""

from __future__ import annotations

import getpass
import os

from neo4j import GraphDatabase

NEO4J_URI = os.environ.get("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.environ.get("NEO4J_USERNAME", "neo4j")

# TODO: paste the exercise query
QUERY = """
// Example: return a few papers
MATCH (p:Paper) RETURN p.paperId AS id, p.title AS title LIMIT 5
"""


def main() -> None:
    pw = os.environ.get("NEO4J_PASSWORD", "").strip() or getpass.getpass("Neo4j password: ")
    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, pw), connection_timeout=120)
    with driver.session() as session:
        result = session.run(QUERY)
        for record in result:
            print(dict(record))
    driver.close()


if __name__ == "__main__":
    main()
