"""
C2 — Lab part C, exercise 2
===========================
Queries and/or updates for exercise C2.
"""

from __future__ import annotations

import getpass
import os

from neo4j import GraphDatabase

NEO4J_URI = os.environ.get("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.environ.get("NEO4J_USERNAME", "neo4j")

READ_QUERY = """
// TODO: C2 read
RETURN 1 AS ok
"""

WRITE_QUERY = """
// optional
"""


def main() -> None:
    pw = os.environ.get("NEO4J_PASSWORD", "").strip() or getpass.getpass("Neo4j password: ")
    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, pw), connection_timeout=120)
    with driver.session() as session:
        for record in session.run(READ_QUERY):
            print(dict(record))
        wq = WRITE_QUERY.strip()
        if wq and not wq.startswith("//"):
            session.run(wq).consume()
    driver.close()


if __name__ == "__main__":
    main()
