"""
C1 — Lab part C, exercise 1
===========================
Run **read** queries and/or **write** updates as required by exercise C1.

Use ``READ_QUERY`` for MATCH/RETURN and ``WRITE_QUERY`` for CREATE/MERGE/DELETE
(leave write empty if not needed).
"""

from __future__ import annotations

import getpass
import os

from neo4j import GraphDatabase

NEO4J_URI = os.environ.get("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.environ.get("NEO4J_USERNAME", "neo4j")

READ_QUERY = """
// TODO: exercise C1 — read part
MATCH (n) RETURN count(n) AS totalNodes
"""

WRITE_QUERY = """
// TODO: exercise C1 — update part (optional)
// MERGE ...
"""


def main() -> None:
    pw = os.environ.get("NEO4J_PASSWORD", "").strip() or getpass.getpass("Neo4j password: ")
    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, pw), connection_timeout=120)
    with driver.session() as session:
        for record in session.run(READ_QUERY):
            print("read:", dict(record))
        wq = WRITE_QUERY.strip()
        if wq and not wq.startswith("//"):
            summary = session.run(wq).consume()
            print("write counters:", summary.counters)
    driver.close()


if __name__ == "__main__":
    main()
