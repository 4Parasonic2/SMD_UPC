"""
D2 — Lab part D, exercise 2
===========================
Implement the algorithm required for D2.

See ``D1.py`` for connection notes and implementation options.
"""

from __future__ import annotations

import getpass
import os

from neo4j import GraphDatabase

NEO4J_URI = os.environ.get("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.environ.get("NEO4J_USERNAME", "neo4j")


def run_algorithm(session) -> None:
    # TODO: replace with D2 solution
    q = """
    MATCH ()-[r]->()
    RETURN type(r) AS relType, count(*) AS cnt
    ORDER BY cnt DESC
    LIMIT 10
    """
    for record in session.run(q):
        print(dict(record))


def main() -> None:
    pw = os.environ.get("NEO4J_PASSWORD", "").strip() or getpass.getpass("Neo4j password: ")
    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, pw), connection_timeout=120)
    with driver.session() as session:
        run_algorithm(session)
    driver.close()


if __name__ == "__main__":
    main()
