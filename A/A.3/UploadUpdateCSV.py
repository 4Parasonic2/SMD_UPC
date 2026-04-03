"""
UploadUpdateCSV.py — A.3
========================
Load the CSV files from ``Update_Csvs/`` (produced by ``FormatUpdateCSV.py``)
into Neo4j using ``LOAD CSV`` + ``MERGE``.

**Before running:** copy the CSV files from ``A/A.3/Update_Csvs/`` into Neo4j’s
import directory, or adjust the ``file:///`` paths below.

Edit the Cypher blocks in ``run_updates()`` to match the files and schema from
your A.3 report (constraints, labels, relationship types).

Prerequisites:
    pip install neo4j
"""

from __future__ import annotations

import getpass
import os
import sys

from neo4j import GraphDatabase

NEO4J_URI = os.environ.get("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.environ.get("NEO4J_USERNAME", "neo4j")


def get_password() -> str:
    env = os.environ.get("NEO4J_PASSWORD", "").strip()
    if env:
        return env
    return getpass.getpass("Neo4j password: ").strip()


def run_updates(session) -> None:
    """
    Add one ``LOAD CSV`` + ``MERGE`` block per file you generated in FormatUpdateCSV.

    Example matches the placeholder files created by ``FormatUpdateCSV.py``.
    Uncomment or replace with your real schema.
    """
    # Optional: uniqueness for new labels (run once; IF NOT EXISTS is safe)
    # session.run(
    #     "CREATE CONSTRAINT tag_id IF NOT EXISTS "
    #     "FOR (t:Tag) REQUIRE t.tagId IS UNIQUE"
    # )

    # Example — adjust paths if your import folder filename differs
    # session.run("""
    #     LOAD CSV WITH HEADERS FROM 'file:///update_tag_nodes.csv'
    #     AS row FIELDTERMINATOR ';'
    #     CALL {
    #         WITH row
    #         WITH row WHERE row.tagId IS NOT NULL AND trim(row.tagId) <> ''
    #         MERGE (t:Tag {tagId: trim(row.tagId)})
    #           SET t.name = trim(row.name)
    #     } IN TRANSACTIONS OF 200 ROWS
    # """)

    # session.run("""
    #     LOAD CSV WITH HEADERS FROM 'file:///rel_paper_tagged.csv'
    #     AS row FIELDTERMINATOR ';'
    #     CALL {
    #         WITH row
    #         MATCH (p:Paper {paperId: trim(row.`:START_ID`)})
    #         MATCH (t:Tag   {tagId:   trim(row.`:END_ID`)})
    #         MERGE (p)-[:tagged_with]->(t)
    #     } IN TRANSACTIONS OF 500 ROWS
    # """)

    print("  [info] No default LOAD CSV blocks enabled — edit UploadUpdateCSV.py "
          "to match your A.3 CSVs and schema.")


def main() -> None:
    pw = get_password()
    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, pw), connection_timeout=120)
    try:
        driver.verify_connectivity()
        print("Connected to", NEO4J_URI)
        with driver.session() as session:
            run_updates(session)
    finally:
        driver.close()
    print("UploadUpdateCSV finished.")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nAborted.", file=sys.stderr)
        sys.exit(1)
