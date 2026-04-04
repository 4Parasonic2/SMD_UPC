"""
UploadUpdateCSV.py — A.3
========================
Loads the Organization nodes and affiliated_with edges produced by
FormatUpdateCSV.py into an existing Neo4j graph.

Before running:
  1. Run FormatUpdateCSV.py to generate the two CSV files.
  2. Copy Update_Csvs/organization_nodes.csv and
          Update_Csvs/rel_affiliated_with.csv
     into Neo4j's import directory (same folder used by UploadCSV.py).
  3. Run this script.

The script is additive — it MERGEs nodes and relationships so it is safe
to re-run without creating duplicates.

Requirements:
    pip install neo4j
"""

from __future__ import annotations

import getpass
import os
import sys

from neo4j import GraphDatabase

# =============================================================================
# CONFIGURATION
# =============================================================================

NEO4J_URI  = os.environ.get("NEO4J_URI",      "bolt://localhost:7687")
NEO4J_USER = os.environ.get("NEO4J_USERNAME", "neo4j")

NODE_BATCH = 200
REL_BATCH  = 500


# =============================================================================
# HELPERS
# =============================================================================

def _get_password() -> str:
    env = os.environ.get("NEO4J_PASSWORD", "").strip()
    if env:
        print("  Neo4j password : (from NEO4J_PASSWORD env var)")
        return env
    try:
        return getpass.getpass("  Neo4j password : ").strip()
    except (KeyboardInterrupt, EOFError):
        print("\n  Aborted.")
        sys.exit(0)


def _run(session, description: str, query: str) -> None:
    """Execute a Cypher query and print a summary of counters."""
    try:
        summary = session.run(query).consume()
        c = summary.counters
        print(f"  ✓  {description}")
        if c.nodes_created:         print(f"       nodes created      : {c.nodes_created:,}")
        if c.relationships_created: print(f"       rels  created      : {c.relationships_created:,}")
        if c.properties_set:        print(f"       properties set     : {c.properties_set:,}")
    except Exception as e:
        print(f"  ✗  {description}")
        print(f"     ERROR: {e}")
        sys.exit(1)


def _pause() -> None:
    """Ask the user to confirm that the CSV files have been copied."""
    print("""
  ─────────────────────────────────────────────────────────
  Copy these files into Neo4j's import directory:
    · organization_nodes.csv
    · rel_affiliated_with.csv
  ─────────────────────────────────────────────────────────
""")
    try:
        ans = input("  Files copied? Press Enter to continue, or 'no' to abort: ").strip().lower()
    except (KeyboardInterrupt, EOFError):
        print("\n  Aborted.")
        sys.exit(0)
    if ans in ("no", "n", "q", "quit", "exit"):
        print("  Upload skipped.")
        sys.exit(0)
    print()


# =============================================================================
# UPLOAD
# =============================================================================

def run_updates(session) -> None:
    # ── Constraint ────────────────────────────────────────────────────────────
    print("\n  -- Constraint")
    _run(session, "Constraint Organization.orgId",
         "CREATE CONSTRAINT organization_orgid IF NOT EXISTS "
         "FOR (o:Organization) REQUIRE o.orgId IS UNIQUE")

    # ── Organization nodes ────────────────────────────────────────────────────
    print("\n  -- Organization nodes")
    _run(session, "Organization nodes", f"""
        LOAD CSV WITH HEADERS FROM 'file:///organization_nodes.csv'
        AS row FIELDTERMINATOR ';'
        CALL {{
            WITH row
            WITH row WHERE row.orgId IS NOT NULL AND trim(row.orgId) <> ''
            MERGE (o:Organization {{orgId: trim(row.orgId)}})
              SET o.name    = trim(row.name),
                  o.orgType = trim(row.orgType)
        }} IN TRANSACTIONS OF {NODE_BATCH} ROWS
    """)

    # ── affiliated_with edges (Author → Organization) ─────────────────────────
    # :START_ID = Author.authorId   :END_ID = Organization.orgId
    # MATCH is used (not MERGE on node) so rows with no matching Author or
    # Organization are silently skipped — no error, no dangling relationships.
    print("\n  -- affiliated_with  (Author → Organization)")
    _run(session, "affiliated_with edges", f"""
        LOAD CSV WITH HEADERS FROM 'file:///rel_affiliated_with.csv'
        AS row FIELDTERMINATOR ';'
        CALL {{
            WITH row
            MATCH (a:Author       {{authorId: trim(row.`:START_ID`)}})
            MATCH (o:Organization {{orgId:    trim(row.`:END_ID`)}})
            MERGE (a)-[:affiliated_with]->(o)
        }} IN TRANSACTIONS OF {REL_BATCH} ROWS
    """)

    # ── Verification ──────────────────────────────────────────────────────────
    print("\n  -- Verification")
    for r in session.run(
        "MATCH (o:Organization) "
        "RETURN o.orgType AS type, count(o) AS n "
        "ORDER BY n DESC"
    ):
        print(f"    Organization ({r['type']:10s}) : {r['n']:,}")

    aff_count = session.run(
        "MATCH (:Author)-[:affiliated_with]->(:Organization) "
        "RETURN count(*) AS n"
    ).single()["n"]
    print(f"    affiliated_with edges       : {aff_count:,}")


# =============================================================================
# MAIN
# =============================================================================

def main() -> None:
    print("=" * 60)
    print("  UploadUpdateCSV.py — A.3  (Organization + affiliated_with)")
    print("=" * 60)

    pw = _get_password()
    _pause()

    print(f"  Connecting to {NEO4J_URI} …")
    try:
        driver = GraphDatabase.driver(
            NEO4J_URI,
            auth=(NEO4J_USER, pw),
            connection_timeout=120,
            max_transaction_retry_time=300,
        )
        driver.verify_connectivity()
        print("  ✓  Connected\n")
    except Exception as e:
        print(f"  ✗  Connection failed: {e}")
        print("     Is Neo4j running? Is the password correct?")
        sys.exit(1)

    with driver.session() as session:
        run_updates(session)

    driver.close()
    print("\n  Update complete.")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n  Aborted.", file=sys.stderr)
        sys.exit(1)
