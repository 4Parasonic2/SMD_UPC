"""
UploadUpdateCSV.py — A.3
========================
Enriches an existing Neo4j graph (built by A.2 UploadCSV.py) with:
  · Organization nodes + affiliated_with edges (Author → Organization)
  · Review enrichment  — sets text and decision on bare Review nodes
  · Venue enrichment   — computes and sets reviewerCount on every Venue node

Before running:
  1. Run FormatUpdateCSV.py to generate the three CSV files.
  2. Copy into Neo4j's import directory (same folder used by UploadCSV.py):
       · organization_nodes.csv
       · rel_affiliated_with.csv
       · review_updates.csv
  3. Run this script.

The script is additive — it MERGEs nodes / relationships and SETs properties,
so it is safe to re-run without creating duplicates or data loss.

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
    · review_updates.csv
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
        CALL (row) {{
            WITH row WHERE row.orgId IS NOT NULL AND trim(row.orgId) <> ''
            MERGE (o:Organization {{orgId: trim(row.orgId)}})
              SET o.name    = trim(row.name),
                  o.orgType = trim(row.orgType)
        }} IN TRANSACTIONS OF {NODE_BATCH} ROWS
    """)

    # ── affiliated_with edges (Author → Organization) ─────────────────────────
    # MATCH is used so rows with no matching Author or Organization are skipped.
    print("\n  -- affiliated_with  (Author → Organization)")
    _run(session, "affiliated_with edges", f"""
        LOAD CSV WITH HEADERS FROM 'file:///rel_affiliated_with.csv'
        AS row FIELDTERMINATOR ';'
        CALL (row) {{
            MATCH (a:Author       {{authorId: trim(row.`:START_ID`)}})
            MATCH (o:Organization {{orgId:    trim(row.`:END_ID`)}})
            MERGE (a)-[:affiliated_with]->(o)
        }} IN TRANSACTIONS OF {REL_BATCH} ROWS
    """)

    # ── Review enrichment — add text + decision ───────────────────────────────
    # The A.2 base graph created Review nodes with only reviewId.
    # This step enriches them with the two missing attributes from the model.
    print("\n  -- Review enrichment  (text + decision)")
    _run(session, "Review text + decision", f"""
        LOAD CSV WITH HEADERS FROM 'file:///review_updates.csv'
        AS row FIELDTERMINATOR ';'
        CALL (row) {{
            WITH row WHERE row.reviewId IS NOT NULL AND trim(row.reviewId) <> ''
            MATCH (r:Review {{reviewId: trim(row.reviewId)}})
              SET r.text     = trim(row.text),
                  r.decision = trim(row.decision)
        }} IN TRANSACTIONS OF {NODE_BATCH} ROWS
    """)

    # ── Venue enrichment — compute reviewerCount ──────────────────────────────
    # reviewerCount = number of distinct authors who reviewed at least one paper
    # published in an issue of that venue.
    # Computed entirely in Cypher — no CSV required.
    print("\n  -- Venue enrichment  (reviewerCount)")
    _run(session, "Venue.reviewerCount", f"""
        MATCH (v:Venue)
        CALL (v) {{
            OPTIONAL MATCH (v)<-[:of_venue]-(:Issue)<-[:published_in]-(:Paper)
                          <-[:about]-(:Review)<-[:wrote_review]-(a:Author)
            WITH v, count(DISTINCT a) AS cnt
            SET v.reviewerCount = cnt
        }} IN TRANSACTIONS OF {NODE_BATCH} ROWS
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

    rev_enriched = session.run(
        "MATCH (r:Review) WHERE r.text IS NOT NULL "
        "RETURN count(r) AS n"
    ).single()["n"]
    print(f"    Reviews with text+decision  : {rev_enriched:,}")

    venue_enriched = session.run(
        "MATCH (v:Venue) WHERE v.reviewerCount IS NOT NULL "
        "RETURN count(v) AS n"
    ).single()["n"]
    print(f"    Venues with reviewerCount   : {venue_enriched:,}")


# =============================================================================
# MAIN
# =============================================================================

def main() -> None:
    print("=" * 60)
    print("  UploadUpdateCSV.py — A.3")
    print("  (Organization + Reviews + Venue reviewerCount)")
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
