"""
C1 — Define the Database research community
============================================
Creates one Community node and connects it to the Keyword nodes that define
the database research community via has_topic edges.

New graph elements asserted:
  · (c:Community)                         communityId, name
  · (Community)-[:has_topic]->(Keyword)   defines which keywords belong to c

WRITE_QUERY runs the MERGE; READ_QUERY verifies what was created.

Keyword list notes
------------------
The assignment specification uses these 7 canonical terms:
  data management, indexing, data modeling, big data, data processing,
  data storage, data querying

However, the synthetic graph was generated from a fixed KEYWORD_NAMES list
in A.2/FormatCSV.py, which does NOT include "big data", "data modeling",
"data processing", "data storage", or "data querying" verbatim.  Only
"data management" and "indexing" are exact matches.

To ensure C1 actually links to keywords that exist in the graph, the query
below uses ALL 20 database-related keywords present in KEYWORD_NAMES.
These cover the same semantic space as the 7 canonical terms:
  - "data management" family  → data management, data integration,
                                 data quality, data cleaning, data provenance,
                                 data warehouse, ETL, data lake,
                                 master data management, schema matching
  - "data querying" family    → query optimization, query processing
  - "data storage" family     → relational database, NoSQL, NewSQL,
                                 distributed database, transaction processing,
                                 OLAP, OLTP
  - "indexing"                → indexing
"""

from __future__ import annotations

import getpass
import os

from neo4j import GraphDatabase

NEO4J_URI  = os.environ.get("NEO4J_URI",      "bolt://localhost:7687")
NEO4J_USER = os.environ.get("NEO4J_USERNAME", "neo4j")

# ---------------------------------------------------------------------------
# Step 1 — assert the community and its keyword membership
# ---------------------------------------------------------------------------
WRITE_QUERY = """
MERGE (c:Community {communityId: "community-db", name: "Database"})
WITH c
MATCH (k:Keyword)
WHERE k.name IN [
  // --- data management & integration family ---
  "data management", "data integration", "schema matching",
  "data quality",    "data cleaning",    "data provenance",
  "data warehouse",  "ETL",              "data lake",
  "master data management",
  // --- databases & querying family ---
  "query optimization",    "query processing",    "relational database",
  "NoSQL",                 "NewSQL",              "distributed database",
  "transaction processing","indexing",            "OLAP",
  "OLTP"
]
MERGE (c)-[:has_topic]->(k)
"""

# Verify: show the community and all linked keywords
READ_QUERY = """
MATCH (c:Community {name: "Database"})-[:has_topic]->(k:Keyword)
RETURN c.name AS community, collect(k.name) AS keywords, count(k) AS total
"""


def _get_password() -> str:
    env = os.environ.get("NEO4J_PASSWORD", "").strip()
    if env:
        return env
    return getpass.getpass("  Neo4j password: ").strip()


def main() -> None:
    print("=" * 60)
    print("  C1 — Define the Database community")
    print("=" * 60)

    pw = _get_password()
    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, pw),
                                  connection_timeout=120)

    with driver.session() as s:
        # ── Write: assert community + has_topic edges ─────────────────────────
        print("\n  -- Asserting Community node and has_topic edges")
        summary = s.run(WRITE_QUERY).consume()
        c = summary.counters
        if c.nodes_created:         print(f"  nodes created      : {c.nodes_created:,}")
        if c.relationships_created: print(f"  rels  created      : {c.relationships_created:,}")
        if c.properties_set:        print(f"  properties set     : {c.properties_set:,}")
        if not (c.nodes_created or c.relationships_created):
            print("  (already exists — no changes)")

        # ── Read: verify ──────────────────────────────────────────────────────
        print("\n  -- Verification")
        for row in s.run(READ_QUERY):
            print(f"  Community : {row['community']}")
            print(f"  Keywords  : {row['total']} linked")
            for kw in sorted(row["keywords"]):
                print(f"    · {kw}")

    driver.close()
    print("\n  Done.")


if __name__ == "__main__":
    main()
