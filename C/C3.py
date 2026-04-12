"""
C3 — Find the top-100 papers of the Database community
=======================================================
Counts how many database-community papers cite each paper (regardless of
where the cited paper is published), ranks them, and asserts the top 100.

"Citations from the community" means the *citing* paper must be published
in a venue marked as community venue by C2.

New graph element asserted:
  · (Paper)-[:top_paper_of {citationCount: int}]->(Community)

Properties on the edge:
  · citationCount — number of distinct community papers that cite this paper
                    (stored on the edge because it is relative to the community;
                     the same paper could be top in another community with a
                     different count)

Prerequisite: C2 must have been run first (includes_venue edges must exist).
"""

from __future__ import annotations

import getpass
import os

from neo4j import GraphDatabase

NEO4J_URI  = os.environ.get("NEO4J_URI",      "bolt://localhost:7687")
NEO4J_USER = os.environ.get("NEO4J_USERNAME", "neo4j")

# ---------------------------------------------------------------------------
# Step 3 — assert top_paper_of edges for the 100 most-cited papers
# ---------------------------------------------------------------------------
WRITE_QUERY = """
// Traverse from community venues → published papers (the "citing" side)
MATCH (c:Community {name: "Database"})-[:includes_venue]->(v:Venue)
MATCH (v)<-[:of_venue]-(:Issue)<-[:published_in]-(citing:Paper)

// Follow cites edges — cited paper can be from any venue
MATCH (citing)-[:cites]->(cited:Paper)

// Aggregate: how many distinct community papers cite each paper?
WITH c, cited, count(DISTINCT citing) AS citation_count
ORDER BY citation_count DESC
LIMIT 100

// Assert the relationship, storing the count as an edge property
MERGE (cited)-[r:top_paper_of]->(c)
  SET r.citationCount = citation_count
"""

# Verify: list the top-10 with their citation counts
READ_QUERY = """
MATCH (p:Paper)-[r:top_paper_of]->(c:Community {name: "Database"})
RETURN p.title          AS title,
       p.year           AS year,
       r.citationCount  AS community_citations
ORDER BY community_citations DESC
LIMIT 10
"""


def _get_password() -> str:
    env = os.environ.get("NEO4J_PASSWORD", "").strip()
    if env:
        return env
    return getpass.getpass("  Neo4j password: ").strip()


def main() -> None:
    print("=" * 60)
    print("  C3 — Identify top-100 Database community papers")
    print("=" * 60)

    pw = _get_password()
    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, pw),
                                  connection_timeout=120)

    with driver.session() as s:
        # ── Write: assert top_paper_of edges ─────────────────────────────────
        print("\n  -- Asserting top_paper_of edges (top 100 by community citations)")
        summary = s.run(WRITE_QUERY).consume()
        c = summary.counters
        if c.relationships_created: print(f"  rels  created  : {c.relationships_created:,}")
        if c.properties_set:        print(f"  properties set : {c.properties_set:,}")
        if not (c.relationships_created or c.properties_set):
            print("  (already exists — no changes)")

        # ── Read: verify ──────────────────────────────────────────────────────
        total = s.run(
            "MATCH (:Paper)-[:top_paper_of]->(:Community {name: 'Database'}) "
            "RETURN count(*) AS n"
        ).single()["n"]
        print(f"\n  Total top papers asserted : {total:,}")

        print("\n  -- Top 10 preview")
        print(f"  {'#':<4} {'Citations':>9}  {'Year':>4}  Title")
        print("  " + "-" * 76)
        for rank, row in enumerate(s.run(READ_QUERY), start=1):
            title = (row["title"] or "")[:55]
            print(f"  {rank:<4} {row['community_citations']:>9,}  "
                  f"{row['year'] or '?':>4}  {title}")

    driver.close()
    print("\n  Done.")


if __name__ == "__main__":
    main()
