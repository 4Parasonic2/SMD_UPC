"""
C4 — Identify potential reviewers and gurus for the Database community
======================================================================
Every author of a top-100 paper (asserted by C3) becomes a potential
reviewer.  Authors of at least TWO top-100 papers are additionally marked
as gurus — reputable authors suitable for top events or journals.

New graph elements asserted:
  · (Author)-[:potential_reviewer_for {topPaperCount: int}]->(Community)
        marks every qualifying author; topPaperCount records how many
        top-100 papers they authored (stored on the edge because it is
        relative to the community)

  · (Author)-[:guru_of]->(Community)
        asserted only when topPaperCount >= 2; kept as a separate
        relationship type (rather than a boolean on potential_reviewer_for)
        so that guru queries can use a simple label-style traversal without
        filtering on property values

Prerequisite: C3 must have been run first (top_paper_of edges must exist).
"""

from __future__ import annotations

import getpass
import os

from neo4j import GraphDatabase

NEO4J_URI  = os.environ.get("NEO4J_URI",      "bolt://localhost:7687")
NEO4J_USER = os.environ.get("NEO4J_USERNAME", "neo4j")

# ---------------------------------------------------------------------------
# Step 4 — assert potential_reviewer_for and guru_of edges
# ---------------------------------------------------------------------------
WRITE_QUERY = """
// Find all authors of top-100 papers
MATCH (p:Paper)-[:top_paper_of]->(c:Community)
MATCH (a:Author)-[:authored]->(p)
WITH c, a, count(DISTINCT p) AS top_paper_count

// Every qualifying author is a potential reviewer
MERGE (a)-[r:potential_reviewer_for]->(c)
  SET r.topPaperCount = top_paper_count

// Authors with >= 2 top papers are gurus (conditional MERGE via FOREACH)
FOREACH (_ IN CASE WHEN top_paper_count >= 2 THEN [1] ELSE [] END |
  MERGE (a)-[:guru_of]->(c)
)
"""

# Verify: list all reviewers, flag gurus, order by top-paper count
READ_QUERY = """
MATCH (a:Author)-[r:potential_reviewer_for]->(c:Community {name: "Database"})
RETURN a.name           AS author,
       r.topPaperCount  AS top_papers,
       CASE WHEN (a)-[:guru_of]->(c) THEN "GURU" ELSE "" END AS status
ORDER BY top_papers DESC, author
LIMIT 25
"""


def _get_password() -> str:
    env = os.environ.get("NEO4J_PASSWORD", "").strip()
    if env:
        return env
    return getpass.getpass("  Neo4j password: ").strip()


def main() -> None:
    print("=" * 60)
    print("  C4 — Identify potential reviewers and gurus")
    print("=" * 60)

    pw = _get_password()
    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, pw),
                                  connection_timeout=120)

    with driver.session() as s:
        # ── Write: assert reviewer / guru edges ──────────────────────────────
        print("\n  -- Asserting potential_reviewer_for and guru_of edges")
        summary = s.run(WRITE_QUERY).consume()
        c = summary.counters
        if c.relationships_created: print(f"  rels  created  : {c.relationships_created:,}")
        if c.properties_set:        print(f"  properties set : {c.properties_set:,}")
        if not (c.relationships_created or c.properties_set):
            print("  (already exists — no changes)")

        # ── Read: summary counts ──────────────────────────────────────────────
        print("\n  -- Summary")
        reviewers = s.run(
            "MATCH (:Author)-[:potential_reviewer_for]"
            "->(:Community {name: 'Database'}) RETURN count(*) AS n"
        ).single()["n"]
        gurus = s.run(
            "MATCH (:Author)-[:guru_of]"
            "->(:Community {name: 'Database'}) RETURN count(*) AS n"
        ).single()["n"]
        print(f"  Potential reviewers : {reviewers:,}")
        print(f"  Gurus (≥ 2 papers)  : {gurus:,}")

        # ── Read: top-25 preview ──────────────────────────────────────────────
        print("\n  -- Top-25 reviewers by top-paper count")
        print(f"  {'Top papers':>10}  {'Status':<6}  Author")
        print("  " + "-" * 56)
        for row in s.run(READ_QUERY):
            print(f"  {row['top_papers']:>10,}  {row['status']:<6}  {row['author']}")

    driver.close()
    print("\n  Done.")


if __name__ == "__main__":
    main()
