"""
D2 — Node Similarity on the Author–Paper Bipartite Graph
=========================================================
Uses the Neo4j Graph Data Science (GDS) library to compute Jaccard Node
Similarity between Author nodes, based on which Paper nodes they have
authored.

Node Similarity compares every pair of authors by looking at the *overlap*
in the set of papers they authored.  The Jaccard coefficient is:

    similarity(A, B) = |papers(A) ∩ papers(B)| / |papers(A) ∪ papers(B)|

A score of 1.0 means two authors co-authored exactly the same papers;
0.0 means they share no papers at all.

Domain interpretation:
  High similarity → two authors frequently collaborate or work on the
  same research problems.  This is useful for:
  • Recommending potential collaborators to a researcher.
  • Detecting conflict-of-interest when assigning peer reviewers (two
    very similar authors should not review each other's work — ties
    directly into the C4 reviewer identification logic).
  • Mapping research clusters based on co-authorship patterns.

Output shows author pairs sampled at different ranking percentiles
(top-1 %, top-10 %, top-20 %, top-50 %) so the similarity decay is visible.

Prerequisites:
  1. Neo4j GDS plugin must be installed and enabled.
  2. Parts A and B must have been run (Author, Paper, authored edges).
"""

from __future__ import annotations

import getpass
import math
import os

from neo4j import GraphDatabase

NEO4J_URI  = os.environ.get("NEO4J_URI",      "bolt://localhost:7687")
NEO4J_USER = os.environ.get("NEO4J_USERNAME", "neo4j")

GRAPH_NAME = "author-similarity-graph"

PERCENTILE_SLICES = [
    ("Top 1 %",  0.00, 0.01),
    ("Top 10 %", 0.09, 0.11),
    ("Top 20 %", 0.19, 0.21),
    ("Top 50 %", 0.49, 0.51),
]


def _safe_drop(session, name: str) -> None:
    """Drop a GDS graph projection if it exists, ignore errors."""
    try:
        result = session.run(
            "CALL gds.graph.exists($name) YIELD exists RETURN exists",
            name=name,
        ).single()
        if result and result["exists"]:
            session.run("CALL gds.graph.drop($name)", name=name).consume()
            print(f"     Dropped leftover projection '{name}'")
    except Exception as e:
        print(f"     (cleanup note: {e})")


def _print_row(rank: int, total: int, rec: dict) -> None:
    pct = rank / total * 100
    a1  = (rec["author1"] or "?")[:25]
    a2  = (rec["author2"] or "?")[:25]
    print(f"  {rank:>6,} / {total:,}  ({pct:5.1f} %)  "
          f"{rec['similarity']:>10.4f}   {a1:<25} ↔ {a2}")


def run_algorithm(session) -> None:
    _safe_drop(session, GRAPH_NAME)

    # Step 1 — project the bipartite Author→authored→Paper graph
    print("\n  -- Step 1: Project Author-authored->Paper into GDS")
    proj = session.run(
        "CALL gds.graph.project("
        "  $name, "
        "  ['Author', 'Paper'], "
        "  {authored: {type: 'authored', orientation: 'NATURAL'}}"
        ") YIELD graphName, nodeCount, relationshipCount",
        name=GRAPH_NAME,
    ).single()
    print(f"     Graph '{proj['graphName']}' created")
    print(f"     Nodes         : {proj['nodeCount']:,}")
    print(f"     Relationships : {proj['relationshipCount']:,}")

    # Step 2 — compute Node Similarity, collect all for percentile sampling
    print("\n  -- Step 2: Run Node Similarity — collecting all pairs …")
    all_rows = []
    result = session.run(
        "CALL gds.nodeSimilarity.stream($name) "
        "YIELD node1, node2, similarity "
        "RETURN gds.util.asNode(node1).name AS author1, "
        "       gds.util.asNode(node2).name AS author2, "
        "       similarity "
        "ORDER BY similarity DESC, author1, author2",
        name=GRAPH_NAME,
    )
    for record in result:
        all_rows.append({
            "author1":    record["author1"],
            "author2":    record["author2"],
            "similarity": record["similarity"],
        })

    total = len(all_rows)
    print(f"     Total similar pairs : {total:,}")

    if total == 0:
        print("\n  ⚠  No results returned — check that Author nodes and authored edges exist.")
    else:
        header = (f"  {'Rank':>14}  {'Pctile':>9}  {'Similarity':>10}   "
                  f"{'Author 1':<25} ↔ Author 2")
        for label, lo, hi in PERCENTILE_SLICES:
            i_lo = max(0, math.floor(total * lo))
            i_hi = min(total, math.ceil(total * hi))
            slc  = all_rows[i_lo:i_hi][:3]
            if not slc:
                continue
            print(f"\n  ── {label} {'─' * 60}")
            print(header)
            print("  " + "-" * 85)
            for offset, rec in enumerate(slc):
                _print_row(i_lo + offset + 1, total, rec)

    # Step 3 — drop the in-memory projection
    session.run("CALL gds.graph.drop($name)", name=GRAPH_NAME).consume()
    print("\n  -- Projection dropped.")


def _get_password() -> str:
    env = os.environ.get("NEO4J_PASSWORD", "").strip()
    if env:
        return env
    return getpass.getpass("  Neo4j password: ").strip()


def main() -> None:
    print("=" * 60)
    print("  D2 — Node Similarity (Author co-authorship)")
    print("=" * 60)

    pw = _get_password()
    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, pw),
                                  connection_timeout=120)
    try:
        with driver.session() as session:
            run_algorithm(session)
    except Exception as e:
        print(f"\n  ERROR: {e}")
    finally:
        driver.close()

    print("\n  Done.")


if __name__ == "__main__":
    main()
