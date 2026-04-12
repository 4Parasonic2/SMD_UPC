"""
D1 — PageRank on the Citation Network
======================================
Uses the Neo4j Graph Data Science (GDS) library to run PageRank on the
Paper→cites→Paper subgraph.

PageRank measures the *influence* of each paper in the citation network.
A paper is considered more important if it is cited by many other important
papers — this goes beyond simple citation counting (C3) by weighting
each incoming citation by the importance of the citing paper.

Domain interpretation:
  High PageRank → the paper is a central, influential work in the field.
  This mirrors how researchers judge seminal papers: they are cited by
  many subsequent works that are themselves highly cited.

Output shows papers sampled at different ranking percentiles (top-1 %,
top-10 %, top-20 %, top-50 %) so the score decay is visible.

Prerequisites:
  1. Neo4j GDS plugin must be installed and enabled.
  2. Parts A and B must have been run (Paper nodes and cites edges exist).
"""

from __future__ import annotations

import getpass
import math
import os

from neo4j import GraphDatabase

NEO4J_URI  = os.environ.get("NEO4J_URI",      "bolt://localhost:7687")
NEO4J_USER = os.environ.get("NEO4J_USERNAME", "neo4j")

GRAPH_NAME = "pagerank-citation-graph"

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


def _print_row(rank: int, total: int, record: dict) -> None:
    pct   = rank / total * 100
    title = (record["paper"] or "")[:50]
    year  = record["year"] or "?"
    print(f"  {rank:>6,} / {total:,}  ({pct:5.1f} %)  "
          f"{record['score']:>8.4f}  {year:>4}  {title}")


def run_algorithm(session) -> None:
    _safe_drop(session, GRAPH_NAME)

    # Step 1 — project the citation subgraph into GDS memory
    print("\n  -- Step 1: Project Paper-cites->Paper into GDS")
    proj = session.run(
        "CALL gds.graph.project($name, 'Paper', 'cites') "
        "YIELD graphName, nodeCount, relationshipCount",
        name=GRAPH_NAME,
    ).single()
    print(f"     Graph '{proj['graphName']}' created")
    print(f"     Nodes         : {proj['nodeCount']:,}")
    print(f"     Relationships : {proj['relationshipCount']:,}")

    # Step 2 — run PageRank, collect all results for percentile sampling
    print("\n  -- Step 2: Run PageRank — collecting all scores …")
    all_rows = []
    result = session.run(
        "CALL gds.pageRank.stream($name) "
        "YIELD nodeId, score "
        "RETURN gds.util.asNode(nodeId).title AS paper, "
        "       gds.util.asNode(nodeId).year  AS year, "
        "       score "
        "ORDER BY score DESC",
        name=GRAPH_NAME,
    )
    for record in result:
        all_rows.append({
            "paper": record["paper"],
            "year":  record["year"],
            "score": record["score"],
        })

    total = len(all_rows)
    print(f"     Total ranked papers : {total:,}")

    if total == 0:
        print("\n  ⚠  No results returned — check that Paper nodes and cites edges exist.")
    else:
        header = (f"  {'Rank':>14}  {'Pctile':>9}  {'Score':>8}  "
                  f"{'Year':>4}  Title")
        for label, lo, hi in PERCENTILE_SLICES:
            i_lo = max(0, math.floor(total * lo))
            i_hi = min(total, math.ceil(total * hi))
            slc  = all_rows[i_lo:i_hi][:3]
            if not slc:
                continue
            print(f"\n  ── {label} {'─' * 60}")
            print(header)
            print("  " + "-" * 80)
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
    print("  D1 — PageRank on the citation network")
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
