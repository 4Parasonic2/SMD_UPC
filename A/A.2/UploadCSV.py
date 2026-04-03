"""
UploadCSV.py — A.2 (Neo4j upload only)
======================================
Loads CSVs from Neo4j’s **import** directory into the database.

Startup:
  1. Reads ``pipeline_config.json`` (written by ``FormatCSV.py``) for LOAD CSV
     row limits. If missing, asks for the same data fraction and computes limits.
  2. Asks for the **Neo4j password** (or ``NEO4J_PASSWORD`` env var).
  3. Asks whether you have **copied** the CSV files into the import folder.
  4. Clears the graph and runs all ``LOAD CSV`` / ``MERGE`` steps.

Requirements:
    pip install neo4j
"""

import getpass
import json
import os
import sys

from neo4j import GraphDatabase

# =============================================================================
# CONFIGURATION
# =============================================================================

SCRIPT_DIR       = os.path.dirname(os.path.abspath(__file__))
CLEANED_FOLDER   = os.path.join(SCRIPT_DIR, "Cleaned_Csvs")
SYNTHETIC_FOLDER = os.path.join(SCRIPT_DIR, "Synthetic_data_Csvs")
PIPELINE_CONFIG  = os.path.join(SCRIPT_DIR, "pipeline_config.json")

NEO4J_URI        = os.environ.get("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USERNAME   = os.environ.get("NEO4J_USERNAME", "neo4j")
NEO4J_PASSWORD   = ""

NODE_BATCH   = 200
REL_BATCH    = 500
DELETE_BATCH = 5000

# Filled by ``_load_upload_limits()`` — must match the last ``FormatCSV.py`` run.
UPLOAD_NODE_LIMIT: int | None = None
UPLOAD_REL_LIMIT:  int | None = None

# Fallback scaling if ``pipeline_config.json`` is missing
_BASE_AUTHORS   = 4_200_000
_BASE_AUTHORED  = 28_000_000


def _row_limit_clause(limit: int | None) -> str:
    return f"WITH row LIMIT {limit}" if limit is not None else ""


def _load_upload_limits() -> None:
    """Set ``UPLOAD_NODE_LIMIT`` / ``UPLOAD_REL_LIMIT`` from config or user input."""
    global UPLOAD_NODE_LIMIT, UPLOAD_REL_LIMIT

    if os.path.isfile(PIPELINE_CONFIG):
        with open(PIPELINE_CONFIG, encoding="utf-8") as f:
            cfg = json.load(f)
        UPLOAD_NODE_LIMIT = cfg.get("upload_node_limit")
        UPLOAD_REL_LIMIT  = cfg.get("upload_rel_limit")
        frac = cfg.get("fraction", "?")
        print("=" * 60)
        print("  UploadCSV.py — using", PIPELINE_CONFIG)
        print(f"    fraction from FormatCSV run: {frac}")
        if UPLOAD_NODE_LIMIT is None:
            print("    DBLP row limits: none (full load)")
        else:
            print(f"    node file limit: {UPLOAD_NODE_LIMIT:,}")
            print(f"    rel  file limit: {UPLOAD_REL_LIMIT:,}")
        print("=" * 60)
        return

    print("=" * 60)
    print("  UploadCSV.py — no pipeline_config.json found")
    print("  (Run FormatCSV.py first, or enter the same fraction as that run.)")
    print("=" * 60)

    env_sf = os.environ.get("SYNTH_SAMPLE_FRACTION", "").strip()
    if env_sf:
        raw = env_sf
    else:
        print("\n  Data fraction [0.01 – 1.0, default 0.10] for LOAD CSV limits:")
        try:
            raw = input("  > ").strip()
        except (KeyboardInterrupt, EOFError):
            print("\n  Aborted.")
            sys.exit(0)

    fraction = 0.10
    if raw:
        try:
            fraction = float(raw)
        except ValueError:
            print(f"  [warning] invalid — using 0.10")
    fraction = max(0.01, min(1.0, fraction))

    if fraction >= 1.0:
        UPLOAD_NODE_LIMIT = None
        UPLOAD_REL_LIMIT  = None
    else:
        UPLOAD_NODE_LIMIT = max(1_000, int(_BASE_AUTHORS * fraction))
        UPLOAD_REL_LIMIT  = max(5_000, int(_BASE_AUTHORED * fraction))

    print(f"  Using fraction {fraction:.0%} → node limit {UPLOAD_NODE_LIMIT}, "
          f"rel limit {UPLOAD_REL_LIMIT}\n")


def _prompt_neo4j_password() -> None:
    global NEO4J_PASSWORD
    env_pw = os.environ.get("NEO4J_PASSWORD", "").strip()
    if env_pw:
        NEO4J_PASSWORD = env_pw
        print("  Neo4j password: (from NEO4J_PASSWORD env var)\n")
        return
    try:
        NEO4J_PASSWORD = getpass.getpass("  Neo4j password: ").strip()
    except (KeyboardInterrupt, EOFError):
        print("\n  Aborted.")
        sys.exit(0)
    if not NEO4J_PASSWORD:
        print("  [warning] Empty password — connection may fail.\n")


# =============================================================================
# PAUSE — copy files before upload
# =============================================================================

def _pause_before_upload() -> None:
    print("\n" + "=" * 60)
    print("ACTION REQUIRED BEFORE UPLOAD")
    print("=" * 60)

    n_clean = len([f for f in os.listdir(CLEANED_FOLDER)   if f.endswith(".csv")])
    n_synth = len([f for f in os.listdir(SYNTHETIC_FOLDER) if f.endswith(".csv")])

    print(f"""
  Steps 1 and 2 are complete.

  Files ready:
    Cleaned CSVs    ({n_clean} files) → {CLEANED_FOLDER}
    Synthetic CSVs  ({n_synth} files) → {SYNTHETIC_FOLDER}

  ─────────────────────────────────────────────────────────
  Copy ALL files to Neo4j's import directory, then come back.

  Find your import folder:
    Neo4j Desktop → "..." next to database → Open folder → Import

  Windows (PowerShell):
    Copy-Item "{CLEANED_FOLDER}\\*.csv"   "<import-dir>"
    Copy-Item "{SYNTHETIC_FOLDER}\\*.csv" "<import-dir>"

  Mac / Linux:
    cp "{CLEANED_FOLDER}"/*.csv   <import-dir>/
    cp "{SYNTHETIC_FOLDER}"/*.csv <import-dir>/
  ─────────────────────────────────────────────────────────
""")

    try:
        ans = input("  Files copied? Press Enter to upload, or type 'no' to exit: ").strip().lower()
    except KeyboardInterrupt:
        print("\n  Aborted.")
        sys.exit(0)

    if ans in ("no", "n", "q", "quit", "exit"):
        print("  Upload skipped — run again once files are copied.")
        sys.exit(0)

    print("  Proceeding with upload …\n")


# =============================================================================
# STEP 3 — UPLOAD TO NEO4J
# =============================================================================

def _run(session, description: str, query: str, fatal: bool = True) -> bool:
    """Run a Cypher query and print the result. Returns True on success."""
    try:
        summary = session.run(query).consume()
        c = summary.counters
        print(f"  ✓  {description}")
        if c.nodes_created:         print(f"       nodes created : {c.nodes_created:,}")
        if c.relationships_created: print(f"       rels  created : {c.relationships_created:,}")
        if c.nodes_deleted:         print(f"       nodes deleted : {c.nodes_deleted:,}")
        return True
    except Exception as e:
        print(f"  ✗  {description}\n     ERROR: {e}")
        if fatal:
            sys.exit(1)
        return False


def _clear_graph(session) -> None:
    print("\n  -- Clearing existing graph")

    total_nodes = session.run("MATCH (n) RETURN count(n) AS c").single()["c"]
    if total_nodes == 0:
        print("  ✓  Graph already empty — nothing to delete")
        return

    total_rels = session.run("MATCH ()-[r]->() RETURN count(r) AS c").single()["c"]
    print(f"  Found {total_nodes:,} nodes and {total_rels:,} relationships")

    if total_rels > 0:
        print(f"  Deleting relationships in batches of {DELETE_BATCH:,} …", end="", flush=True)
        iteration = 0
        while True:
            deleted = session.run(
                f"MATCH ()-[r]->() WITH r LIMIT {DELETE_BATCH} DELETE r "
                f"RETURN count(r) AS deleted"
            ).single()["deleted"]
            iteration += 1
            if deleted == 0:
                break
            if iteration % 10 == 0:
                print(".", end="", flush=True)
        print(f"  done  ({iteration} batches)")

    print(f"  Deleting nodes in batches of {DELETE_BATCH:,} …", end="", flush=True)
    iteration = 0
    while True:
        deleted = session.run(
            f"MATCH (n) WITH n LIMIT {DELETE_BATCH} DELETE n "
            f"RETURN count(n) AS deleted"
        ).single()["deleted"]
        iteration += 1
        if deleted == 0:
            break
        if iteration % 10 == 0:
            print(".", end="", flush=True)
    print(f"  done  ({iteration} batches)")
    print("  ✓  Graph cleared")


def run_step_upload() -> None:
    print("\n" + "=" * 60)
    print("STEP 3 — Upload to Neo4j")
    print("=" * 60)

    # Show active limits so it is always clear what will be loaded.
    if UPLOAD_NODE_LIMIT is None:
        print("  Row limits       : none (full dataset)")
    else:
        print(f"  Node row limit   : {UPLOAD_NODE_LIMIT:,}  per DBLP node file")
        print(f"  Rel  row limit   : {UPLOAD_REL_LIMIT:,}  per DBLP rel  file")
        print("  (Synthetic files are already pre-capped — no extra limit needed)")

    print(f"  Connecting to {NEO4J_URI} …")
    try:
        driver = GraphDatabase.driver(
            NEO4J_URI,
            auth=(NEO4J_USERNAME, NEO4J_PASSWORD),
            connection_timeout=120,
            max_transaction_retry_time=600,
        )
        driver.verify_connectivity()
        print("  ✓  Connected")
    except Exception as e:
        print(f"  ✗  Connection failed: {e}")
        print("     Is Neo4j running? Is the password correct?")
        sys.exit(1)

    # Convenience alias — reads the current module-level globals at call time.
    nl = _row_limit_clause(UPLOAD_NODE_LIMIT)   # e.g. "WITH row LIMIT 420000" or ""
    rl = _row_limit_clause(UPLOAD_REL_LIMIT)    # e.g. "WITH row LIMIT 2800000" or ""

    with driver.session() as s:

        # ── 0. Clear ─────────────────────────────────────────────────────────
        _clear_graph(s)

        # ── 1. Constraints ────────────────────────────────────────────────────
        print("\n  -- Constraints")
        for label, prop in [
            ("Author",  "authorId"),  ("Paper",   "paperId"),
            ("Venue",   "venueId"),   ("Issue",   "issueId"),
            ("Keyword", "keywordId"), ("Review",  "reviewId"),
        ]:
            _run(s, f"Constraint {label}.{prop}",
                 f"CREATE CONSTRAINT {label.lower()}_{prop.lower()} IF NOT EXISTS "
                 f"FOR (n:{label}) REQUIRE n.{prop} IS UNIQUE")

        # ── 2. Nodes ──────────────────────────────────────────────────────────
        print(f"\n  -- Nodes  (batch={NODE_BATCH})")

        # DBLP node files: apply UPLOAD_NODE_LIMIT via the 'nl' clause.
        _run(s, "Author nodes", f"""
            LOAD CSV WITH HEADERS
            FROM 'file:///output_author_clean.csv'
            AS row FIELDTERMINATOR ';'
            {nl}
            CALL {{
                WITH row
                WITH row WHERE row.`:ID` IS NOT NULL AND trim(row.`:ID`) <> ''
                MERGE (a:Author {{authorId: trim(row.`:ID`)}})
                  SET a.name = trim(row.author)
            }} IN TRANSACTIONS OF {NODE_BATCH} ROWS
        """)

        _run(s, "Paper nodes — inproceedings", f"""
            LOAD CSV WITH HEADERS
            FROM 'file:///output_inproceedings_clean.csv'
            AS row FIELDTERMINATOR ';'
            {nl}
            CALL {{
                WITH row
                WITH row WHERE row.inproceedings IS NOT NULL
                           AND trim(row.inproceedings) <> ''
                MERGE (p:Paper {{paperId: trim(row.inproceedings)}})
                  SET p.title = trim(row.title),
                      p.year  = toIntegerOrNull(row.year),
                      p.key   = trim(row.key),
                      p.doi   = row.ee
            }} IN TRANSACTIONS OF {NODE_BATCH} ROWS
        """)

        _run(s, "Paper nodes — articles", f"""
            LOAD CSV WITH HEADERS
            FROM 'file:///output_article_clean.csv'
            AS row FIELDTERMINATOR ';'
            {nl}
            CALL {{
                WITH row
                WITH row WHERE row.article IS NOT NULL AND trim(row.article) <> ''
                MERGE (p:Paper {{paperId: trim(row.article)}})
                  SET p.title = trim(row.title),
                      p.year  = toIntegerOrNull(row.year),
                      p.key   = trim(row.key),
                      p.doi   = row.ee
            }} IN TRANSACTIONS OF {NODE_BATCH} ROWS
        """)

        # Synthetic node files: already capped during Step 2 — no extra limit.
        _run(s, "Venue nodes", f"""
            LOAD CSV WITH HEADERS FROM 'file:///venue_nodes.csv'
            AS row FIELDTERMINATOR ';'
            CALL {{
                WITH row
                WITH row WHERE row.venueId IS NOT NULL
                MERGE (v:Venue {{venueId: trim(row.venueId)}})
                  SET v.name = trim(row.name), v.venueType = trim(row.venueType)
            }} IN TRANSACTIONS OF {NODE_BATCH} ROWS
        """)

        _run(s, "Issue nodes", f"""
            LOAD CSV WITH HEADERS FROM 'file:///issue_nodes.csv'
            AS row FIELDTERMINATOR ';'
            CALL {{
                WITH row
                WITH row WHERE row.issueId IS NOT NULL
                MERGE (i:Issue {{issueId: trim(row.issueId)}})
                  SET i.issueType = trim(row.issueType),
                      i.year      = toIntegerOrNull(row.year),
                      i.city      = trim(row.city),
                      i.volumeNo  = row.volumeNo
            }} IN TRANSACTIONS OF {NODE_BATCH} ROWS
        """)

        _run(s, "Keyword nodes", f"""
            LOAD CSV WITH HEADERS FROM 'file:///keyword_nodes.csv'
            AS row FIELDTERMINATOR ';'
            CALL {{
                WITH row
                WITH row WHERE row.keywordId IS NOT NULL
                MERGE (k:Keyword {{keywordId: trim(row.keywordId)}})
                  SET k.name = trim(row.name)
            }} IN TRANSACTIONS OF {NODE_BATCH} ROWS
        """)

        _run(s, "Review nodes", f"""
            LOAD CSV WITH HEADERS FROM 'file:///review_nodes.csv'
            AS row FIELDTERMINATOR ';'
            CALL {{
                WITH row
                WITH row WHERE row.reviewId IS NOT NULL
                MERGE (r:Review {{reviewId: trim(row.reviewId)}})
                  SET r.text = trim(row.text), r.decision = trim(row.decision)
            }} IN TRANSACTIONS OF {NODE_BATCH} ROWS
        """)

        # ── 3. Relationships ──────────────────────────────────────────────────
        print(f"\n  -- Relationships  (batch={REL_BATCH})")

        # DBLP relationship files: apply UPLOAD_REL_LIMIT via the 'rl' clause.
        _run(s, "authored  (Author → Paper)", f"""
            LOAD CSV WITH HEADERS
            FROM 'file:///output_author_authored_by_clean.csv'
            AS row FIELDTERMINATOR ';'
            {rl}
            CALL {{
                WITH row
                MATCH (a:Author {{authorId: trim(row.`:END_ID`)}})
                MATCH (p:Paper  {{paperId:  trim(row.`:START_ID`)}})
                MERGE (a)-[:authored]->(p)
            }} IN TRANSACTIONS OF {REL_BATCH} ROWS
        """)

        _run(s, "cites  (Paper → Paper)", f"""
            LOAD CSV WITH HEADERS
            FROM 'file:///output_cite_has_citation_clean.csv'
            AS row FIELDTERMINATOR ';'
            {rl}
            CALL {{
                WITH row
                MATCH (p1:Paper {{paperId: trim(row.`:START_ID`)}})
                MATCH (p2:Paper {{paperId: trim(row.`:END_ID`)}})
                MERGE (p1)-[:cites]->(p2)
            }} IN TRANSACTIONS OF {REL_BATCH} ROWS
        """)

        # Synthetic relationship files: no extra limit needed.
        _run(s, "published_in  (Paper → Issue)", f"""
            LOAD CSV WITH HEADERS FROM 'file:///rel_published_in.csv'
            AS row FIELDTERMINATOR ';'
            CALL {{
                WITH row
                MATCH (p:Paper {{paperId: trim(row.`:START_ID`)}})
                MATCH (i:Issue {{issueId: trim(row.`:END_ID`)}})
                MERGE (p)-[:published_in]->(i)
            }} IN TRANSACTIONS OF {REL_BATCH} ROWS
        """)

        _run(s, "of_venue  (Issue → Venue)", f"""
            LOAD CSV WITH HEADERS FROM 'file:///rel_of_venue.csv'
            AS row FIELDTERMINATOR ';'
            CALL {{
                WITH row
                MATCH (i:Issue {{issueId: trim(row.`:START_ID`)}})
                MATCH (v:Venue {{venueId: trim(row.`:END_ID`)}})
                MERGE (i)-[:of_venue]->(v)
            }} IN TRANSACTIONS OF {REL_BATCH} ROWS
        """)

        _run(s, "has_keyword  (Paper → Keyword)", f"""
            LOAD CSV WITH HEADERS FROM 'file:///rel_has_keyword.csv'
            AS row FIELDTERMINATOR ';'
            CALL {{
                WITH row
                MATCH (p:Paper   {{paperId:   trim(row.`:START_ID`)}})
                MATCH (k:Keyword {{keywordId: trim(row.`:END_ID`)}})
                MERGE (p)-[:has_keyword]->(k)
            }} IN TRANSACTIONS OF {REL_BATCH} ROWS
        """)

        _run(s, "wrote_review  (Author → Review)", f"""
            LOAD CSV WITH HEADERS FROM 'file:///rel_wrote_review.csv'
            AS row FIELDTERMINATOR ';'
            CALL {{
                WITH row
                MATCH (a:Author {{authorId: trim(row.`:START_ID`)}})
                MATCH (r:Review {{reviewId: trim(row.`:END_ID`)}})
                MERGE (a)-[wr:wrote_review]->(r)
                  SET wr.corresponding = (row.corresponding = 'true')
            }} IN TRANSACTIONS OF {REL_BATCH} ROWS
        """)

        _run(s, "about  (Review → Paper)", f"""
            LOAD CSV WITH HEADERS FROM 'file:///rel_about.csv'
            AS row FIELDTERMINATOR ';'
            CALL {{
                WITH row
                MATCH (r:Review {{reviewId: trim(row.`:START_ID`)}})
                MATCH (p:Paper  {{paperId:  trim(row.`:END_ID`)}})
                MERGE (r)-[:about]->(p)
            }} IN TRANSACTIONS OF {REL_BATCH} ROWS
        """)

        # ── 4. Verify ─────────────────────────────────────────────────────────
        print("\n  -- Verification")
        print("  Node counts:")
        for r in s.run("MATCH (n) RETURN labels(n)[0] AS l, count(n) AS c ORDER BY c DESC"):
            print(f"    {r['l']:20s} {r['c']:>10,}")
        print("  Relationship counts:")
        for r in s.run("MATCH ()-[r]->() RETURN type(r) AS t, count(r) AS c ORDER BY c DESC"):
            print(f"    {r['t']:25s} {r['c']:>10,}")

    driver.close()
    print("\n  Graph is ready.")


# =============================================================================
# MAIN
# =============================================================================

def main() -> None:
    _load_upload_limits()
    _prompt_neo4j_password()
    _pause_before_upload()
    run_step_upload()


if __name__ == "__main__":
    main()