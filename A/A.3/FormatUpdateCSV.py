"""
FormatUpdateCSV.py — A.3
========================
Extends the graph schema by adding / enriching:
  · Organization nodes  (universities / companies from DBLP school data)
  · affiliated_with     (Author → Organization)
  · Review enrichment   — adds text + decision to bare Review nodes created by A.2
  · Venue enrichment    — reviewerCount is computed directly in UploadUpdateCSV.py
                          via Cypher (no CSV required)

Source files:
  A.2/filteredoutcsvs/output_school.csv              — :ID ; school:string
  A.2/filteredoutcsvs/output_school_submitted_at.csv — :START_ID ; :END_ID
  A.2/Synthetic_data_Csvs/review_nodes.csv           — reviewId  (already loaded)

Output files written to Update_Csvs/ next to this script:
  organization_nodes.csv   — orgId ; name ; orgType
  rel_affiliated_with.csv  — :START_ID ; :END_ID
  review_updates.csv       — reviewId ; text ; decision

Copy all three files into Neo4j's import directory, then run UploadUpdateCSV.py.

Requirements:
    pip install pandas
"""

from __future__ import annotations

import csv
import os
import random
import re

import pandas as pd

# =============================================================================
# PATHS
# =============================================================================

SCRIPT_DIR  = os.path.dirname(os.path.abspath(__file__))
UPDATE_DIR  = os.path.join(SCRIPT_DIR, "Update_Csvs")

# Raw DBLP source files (A.2 filteredoutcsvs)
RAW_FOLDER        = os.path.join(SCRIPT_DIR, "..", "A.2", "filteredoutcsvs")
SCHOOL_FILE       = os.path.join(RAW_FOLDER, "output_school.csv")
SUBMITTED_AT_FILE = os.path.join(RAW_FOLDER, "output_school_submitted_at.csv")

# Cleaned DBLP CSVs produced by A.2 FormatCSV.py Step 1
# Used to cross-reference author IDs and authored-by relationships.
CLEANED_FOLDER      = os.path.join(SCRIPT_DIR, "..", "A.2", "Cleaned_Csvs")
AUTHOR_CLEAN_FILE   = os.path.join(CLEANED_FOLDER, "output_author_clean.csv")
AUTHORED_BY_FILE    = os.path.join(CLEANED_FOLDER, "output_author_authored_by_clean.csv")

# Synthetic data produced by A.2 FormatCSV.py (needed for Review enrichment)
SYNTH_FOLDER      = os.path.join(SCRIPT_DIR, "..", "A.2", "Synthetic_data_Csvs")
REVIEW_NODES_FILE = os.path.join(SYNTH_FOLDER, "review_nodes.csv")

# =============================================================================
# SAMPLING PARAMETERS  ← edit here to control how much of the DBLP affiliation
#                        data is loaded.  Lower values = much faster runtime.
# =============================================================================

# Fraction of the author and authored-by files to read (0 < x ≤ 1.0).
# 0.30 = every 3rd row ≈ 30 % of DBLP authors/papers loaded.
# Enough rows to guarantee connections between authors and organisations.
AFFIL_SAMPLE_FRACTION: float = 0.30

# Chunk size used when reading the (potentially huge) authored-by file.
_AUTHORED_CHUNK: int = 50_000

# Pre-computed step: read 1 row, skip (step-1) rows, repeat.
_SAMPLE_STEP: int = max(1, round(1.0 / AFFIL_SAMPLE_FRACTION))

random.seed(42)
os.makedirs(UPDATE_DIR, exist_ok=True)

# =============================================================================
# UTILITIES
# =============================================================================

def _clean(value: str) -> str:
    """Minimal field cleaning — strips whitespace, removes null bytes."""
    if not isinstance(value, str):
        return ""
    return value.replace("\x00", "").replace("\n", " ").replace("\r", " ").strip()


_UNIVERSITY_KEYWORDS = re.compile(
    r"universit|univ\b|college|institute|school|faculty|academy|politecn|"
    r"hochschule|ecole|universidad|universidade|università|université",
    re.IGNORECASE,
)

def _org_type(name: str) -> str:
    """Heuristic: classify as University or Company from the name."""
    return "University" if _UNIVERSITY_KEYWORDS.search(name) else "Company"


def _load_csv(path: str) -> pd.DataFrame:
    """Load a raw DBLP CSV (semicolon-delimited, UTF-8 with BOM tolerance)."""
    if not os.path.exists(path):
        raise FileNotFoundError(f"Expected file not found: {path}")
    return pd.read_csv(
        path,
        sep=";",
        dtype=str,
        encoding="utf-8-sig",
        on_bad_lines="skip",
    )


def _write_csv(filename: str, rows: list[dict], fieldnames: list[str]) -> None:
    path = os.path.join(UPDATE_DIR, filename)
    with open(path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(
            f, fieldnames=fieldnames, delimiter=";", quoting=csv.QUOTE_ALL
        )
        writer.writeheader()
        for row in rows:
            writer.writerow({k: row.get(k, "") for k in fieldnames})
    print(f"  wrote  {filename:40s}  ({len(rows):,} rows)")


# =============================================================================
# STEP 1 — Build Organization nodes
# =============================================================================

def build_organization_nodes() -> dict[str, str]:
    """
    Read output_school.csv, clean names, classify org type.
    Returns a mapping  school_id → orgId  (same value; :ID is already unique).
    Writes organization_nodes.csv.
    """
    print("\n  -- Organizations from output_school.csv")
    df = _load_csv(SCHOOL_FILE)

    # Locate columns regardless of type annotations (e.g. 'school:string')
    id_col   = next((c for c in df.columns if c.strip().lower() == ":id"), None)
    name_col = next(
        (c for c in df.columns if "school" in c.lower() and c.strip().lower() != ":id"),
        None,
    )

    if not id_col or not name_col:
        raise ValueError(
            f"Could not find :ID / school columns in {SCHOOL_FILE}.\n"
            f"  Found columns: {list(df.columns)}"
        )

    org_rows: list[dict] = []
    id_map: dict[str, str] = {}   # raw school :ID → orgId (kept identical)

    for _, row in df.iterrows():
        raw_id   = _clean(str(row[id_col]))
        raw_name = _clean(str(row[name_col]))
        if not raw_id or not raw_name or raw_name.lower() == "nan":
            continue
        otype = _org_type(raw_name)
        id_map[raw_id] = raw_id          # orgId == original school :ID
        org_rows.append({
            "orgId":   raw_id,
            "name":    raw_name,
            "orgType": otype,
        })

    _write_csv("organization_nodes.csv", org_rows, ["orgId", "name", "orgType"])
    univ  = sum(1 for r in org_rows if r["orgType"] == "University")
    comp  = len(org_rows) - univ
    print(f"  Universities : {univ:,}")
    print(f"  Companies    : {comp:,}")
    return id_map


# =============================================================================
# STEP 2 — Build affiliated_with edges
# =============================================================================

def _load_author_id_set() -> set[str]:
    """
    Return a sampled set of Author IDs from the cleaned DBLP author file.
    Reads every _SAMPLE_STEP-th data row (≈ AFFIL_SAMPLE_FRACTION of the file).
    """
    if not os.path.exists(AUTHOR_CLEAN_FILE):
        print(f"  [warning] {AUTHOR_CLEAN_FILE} not found — author ID check skipped.")
        return set()
    df = pd.read_csv(
        AUTHOR_CLEAN_FILE, sep=";", dtype=str, encoding="utf-8",
        on_bad_lines="skip",
        skiprows=lambda i: i > 0 and i % _SAMPLE_STEP != 0,
    )
    id_col = next((c for c in df.columns if c.strip().lower() == ":id"), None)
    if not id_col:
        return set()
    ids = df[id_col].dropna().str.strip()
    return {v for v in ids if v and v.lower() != "nan"}


def _load_paper_to_authors() -> dict[str, set[str]]:
    """
    Build a sampled mapping  paper_id → {author_id, ...}  from the cleaned
    authored-by file.  Reads the file in chunks of _AUTHORED_CHUNK rows and
    keeps every _SAMPLE_STEP-th row within each chunk (≈ AFFIL_SAMPLE_FRACTION).

    In the DBLP authored-by file: :START_ID = paper, :END_ID = author.
    """
    if not os.path.exists(AUTHORED_BY_FILE):
        print(f"  [warning] {AUTHORED_BY_FILE} not found — paper→author fallback disabled.")
        return {}

    kw = dict(sep=";", dtype=str, encoding="utf-8", on_bad_lines="skip",
              chunksize=_AUTHORED_CHUNK)
    mapping: dict[str, set[str]] = {}
    start_col = end_col = None

    for chunk in pd.read_csv(AUTHORED_BY_FILE, **kw):
        if start_col is None:
            start_col = next(
                (c for c in chunk.columns if c.strip().lower() == ":start_id"), None
            )
            end_col = next(
                (c for c in chunk.columns if c.strip().lower() == ":end_id"), None
            )
            if not start_col or not end_col:
                break

        sample = chunk.iloc[::_SAMPLE_STEP]
        mask   = sample[start_col].notna() & sample[end_col].notna()
        for pid, grp in sample[mask].groupby(start_col):
            pid = str(pid).strip()
            if pid not in mapping:
                mapping[pid] = set()
            mapping[pid].update(grp[end_col].str.strip())

    return mapping


def build_affiliated_with(id_map: dict[str, str]) -> None:
    """
    Read output_school_submitted_at.csv and produce rel_affiliated_with.csv.

    The DBLP submitted_at file may contain either author IDs or paper IDs as
    :START_ID depending on the export version.  This function handles both:

      · If :START_ID matches a known Author ID → use it directly.
      · If :START_ID matches a known Paper ID  → look up the paper's authors
        via the authored-by table and emit one edge per author.

    :END_ID is always the school/org ID (matched against id_map from Step 1).
    """
    print("\n  -- Affiliations from output_school_submitted_at.csv")
    print(f"  Sample fraction  : {AFFIL_SAMPLE_FRACTION:.0%}  "
          f"(every {_SAMPLE_STEP}th row — edit AFFIL_SAMPLE_FRACTION to change)")

    # ── Load reference tables for ID resolution ───────────────────────────────
    print("  Loading author ID set …")
    author_id_set = _load_author_id_set()
    print(f"  Known author IDs : {len(author_id_set):,}")

    print("  Loading paper → author map …")
    paper_to_authors = _load_paper_to_authors()
    print(f"  Papers in map    : {len(paper_to_authors):,}")

    # ── Read the submitted_at CSV (sampled) ──────────────────────────────────
    df = pd.read_csv(
        SUBMITTED_AT_FILE, sep=";", dtype=str, encoding="utf-8-sig",
        on_bad_lines="skip",
        skiprows=lambda i: i > 0 and i % _SAMPLE_STEP != 0,
    )

    start_col = next(
        (c for c in df.columns if c.strip().lower() == ":start_id"), None
    )
    end_col = next(
        (c for c in df.columns if c.strip().lower() == ":end_id"), None
    )

    if not start_col or not end_col:
        raise ValueError(
            f"Could not find :START_ID / :END_ID columns in {SUBMITTED_AT_FILE}.\n"
            f"  Found columns: {list(df.columns)}"
        )

    rel_rows: list[dict] = []
    skipped        = 0
    direct_hits    = 0
    resolved_hits  = 0

    for _, row in df.iterrows():
        start_id  = _clean(str(row[start_col]))
        school_id = _clean(str(row[end_col]))

        if not start_id or not school_id or school_id not in id_map:
            skipped += 1
            continue

        org_id = id_map[school_id]

        # ── Strategy 1: :START_ID is already an Author ID ─────────────────────
        if not author_id_set or start_id in author_id_set:
            rel_rows.append({":START_ID": start_id, ":END_ID": org_id})
            direct_hits += 1
            continue

        # ── Strategy 2: :START_ID is a Paper ID — resolve through authored_by ─
        authors = paper_to_authors.get(start_id)
        if authors:
            for aid in authors:
                rel_rows.append({":START_ID": aid, ":END_ID": org_id})
            resolved_hits += 1
            continue

        skipped += 1

    # ── Deduplicate (author, org) pairs ───────────────────────────────────────
    seen: set[tuple] = set()
    unique_rows: list[dict] = []
    for r in rel_rows:
        key = (r[":START_ID"], r[":END_ID"])
        if key not in seen:
            seen.add(key)
            unique_rows.append(r)

    _write_csv("rel_affiliated_with.csv", unique_rows, [":START_ID", ":END_ID"])
    print(f"  Direct author matches    : {direct_hits:,}")
    print(f"  Resolved via paper→author: {resolved_hits:,}")
    print(f"  Skipped (unresolved)     : {skipped:,}")
    print(f"  Unique edges written     : {len(unique_rows):,}")


# =============================================================================
# STEP 3 — Enrich Review nodes  (add text + decision)
# =============================================================================

_REVIEW_TEMPLATES = [
    "Solid contribution with a clear and well-structured methodology.",
    "Interesting approach but the evaluation needs additional baselines.",
    "Well-written and thorough. The experiments convincingly support the claims.",
    "The related work section omits several key recent publications.",
    "Novel method, but scalability concerns are not sufficiently addressed.",
    "Strong theoretical foundation paired with good empirical results.",
    "The motivation is clear, but the novelty over existing methods is limited.",
    "Good paper overall. Minor revisions needed in the introduction.",
    "The experimental setup is not described in sufficient detail for reproducibility.",
    "Excellent contribution. Ready for publication with only minor edits.",
    "The paper tackles an important problem but the solution is over-simplified.",
    "Comprehensive study with thorough analysis. Recommended for acceptance.",
]

_DECISIONS = ["accept", "reject", "minor revision", "major revision"]


def build_review_updates() -> None:
    """
    Read the existing review_nodes.csv produced by A.2 FormatCSV.py and
    assign a synthetic text and decision to every review.
    Writes review_updates.csv.
    """
    print("\n  -- Review enrichment from Synthetic_data_Csvs/review_nodes.csv")

    if not os.path.exists(REVIEW_NODES_FILE):
        print(f"  [warning] {REVIEW_NODES_FILE} not found — skipping review enrichment.")
        print("  Run A.2/FormatCSV.py first to generate review_nodes.csv.")
        return

    df = pd.read_csv(REVIEW_NODES_FILE, sep=";", dtype=str, encoding="utf-8")
    id_col = next((c for c in df.columns if c.strip().lower() == "reviewid"), None)
    if not id_col:
        print(f"  [warning] 'reviewId' column not found in {REVIEW_NODES_FILE} — skipping.")
        return

    review_ids = df[id_col].dropna().str.strip().tolist()
    review_ids = [r for r in review_ids if r and r.lower() != "nan"]

    rows = [
        {
            "reviewId": rid,
            "text":     random.choice(_REVIEW_TEMPLATES),
            "decision": random.choice(_DECISIONS),
        }
        for rid in review_ids
    ]

    _write_csv("review_updates.csv", rows, ["reviewId", "text", "decision"])
    print(f"  Reviews enriched : {len(rows):,}")


# =============================================================================
# MAIN
# =============================================================================

def main() -> None:
    print("=" * 60)
    print("  FormatUpdateCSV.py — A.3")
    print("  (Organization + affiliated_with + Review enrichment)")
    print("=" * 60)
    print(f"  DBLP source folder  : {RAW_FOLDER}")
    print(f"  Synth source folder : {SYNTH_FOLDER}")
    print(f"  Output folder       : {UPDATE_DIR}")

    id_map = build_organization_nodes()
    build_affiliated_with(id_map)
    build_review_updates()

    print(f"""
  Done.  Copy these files into Neo4j's import directory:
    {os.path.join(UPDATE_DIR, 'organization_nodes.csv')}
    {os.path.join(UPDATE_DIR, 'rel_affiliated_with.csv')}
    {os.path.join(UPDATE_DIR, 'review_updates.csv')}

  Then run:  python UploadUpdateCSV.py
  (reviewerCount on Venue nodes is computed directly in Cypher — no extra CSV.)
""")


if __name__ == "__main__":
    main()
