"""
FormatUpdateCSV.py — A.3
========================
Extends the graph schema by adding:
  · Organization nodes  (universities / companies from DBLP school data)
  · affiliated_with     (Author → Organization)

Source files (read from A.2/filteredoutcsvs):
  output_school.csv              — :ID ; school:string
  output_school_submitted_at.csv — :START_ID (author) ; :END_ID (school)

Output files written to Update_Csvs/ next to this script:
  organization_nodes.csv   — orgId ; name ; orgType
  rel_affiliated_with.csv  — :START_ID ; :END_ID

Copy these two files into Neo4j's import directory, then run UploadUpdateCSV.py.

Requirements:
    pip install pandas
"""

from __future__ import annotations

import csv
import os
import re

import pandas as pd

# =============================================================================
# PATHS
# =============================================================================

SCRIPT_DIR  = os.path.dirname(os.path.abspath(__file__))
UPDATE_DIR  = os.path.join(SCRIPT_DIR, "Update_Csvs")

# Raw source files live in the A.2 filteredoutcsvs folder.
RAW_FOLDER  = os.path.join(SCRIPT_DIR, "..", "A.2", "filteredoutcsvs")
SCHOOL_FILE          = os.path.join(RAW_FOLDER, "output_school.csv")
SUBMITTED_AT_FILE    = os.path.join(RAW_FOLDER, "output_school_submitted_at.csv")

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

def build_affiliated_with(id_map: dict[str, str]) -> None:
    """
    Read output_school_submitted_at.csv.
    :START_ID = author numeric ID  (matches Author.authorId in the graph)
    :END_ID   = school numeric ID  (matches orgId from Step 1)
    Writes rel_affiliated_with.csv.
    """
    print("\n  -- Affiliations from output_school_submitted_at.csv")
    df = _load_csv(SUBMITTED_AT_FILE)

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
    skipped = 0
    for _, row in df.iterrows():
        author_id = _clean(str(row[start_col]))
        school_id = _clean(str(row[end_col]))
        if not author_id or not school_id:
            skipped += 1
            continue
        if school_id not in id_map:
            skipped += 1          # school not in organization_nodes.csv
            continue
        rel_rows.append({":START_ID": author_id, ":END_ID": school_id})

    # Remove duplicate (author, org) pairs before writing
    seen: set[tuple] = set()
    unique_rows = []
    for r in rel_rows:
        key = (r[":START_ID"], r[":END_ID"])
        if key not in seen:
            seen.add(key)
            unique_rows.append(r)

    _write_csv("rel_affiliated_with.csv", unique_rows, [":START_ID", ":END_ID"])
    print(f"  Skipped (missing IDs) : {skipped:,}")


# =============================================================================
# MAIN
# =============================================================================

def main() -> None:
    print("=" * 60)
    print("  FormatUpdateCSV.py — A.3  (Organization + affiliated_with)")
    print("=" * 60)
    print(f"  Source folder : {RAW_FOLDER}")
    print(f"  Output folder : {UPDATE_DIR}")

    id_map = build_organization_nodes()
    build_affiliated_with(id_map)

    print(f"""
  Done.  Copy these files into Neo4j's import directory:
    {os.path.join(UPDATE_DIR, 'organization_nodes.csv')}
    {os.path.join(UPDATE_DIR, 'rel_affiliated_with.csv')}

  Then run:  python UploadUpdateCSV.py
""")


if __name__ == "__main__":
    main()
