"""
GenerateSynthetic.py
====================
Generates synthetic CSV files for all nodes and edges that are
missing from the DBLP export but required by the graph model.

The script is structured in clearly separated steps so you can
read, understand, and modify each part independently.

STEP 0  — Load real data from DBLP (papers, authors, authorship)
STEP 1  — Generate Venue nodes
STEP 2  — Generate Issue nodes  (one per venue+year group)
STEP 3  — Generate Keyword nodes
STEP 4  — Generate Review nodes (respecting conflict-of-interest rule)
STEP 5  — Generate all edge CSVs

Run AFTER FormatCSV.py has produced the cleaned CSVs.

Requirements:
    pip install pandas
"""

import os
import random
import pandas as pd

# ── Reproducibility ───────────────────────────────────────────────────────────
# Using a fixed seed means every run produces IDENTICAL output.
# This is important for academic work: your graph is reproducible.
random.seed(42)

# ── Folder paths ──────────────────────────────────────────────────────────────
# Paths are relative to THIS script's location, so the script works correctly
# regardless of where it is run from (workspace root, A.2 folder, etc.).
SCRIPT_DIR       = os.path.dirname(os.path.abspath(__file__))
CLEANED_FOLDER   = os.path.join(SCRIPT_DIR, "Cleaned_Csvs")
SYNTHETIC_FOLDER = os.path.join(SCRIPT_DIR, "Synthetic_data_Csvs")

os.makedirs(SYNTHETIC_FOLDER, exist_ok=True)


# =============================================================================
# UTILITY FUNCTIONS
# =============================================================================

def save_csv(data, filename):
    """
    Save a list of dicts (or a DataFrame) as a semicolon-delimited CSV file.
    Semicolons match the delimiter used by Clean_Csvs.py so all CSVs in the
    pipeline are consistent.
    Prints a confirmation so you can see exactly what was written.
    """
    df   = pd.DataFrame(data) if isinstance(data, list) else data
    path = os.path.join(SYNTHETIC_FOLDER, filename)
    df.to_csv(path, index=False, encoding="utf-8", sep=";")
    print(f"  saved  {filename:45s}  ({len(df)} rows)")
    return df


def load_clean(filename):
    """
    Load a semicolon-delimited cleaned DBLP CSV.

    DBLP exports split headers and data into two separate files:
      - <name>_header_clean.csv  — one row with Neo4j column type annotations
      - <name>_clean.csv         — data rows with NO header row

    If a companion header file exists this function applies those column names
    when reading the data file.  Otherwise it reads normally (the file already
    contains its own header row, e.g. output_author_clean.csv).

    Returns an empty DataFrame if the file is missing so the rest of the
    script continues gracefully.
    """
    path = os.path.join(CLEANED_FOLDER, filename)
    if not os.path.exists(path):
        print(f"  [warning] file not found: {filename}")
        return pd.DataFrame()

    header_filename = filename.replace("_clean.csv", "_header_clean.csv")
    header_path     = os.path.join(CLEANED_FOLDER, header_filename)

    if os.path.exists(header_path):
        try:
            # The header file contains exactly one row — column names with type
            # annotations such as  inproceedings:ID  or  booktitle:string .
            header_df = pd.read_csv(
                header_path, sep=";", dtype=str,
                encoding="utf-8", quotechar='"',
            )
            col_names = header_df.columns.tolist()
            return pd.read_csv(
                path, sep=";", dtype=str,
                header=None, names=col_names,
                on_bad_lines="skip", encoding="utf-8", quotechar='"',
            )
        except Exception as exc:
            print(f"  [warning] could not apply header from {header_filename}: {exc}")

    # No companion header file — the data file contains its own header row.
    return pd.read_csv(
        path, sep=";", dtype=str,
        on_bad_lines="skip", encoding="utf-8", quotechar='"',
    )


def find_column(df, candidates):
    """
    Return the first column name in df that matches any name in candidates.

    DBLP Neo4j export column names carry type annotations, e.g.:
        inproceedings:ID   booktitle:string   year:int   :START_ID

    Strategy (in order):
      1. Case-insensitive EXACT match
      2. Case-insensitive PARTIAL match (candidate is a substring of the column)

    Examples:
        find_column(df, [":id"])        matches  "inproceedings:ID"
        find_column(df, ["booktitle"])  matches  "booktitle:string"
        find_column(df, [":start_id"])  matches  ":START_ID"
    """
    cols_lower = {col: col.strip().lower() for col in df.columns}
    for candidate in candidates:
        cand_lower = candidate.lower()
        for col, col_l in cols_lower.items():
            if col_l == cand_lower:
                return col
    for candidate in candidates:
        cand_lower = candidate.lower()
        for col, col_l in cols_lower.items():
            if cand_lower in col_l:
                return col
    return None


# =============================================================================
# STEP 0 — LOAD REAL DATA FROM DBLP
# =============================================================================
# We need real paper IDs and author IDs so synthetic data can reference
# them with valid foreign keys.
# We also need the authorship map to enforce the conflict-of-interest rule.

print("\n" + "=" * 60)
print("STEP 0 — Loading real DBLP data")
print("=" * 60)

# Load papers (two types: conference papers and journal articles).
# The data files have NO header row; load_clean reads the companion header
# file automatically and applies the correct column names.
inproc_df  = load_clean("output_inproceedings_clean.csv")
article_df = load_clean("output_article_clean.csv")

# The primary key column is named  <label>:ID  (e.g. "inproceedings:ID").
# find_column with ":id" matches it via partial search.
inproc_key_col  = find_column(inproc_df,  [":id"])
article_key_col = find_column(article_df, [":id"])

paper_ids = []
if inproc_key_col:
    paper_ids += [p for p in inproc_df[inproc_key_col].dropna().str.strip() if p]
if article_key_col:
    paper_ids += [p for p in article_df[article_key_col].dropna().str.strip() if p]
paper_ids = list(dict.fromkeys(paper_ids))   # deduplicate, preserve order
print(f"  Papers loaded   : {len(paper_ids)}")

# Venue name per paper (from booktitle / journal column)
paper_venue_name  = {}
inproc_venue_col  = find_column(inproc_df,  ["booktitle", "venue"])
article_venue_col = find_column(article_df, ["journal", "booktitle", "venue"])

if inproc_key_col and inproc_venue_col:
    for _, row in inproc_df.iterrows():
        pid   = str(row.get(inproc_key_col,   "")).strip()
        vname = str(row.get(inproc_venue_col, "")).strip()
        if pid and vname and vname.lower() != "nan":
            paper_venue_name[pid] = vname

if article_key_col and article_venue_col:
    for _, row in article_df.iterrows():
        pid   = str(row.get(article_key_col,   "")).strip()
        vname = str(row.get(article_venue_col, "")).strip()
        if pid and vname and vname.lower() != "nan":
            paper_venue_name[pid] = vname

# Year per paper
paper_year       = {}
inproc_year_col  = find_column(inproc_df,  ["year"])
article_year_col = find_column(article_df, ["year"])

if inproc_key_col and inproc_year_col:
    for _, row in inproc_df.iterrows():
        pid = str(row.get(inproc_key_col,  "")).strip()
        yr  = str(row.get(inproc_year_col, "")).strip()
        if pid and yr and yr.lower() != "nan":
            paper_year[pid] = yr

if article_key_col and article_year_col:
    for _, row in article_df.iterrows():
        pid = str(row.get(article_key_col,  "")).strip()
        yr  = str(row.get(article_year_col, "")).strip()
        if pid and yr and yr.lower() != "nan":
            paper_year[pid] = yr

# Authors — load the numeric :ID column, NOT the name column.
# output_author_clean.csv has its own header row: ":ID"  "author:string"
author_df  = load_clean("output_author_clean.csv")
author_col = find_column(author_df, [":id"])
author_ids = []
if author_col:
    author_ids = [
        a for a in author_df[author_col].dropna().str.strip()
        if a and a.lower() not in ("nan", ":id")
    ]
print(f"  Authors loaded  : {len(author_ids)}")

# Authored-by map: paper_id (numeric) -> set of author_ids (numeric)
# output_author_authored_by_clean.csv has its own header: ":START_ID"  ":END_ID"
authored_df  = load_clean("output_author_authored_by_clean.csv")
ab_paper_col = find_column(authored_df, [":start_id"])
ab_auth_col  = find_column(authored_df, [":end_id"])

paper_to_authors = {}
if ab_paper_col and ab_auth_col:
    for _, row in authored_df.iterrows():
        pid = str(row.get(ab_paper_col, "")).strip()
        aid = str(row.get(ab_auth_col,  "")).strip()
        if pid and aid:
            paper_to_authors.setdefault(pid, set()).add(aid)

print(f"  Authorship map  : {len(paper_to_authors)} papers with known authors")


# =============================================================================
# STEP 1 — GENERATE VENUE NODES
# =============================================================================
# Extract every unique venue name from DBLP paper records,
# then classify each as Conference / Workshop / Journal by name keywords.
# This gives us real venue names without needing any extra data source.

print("\n" + "=" * 60)
print("STEP 1 — Venue nodes")
print("=" * 60)
print("  Extracting unique venue names from DBLP papers, then classifying.")

WORKSHOP_KEYWORDS = ["workshop", "ws ", "wksp", "seminar"]
JOURNAL_KEYWORDS  = ["journal", "transactions", "letters", "review", "magazine"]


def classify_venue_type(name):
    n = name.lower()
    if any(kw in n for kw in JOURNAL_KEYWORDS):
        return "Journal"
    if any(kw in n for kw in WORKSHOP_KEYWORDS):
        return "Workshop"
    return "Conference"


all_venue_names  = sorted(set(paper_venue_name.values()))
venue_rows       = []
venue_name_to_id = {}

for i, name in enumerate(all_venue_names):
    venue_id = f"VEN-{i:05d}"
    venue_name_to_id[name] = venue_id
    venue_rows.append({
        "venueId":   venue_id,
        "name":      name,
        "venueType": classify_venue_type(name),
    })

# Fallback if DBLP gave no venue names
if not venue_rows:
    print("  [fallback] No venues from DBLP — using hardcoded list")
    fallback = [
        ("The Web Conference",       "Conference"),
        ("VLDB",                     "Conference"),
        ("ISWC",                     "Conference"),
        ("KDD",                      "Conference"),
        ("Journal of Web Semantics", "Journal"),
        ("Semantic Web Journal",     "Journal"),
        ("ESWC Workshop on KG",      "Workshop"),
    ]
    for i, (name, vtype) in enumerate(fallback):
        vid = f"VEN-{i:05d}"
        venue_name_to_id[name] = vid
        venue_rows.append({"venueId": vid, "name": name, "venueType": vtype})

venue_df      = save_csv(venue_rows, "venue_nodes.csv")
venue_ids     = venue_df["venueId"].tolist()
venue_type_lk = dict(zip(venue_df["venueId"], venue_df["venueType"]))


# =============================================================================
# STEP 2 — GENERATE ISSUE NODES
# =============================================================================
# An Issue is either a conference edition (proceedings) or a journal volume.
#
# Papers from the SAME venue in the SAME year share ONE Issue — this is
# realistic (all ICDE 2020 papers belong to the same proceedings volume).
#
# Previously the script created one Issue per paper, which was incorrect
# and inflated the graph with thousands of single-paper proceedings.

print("\n" + "=" * 60)
print("STEP 2 — Issue nodes")
print("=" * 60)
print("  One Issue per (venue, year) group. Type = 'proceedings' or 'volume'.")

CITIES = [
    "Barcelona", "London", "Paris", "Berlin", "Amsterdam",
    "New York", "San Francisco", "Tokyo", "Sydney", "Toronto",
    "Vienna", "Zurich", "Stockholm", "Copenhagen", "Seoul",
    "Singapore", "Chicago", "Boston", "Montreal", "Brussels",
    "Madrid", "Rome", "Prague", "Warsaw", "Helsinki",
]

issue_rows           = []
paper_to_issue       = {}
issue_to_venue       = {}
venue_volume_counter = {}

# Key: (venue_id, year) → issue_id.  Papers with the same venue+year share
# a single Issue node instead of each getting their own.
issue_group_to_id: dict = {}

for paper_id in paper_ids:

    venue_name = paper_venue_name.get(paper_id, "")
    venue_id   = venue_name_to_id.get(venue_name) or random.choice(venue_ids)
    venue_type = venue_type_lk.get(venue_id, "Conference")
    is_journal = (venue_type == "Journal")
    year       = paper_year.get(paper_id, str(random.randint(2000, 2023)))

    group_key = (venue_id, year)

    if group_key not in issue_group_to_id:
        issue_id = f"ISS-{len(issue_group_to_id):06d}"
        issue_group_to_id[group_key] = issue_id
        issue_to_venue[issue_id] = venue_id

        if is_journal:
            venue_volume_counter[venue_id] = venue_volume_counter.get(venue_id, 0) + 1
            issue_rows.append({
                "issueId":   issue_id,
                "issueType": "volume",
                "year":      year,
                "city":      "",
                "volumeNo":  venue_volume_counter[venue_id],
            })
        else:
            issue_rows.append({
                "issueId":   issue_id,
                "issueType": "proceedings",
                "year":      year,
                "city":      random.choice(CITIES),
                "volumeNo":  "",
            })

    paper_to_issue[paper_id] = issue_group_to_id[group_key]

save_csv(issue_rows, "issue_nodes.csv")
print(f"  {len(paper_ids)} papers grouped into {len(issue_rows)} issues")


# =============================================================================
# STEP 3 — GENERATE KEYWORD NODES
# =============================================================================
# Completely synthetic: a curated list of 30 CS/data management keywords.
# Using a fixed list ensures the same keywords appear across multiple papers,
# which makes keyword-based queries actually return results.

print("\n" + "=" * 60)
print("STEP 3 — Keyword nodes")
print("=" * 60)
print("  Fixed curated list of 30 domain-relevant keywords.")

KEYWORD_NAMES = [
    "knowledge graph",     "ontology",              "RDF",
    "SPARQL",              "graph database",        "semantic web",
    "linked data",         "property graph",        "graph neural network",
    "knowledge representation",
    "data integration",    "schema matching",       "data quality",
    "data management",     "information extraction","entity linking",
    "relation extraction", "machine learning",      "deep learning",
    "natural language processing",
    "question answering",  "embedding",             "reasoning",
    "inference",           "graph processing",      "graph algorithms",
    "distributed systems", "query optimization",    "recommender systems",
    "knowledge base",
]

keyword_rows = [
    {"keywordId": f"KW-{i:04d}", "name": name}
    for i, name in enumerate(KEYWORD_NAMES)
]
keyword_df  = save_csv(keyword_rows, "keyword_nodes.csv")
keyword_ids = keyword_df["keywordId"].tolist()


# =============================================================================
# STEP 4 — GENERATE REVIEW NODES
# =============================================================================
# Most constrained step. Rules that MUST hold:
#   1. Each paper gets exactly 3 reviewers
#   2. No reviewer can be an author of the paper (conflict of interest)
#   3. Exactly 1 of the 3 has corresponding = true on the wrote_review edge

print("\n" + "=" * 60)
print("STEP 4 — Review nodes")
print("=" * 60)
print("  3 reviews per paper, conflict-of-interest safe.")
print("  Exactly 1 reviewer per paper marked as corresponding=true.")

DECISIONS = ["accept", "reject", "minor revision", "major revision"]

REVIEW_TEMPLATES = [
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

review_rows    = []
wrote_rev_rows = []
about_rows     = []
skipped        = 0

for paper_id in paper_ids:

    # Authors of this paper — they are EXCLUDED from reviewing it
    authors_of_paper = paper_to_authors.get(paper_id, set())

    # Eligible reviewers = all authors except this paper's own authors
    eligible = [a for a in author_ids if a not in authors_of_paper]

    if len(eligible) < 3:
        skipped += 1
        continue

    # Pick exactly 3 reviewers at random (no repetition within the same paper)
    selected = random.sample(eligible, 3)

    for rank, reviewer_id in enumerate(selected):

        review_id = f"REV-{len(review_rows):06d}"

        review_rows.append({
            "reviewId": review_id,
            "text":     random.choice(REVIEW_TEMPLATES),
            "decision": random.choice(DECISIONS),
        })

        # wrote_review edge: 'corresponding' lives on the EDGE (not the Review node)
        wrote_rev_rows.append({
            ":START_ID":     reviewer_id,
            ":END_ID":       review_id,
            "corresponding": "true" if rank == 0 else "false",
        })

        # about edge: Review -> Paper
        about_rows.append({
            ":START_ID": review_id,
            ":END_ID":   paper_id,
        })

print(f"  Papers skipped (too few eligible reviewers): {skipped}")
save_csv(review_rows, "review_nodes.csv")


# =============================================================================
# STEP 5 — GENERATE EDGE CSVs
# =============================================================================

print("\n" + "=" * 60)
print("STEP 5 — Edge CSVs")
print("=" * 60)

# published_in: Paper -> Issue  (many-to-one, built during Step 2)
save_csv(
    [{":START_ID": pid, ":END_ID": iid} for pid, iid in paper_to_issue.items()],
    "rel_published_in.csv"
)

# of_venue: Issue -> Venue  (1-to-1, built during Step 2)
save_csv(
    [{":START_ID": iid, ":END_ID": vid} for iid, vid in issue_to_venue.items()],
    "rel_of_venue.csv"
)

# has_keyword: Paper -> Keyword  (2-4 per paper, random)
has_kw_rows = []
for paper_id in paper_ids:
    for kw_id in random.sample(keyword_ids, k=random.randint(2, 4)):
        has_kw_rows.append({":START_ID": paper_id, ":END_ID": kw_id})
has_kw_df = pd.DataFrame(has_kw_rows).drop_duplicates()
save_csv(has_kw_df, "rel_has_keyword.csv")

# wrote_review and about (built in Step 4)
save_csv(wrote_rev_rows, "rel_wrote_review.csv")
save_csv(about_rows,     "rel_about.csv")


# =============================================================================
# SUMMARY
# =============================================================================

print("\n" + "=" * 60)
print("SUMMARY")
print("=" * 60)
print(f"  Venue nodes        : {len(venue_rows)}")
print(f"  Issue nodes        : {len(issue_rows)}  (from {len(paper_ids)} papers)")
print(f"  Keyword nodes      : {len(keyword_rows)}")
print(f"  Review nodes       : {len(review_rows)}")
print(f"  published_in edges : {len(paper_to_issue)}")
print(f"  of_venue edges     : {len(issue_to_venue)}")
print(f"  has_keyword edges  : {len(has_kw_df)}")
print(f"  wrote_review edges : {len(wrote_rev_rows)}")
print(f"  about edges        : {len(about_rows)}")
print(f"\nAll files written to: {SYNTHETIC_FOLDER}")
