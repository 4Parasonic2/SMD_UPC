"""
FormatCSV.py — A.2 (generate CSVs only)
========================================
  STEP 1 · Clean CSVs     — normalises raw DBLP CSVs
  STEP 2 · Synthetic data — Venues, Issues, Keywords, Reviews + edge CSVs

Writes ``pipeline_config.json`` in this folder so ``UploadCSV.py`` can apply the
same data fraction when loading into Neo4j (row limits on large DBLP files).

At startup:
  • Data fraction only (e.g. 0.05 = 5 % test, 1.0 = full)

Requirements:
    pip install pandas
"""

import csv
import json
import os
import re
import random
import sys

import pandas as pd

# =============================================================================
# CONFIGURATION
# =============================================================================

SCRIPT_DIR       = os.path.dirname(os.path.abspath(__file__))
RAW_FOLDER       = os.path.join(SCRIPT_DIR, "filteredoutcsvs")
CLEANED_FOLDER   = os.path.join(SCRIPT_DIR, "Cleaned_Csvs")
SYNTHETIC_FOLDER = os.path.join(SCRIPT_DIR, "Synthetic_data_Csvs")

os.makedirs(CLEANED_FOLDER,   exist_ok=True)
os.makedirs(SYNTHETIC_FOLDER, exist_ok=True)

PIPELINE_CONFIG = os.path.join(SCRIPT_DIR, "pipeline_config.json")

# ── Sampling — written by _prompt_format_settings() ──────────────────────────
SYNTH_SAMPLE_FRACTION: float = 0.10
SYNTH_MAX_PAPERS:      int   = 80_000
SYNTH_MAX_AUTHORS:     int   = 120_000

# Full-dataset row estimates used for scaling.
_BASE_AUTHORS     = 4_200_000
_BASE_AUTHORED    = 28_000_000
_BASE_MAX_PAPERS  = 800_000
_BASE_MAX_AUTHORS = 1_200_000

random.seed(42)


# =============================================================================
# STARTUP — data fraction only (no Neo4j)
# =============================================================================

def _prompt_format_settings() -> None:
    """Ask for the data fraction; set synth globals and write ``pipeline_config.json``."""
    global SYNTH_SAMPLE_FRACTION, SYNTH_MAX_PAPERS, SYNTH_MAX_AUTHORS

    print("=" * 60)
    print("  FormatCSV.py — data sampling")
    print("=" * 60)

    env_sf = os.environ.get("SYNTH_SAMPLE_FRACTION", "").strip()
    if env_sf:
        raw, source = env_sf, "env var SYNTH_SAMPLE_FRACTION"
    else:
        print()
        print("  Data fraction controls sampling and caps for CSV generation")
        print("  and the matching LOAD CSV row limits for UploadCSV.py.")
        print("  Examples:  0.05 = 5 %   0.25 = 25 %   1.0 = full")
        try:
            raw = input("  Data fraction [0.01 – 1.0, default 0.10] : ").strip()
        except (KeyboardInterrupt, EOFError):
            print("\n  Aborted.")
            sys.exit(0)
        source = "user input"

    fraction = 0.10
    if raw:
        try:
            fraction = float(raw)
        except ValueError:
            print(f"  [warning] '{raw}' is not a number — using 0.10")
    fraction = max(0.01, min(1.0, fraction))

    SYNTH_SAMPLE_FRACTION = fraction
    SYNTH_MAX_PAPERS      = max(1_000, int(_BASE_MAX_PAPERS  * fraction))
    SYNTH_MAX_AUTHORS     = max(1_000, int(_BASE_MAX_AUTHORS * fraction))

    if fraction >= 1.0:
        upload_node_limit = None
        upload_rel_limit  = None
    else:
        upload_node_limit = max(1_000, int(_BASE_AUTHORS * fraction))
        upload_rel_limit  = max(5_000, int(_BASE_AUTHORED * fraction))

    print(f"\n  Confirmed ({source}):")
    print(f"    Data fraction    : {fraction:.0%}")
    print(f"    Synth papers cap : {SYNTH_MAX_PAPERS:>10,}")
    print(f"    Synth author cap : {SYNTH_MAX_AUTHORS:>10,}")
    if upload_node_limit is None:
        print("    Upload row limits: full DBLP (no cap)")
    else:
        print(f"    Upload node rows : {upload_node_limit:>10,}  per DBLP node file")
        print(f"    Upload rel  rows : {upload_rel_limit:>10,}  per DBLP rel  file")

    cfg = {
        "fraction":           fraction,
        "synth_max_papers":   SYNTH_MAX_PAPERS,
        "synth_max_authors":  SYNTH_MAX_AUTHORS,
        "upload_node_limit":  upload_node_limit,
        "upload_rel_limit":   upload_rel_limit,
    }
    with open(PIPELINE_CONFIG, "w", encoding="utf-8") as f:
        json.dump(cfg, f, indent=2)
    print(f"\n  Saved {PIPELINE_CONFIG}")
    print("  (UploadCSV.py reads this so the graph load matches this run.)\n")


# =============================================================================
# UTILITIES
# =============================================================================

def folder_has_csvs(folder: str) -> bool:
    if not os.path.isdir(folder):
        return False
    return any(f.lower().endswith(".csv") for f in os.listdir(folder))


# =============================================================================
# STEP 1 — CLEAN CSVs
# =============================================================================

_TYPE_SUFFIX = re.compile(
    r':(?:ID|string(?:\[\])?|int|long|float|double|boolean|date(?:time)?)$',
    re.IGNORECASE,
)

def _strip_type(col: str) -> str:
    if col.startswith(":"):
        return col
    return _TYPE_SUFFIX.sub("", col)

def _clean_field(field: str) -> str:
    if field is None:
        return ""
    field = field.replace("\n", " ").replace("\r", " ")
    field = field.replace("\x00", "")
    field = field.strip()
    field = field.replace('"', "'")
    field = field.replace("|", ",")
    return field

def _clean_file(input_path: str, output_path: str,
                header_path: str | None = None) -> None:
    with open(output_path, "w", encoding="utf-8", newline="") as outfile:
        writer = csv.writer(outfile, delimiter=";", quotechar='"',
                            quoting=csv.QUOTE_ALL)
        if header_path and os.path.exists(header_path):
            with open(header_path, "r", encoding="utf-8-sig", errors="replace") as hf:
                for hrow in csv.reader(hf, delimiter=";", quotechar='"'):
                    writer.writerow([_strip_type(_clean_field(c)) for c in hrow])
                    break
            with open(input_path, "r", encoding="utf-8-sig", errors="replace") as infile:
                for row in csv.reader(infile, delimiter=";", quotechar='"'):
                    writer.writerow([_clean_field(f) for f in row])
        else:
            with open(input_path, "r", encoding="utf-8-sig", errors="replace") as infile:
                first = True
                for row in csv.reader(infile, delimiter=";", quotechar='"'):
                    if first:
                        writer.writerow([_strip_type(_clean_field(c)) for c in row])
                        first = False
                    else:
                        writer.writerow([_clean_field(f) for f in row])

def _cleaned_files_have_headers() -> bool:
    sentinel = os.path.join(CLEANED_FOLDER, "output_inproceedings_clean.csv")
    if not os.path.exists(sentinel):
        return False
    try:
        with open(sentinel, "r", encoding="utf-8", errors="replace") as f:
            return "inproceedings" in f.readline().lower()
    except Exception:
        return False

def run_step_clean() -> None:
    print("\n" + "=" * 60)
    print("STEP 1 — Clean CSVs")
    print("=" * 60)

    if folder_has_csvs(CLEANED_FOLDER) and _cleaned_files_have_headers():
        print(f"  [skip] {CLEANED_FOLDER} already up-to-date.")
        return

    all_csv         = sorted(f for f in os.listdir(RAW_FOLDER) if f.lower().endswith(".csv"))
    header_file_set = {f for f in all_csv if "_header." in f.lower()}
    data_files      = [f for f in all_csv if f not in header_file_set]
    processed       = 0

    for filename in data_files:
        companion_path = os.path.join(RAW_FOLDER, filename.replace(".csv", "_header.csv"))
        has_companion  = os.path.exists(companion_path)
        output_path    = os.path.join(CLEANED_FOLDER, filename.replace(".csv", "_clean.csv"))
        print(f"  {filename}" + (" (+ header)" if has_companion else ""))
        _clean_file(os.path.join(RAW_FOLDER, filename), output_path,
                    header_path=companion_path if has_companion else None)
        processed += 1

    for filename in sorted(header_file_set):
        output_path = os.path.join(CLEANED_FOLDER, filename.replace(".csv", "_clean.csv"))
        print(f"  {filename} [header-only copy]")
        _clean_file(os.path.join(RAW_FOLDER, filename), output_path)
        processed += 1

    print(f"\n  {processed} files → {CLEANED_FOLDER}")


# =============================================================================
# STEP 2 — SYNTHETIC DATA
# =============================================================================

def _save_synth(data, filename: str) -> pd.DataFrame:
    df   = pd.DataFrame(data) if isinstance(data, list) else data
    path = os.path.join(SYNTHETIC_FOLDER, filename)
    df.to_csv(path, index=False, encoding="utf-8", sep=";")
    print(f"  saved  {filename:45s}  ({len(df)} rows)")
    return df

_LOAD_CHUNK = 50_000

def _load_clean(filename: str, sample_fraction: float = 1.0) -> pd.DataFrame:
    path = os.path.join(CLEANED_FOLDER, filename)
    if not os.path.exists(path):
        print(f"  [warning] not found: {filename}")
        return pd.DataFrame()
    kw   = dict(sep=";", dtype=str, on_bad_lines="skip",
                encoding="utf-8", quotechar='"')
    step = max(1, int(round(1.0 / sample_fraction))) if sample_fraction < 1 else 1
    if step <= 1:
        return pd.read_csv(path, **kw)
    chunks = []
    for chunk in pd.read_csv(path, chunksize=_LOAD_CHUNK, **kw):
        chunks.append(chunk.iloc[::step])
    return pd.concat(chunks, ignore_index=True) if chunks else pd.DataFrame()

def _find_col(df: pd.DataFrame, candidates: list) -> str | None:
    lower = {c: c.strip().lower() for c in df.columns}
    for cand in candidates:
        c = cand.lower()
        for col, col_l in lower.items():
            if col_l == c:
                return col
    for cand in candidates:
        c = cand.lower()
        for col, col_l in lower.items():
            if c in col_l:
                return col
    return None

def run_step_synth() -> None:
    print("\n" + "=" * 60)
    print("STEP 2 — Synthetic data")
    print("=" * 60)

    def _row_count(fname):
        p = os.path.join(SYNTHETIC_FOLDER, fname)
        if not os.path.exists(p):
            return 0
        try:
            return sum(1 for _ in open(p, encoding="utf-8")) - 1
        except Exception:
            return 0

    if folder_has_csvs(SYNTHETIC_FOLDER):
        if _row_count("venue_nodes.csv") > 0 and _row_count("review_nodes.csv") > 0:
            print(f"  [skip] {SYNTHETIC_FOLDER} already populated.")
            return
        print("  [re-generate] Incomplete synthetic files — regenerating.")

    sf = SYNTH_SAMPLE_FRACTION
    print(f"  Sample fraction  : {sf:.2f}")
    print(f"  Max papers       : {SYNTH_MAX_PAPERS:,}")
    print(f"  Max authors      : {SYNTH_MAX_AUTHORS:,}")

    # ── 2.0 Load real DBLP data ──────────────────────────────────────────────
    print("\n  -- 2.0  Loading real DBLP data")

    article_df   = _load_clean("output_article_clean.csv", sf)
    art_id_col   = _find_col(article_df, ["article", ":id"])
    art_year_col = _find_col(article_df, ["year"])
    art_vol_col  = _find_col(article_df, ["volume"])

    inproc_df        = _load_clean("output_inproceedings_clean.csv", sf)
    inproc_id_col    = _find_col(inproc_df, ["inproceedings", ":id"])
    inproc_cross_col = _find_col(inproc_df, ["crossref"])

    proceedings_df = _load_clean("output_proceedings_clean.csv", sf)
    proc_id_col    = _find_col(proceedings_df, ["proceedings", ":id"])
    proc_bt_col    = _find_col(proceedings_df, ["booktitle"])
    proc_key_col   = _find_col(proceedings_df, ["key"])
    proc_year_col  = _find_col(proceedings_df, ["year"])
    proc_addr_col  = _find_col(proceedings_df, ["address"])

    journal_df = _load_clean("output_journal_clean.csv")
    j_id_col   = _find_col(journal_df, [":id"])
    j_name_col = _find_col(journal_df, ["journal"])

    art_jnl_df   = _load_clean("output_journal_published_in_clean.csv")
    aj_start_col = _find_col(art_jnl_df, [":start_id"])
    aj_end_col   = _find_col(art_jnl_df, [":end_id"])

    author_df  = _load_clean("output_author_clean.csv", sf)
    author_col = _find_col(author_df, [":id"])

    authored_df  = _load_clean("output_author_authored_by_clean.csv", sf)
    ab_paper_col = _find_col(authored_df, [":start_id"])
    ab_auth_col  = _find_col(authored_df, [":end_id"])

    article_ids = [p for p in (article_df[art_id_col].dropna().str.strip() if art_id_col else []) if p]
    inproc_ids  = [p for p in (inproc_df[inproc_id_col].dropna().str.strip() if inproc_id_col else []) if p]
    article_id_set = set(article_ids)
    paper_ids      = list(dict.fromkeys(article_ids + inproc_ids))
    loaded_papers  = len(paper_ids)
    if SYNTH_MAX_PAPERS > 0 and len(paper_ids) > SYNTH_MAX_PAPERS:
        paper_ids = paper_ids[:SYNTH_MAX_PAPERS]
    print(f"  Articles      : {len(article_ids):>7,}")
    print(f"  Inproceedings : {len(inproc_ids):>7,}")
    print(f"  Papers kept   : {len(paper_ids):>7,}  (from {loaded_papers:,})")

    author_ids = [
        a for a in (author_df[author_col].dropna().str.strip() if author_col else [])
        if a and a.lower() not in ("nan", ":id")
    ]
    if SYNTH_MAX_AUTHORS > 0 and len(author_ids) > SYNTH_MAX_AUTHORS:
        author_ids = author_ids[:SYNTH_MAX_AUTHORS]
    print(f"  Authors kept  : {len(author_ids):>7,}")

    paper_to_authors: dict = {}
    if ab_paper_col and ab_auth_col:
        mask = authored_df[ab_paper_col].notna() & authored_df[ab_auth_col].notna()
        for pid, grp in authored_df[mask].groupby(ab_paper_col):
            paper_to_authors[str(pid).strip()] = set(grp[ab_auth_col].str.strip())
    print(f"  Authorship map: {len(paper_to_authors):,} papers")

    # ── 2.1 Venue nodes ──────────────────────────────────────────────────────
    print("\n  -- 2.1  Venue nodes")
    venue_rows: list = []
    journal_id_to_name: dict = {}

    if j_id_col and j_name_col:
        mask   = journal_df[j_id_col].notna() & journal_df[j_name_col].notna()
        sub    = journal_df[mask]
        jids   = sub[j_id_col].str.strip()
        jnames = sub[j_name_col].str.strip()
        for jid, jname in zip(jids[jnames.str.lower() != "nan"],
                               jnames[jnames.str.lower() != "nan"]):
            journal_id_to_name[jid] = jname
            venue_rows.append({"venueId": jid, "name": jname, "venueType": "Journal"})
    print(f"  Journal venues     : {len(journal_id_to_name):,}")

    conf_bt_to_id: dict = {}
    if proc_bt_col:
        bts = proceedings_df[proc_bt_col].dropna().str.strip()
        bts = bts[(bts != "") & (bts.str.lower() != "nan")].unique()
        for i, bt in enumerate(sorted(bts)):
            cid = f"CONF-{i:05d}"
            conf_bt_to_id[bt] = cid
            venue_rows.append({"venueId": cid, "name": bt, "venueType": "Conference"})
    print(f"  Conference venues  : {len(conf_bt_to_id):,}")
    _save_synth(venue_rows, "venue_nodes.csv")

    # ── 2.2 Issue / Edition nodes ────────────────────────────────────────────
    print("\n  -- 2.2  Issue and Edition nodes")
    issue_rows:     list = []
    paper_to_issue: dict = {}
    issue_to_venue: dict = {}

    art_to_journal: dict = {}
    if aj_start_col and aj_end_col:
        mask = art_jnl_df[aj_start_col].notna() & art_jnl_df[aj_end_col].notna()
        art_to_journal = dict(zip(art_jnl_df[mask][aj_start_col].str.strip(),
                                  art_jnl_df[mask][aj_end_col].str.strip()))

    art_year_map: dict = {}
    art_vol_map:  dict = {}
    if art_id_col:
        ids = article_df[art_id_col].str.strip()
        if art_year_col:
            y = article_df[art_year_col].str.strip()
            art_year_map = dict(zip(ids[ids.notna() & y.notna()], y[ids.notna() & y.notna()]))
        if art_vol_col:
            v = article_df[art_vol_col].str.strip()
            art_vol_map = dict(zip(ids[ids.notna() & v.notna()], v[ids.notna() & v.notna()]))

    ji_group_to_id: dict = {}
    for art_id in (a for a in paper_ids if a in article_id_set):
        journal_id = art_to_journal.get(art_id)
        if not journal_id:
            continue
        group = (journal_id, art_year_map.get(art_id, ""), art_vol_map.get(art_id, ""))
        if group not in ji_group_to_id:
            iid = f"JI-{len(ji_group_to_id):06d}"
            ji_group_to_id[group] = iid
            issue_rows.append({"issueId": iid, "issueType": "volume",
                                "year": group[1], "city": "", "volumeNo": group[2]})
            issue_to_venue[iid] = journal_id
        paper_to_issue[art_id] = ji_group_to_id[group]
    print(f"  Journal issues     : {len(ji_group_to_id):,}")

    proc_key_to_data: dict = {}
    if proc_id_col and proc_key_col:
        mask  = proceedings_df[proc_id_col].notna() & proceedings_df[proc_key_col].notna()
        sub   = proceedings_df[mask]
        p_yrs = sub[proc_year_col].str.strip() if proc_year_col else pd.Series([""] * len(sub), index=sub.index)
        p_adr = sub[proc_addr_col].str.strip() if proc_addr_col else pd.Series([""] * len(sub), index=sub.index)
        p_bts = sub[proc_bt_col].str.strip()   if proc_bt_col   else pd.Series([""] * len(sub), index=sub.index)
        for eid, ekey, eyear, ecity, ebt in zip(
            sub[proc_id_col].str.strip(), sub[proc_key_col].str.strip(),
            p_yrs, p_adr, p_bts
        ):
            if ekey and ekey.lower() != "nan":
                proc_key_to_data[ekey] = {"id": eid, "year": eyear, "city": ecity, "booktitle": ebt}

    seen_eids: set = set()
    for edata in proc_key_to_data.values():
        eid = edata["id"]
        if eid in seen_eids:
            continue
        seen_eids.add(eid)
        issue_rows.append({"issueId": eid, "issueType": "proceedings",
                           "year": edata["year"], "city": edata["city"], "volumeNo": ""})
        conf_id = conf_bt_to_id.get(edata["booktitle"])
        if conf_id:
            issue_to_venue[eid] = conf_id
    print(f"  Conference editions: {len(seen_eids):,}")

    if inproc_id_col and inproc_cross_col:
        xdf = inproc_df[[inproc_id_col, inproc_cross_col]].copy()
        xdf.columns = ["inproc_id", "crossref"]
        xdf = xdf.dropna(subset=["crossref"])
        xdf = xdf[xdf["crossref"].str.lower() != "nan"]
        xdf["inproc_id"] = xdf["inproc_id"].str.strip()
        xdf["crossref"]  = xdf["crossref"].str.strip()
        proc_key_df = pd.DataFrame([{"crossref": k, "proc_id": v["id"]}
                                     for k, v in proc_key_to_data.items()])
        if not proc_key_df.empty:
            merged = xdf.merge(proc_key_df, on="crossref", how="inner")
            for iid, eid in zip(merged["inproc_id"], merged["proc_id"]):
                paper_to_issue[iid] = eid
    print(f"  published_in edges : {len(paper_to_issue):,}")
    _save_synth(issue_rows, "issue_nodes.csv")

    # ── 2.3 Keyword nodes ────────────────────────────────────────────────────
    print("\n  -- 2.3  Keyword nodes  (synthetic fixed list)")
    KEYWORD_NAMES = [
        "knowledge graph", "ontology", "RDF", "SPARQL", "graph database",
        "semantic web", "linked data", "property graph", "graph neural network",
        "knowledge representation", "data integration", "schema matching",
        "data quality", "data management", "information extraction",
        "entity linking", "relation extraction", "machine learning",
        "deep learning", "natural language processing", "question answering",
        "embedding", "reasoning", "inference", "graph processing",
        "graph algorithms", "distributed systems", "query optimization",
        "recommender systems", "knowledge base",
    ]
    keyword_rows = [{"keywordId": f"KW-{i:04d}", "name": n}
                    for i, n in enumerate(KEYWORD_NAMES)]
    keyword_df  = _save_synth(keyword_rows, "keyword_nodes.csv")
    keyword_ids = keyword_df["keywordId"].tolist()

    # ── 2.4 Review nodes ─────────────────────────────────────────────────────
    print("\n  -- 2.4  Review nodes  (3 per paper, COI-safe)")
    DECISIONS = ["accept", "reject", "minor revision", "major revision"]
    TEMPLATES = [
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
    review_rows:    list = []
    wrote_rev_rows: list = []
    about_rows:     list = []
    skipped = 0
    for paper_id in paper_ids:
        eligible = [a for a in author_ids if a not in paper_to_authors.get(paper_id, set())]
        if len(eligible) < 3:
            skipped += 1
            continue
        for rank, rid in enumerate(random.sample(eligible, 3)):
            rev_id = f"REV-{len(review_rows):06d}"
            review_rows.append({"reviewId": rev_id,
                                 "text":     random.choice(TEMPLATES),
                                 "decision": random.choice(DECISIONS)})
            wrote_rev_rows.append({":START_ID": rid, ":END_ID": rev_id,
                                    "corresponding": "true" if rank == 0 else "false"})
            about_rows.append({":START_ID": rev_id, ":END_ID": paper_id})
    print(f"  Papers skipped : {skipped:,}")
    print(f"  Reviews created: {len(review_rows):,}")
    _save_synth(review_rows, "review_nodes.csv")

    # ── 2.5 Edge CSVs ────────────────────────────────────────────────────────
    print("\n  -- 2.5  Edge CSVs")
    _save_synth([{":START_ID": p, ":END_ID": i} for p, i in paper_to_issue.items()],
                "rel_published_in.csv")
    _save_synth([{":START_ID": i, ":END_ID": v} for i, v in issue_to_venue.items()],
                "rel_of_venue.csv")
    has_kw = [{":START_ID": pid, ":END_ID": kw}
              for pid in paper_ids
              for kw in random.sample(keyword_ids, k=random.randint(2, 4))]
    _save_synth(pd.DataFrame(has_kw).drop_duplicates(), "rel_has_keyword.csv")
    _save_synth(wrote_rev_rows, "rel_wrote_review.csv")
    _save_synth(about_rows,     "rel_about.csv")

    print(f"\n  Venues   : {len(venue_rows):,}")
    print(f"  Issues   : {len(issue_rows):,}")
    print(f"  Keywords : {len(keyword_rows):,}")
    print(f"  Reviews  : {len(review_rows):,}")
    print(f"\n  Files → {SYNTHETIC_FOLDER}")


def main() -> None:
    _prompt_format_settings()
    run_step_clean()
    run_step_synth()
    print("\n  Next: copy CSVs to Neo4j’s import folder, then run UploadCSV.py")


if __name__ == "__main__":
    main()