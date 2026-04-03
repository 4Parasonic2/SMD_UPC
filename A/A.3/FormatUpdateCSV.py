"""
FormatUpdateCSV.py — A.3
========================
Generate CSV files for **new** nodes and edges to extend the graph schema
defined in your updated A.3 design.

Edit ``generate_update_rows()`` below to match your report’s new entities and
relationships. Output files are written to ``Update_Csvs/`` next to this script.

These files are meant to be consumed by ``UploadUpdateCSV.py`` (MERGE / LOAD CSV).

Prerequisites:
    pip install pandas   # optional; plain csv module works too
"""

from __future__ import annotations

import csv
import os

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
UPDATE_DIR = os.path.join(SCRIPT_DIR, "Update_Csvs")


def ensure_dir() -> None:
    os.makedirs(UPDATE_DIR, exist_ok=True)


def write_csv(filename: str, rows: list[dict], fieldnames: list[str]) -> None:
    path = os.path.join(UPDATE_DIR, filename)
    with open(path, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, delimiter=";", quoting=csv.QUOTE_MINIMAL)
        w.writeheader()
        for row in rows:
            w.writerow({k: row.get(k, "") for k in fieldnames})
    print(f"  wrote {path}")


def generate_update_rows() -> None:
    """
    Replace this example with your A.3 extension (new labels, properties, edges).

    Example: add a ``Tag`` node type and ``tagged_with`` edges from ``Paper``.
    """
    # --- Example new node file (customise column names to match UploadUpdateCSV.py) ---
    tag_nodes = [
        {"tagId": "TAG-001", "name": "machine learning"},
        {"tagId": "TAG-002", "name": "graphs"},
    ]
    write_csv("update_tag_nodes.csv", tag_nodes, ["tagId", "name"])

    # --- Example relationship file (:START_ID / :END_ID convention) ---
    tagged_edges = [
        {":START_ID": "some-paper-id", ":END_ID": "TAG-001"},
    ]
    write_csv(
        "rel_paper_tagged.csv",
        tagged_edges,
        [":START_ID", ":END_ID"],
    )


def main() -> None:
    ensure_dir()
    print("FormatUpdateCSV — writing CSVs to", UPDATE_DIR)
    generate_update_rows()
    print("Done.")


if __name__ == "__main__":
    main()
