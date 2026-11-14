"""Preserve a source CSV slice and create separately labelled, derived input cases."""

import csv
from pathlib import Path

from recap.demo_data import BASE, EDGE_CASES
from recap.ingestion import REQUIRED_COLUMNS
from recap.provenance import sha256_file, write_json


def prepare_paysim(source, output, rows=1000):
    source, output = Path(source), Path(output)
    if rows < 1:
        raise ValueError("rows must be positive")
    if output.exists():
        raise ValueError("Output directory already exists; choose a new directory")
    selected = []
    with source.open(newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        fields = reader.fieldnames
        if not REQUIRED_COLUMNS <= set(fields or []):
            raise ValueError("Missing required PaySim columns")
        for row in reader:
            if None in row or any(v is None for v in row.values()):
                raise ValueError("Malformed source CSV row")
            selected.append(row)
            if len(selected) == rows:
                break
    if len(selected) != rows:
        raise ValueError(f"Requested {rows} source rows but found only {len(selected)}")
    parent = {"file": source.name, "sha256": sha256_file(source), "start_row": 1, "rows": rows}
    output.mkdir(parents=True)
    original = output / "paysim-original.csv"
    with original.open("x", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, lineterminator="\n")
        writer.writeheader()
        writer.writerows(selected)
    write_json(
        original.with_suffix(".cases.json"),
        {
            "source": "Original source CSV rows; field values unchanged",
            "parent_dataset": parent,
            "sha256": sha256_file(original),
            "cases": [],
        },
    )
    # Use one original TRANSFER row for identity, but explicitly normalize its
    # monetary fields to isolate each validation case. All edits are recorded.
    origin_index = next((i for i, r in enumerate(selected) if r["type"] == "TRANSFER"), 0)
    origin = selected[origin_index]
    baseline = {**origin, **{k: v for k, v in BASE.items() if k in fields}}
    for key in ("step", "nameOrig", "nameDest"):
        baseline[key] = origin[key]
    edge_path = output / "paysim-input-edges.csv"
    cases = []
    with edge_path.open("x", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, lineterminator="\n")
        writer.writeheader()
        for row_number, (name, overrides, expectation, explanation) in enumerate(EDGE_CASES, 1):
            value = {**baseline, **overrides}
            value = {key: value[key] for key in fields}
            writer.writerow(value)
            cases.append(
                {
                    "row": row_number,
                    "case": name,
                    "expected_validation": expectation,
                    "explanation": explanation,
                    "original_source_row": origin_index + 1,
                    "original": origin,
                    "changes": {
                        k: {"before": origin[k], "after": v}
                        for k, v in value.items()
                        if v != origin[k]
                    },
                }
            )
    write_json(
        edge_path.with_suffix(".cases.json"),
        {
            "source": "Added input edge cases derived from a source row; modified, not original PaySim rows",
            "parent_dataset": parent,
            "sha256": sha256_file(edge_path),
            "cases": cases,
        },
    )
    return {
        "original_csv": str(original),
        "original_rows": rows,
        "edge_csv": str(edge_path),
        "edge_rows": len(cases),
        "parent_dataset": parent,
    }
