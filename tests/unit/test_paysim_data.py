import csv
import json
import threading
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import urlopen

import pytest

from recap.config import Settings
from recap.dashboard import RunReport, create_server
from recap.ingestion import prepare
from recap.paysim_data import prepare_paysim
from recap.runner import simulate

SOURCE = Path(__file__).parents[1] / "fixtures/paysim_sample.csv"


def test_source_rows_preserved_and_all_mutations_documented(tmp_path):
    result = prepare_paysim(SOURCE, tmp_path / "inputs", rows=10)
    with SOURCE.open(newline="") as handle:
        original = list(csv.DictReader(handle))[:10]
    with open(result["original_csv"], newline="") as handle:
        assert list(csv.DictReader(handle)) == original
    path = Path(result["edge_csv"])
    catalog = json.loads(path.with_suffix(".cases.json").read_text())
    with path.open(newline="") as handle:
        cases = list(csv.DictReader(handle))
    assert len(cases) == len(catalog["cases"]) == 17
    for row, label in zip(cases, catalog["cases"]):
        rebuilt = {**label["original"], **{k: v["after"] for k, v in label["changes"].items()}}
        assert row == rebuilt
    with pytest.raises(ValueError, match="already exists"):
        prepare_paysim(SOURCE, tmp_path / "inputs", rows=10)
    with pytest.raises(ValueError, match="found only"):
        prepare_paysim(SOURCE, tmp_path / "too-many", rows=1000)


def test_case_labels_are_checksum_bound_and_do_not_override_validation(tmp_path):
    result = prepare_paysim(SOURCE, tmp_path / "inputs", rows=10)
    source = Path(result["edge_csv"])
    catalog = source.with_suffix(".cases.json")
    data = json.loads(catalog.read_text())
    data["cases"][0]["expected_validation"] = "accepted"
    catalog.write_text(json.dumps(data))
    out = tmp_path / "prepared"
    manifest = prepare(source, out, Settings())
    assert manifest["counts"]["rejected"] == 10
    assert json.loads((out / "rejected_rows.jsonl").read_text().splitlines()[0])["row"] == 1
    source.write_text(source.read_text() + "\n")
    with pytest.raises(ValueError, match="checksum"):
        prepare(source, tmp_path / "tampered", Settings())


def test_separate_run_http_selection_and_actual_handling(tmp_path, db, clock):
    inputs = prepare_paysim(SOURCE, tmp_path / "inputs", rows=10)
    settings = Settings(failure_mix={"clean": 1})
    main, edges = tmp_path / "main", tmp_path / "edges"
    simulate(db, inputs["original_csv"], main, settings, clock=clock, sleep=clock.sleep)
    simulate(db, inputs["edge_csv"], edges, settings, clock=clock, sleep=clock.sleep)
    report = RunReport(edges)
    cases = {r["case"]: r for r in report.rows("cases")}
    assert len(cases) == 17 and all(r["expectation_met"] for r in cases.values())
    assert "Rejected before Kafka" in cases["negative_amount"]["handling"]
    assert "235 minor units" in cases["fractional_cent"]["handling"]
    assert "origin_balance_inconsistent" in cases["balance_mismatch"]["handling"]
    assert "submit → SUCCESS" in cases["zero_amount"]["handling"]
    assert len(cases["zero_amount"]["transaction"]["audit"]) > 0
    server = create_server(main, 0, compare_output=edges)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    base = f"http://127.0.0.1:{server.server_port}"
    try:
        with urlopen(base + "/api/summary?run=main") as response:
            assert json.load(response)["metrics"]["source_rows_read"] == 10
        with urlopen(base + "/api/summary?run=edges") as response:
            result = json.load(response)
            assert result["metrics"]["source_rows_read"] == 17
            assert len(result["available_runs"]) == 2
        with urlopen(base + "/api/rows?run=edges&kind=cases") as response:
            assert json.load(response)["total"] == 17
        with urlopen(base + "/download?run=edges&file=metrics.json") as response:
            assert json.load(response)["source_rows_rejected"] == 10
        with pytest.raises(HTTPError):
            urlopen(base + "/api/summary?run=../")
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)
