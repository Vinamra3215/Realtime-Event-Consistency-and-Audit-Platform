import json
import threading
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import Request, urlopen

import pytest

from recap.cli import main
from recap.config import Settings
from recap.dashboard import RunReport, create_server, serve_dashboard
from recap.provenance import sha256_file
from recap.runner import simulate


@pytest.fixture
def report_folder(db, tmp_path, clock):
    folder = tmp_path / "report"
    simulate(
        db,
        Path(__file__).parents[1] / "fixtures/paysim_sample.csv",
        folder,
        Settings(),
        clock=clock,
        sleep=clock.sleep,
    )
    return folder


def test_report_details_match_pipeline_and_checksums(report_folder):
    report = RunReport(report_folder)
    summary = report.summary()
    records = report.page("transactions", size=100)
    assert records["total"] == summary["metrics"]["logical_transactions"] == 11
    assert sum(r["recovered"] for r in records["items"]) == 3
    assert report.page("rejected")["total"] == 3
    assert report.page("audit")["total"] == summary["metrics"]["audit_records"]
    tx_id = records["items"][0]["transaction_id"]
    assert report.page("transactions", query=tx_id)["total"] == 1
    assert report.page("transactions", status="FAILED")["total"] == 0
    assert len(report.page("transactions", page=2, size=3)["items"]) == 3
    for name, checksum in report.provenance["artifacts"].items():
        assert sha256_file(report_folder / name) == checksum


def test_historical_report_does_not_invent_missing_evidence(report_folder):
    (report_folder / "transactions.jsonl").unlink()
    (report_folder / "audit.jsonl").unlink()
    report = RunReport(report_folder)
    assert not report.summary()["audit_available"]
    assert not report.summary()["detailed_transactions"]
    assert report.page("transactions")["total"] == 11
    assert all(r["recovered"] is None for r in report.page("transactions")["items"])
    assert report.page("audit")["total"] == 0


def test_dashboard_port_cannot_be_shared_and_fallback_uses_new_port(report_folder, capsys):
    with create_server(report_folder, 0) as first:
        with pytest.raises(OSError):
            create_server(report_folder, first.server_port)
        assert (
            serve_dashboard(report_folder, first.server_port, open_browser=False, stop=lambda: True)
            == 0
        )
        output = capsys.readouterr().out
        assert f"Port {first.server_port} is unavailable" in output
        assert f"RECAP dashboard: http://127.0.0.1:{first.server_port}\n" not in output


def test_local_http_ui_downloads_and_access_boundaries(report_folder):
    server = create_server(report_folder, 0)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    base = f"http://127.0.0.1:{server.server_port}"
    try:
        with urlopen(base + "/") as response:
            assert b"Run explorer" in response.read()
            assert "frame-ancestors 'none'" in response.headers["Content-Security-Policy"]
        with urlopen(base + "/api/rows?kind=transactions&size=2") as response:
            assert len(json.load(response)["items"]) == 2
        with urlopen(base + "/download?file=metrics.json") as response:
            assert "attachment" in response.headers["Content-Disposition"]
            assert json.load(response)["run_status"] == "complete"
        for path in ("/download?file=../.env", "/api/rows?size=0", "/api/rows?kind=unknown"):
            with pytest.raises(HTTPError) as error:
                urlopen(base + path)
            assert error.value.code == 400
        with pytest.raises(HTTPError) as error:
            urlopen(Request(base + "/", headers={"Host": "external.example"}))
        assert error.value.code == 403
        with pytest.raises(HTTPError) as error:
            urlopen(base + "/.env")
        assert error.value.code == 404
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


def test_dashboard_cli_does_not_need_database_or_config(monkeypatch, report_folder, tmp_path):
    called = []
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr("recap.dashboard.serve_dashboard", lambda *args: called.append(args) or 0)
    assert main(["dashboard", "--output", str(report_folder), "--no-browser"]) == 0
    assert called == [(str(report_folder), 8765, False)]


def test_simulate_ui_opens_only_after_reports_exist(monkeypatch, tmp_path):
    called = []

    def serve(output, port, open_browser, stop):
        report = RunReport(output)
        assert report.metrics["run_status"] == "complete"
        assert report.summary()["audit_available"]
        assert not stop()
        called.append((port, open_browser))
        return 0

    monkeypatch.setattr("recap.dashboard.serve_dashboard", serve)
    assert (
        main(
            [
                "--database-url",
                "sqlite:///" + str(tmp_path / "ui.db"),
                "simulate",
                "--csv",
                str(Path(__file__).parents[1] / "fixtures/paysim_sample.csv"),
                "--output",
                str(tmp_path / "ui-run"),
                "--ui",
                "--port",
                "0",
                "--no-browser",
            ]
        )
        == 0
    )
    assert called == [(0, False)]
