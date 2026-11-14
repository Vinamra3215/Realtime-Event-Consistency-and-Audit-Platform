"""Loopback-only, read-only dashboard for saved report artifacts. No database required."""

import json
import logging
import shutil
import socket
import webbrowser
from collections import defaultdict
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from importlib.resources import files
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

ARTIFACTS = (
    "metrics.json",
    "metrics.csv",
    "report.md",
    "provenance.json",
    "reconciliation.jsonl",
    "transactions.jsonl",
    "audit.jsonl",
    "prepared/events.jsonl",
    "prepared/rejected_rows.jsonl",
    "prepared/manifest.json",
)

SCENARIO_HELP = {
    "clean": "Provider replies normally; no failure is injected.",
    "timeout_after_completion": "Provider commits a payment but the first reply times out. Recovery observes status or retries using the same key.",
    "dropped_response": "The first provider reply is lost after its effect is stored. Recovery resolves uncertainty using the configured policy.",
    "delayed_response": "Initial replies may be unavailable until the configured delay. Status checks can confirm the stored outcome.",
    "duplicate_submission": "An additional event uses the same transaction key. The service records the duplicate without another payment effect.",
    "provider_unavailable": "Initial submissions return retryable unavailability. Recovery schedules attempts and checks status when configured.",
    "permanent_failure": "Provider confirms failure. The transaction becomes FAILED and retries stop.",
    "always_unknown": "Provider never gives a definite answer. Once attempts are exhausted, the transaction remains UNRESOLVED.",
}


class LocalReportServer(ThreadingHTTPServer):
    # HTTPServer enables SO_REUSEADDR by default. On Windows this can allow two
    # viewers to bind one port, sending the browser to an older report.
    allow_reuse_address = False
    allow_reuse_port = False

    def server_bind(self):
        if hasattr(socket, "SO_EXCLUSIVEADDRUSE"):
            self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_EXCLUSIVEADDRUSE, 1)
        super().server_bind()


class RunReport:
    def __init__(self, output):
        self.root = Path(output).resolve()
        self.metrics = self.read_json("metrics.json")
        self.provenance = self.read_json("provenance.json")
        self.manifest = self.provenance["prepared_manifest"]
        if self.metrics["run_id"] != self.manifest["run_id"]:
            raise ValueError("Metrics and manifest belong to different runs")

    def artifact(self, name):
        if name not in ARTIFACTS:
            raise ValueError("Unknown artifact")
        path = (self.root / name).resolve()
        if not path.is_relative_to(self.root):
            raise ValueError("Artifact must remain inside the report directory")
        return path

    def read_json(self, name):
        return json.loads(self.artifact(name).read_text(encoding="utf-8"))

    def records(self, name):
        path = self.artifact(name)
        if path.is_file():
            with path.open(encoding="utf-8") as handle:
                for line in handle:
                    if line.strip():
                        yield json.loads(line)

    def summary(self):
        return {
            "metrics": self.metrics,
            "manifest": self.manifest,
            "provenance": self.provenance,
            "output_path": str(self.root),
            "artifacts": [name for name in ARTIFACTS if self.artifact(name).is_file()],
            "detailed_transactions": self.artifact("transactions.jsonl").is_file(),
            "audit_available": self.artifact("audit.jsonl").is_file(),
        }

    def rows(self, kind):
        labels = {c["row"]: c for c in self.manifest.get("input_catalog", {}).get("cases", [])}
        if kind == "cases":
            accepted = {r["row"]: r for r in self.rows("transactions")}
            rejected = {r["row"]: r for r in self.records("prepared/rejected_rows.jsonl")}
            for row_number, case in labels.items():
                tx, rejection = accepted.get(row_number), rejected.get(row_number)
                actual = "rejected" if rejection else "accepted" if tx else "not observed"
                if rejection:
                    handling = "Rejected before Kafka: " + rejection["reason"]
                elif tx:
                    findings = ", ".join(tx["quality"]) or "no source-quality flags"
                    handling = (
                        f"Accepted; normalized amount {tx['input']['amount_minor']} minor units; "
                        f"{findings}. {tx['handling']}"
                    )
                else:
                    handling = "No evidence in this run's selected input slice."
                yield {
                    **case,
                    "actual_validation": actual,
                    "status": tx["status"] if tx else actual,
                    "handling": handling,
                    "expectation_met": actual == case["expected_validation"],
                    "transaction": tx,
                    "rejection": rejection,
                }
        elif kind == "transactions":
            checks = {r["transaction_id"]: r for r in self.records("reconciliation.jsonl")}
            history = defaultdict(list)
            for item in self.records("audit.jsonl"):
                history[item["transaction_id"]].append(item)
            detailed = self.artifact("transactions.jsonl").is_file()
            source = "transactions.jsonl" if detailed else "prepared/events.jsonl"
            seen = set()
            for record in self.records(source):
                event = record["event"] if detailed else record
                tx_id = event["transaction_id"]
                if tx_id in seen:
                    continue
                seen.add(tx_id)
                check = checks.get(tx_id, {})
                audit_rows = history.get(tx_id, [])
                actions = [
                    f"Attempt {a['attempt_number']}: {a['event_type']} → "
                    f"{a['details'].get('outcome', a['next_state'])} "
                    f"({a['details'].get('reason', '')})"
                    for a in audit_rows
                    if a["event_type"]
                    in ("submit", "retry", "status_check", "exhaustion", "duplicate_submission")
                ]
                label = labels.get(event["source"]["row"])
                yield {
                    "case": label["case"] if label else "Original / unlabelled input",
                    "case_definition": label,
                    "scenario_explanation": SCENARIO_HELP.get(
                        event["scenario"], "Unknown scenario"
                    ),
                    "handling": "; ".join(actions)
                    if actions
                    else "Detailed actions not exported in this report.",
                    "audit": audit_rows,
                    "transaction_id": tx_id,
                    "row": event["source"]["row"],
                    "type": event["transaction_type"],
                    "originator": event["originator"],
                    "destination": event["destination"],
                    "amount": event["amount_minor"] / 100,
                    "currency": event.get("currency"),
                    "scenario": event["scenario"],
                    "status": record.get("status", check.get("internal_outcome")),
                    "attempts": record.get("attempts") if detailed else None,
                    "recovered": (
                        record.get("first_unresolved_at") is not None
                        and record.get("status") == "SUCCESS"
                    )
                    if detailed
                    else None,
                    "reconciliation": check.get("classification"),
                    "quality": check.get("details", {}).get("data_quality", []),
                    "input": event,
                    "processing": record if detailed else None,
                    "comparison": check,
                }
        elif kind in ("rejected", "audit", "reconciliation"):
            name = {
                "rejected": "prepared/rejected_rows.jsonl",
                "audit": "audit.jsonl",
                "reconciliation": "reconciliation.jsonl",
            }[kind]
            for row in self.records(name):
                if kind == "rejected":
                    case = labels.get(row["row"])
                    row = {**row, "case": case["case"] if case else "Unlabelled validation failure"}
                yield row
        else:
            raise ValueError("Unknown table")

    def page(self, kind, query="", status="", page=1, size=25):
        if page < 1 or not 1 <= size <= 100:
            raise ValueError("Invalid page or page size")
        items, total = [], 0
        for row in self.rows(kind):
            if status and row.get("status") != status:
                continue
            if query and query.casefold() not in json.dumps(row).casefold():
                continue
            if (page - 1) * size <= total < page * size:
                items.append(row)
            total += 1
        return {"items": items, "total": total, "page": page, "size": size}


def create_server(output, port=8765, compare_output=None):
    reports = {"main": RunReport(output)}
    if compare_output:
        reports["edges"] = RunReport(compare_output)

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def send_headers(self, length, content_type, status=200, attachment=None):
            self.send_response(status)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(length))
            self.send_header("Cache-Control", "no-store")
            self.send_header("X-Content-Type-Options", "nosniff")
            self.send_header("Referrer-Policy", "no-referrer")
            self.send_header(
                "Content-Security-Policy",
                "default-src 'self'; style-src 'self' 'unsafe-inline'; "
                "object-src 'none'; frame-ancestors 'none'; base-uri 'none'",
            )
            if attachment:
                self.send_header("Content-Disposition", f'attachment; filename="{attachment}"')
            self.end_headers()

        def send_json(self, data, status=200):
            body = json.dumps(data, allow_nan=False).encode()
            self.send_headers(len(body), "application/json; charset=utf-8", status)
            self.wfile.write(body)

        def do_GET(self):
            host = self.headers.get("Host", "")
            allowed = {
                f"127.0.0.1:{self.server.server_port}",
                f"localhost:{self.server.server_port}",
            }
            if host not in allowed:
                self.send_json({"error": "Local requests only"}, 403)
                return
            url = urlsplit(self.path)
            params = parse_qs(url.query)

            def value(name, default=""):
                return params.get(name, [default])[0]

            try:
                selection = value("run", "main")
                if selection not in reports:
                    raise ValueError("Unknown run selection")
                report = reports[selection]
                if url.path in ("/", "/style.css", "/app.js"):
                    name, mime = {
                        "/": ("index.html", "text/html"),
                        "/style.css": ("style.css", "text/css"),
                        "/app.js": ("app.js", "text/javascript"),
                    }[url.path]
                    body = files("recap").joinpath("web", name).read_bytes()
                    self.send_headers(len(body), mime + "; charset=utf-8")
                    self.wfile.write(body)
                elif url.path == "/api/summary":
                    self.send_json(
                        {
                            **report.summary(),
                            "selected_run": selection,
                            "available_runs": [
                                {
                                    "key": key,
                                    "name": r.manifest["dataset"]["name"],
                                    "rows": r.metrics["source_rows_read"],
                                }
                                for key, r in reports.items()
                            ],
                        }
                    )
                elif url.path == "/api/rows":
                    self.send_json(
                        report.page(
                            value("kind", "transactions"),
                            value("q"),
                            value("status"),
                            int(value("page", "1")),
                            int(value("size", "25")),
                        )
                    )
                elif url.path == "/download":
                    path = report.artifact(value("file"))
                    with path.open("rb") as handle:
                        self.send_headers(
                            path.stat().st_size, "application/octet-stream", attachment=path.name
                        )
                        shutil.copyfileobj(handle, self.wfile)
                else:
                    self.send_json({"error": "Not found"}, 404)
            except (ValueError, KeyError) as exc:
                self.send_json({"error": str(exc)}, 400)
            except OSError:
                self.send_json({"error": "Report artifact unavailable"}, 404)

    if not 0 <= port <= 65535:
        raise ValueError("Port must be between 0 and 65535")
    server = LocalReportServer(("127.0.0.1", port), Handler)
    server.daemon_threads = True
    return server


def serve_dashboard(output, port=8765, open_browser=True, stop=lambda: False, compare_output=None):
    try:
        server = create_server(output, port, compare_output)
    except OSError as exc:
        if exc.errno not in (48, 98, 10048) and getattr(exc, "winerror", None) not in (
            10013,
            10048,
        ):
            raise
        print(f"Port {port} is unavailable; choosing a separate dashboard port.", flush=True)
        server = create_server(output, 0, compare_output)
    with server:
        server.timeout = 0.4
        url = f"http://127.0.0.1:{server.server_port}"
        print(f"\nRECAP dashboard: {url}\nResults: {Path(output).resolve()}", flush=True)
        print("Keep this terminal open. Press Ctrl+C to stop the dashboard.", flush=True)
        if open_browser:
            try:
                webbrowser.open(url)
            except webbrowser.Error:
                logging.getLogger(__name__).warning("Open the dashboard URL manually")
        try:
            while not stop():
                server.handle_request()
        except KeyboardInterrupt:
            pass
    return 0
