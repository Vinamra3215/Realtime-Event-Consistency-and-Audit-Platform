"""Runnable composition root. Run `recap --help` for every supported command."""

import argparse
import json
import logging
import secrets
import signal
import time
from pathlib import Path

from sqlalchemy import delete, text

from recap.analytics import write_report
from recap.config import POLICIES, database_url, kafka_servers, load_environment, load_settings
from recap.database import Database, metadata
from recap.ingestion import prepare
from recap.logging_config import configure_logging
from recap.reconciliation import reconcile
from recap.recovery import RecoveryWorker
from recap.runner import benchmark, finish_run, simulate


def parser():
    root = argparse.ArgumentParser(description="RECAP academic transaction recovery platform")
    root.add_argument("--config", default="config/app.yaml")
    root.add_argument(
        "--database-url",
        default=None,
        help="Prefer DATABASE_URL env for secrets; SQLite is demo/test only",
    )
    root.add_argument("--kafka-servers", default=None)
    commands = root.add_subparsers(dest="command", required=True)
    commands.add_parser(
        "init-env", help="Create ignored .env with a generated local database password"
    )
    commands.add_parser("migrate")
    commands.add_parser("topics")
    commands.add_parser("doctor", help="Check database and Kafka connectivity")
    generate = commands.add_parser("generate-fixture", help="Generate synthetic PaySim-format CSV")
    generate.add_argument("--output", required=True, help="New .csv file path")
    generate.add_argument("--rows", type=int, default=1000, help="Normal rows, before edge cases")
    generate.add_argument("--seed", type=int, default=25)
    generate.add_argument("--edge-cases", action="store_true", help="Append 17 labeled input cases")
    dashboard = commands.add_parser("dashboard", help="Open a saved run in a local browser")
    dashboard.add_argument("--output", required=True, help="Directory containing metrics.json")
    dashboard.add_argument("--port", type=int, default=8765)
    dashboard.add_argument("--no-browser", action="store_true")
    reset = commands.add_parser(
        "reset", help="Destructively remove all RECAP run data; Kafka is separate"
    )
    reset.add_argument("--confirm", required=True, choices=["DELETE-ALL-RECAP-DATA"])
    for name in ("prepare", "simulate", "benchmark"):
        child = commands.add_parser(name)
        child.add_argument("--csv", required=True)
        child.add_argument("--output", required=True)
        child.add_argument("--limit", type=int, default=1000)
        child.add_argument("--seed", type=int)
        child.add_argument("--policy", choices=POLICIES)
        if name != "benchmark":
            child.add_argument("--start-row", type=int, default=1)
        if name != "prepare":
            child.add_argument("--transport", choices=["direct", "kafka"], default="direct")
            child.add_argument("--timeout", type=float, default=120)
        if name == "simulate":
            child.add_argument(
                "--ui", action="store_true", help="Open results in a local dashboard"
            )
            child.add_argument("--port", type=int, default=8765)
            child.add_argument("--no-browser", action="store_true")
    publish = commands.add_parser("publish")
    publish.add_argument("--prepared", required=True)
    consume = commands.add_parser("consume")
    consume.add_argument("--max-messages", type=int, default=0)
    consume.add_argument("--idle-timeout", type=float, default=10)
    consume.add_argument("--forever", action="store_true")
    recover = commands.add_parser("recover")
    recover.add_argument("--run-id")
    recover.add_argument("--timeout", type=float, default=120)
    recover.add_argument("--forever", action="store_true")
    for name in ("reconcile", "report", "finish"):
        child = commands.add_parser(name)
        child.add_argument("--run-id", required=True)
        if name == "report":
            child.add_argument("--output", required=True)
    return root


def main(argv=None):
    configure_logging()
    args = parser().parse_args(argv)
    if args.command == "generate-fixture":
        from recap.demo_data import generate_fixture

        try:
            result = generate_fixture(args.output, args.rows, args.seed, args.edge_cases)
            print(json.dumps(result, indent=2))
            return 0
        except (OSError, ValueError) as exc:
            logging.getLogger(__name__).error("Cannot generate fixture: %s", exc)
            return 1
    if args.command == "dashboard":
        from recap.dashboard import serve_dashboard

        try:
            return serve_dashboard(args.output, args.port, not args.no_browser)
        except (OSError, ValueError, KeyError) as exc:
            logging.getLogger(__name__).error("Cannot open dashboard: %s", exc)
            return 1
    load_environment()
    if args.command == "init-env":
        path = Path(".env")
        if path.exists():
            raise SystemExit(".env already exists; existing configuration preserved")
        with path.open("x", encoding="utf-8") as handle:
            handle.write(
                "POSTGRES_USER=recap\nPOSTGRES_DB=recap\nPOSTGRES_PASSWORD="
                + secrets.token_hex(24)
                + "\n"
            )
        print("Created .env with a generated local-only database password. Do not commit it.")
        return 0
    settings = load_settings(
        args.config,
        seed=getattr(args, "seed", None),
        policy=getattr(args, "policy", None),
    )
    stop_requested = False

    def stop(signum, frame):
        nonlocal stop_requested
        stop_requested = True

    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, stop)
    servers = args.kafka_servers or kafka_servers()
    if args.command == "prepare":
        result = prepare(args.csv, args.output, settings, args.limit, args.start_row)
        print(json.dumps({"run_id": result["run_id"], "counts": result["counts"]}, indent=2))
        return 0
    db = Database(args.database_url or database_url())
    try:
        result = None
        if args.command == "migrate":
            db.migrate()
            result = {"migrations": "applied", "database": db.engine.dialect.name}
        elif args.command == "topics":
            from recap.streaming import create_topics

            create_topics(servers, settings)
        elif args.command == "doctor":
            from confluent_kafka.admin import AdminClient

            with db.engine.connect() as connection:
                connection.execute(text("SELECT 1"))
            brokers = AdminClient({"bootstrap.servers": servers}).list_topics(timeout=10).brokers
            result = {"database": "reachable", "kafka_brokers": len(brokers)}
        elif args.command == "reset":
            with db.transaction() as connection:
                for table in reversed(metadata.sorted_tables):
                    connection.execute(delete(table))
            result = {"reset": "all RECAP run records removed; Kafka offsets and topics unchanged"}
        elif args.command in ("simulate", "benchmark"):
            common = dict(
                db=db,
                csv_path=args.csv,
                output=args.output,
                settings=settings,
                limit=args.limit,
                transport=args.transport,
                servers=servers,
                timeout=args.timeout,
            )
            result = (
                simulate(**common, start_row=args.start_row)
                if args.command == "simulate"
                else benchmark(**common)
            )
        elif args.command == "publish":
            from recap.streaming import publish_prepared

            result = {"published": publish_prepared(db, args.prepared, settings, servers)}
        elif args.command == "consume":
            from recap.streaming import consume

            while not stop_requested:
                count = consume(
                    db,
                    settings,
                    servers,
                    args.max_messages,
                    args.idle_timeout,
                    stop=lambda: stop_requested,
                )
                if not args.forever:
                    result = {"consumed": count}
                    break
        elif args.command == "recover":
            worker = RecoveryWorker(db)
            while not stop_requested:
                worker.drain(args.run_id, args.timeout, stop=lambda: stop_requested)
                if not args.forever:
                    break
                time.sleep(0.25)
        elif args.command == "reconcile":
            result = reconcile(db, args.run_id)
        elif args.command == "finish":
            finish_run(db, args.run_id)
        elif args.command == "report":
            reconcile(db, args.run_id)
            result = write_report(db, args.run_id, args.output)
        if result is not None:
            print(json.dumps(result, indent=2, default=str))
        if getattr(args, "ui", False):
            from recap.dashboard import serve_dashboard

            db.dispose()
            return serve_dashboard(
                args.output, args.port, not args.no_browser, stop=lambda: stop_requested
            )
        return 0
    except (ValueError, TimeoutError, OSError) as exc:
        logging.getLogger(__name__).error(
            "command_failed type=%s reason=%s", type(exc).__name__, exc
        )
        return 1
    finally:
        db.dispose()


if __name__ == "__main__":
    raise SystemExit(main())
