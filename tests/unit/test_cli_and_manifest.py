import json
from pathlib import Path

import pytest
from sqlalchemy import func, select

from recap.cli import main
from recap.config import Settings
from recap.database import metadata, register_prepared, transactions
from recap.ingestion import prepare
from recap.provenance import stable_hash
from recap.runner import finish_run
from recap.service import TransactionService
from recap.validation import InvalidEvent, business_payload


def test_reset_requires_deliberate_confirmation(db, prepared):
    with pytest.raises(SystemExit):
        main(["--database-url", str(db.engine.url), "reset"])
    TransactionService(db).accept(prepared[1][0])
    assert (
        main(["--database-url", str(db.engine.url), "reset", "--confirm", "DELETE-ALL-RECAP-DATA"])
        == 0
    )
    with db.engine.connect() as connection:
        for table in metadata.sorted_tables:
            assert connection.execute(select(func.count()).select_from(table)).scalar_one() == 0


def test_finish_refuses_missing_events(db, prepared):
    with pytest.raises(ValueError, match="not complete"):
        finish_run(db, prepared[0]["run_id"])


def test_key_payload_conflict_does_not_mutate_existing_transaction(db, prepared):
    event = prepared[1][0]
    service = TransactionService(db)
    service.accept(event)
    changed = {**event, "amount_minor": event["amount_minor"] + 1}
    changed["payload_hash"] = stable_hash(business_payload(changed))
    with pytest.raises(InvalidEvent):
        service.accept(changed)
    with db.engine.connect() as connection:
        assert (
            connection.execute(select(transactions.c.amount_minor)).scalar_one()
            == event["amount_minor"]
        )


@pytest.mark.parametrize("tamper", ["counts", "checksums", "file"])
def test_manifest_and_artifact_integrity(db, tmp_path, tamper):
    output = tmp_path / "prepared"
    manifest = prepare(
        Path(__file__).parents[1] / "fixtures" / "paysim_sample.csv", output, Settings()
    )
    if tamper == "counts":
        manifest["counts"]["published_events"] += 1
    elif tamper == "checksums":
        manifest["artifacts"] = {}
    else:
        with (output / "events.jsonl").open("a") as handle:
            handle.write("{}\n")
    (output / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    with pytest.raises(ValueError):
        register_prepared(db, output)
