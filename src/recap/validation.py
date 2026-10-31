import base64
import json
from datetime import datetime
from functools import lru_cache
from uuid import NAMESPACE_URL, uuid5

from jsonschema import Draft202012Validator, FormatChecker

from recap.provenance import stable_hash, utc_now
from recap.resources import resource_directory


class InvalidEvent(ValueError):
    pass


def business_payload(event):
    return {
        key: event[key]
        for key in (
            "source",
            "transaction_type",
            "originator",
            "destination",
            "amount_minor",
            "currency",
            "balances",
        )
    }


@lru_cache
def validator():
    path = resource_directory("schemas") / "transaction_event.schema.json"
    return Draft202012Validator(
        json.loads(path.read_text(encoding="utf-8")), format_checker=FormatChecker()
    )


def validate_event(event):
    errors = sorted(validator().iter_errors(event), key=lambda e: str(e.path))
    if errors:
        raise InvalidEvent(
            "; ".join(f"{'.'.join(map(str, e.path)) or '$'}: {e.message}" for e in errors[:5])
        )
    try:
        datetime.fromisoformat(event["timestamp"].replace("Z", "+00:00"))
    except ValueError as exc:
        raise InvalidEvent("timestamp must be a valid UTC datetime") from exc
    if event["payload_hash"] != stable_hash(business_payload(event)):
        raise InvalidEvent("payload_hash does not match the business payload")
    identity = stable_hash(
        ["paysim-row-v1", event["source"]["dataset_sha256"], event["source"]["row"]]
    )
    if event["idempotency_key"] != identity or event["transaction_id"] != str(
        uuid5(NAMESPACE_URL, identity)
    ):
        raise InvalidEvent("Transaction identity does not match the documented source-row identity")
    return event


def decode_event(raw):
    try:
        value = json.loads(raw)
    except (ValueError, UnicodeError) as exc:
        raise InvalidEvent(f"Invalid UTF-8 JSON: {exc}") from exc
    return validate_event(value)


def dead_letter(raw: bytes, reason: str, metadata: dict):
    return {
        "rejection_id": stable_hash([base64.b64encode(raw).decode(), metadata]),
        "raw_base64": base64.b64encode(raw).decode(),
        "reason": reason,
        "metadata": metadata,
        "received_at": utc_now(),
    }
