"""Validated run configuration; secrets are never included in manifests."""

import math
import os
from dataclasses import asdict, dataclass, field
from datetime import datetime
from pathlib import Path

import yaml

SCENARIOS = (
    "clean",
    "timeout_after_completion",
    "dropped_response",
    "delayed_response",
    "duplicate_submission",
    "provider_unavailable",
    "permanent_failure",
    "always_unknown",
)
POLICIES = ("fixed", "exponential", "jitter", "status_check_first")


@dataclass(frozen=True)
class Settings:
    seed: int = 25
    epoch: str = "2026-01-01T00:00:00Z"
    currency: str | None = None
    time_bucket_seconds: int = 60
    policy: str = "status_check_first"
    base_delay: float = 0.1
    max_delay: float = 2.0
    max_attempts: int = 5
    lease_seconds: float = 30.0
    unavailable_attempts: int = 2
    delayed_seconds: float = 0.2
    input_rate: float = 0
    kafka_topic: str = "recap.requests.v1"
    dead_letter_topic: str = "recap.dead-letter.v1"
    consumer_group: str = "recap-workers-v1"
    failure_mix: dict = field(
        default_factory=lambda: {
            "clean": 0.88,
            "timeout_after_completion": 0.04,
            "dropped_response": 0.03,
            "delayed_response": 0.03,
            "duplicate_submission": 0.01,
            "provider_unavailable": 0.01,
        }
    )

    def __post_init__(self):
        if self.policy not in POLICIES:
            raise ValueError(f"Unknown policy {self.policy}")
        for name in ("seed", "time_bucket_seconds", "max_attempts", "unavailable_attempts"):
            value = getattr(self, name)
            if type(value) is not int or value < (
                0 if name in ("seed", "unavailable_attempts") else 1
            ):
                raise ValueError(f"Invalid {name}")
        for name in ("base_delay", "max_delay", "lease_seconds", "delayed_seconds", "input_rate"):
            value = getattr(self, name)
            if (
                isinstance(value, bool)
                or not isinstance(value, (int, float))
                or not math.isfinite(value)
                or value < 0
            ):
                raise ValueError(f"Invalid {name}")
        if self.lease_seconds <= 0 or self.max_delay < self.base_delay:
            raise ValueError("Require positive lease_seconds and max_delay >= base_delay")
        epoch = datetime.fromisoformat(self.epoch.replace("Z", "+00:00"))
        if epoch.utcoffset() is None or epoch.utcoffset().total_seconds() != 0:
            raise ValueError("epoch must be UTC")
        if self.currency is not None and (
            len(self.currency) != 3 or not self.currency.isupper() or not self.currency.isalpha()
        ):
            raise ValueError("currency must be null or an explicit three-letter uppercase code")
        if not self.failure_mix or set(self.failure_mix) - set(SCENARIOS):
            raise ValueError("Unknown or empty failure mix")
        if any(
            type(v) not in (int, float) or not math.isfinite(v) or v < 0
            for v in self.failure_mix.values()
        ):
            raise ValueError("Failure weights must be finite and nonnegative")
        if not math.isclose(sum(self.failure_mix.values()), 1, abs_tol=1e-9):
            raise ValueError("Failure mix must sum to 1")
        for name in ("kafka_topic", "dead_letter_topic", "consumer_group"):
            if not getattr(self, name):
                raise ValueError(f"{name} cannot be empty")

    def public_dict(self):
        return asdict(self)


def load_settings(path: str | Path | None = None, **overrides) -> Settings:
    values = {}
    if path:
        with open(path, encoding="utf-8") as handle:
            values = yaml.safe_load(handle) or {}
        if not isinstance(values, dict):
            raise ValueError("Configuration must be a YAML mapping")
    values.update({k: v for k, v in overrides.items() if v is not None})
    return Settings(**values)


def database_url() -> str:
    if "DATABASE_URL" not in os.environ and os.environ.get("POSTGRES_PASSWORD"):
        from sqlalchemy import URL

        return URL.create(
            "postgresql+psycopg",
            username=os.environ.get("POSTGRES_USER", "recap"),
            password=os.environ["POSTGRES_PASSWORD"],
            host=os.environ.get("POSTGRES_HOST", "127.0.0.1"),
            port=int(os.environ.get("POSTGRES_PORT", "55432")),
            database=os.environ.get("POSTGRES_DB", "recap"),
        ).render_as_string(hide_password=False)
    return os.environ.get("DATABASE_URL", "postgresql+psycopg://recap@127.0.0.1:55432/recap")


def load_environment(path=".env"):
    """Read only supported connection settings, without overriding exported environment."""
    path = Path(path)
    if not path.is_file():
        return
    allowed = {
        "POSTGRES_USER",
        "POSTGRES_DB",
        "POSTGRES_PASSWORD",
        "POSTGRES_HOST",
        "POSTGRES_PORT",
        "DATABASE_URL",
        "KAFKA_BOOTSTRAP_SERVERS",
        "RECAP_IMAGE_TAG",
    }
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip() or line.lstrip().startswith("#"):
            continue
        key, separator, value = line.partition("=")
        if separator and key.strip() in allowed:
            os.environ.setdefault(key.strip(), value.strip())


def kafka_servers() -> str:
    return os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092")
