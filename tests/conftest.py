import os
from pathlib import Path

import pytest

from recap.config import Settings
from recap.database import Database, register_prepared
from recap.ingestion import prepare, read_events


def pytest_addoption(parser):
    parser.addoption("--live", action="store_true", help="Run actual PostgreSQL/Kafka tests")
    parser.addoption(
        "--performance", action="store_true", help="Run explicit benchmark measurements"
    )


def pytest_configure(config):
    if config.getoption("--live"):
        os.environ["RECAP_INTEGRATION"] = "1"


class Clock:
    def __init__(self):
        self.now = 1_800_000_000.0

    def __call__(self):
        return self.now

    def sleep(self, seconds):
        self.now += max(seconds, 0.000001)


@pytest.fixture
def clock():
    return Clock()


@pytest.fixture
def db(tmp_path):
    value = Database("sqlite:///" + str(tmp_path / "test.db"))
    value.migrate()
    yield value
    value.dispose()


@pytest.fixture
def prepared(tmp_path, db, clock):
    folder = tmp_path / "prepared"
    manifest = prepare(
        Path(__file__).parent / "fixtures" / "paysim_sample.csv",
        folder,
        Settings(failure_mix={"clean": 1}),
    )
    register_prepared(db, folder, clock)
    return manifest, list(read_events(folder / "events.jsonl"))
