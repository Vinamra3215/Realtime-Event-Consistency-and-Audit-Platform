# Reuse the real PostgreSQL isolation fixture, not the portable SQLite fixture.
from tests.integration.conftest import postgres_db  # noqa: F401
