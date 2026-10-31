import os
from uuid import uuid4

import pytest
from sqlalchemy import text
from sqlalchemy.engine import make_url

from recap.config import database_url, load_environment
from recap.database import Database


@pytest.fixture
def postgres_db():
    if os.environ.get("RECAP_INTEGRATION") != "1":
        pytest.skip(
            "Set RECAP_INTEGRATION=1 and PostgreSQL connection environment to run live tests"
        )
    load_environment()
    url = make_url(os.environ.get("RECAP_TEST_DATABASE_URL") or database_url())
    if url.get_backend_name() != "postgresql":
        pytest.fail("Integration tests require actual PostgreSQL")
    base = Database(url.render_as_string(hide_password=False))
    schema = "recap_test_" + uuid4().hex
    with base.transaction() as connection:
        connection.execute(text(f'CREATE SCHEMA "{schema}"'))
    isolated_url = url.update_query_dict({"options": f"-csearch_path={schema}"})
    db = Database(isolated_url.render_as_string(hide_password=False))
    try:
        db.migrate()
        yield db
    finally:
        db.dispose()
        with base.transaction() as connection:
            connection.execute(text(f'DROP SCHEMA "{schema}" CASCADE'))
        base.dispose()
