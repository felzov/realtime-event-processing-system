"""Fixtures for tests against a real Kafka broker and PostgreSQL (`make up`).

Every test creates its own topics with a unique `test.` name, so tests never share state and never
touch `orders.placed.v1`. Database tests use only the database named by `TEST_DATABASE_URL`, which
must end in `_test`.
"""

import os
import uuid
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest
from confluent_kafka import KafkaException
from confluent_kafka.admin import AdminClient
from pydantic import SecretStr
from sqlalchemy import create_engine, text
from sqlalchemy.engine import make_url
from sqlalchemy.exc import DBAPIError

from event_pipeline.config import Settings
from tests.integration.db_helpers import (
    TEST_DATABASE_SUFFIX,
    NotATestDatabaseError,
    require_test_database,
)

_HERE = Path(__file__).parent


def pytest_collection_modifyitems(items: list[pytest.Item]) -> None:
    # A module-level `pytestmark` in a conftest is not applied to test modules, so the marker is
    # added here: every test in this directory is an integration test, and `make test` skips it.
    for item in items:
        if _HERE in item.path.parents:
            item.add_marker(pytest.mark.integration)


ADMIN_TIMEOUT_S = 10.0
DB_CHECK_CONNECT_TIMEOUT_S = 5


@pytest.fixture(scope="session")
def kafka_bootstrap() -> str:
    # Fail, not skip: a CI job that silently skips its broker tests would look green.
    servers = os.environ.get("TEST_KAFKA_BOOTSTRAP_SERVERS")
    if not servers:
        pytest.fail(
            "TEST_KAFKA_BOOTSTRAP_SERVERS is not set; run `make up` and `make test-integration`"
        )
    try:
        AdminClient({"bootstrap.servers": servers}).list_topics(timeout=ADMIN_TIMEOUT_S)
    except KafkaException as exc:
        pytest.fail(f"Kafka at {servers} did not answer: {exc.args[0].str()}")
    return servers


@pytest.fixture
def admin(kafka_bootstrap: str) -> AdminClient:
    return AdminClient({"bootstrap.servers": kafka_bootstrap})


@pytest.fixture
def unique_topic() -> Callable[[], str]:
    return lambda: f"test.{uuid.uuid4().hex}"


@pytest.fixture
def settings_for(kafka_bootstrap: str) -> Callable[..., Settings]:
    def make(**overrides: Any) -> Settings:
        return Settings(  # type: ignore[call-arg]
            _env_file=None, kafka_bootstrap_servers=kafka_bootstrap, **overrides
        )

    return make


@pytest.fixture(scope="session")
def test_database_url() -> SecretStr:
    """`TEST_DATABASE_URL`, checked to be a reachable database whose name ends in `_test`.

    Fails, not skips, as for Kafka. Returned as a SecretStr: pytest prints a failing test's
    arguments, and the URL holds the password. Every failure below is raised with `pytrace=False`
    outside the `except` block, so pytest prints only the message, not a chained traceback whose
    frames would show the URL and psycopg's connection parameters.
    """
    url = os.environ.get("TEST_DATABASE_URL")
    if not url:
        pytest.fail(
            "TEST_DATABASE_URL is not set; run `make up` and `make test-integration`", pytrace=False
        )
    problem = _test_database_problem(url)
    if problem is not None:
        pytest.fail(problem, pytrace=False)
    return SecretStr(url)


def _test_database_problem(url: str) -> str | None:
    try:
        require_test_database(url)
    except NotATestDatabaseError as exc:
        return str(exc)
    shown = make_url(url).render_as_string(hide_password=True)
    engine = create_engine(url, connect_args={"connect_timeout": DB_CHECK_CONNECT_TIMEOUT_S})
    try:
        with engine.connect() as connection:
            # The server's answer, not the URL's path: catches a database chosen any other way
            # (a libpq service file, PGDATABASE-style defaults).
            database = connection.execute(text("SELECT current_database()")).scalar_one()
    except DBAPIError as exc:
        # Any driver error, not only OperationalError: an invalid connection option, for example,
        # is a ProgrammingError. psycopg's message names host, port, user and database, never the
        # password.
        return (
            f"PostgreSQL at {shown} refused the connection: {exc.orig}. Not running: `make up`. "
            "Database missing: the postgres-data volume predates docker/postgres-init."
        )
    finally:
        engine.dispose()
    if not database.endswith(TEST_DATABASE_SUFFIX):
        return f"TEST_DATABASE_URL connected to a database not ending in {TEST_DATABASE_SUFFIX!r}"
    return None


@pytest.fixture
def db_settings_for(test_database_url: SecretStr) -> Callable[..., Settings]:
    def make(**overrides: Any) -> Settings:
        return Settings(  # type: ignore[call-arg]
            _env_file=None, database_url=test_database_url, **overrides
        )

    return make
