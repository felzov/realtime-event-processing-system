"""`/ready`'s database check against the real test database (phase 3 acceptance criterion 8).

The shared `_test` database stays at head: a "never migrated" or "wrong rows" database is a schema
of its own, created and dropped by the test and reached through the connection's search_path.
"""

import time
import uuid
from collections.abc import Callable, Iterator

import pytest
from pydantic import SecretStr
from sqlalchemy import Engine, create_engine, text

from event_pipeline.config import Settings
from event_pipeline.data.engine import make_readiness_engine
from event_pipeline.data.tables import EXPECTED_REVISION
from event_pipeline.services.readiness import (
    CAUSE_DB_UNAVAILABLE,
    CAUSE_MIGRATION_MISMATCH,
    CAUSE_MIGRATION_MISSING,
    DatabaseReadiness,
)
from tests.api.helpers import NO_DATABASE_URL


@pytest.fixture
def readiness_engine(
    migrated_engine: Engine, db_settings_for: Callable[..., Settings]
) -> Iterator[Engine]:
    """The application's readiness engine on the test database, which is at head."""
    engine = make_readiness_engine(db_settings_for())
    yield engine
    engine.dispose()


@pytest.fixture
def own_schema(
    migrated_engine: Engine, test_database_url: SecretStr
) -> Iterator[Callable[..., Engine]]:
    """An engine whose search_path is a fresh schema: no alembic_version (`None`), or one holding
    the given rows in a column named `column`. The schema is dropped afterwards."""
    schema = f"readiness_{uuid.uuid4().hex[:12]}"
    engines: list[Engine] = []

    def make(rows: list[str] | None, column: str = "version_num") -> Engine:
        with migrated_engine.begin() as connection:
            connection.execute(text(f'CREATE SCHEMA "{schema}"'))
            if rows is not None:
                connection.execute(
                    text(f'CREATE TABLE "{schema}".alembic_version ("{column}" text)')
                )
                for row in rows:
                    connection.execute(
                        text(f'INSERT INTO "{schema}".alembic_version VALUES (:v)'), {"v": row}
                    )
        engine = create_engine(
            test_database_url.get_secret_value(),
            connect_args={"options": f"-c search_path={schema}"},
        )
        engines.append(engine)
        return engine

    yield make
    for engine in engines:
        engine.dispose()
    with migrated_engine.begin() as connection:
        connection.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))


def test_a_migrated_database_is_ready(readiness_engine: Engine) -> None:
    assert DatabaseReadiness(readiness_engine).unready_cause() is None


def test_a_database_at_another_revision_is_not_ready(readiness_engine: Engine) -> None:
    # The code expecting a revision the database does not have: a deploy ahead of its migration.
    probe = DatabaseReadiness(readiness_engine, expected_revision="9999")

    assert probe.unready_cause() == CAUSE_MIGRATION_MISMATCH


def test_a_database_never_migrated_is_not_ready(own_schema: Callable[..., Engine]) -> None:
    assert DatabaseReadiness(own_schema(None)).unready_cause() == CAUSE_MIGRATION_MISSING


@pytest.mark.parametrize(
    "rows",
    [[], [EXPECTED_REVISION, EXPECTED_REVISION], ["0000"]],
    ids=["no_row", "two_rows", "older_revision"],
)
def test_alembic_version_with_anything_but_the_expected_row_is_a_mismatch(
    rows: list[str], own_schema: Callable[..., Engine]
) -> None:
    assert DatabaseReadiness(own_schema(rows)).unready_cause() == CAUSE_MIGRATION_MISMATCH


def test_any_other_database_error_is_reported_by_its_sqlstate(
    own_schema: Callable[..., Engine],
) -> None:
    # The table exists but not the column: 42703 (undefined_column), not a missing migration.
    engine = own_schema([EXPECTED_REVISION], column="revision")

    assert DatabaseReadiness(engine).unready_cause() == "db_error:42703"


def test_an_unreachable_database_is_not_ready() -> None:
    settings = Settings(_env_file=None, database_url=SecretStr(NO_DATABASE_URL))  # type: ignore[call-arg]
    engine = make_readiness_engine(settings)
    try:
        assert DatabaseReadiness(engine).unready_cause() == CAUSE_DB_UNAVAILABLE
    finally:
        engine.dispose()


def test_a_blocked_check_gives_up_after_the_ready_timeout(
    migrated_engine: Engine, db_settings_for: Callable[..., Settings]
) -> None:
    # Another transaction holds a lock that blocks every read of alembic_version: the check waits
    # until the readiness engine's statement timeout (READY_TIMEOUT_S) cancels it.
    ready_timeout_s = 0.5
    engine = make_readiness_engine(db_settings_for(ready_timeout_s=ready_timeout_s))
    locker = migrated_engine.connect()
    try:
        locker.begin()
        # If the statement timeout were ever missing, the check would wait on this lock forever and
        # the job would hang: PostgreSQL ends this session after 5 s instead, and the test fails.
        locker.execute(text("SET idle_in_transaction_session_timeout = '5s'"))
        locker.execute(text("LOCK TABLE alembic_version IN ACCESS EXCLUSIVE MODE"))
        started = time.monotonic()
        cause = DatabaseReadiness(engine).unready_cause()
        elapsed = time.monotonic() - started
    finally:
        # Released before anything else runs: the next test's migration needs the table.
        locker.rollback()
        locker.close()
        engine.dispose()

    assert cause == CAUSE_DB_UNAVAILABLE
    assert ready_timeout_s <= elapsed < ready_timeout_s + 1.5


def test_the_readiness_engine_connects_with_the_ready_timeout_limits(
    migrated_engine: Engine, db_settings_for: Callable[..., Settings]
) -> None:
    engine = make_readiness_engine(db_settings_for(ready_timeout_s=0.5))
    try:
        with engine.connect() as connection:
            statement_timeout = connection.execute(text("SHOW statement_timeout")).scalar_one()
            # What libpq actually used for this connection, not what was passed in.
            parameters = connection.connection.dbapi_connection.info.get_parameters()  # type: ignore[union-attr]
    finally:
        engine.dispose()

    assert statement_timeout == "500ms"
    assert parameters["connect_timeout"] == "2"
