"""The Alembic migrations against the real test database: up → down → up, the schema they leave
behind, the CHECK constraints of revisions 0001 and 0002, and what 0002 does to existing rows."""

import os
import subprocess
import sys
import uuid
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from alembic.autogenerate import compare_metadata
from alembic.migration import MigrationContext
from psycopg import errors
from pydantic import SecretStr
from sqlalchemy import Connection, Engine, Table, inspect, text
from sqlalchemy.exc import IntegrityError

from event_pipeline.data.tables import (
    EXPECTED_REVISION,
    metadata,
    order_aggregates,
    processed_events,
)
from event_pipeline.domain.events import OrderPlacedEvent, parse_event
from event_pipeline.domain.windows import window_start
from tests.migration_helpers import ALEMBIC_INI, migrate
from tests.samples import encode, valid_event

APP_TABLES = {"processed_events", "order_aggregates"}
WINDOW = datetime(2026, 11, 22, 12, 30, tzinfo=UTC)


def table_names(engine: Engine) -> set[str]:
    return set(inspect(engine).get_table_names())


def current_revision(engine: Engine) -> str | None:
    with engine.connect() as connection:
        return MigrationContext.configure(connection).get_current_revision()


def test_upgrade_downgrade_upgrade(db_engine: Engine) -> None:
    # Start from base whatever state an earlier run left behind.
    migrate(db_engine, "down", "base")
    assert table_names(db_engine).isdisjoint(APP_TABLES)
    assert current_revision(db_engine) is None

    migrate(db_engine, "up", "head")
    assert table_names(db_engine) >= APP_TABLES
    assert current_revision(db_engine) == EXPECTED_REVISION

    migrate(db_engine, "down", "base")
    assert table_names(db_engine).isdisjoint(APP_TABLES)
    assert current_revision(db_engine) is None

    # The downgrade left nothing behind that blocks a second upgrade (a type, an index, a row in
    # alembic_version).
    migrate(db_engine, "up", "head")
    assert table_names(db_engine) >= APP_TABLES
    assert current_revision(db_engine) == EXPECTED_REVISION


def test_alembic_command_line_migrates_the_database_in_database_url(
    db_engine: Engine, test_database_url: SecretStr
) -> None:
    # What `make db-migrate` runs: env.py without a connection handed in, reading DATABASE_URL
    # through Settings and building the engine with make_engine.
    migrate(db_engine, "down", "base")
    env = {**os.environ, "DATABASE_URL": test_database_url.get_secret_value()}

    result = subprocess.run(
        [sys.executable, "-m", "alembic", "-c", str(ALEMBIC_INI), "upgrade", "head"],
        env=env,
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )

    # The output, not the environment, is shown on failure: the URL holds the password.
    assert result.returncode == 0, result.stderr
    assert current_revision(db_engine) == EXPECTED_REVISION


def test_migrated_schema_matches_the_table_definitions(migrated_engine: Engine) -> None:
    # Columns, types, nullability and server defaults of data/tables.py against the database.
    # compare_metadata does not compare primary keys or CHECK constraints: the keys are compared
    # below, the CHECKs by the insert tests further down.
    with migrated_engine.connect() as connection:
        context = MigrationContext.configure(
            connection, opts={"compare_type": True, "compare_server_default": True}
        )
        differences = compare_metadata(context, metadata)

    assert differences == []


@pytest.mark.parametrize("table", [processed_events, order_aggregates], ids=lambda t: t.name)
def test_primary_keys_match_the_table_definitions(migrated_engine: Engine, table: Table) -> None:
    key = inspect(migrated_engine).get_pk_constraint(table.name)

    assert key["name"] == table.primary_key.name
    assert key["constrained_columns"] == [column.name for column in table.primary_key.columns]


def test_aggregate_key_order_serves_the_query_api(migrated_engine: Engine) -> None:
    # One store, a window_start range, sorted by window_start then currency: one index range scan.
    key = inspect(migrated_engine).get_pk_constraint("order_aggregates")

    assert key["constrained_columns"] == ["store_id", "window_start", "currency"]


@pytest.fixture
def connection(migrated_engine: Engine) -> Iterator[Connection]:
    # Every write below is rolled back, so the tests leave the tables empty.
    with migrated_engine.connect() as connection:
        transaction = connection.begin()
        yield connection
        transaction.rollback()


def aggregate_row(**overrides: Any) -> dict[str, Any]:
    row = {
        "store_id": "store-1",
        "window_start": WINDOW,
        "currency": "EUR",
        "order_count": 1,
        "revenue_minor": 1250,
    }
    return {**row, **overrides}


def insert_aggregate(connection: Connection, **overrides: Any) -> None:
    # A savepoint, so a rejected row does not abort the rest of the test's transaction.
    with connection.begin_nested():
        connection.execute(order_aggregates.insert().values(aggregate_row(**overrides)))


def test_a_valid_aggregate_row_is_stored_with_defaults(connection: Connection) -> None:
    insert_aggregate(connection, store_id="a" * 64, revenue_minor=0)

    # Filtered: other tests (test_aggregation.py) commit rows of their own stores.
    query = order_aggregates.select().where(order_aggregates.c.store_id == "a" * 64)
    row = connection.execute(query).one()
    assert row.store_id == "a" * 64
    assert row.revenue_minor == 0
    assert row.updated_at is not None


def test_window_alignment_does_not_depend_on_the_session_time_zone(connection: Connection) -> None:
    # Amsterdam was UTC+00:19:32 in 1930. Truncating to the minute in that zone would turn a
    # whole UTC minute into "unaligned"; the CHECK truncates in UTC, so the row is accepted.
    connection.execute(text("SET LOCAL TIME ZONE 'Europe/Amsterdam'"))

    insert_aggregate(connection, window_start=datetime(1930, 1, 1, tzinfo=UTC))


def accepted_event(occurred_at: str, **payload: Any) -> OrderPlacedEvent:
    """An event the contract accepts, parsed the way the consumer will parse it."""
    event = valid_event()
    event["occurred_at"] = occurred_at
    event["payload"].update(payload)
    return parse_event(encode(event))


@pytest.mark.parametrize(
    ("occurred_at", "payload"),
    [
        ("2026-11-22T12:30:00Z", {"store_id": "my_store"}),
        ("2026-11-22T12:30:00Z", {"store_id": "a"}),
        ("2026-11-22T12:30:00Z", {"store_id": "123"}),
        ("2026-11-22T12:30:00Z", {"store_id": "a-b_c-" + "9" * 58}),
        ("2026-11-22T12:30:00Z", {"currency": "JPY"}),
        ("2026-11-22T18:15:59.999999+05:45", {}),
        ("2026-11-22T09:00:30-03:30", {}),
        ("1969-12-31T23:59:59.5Z", {}),
    ],
    ids=[
        "store_id_underscore",
        "store_id_one_character",
        "store_id_digits",
        "store_id_64_characters",
        "currency_other_code",
        "window_from_plus_0545_end_of_minute",
        "window_from_minus_0330",
        "window_before_1970",
    ],
)
def test_check_constraints_accept_rows_built_from_accepted_events(
    connection: Connection, occurred_at: str, payload: dict[str, Any]
) -> None:
    # The other side of the CHECKs: a CHECK stricter than the contract would make the database
    # refuse an event parse_event accepted, and the consumer would stop on it (ADR 0004,
    # decision 6) again after every restart.
    event = accepted_event(occurred_at, **payload)

    insert_aggregate(
        connection,
        store_id=event.payload.store_id,
        currency=event.payload.currency,
        window_start=window_start(event.occurred_at),
    )


@pytest.mark.parametrize(
    ("overrides", "constraint"),
    [
        (
            {"window_start": WINDOW + timedelta(seconds=30)},
            "ck_order_aggregates_window_start_minute",
        ),
        (
            {"window_start": WINDOW + timedelta(microseconds=1)},
            "ck_order_aggregates_window_start_minute",
        ),
        ({"currency": "eur"}, "ck_order_aggregates_currency_format"),
        ({"currency": "EU"}, "ck_order_aggregates_currency_format"),
        ({"currency": "EURO"}, "ck_order_aggregates_currency_format"),
        ({"store_id": ""}, "ck_order_aggregates_store_id_format"),
        ({"store_id": "Store-1"}, "ck_order_aggregates_store_id_format"),
        ({"store_id": "store 1"}, "ck_order_aggregates_store_id_format"),
        ({"store_id": "store.1"}, "ck_order_aggregates_store_id_format"),
        ({"store_id": "store-1\n"}, "ck_order_aggregates_store_id_format"),
        ({"store_id": "störe"}, "ck_order_aggregates_store_id_format"),
        ({"store_id": "a" * 65}, "ck_order_aggregates_store_id_format"),
        ({"order_count": 0}, "ck_order_aggregates_order_count_positive"),
        ({"order_count": -1}, "ck_order_aggregates_order_count_positive"),
        ({"revenue_minor": -1}, "ck_order_aggregates_revenue_minor_non_negative"),
    ],
    ids=[
        "window_start_mid_minute",
        "window_start_one_microsecond_late",
        "currency_lowercase",
        "currency_too_short",
        "currency_too_long",
        "store_id_empty",
        "store_id_uppercase",
        "store_id_space",
        "store_id_dot",
        "store_id_trailing_newline",
        "store_id_non_ascii",
        "store_id_too_long",
        "order_count_zero",
        "order_count_negative",
        "revenue_negative",
    ],
)
def test_check_constraints_reject_rows_the_contract_rejects(
    connection: Connection, overrides: dict[str, Any], constraint: str
) -> None:
    with pytest.raises(IntegrityError) as exc_info:
        insert_aggregate(connection, **overrides)

    assert isinstance(exc_info.value.orig, errors.CheckViolation)
    assert exc_info.value.orig.diag.constraint_name == constraint


def test_one_row_per_store_window_and_currency(connection: Connection) -> None:
    insert_aggregate(connection)
    insert_aggregate(connection, currency="USD")
    insert_aggregate(connection, window_start=WINDOW + timedelta(minutes=1))

    with pytest.raises(IntegrityError) as exc_info:
        insert_aggregate(connection, order_count=5)

    assert isinstance(exc_info.value.orig, errors.UniqueViolation)
    assert exc_info.value.orig.diag.constraint_name == "pk_order_aggregates"


def test_an_event_id_is_recorded_once(connection: Connection) -> None:
    event_id = uuid.uuid4()
    insert = text("INSERT INTO processed_events (event_id) VALUES (:event_id)")
    connection.execute(insert, {"event_id": event_id})

    with pytest.raises(IntegrityError) as exc_info, connection.begin_nested():
        connection.execute(insert, {"event_id": event_id})

    assert isinstance(exc_info.value.orig, errors.UniqueViolation)
    assert exc_info.value.orig.diag.constraint_name == "pk_processed_events"


# --- Revision 0002: processed_events.body_sha256 (ADR 0004, decision 13) -------------------------


def processed_rows(engine: Engine) -> dict[uuid.UUID, bytes | None]:
    with engine.connect() as connection:
        rows = connection.execute(text("SELECT event_id, body_sha256 FROM processed_events"))
        return {row.event_id: row.body_sha256 for row in rows}


def column_names(engine: Engine, table: str) -> set[str]:
    return {column["name"] for column in inspect(engine).get_columns(table)}


def test_revision_0002_keeps_existing_rows_without_a_fingerprint_both_ways(
    db_engine: Engine,
) -> None:
    migrate(db_engine, "down", "base")
    migrate(db_engine, "up", "0001")
    before = uuid.uuid4()
    with db_engine.begin() as connection:
        connection.execute(
            text("INSERT INTO processed_events (event_id) VALUES (:id)"), {"id": before}
        )

    # Up: the existing row stays, with no fingerprint (nothing to backfill from).
    migrate(db_engine, "up", "0002")
    assert processed_rows(db_engine) == {before: None}
    after = uuid.uuid4()
    with db_engine.begin() as connection:
        connection.execute(processed_events.insert().values(event_id=after, body_sha256=bytes(32)))

    # Down: both event_ids stay; only the fingerprints go.
    migrate(db_engine, "down", "0001")
    assert "body_sha256" not in column_names(db_engine, "processed_events")
    with db_engine.connect() as connection:
        ids = connection.execute(text("SELECT event_id FROM processed_events")).scalars()
        assert set(ids) == {before, after}

    # Up again: every row is now like one from before the revision.
    migrate(db_engine, "up", "head")
    assert processed_rows(db_engine) == {before: None, after: None}


@pytest.mark.parametrize("length", [0, 31, 33])
def test_a_fingerprint_must_be_32_bytes(connection: Connection, length: int) -> None:
    with pytest.raises(IntegrityError) as exc_info, connection.begin_nested():
        connection.execute(
            processed_events.insert().values(event_id=uuid.uuid4(), body_sha256=bytes(length))
        )

    assert isinstance(exc_info.value.orig, errors.CheckViolation)
    assert exc_info.value.orig.diag.constraint_name == "ck_processed_events_body_sha256_length"


def test_a_row_without_a_fingerprint_is_accepted(connection: Connection) -> None:
    connection.execute(processed_events.insert().values(event_id=uuid.uuid4(), body_sha256=None))
