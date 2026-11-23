"""The aggregate query against the real test database (phase 3 acceptance criterion 7).

Rows are inserted directly into order_aggregates, so these tests depend on the table only, not on
the consumer. Every test uses its own store ids; the database keeps committed rows between tests,
so no assertion is table-wide.
"""

import logging
import uuid
from collections.abc import Callable, Iterator
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from fastapi import FastAPI
from pydantic import SecretStr
from sqlalchemy import Engine, create_engine, text

from event_pipeline.config import Settings
from event_pipeline.data.engine import make_engine
from event_pipeline.data.tables import order_aggregates
from event_pipeline.main import create_app
from tests.api.helpers import FakePublisher, error_of, send

NOON = datetime(2026, 11, 22, 12, 0, tzinfo=UTC)
MINUTE = timedelta(minutes=1)
BIGINT_MAX = 2**63 - 1


@pytest.fixture
def new_store() -> Callable[[], str]:
    return lambda: f"store-{uuid.uuid4().hex[:12]}"


@pytest.fixture
def insert(migrated_engine: Engine) -> Callable[..., None]:
    """Insert aggregate rows directly, bypassing the apply service."""

    def insert_rows(store_id: str, *rows: tuple[datetime, str, int, int]) -> None:
        with migrated_engine.begin() as connection:
            connection.execute(
                order_aggregates.insert(),
                [
                    {
                        "store_id": store_id,
                        "window_start": window_start,
                        "currency": currency,
                        "order_count": count,
                        "revenue_minor": revenue,
                    }
                    for window_start, currency, count, revenue in rows
                ],
            )

    return insert_rows


@pytest.fixture
def app(migrated_engine: Engine) -> FastAPI:
    return create_app(FakePublisher(), engine=migrated_engine)


def query(app: FastAPI, store_id: str, **params: str) -> Any:
    response = send(app, method="GET", path=f"/stores/{store_id}/aggregates", params=params)
    assert response.status_code == 200, response.text
    return response.json()


def row(window_start: str, currency: str, count: int, revenue: int) -> dict[str, Any]:
    return {
        "window_start": window_start,
        "currency": currency,
        "order_count": count,
        "revenue_minor": revenue,
    }


def test_rows_in_the_half_open_range_come_back_sorted(
    app: FastAPI, insert: Callable[..., None], new_store: Callable[[], str]
) -> None:
    store_id = new_store()
    # Inserted out of order; the response is sorted by window_start, then currency.
    insert(
        store_id,
        (NOON + 30 * MINUTE, "EUR", 9, 900),  # == to: excluded
        (NOON + MINUTE, "EUR", 3, 300),
        (NOON, "USD", 2, 200),
        (NOON - MINUTE, "EUR", 8, 800),  # before from: excluded
        (NOON, "EUR", 1, 100),  # == from: included
        (NOON + 29 * MINUTE, "GBP", 4, BIGINT_MAX),
    )

    rows = query(app, store_id, **{"from": "2026-11-22T12:00:00Z", "to": "2026-11-22T12:30:00Z"})

    assert rows == [
        row("2026-11-22T12:00:00Z", "EUR", 1, 100),
        row("2026-11-22T12:00:00Z", "USD", 2, 200),
        row("2026-11-22T12:01:00Z", "EUR", 3, 300),
        row("2026-11-22T12:29:00Z", "GBP", 4, BIGINT_MAX),
    ]


def test_a_bound_inside_a_window_selects_by_window_start(
    app: FastAPI, insert: Callable[..., None], new_store: Callable[[], str]
) -> None:
    # The 12:00 window starts before `from`, so it is out; the 12:01 window starts before `to`, so
    # it is in, although part of it lies after `to`.
    store_id = new_store()
    insert(store_id, (NOON, "EUR", 1, 100), (NOON + MINUTE, "EUR", 2, 200))

    rows = query(app, store_id, **{"from": "2026-11-22T12:00:30Z", "to": "2026-11-22T12:01:30Z"})

    assert rows == [row("2026-11-22T12:01:00Z", "EUR", 2, 200)]


def test_parameters_with_an_offset_select_the_same_rows(
    app: FastAPI, insert: Callable[..., None], new_store: Callable[[], str]
) -> None:
    store_id = new_store()
    insert(store_id, (NOON, "EUR", 1, 100))

    rows = query(
        app, store_id, **{"from": "2026-11-22T14:00:00+02:00", "to": "2026-11-22T07:01:00-05:00"}
    )

    assert rows == [row("2026-11-22T12:00:00Z", "EUR", 1, 100)]


def test_the_currency_filter_keeps_only_that_currency(
    app: FastAPI, insert: Callable[..., None], new_store: Callable[[], str]
) -> None:
    store_id = new_store()
    insert(store_id, (NOON, "EUR", 1, 100), (NOON, "USD", 2, 200), (NOON + MINUTE, "USD", 3, 300))

    rows = query(
        app,
        store_id,
        **{"from": "2026-11-22T12:00:00Z", "to": "2026-11-22T13:00:00Z", "currency": "USD"},
    )

    assert rows == [
        row("2026-11-22T12:00:00Z", "USD", 2, 200),
        row("2026-11-22T12:01:00Z", "USD", 3, 300),
    ]


def test_an_unknown_store_and_other_stores_rows_are_not_returned(
    app: FastAPI, insert: Callable[..., None], new_store: Callable[[], str]
) -> None:
    other = new_store()
    insert(other, (NOON, "EUR", 1, 100))
    params = {"from": "2026-11-22T12:00:00Z", "to": "2026-11-22T13:00:00Z"}

    assert query(app, new_store(), **params) == []
    # A store whose id is a prefix of another's matches only itself.
    assert query(app, other[:-1], **params) == []
    assert query(app, other, **params) == [row("2026-11-22T12:00:00Z", "EUR", 1, 100)]


@pytest.fixture
def kolkata_engine(migrated_engine: Engine, test_database_url: SecretStr) -> Iterator[Engine]:
    """An engine whose sessions use another time zone than the server's (UTC in Compose and CI).

    Not `make_engine`: the time zone needs its own startup option. `migrated_engine` is requested
    so the schema is at head before this engine is used.
    """
    engine = create_engine(
        test_database_url.get_secret_value(),
        connect_args={"options": "-c timezone=Asia/Kolkata"},
        hide_parameters=True,
    )
    yield engine
    engine.dispose()


def test_windows_come_back_in_utc_whatever_the_session_time_zone(
    kolkata_engine: Engine, insert: Callable[..., None], new_store: Callable[[], str]
) -> None:
    with kolkata_engine.connect() as connection:
        assert connection.execute(text("SHOW timezone")).scalar_one() == "Asia/Kolkata"
    store_id = new_store()
    insert(store_id, (NOON, "EUR", 1, 100))
    app = create_app(FakePublisher(), engine=kolkata_engine)

    rows = query(app, store_id, **{"from": "2026-11-22T12:00:00Z", "to": "2026-11-22T12:01:00Z"})

    # PostgreSQL returns timestamptz in the session's zone (17:30+05:30); the API speaks UTC.
    assert rows == [row("2026-11-22T12:00:00Z", "EUR", 1, 100)]


def test_a_query_past_the_statement_timeout_answers_503(
    migrated_engine: Engine,
    db_settings_for: Callable[..., Settings],
    caplog: pytest.LogCaptureFixture,
) -> None:
    # The application's own engine with a short statement timeout; another transaction holds a
    # lock that blocks every read of the table, so the select waits until the server cancels it.
    engine = make_engine(db_settings_for(db_statement_timeout_ms=300))
    locker = migrated_engine.connect()
    try:
        locker.begin()
        locker.execute(text("LOCK TABLE order_aggregates IN ACCESS EXCLUSIVE MODE"))
        app = create_app(FakePublisher(), engine=engine)

        with caplog.at_level(logging.WARNING, logger="event_pipeline.api.errors"):
            response = send(
                app,
                method="GET",
                path="/stores/store-locked/aggregates",
                params={"from": "2026-11-22T12:00:00Z", "to": "2026-11-22T13:00:00Z"},
            )
    finally:
        # Released before anything else runs: the next test's migration needs the table.
        locker.rollback()
        locker.close()
        engine.dispose()

    assert response.status_code == 503
    assert error_of(response)["code"] == "database_unavailable"
    (record,) = caplog.records
    assert record.getMessage() == (
        "database unavailable: OperationalError QueryCanceled sqlstate=57014"
    )
