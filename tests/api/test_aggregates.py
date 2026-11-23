"""`GET /stores/{store_id}/aggregates` without a database: parameters, envelope, error mapping.

The routing, validation and error handlers are the real ones; only the service is replaced, except
in the unreachable-database test, which uses a real engine with nothing behind it. The rows
themselves are tested against PostgreSQL in tests/integration/test_query_api.py.
"""

import asyncio
import logging
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx
import pytest
from fastapi import FastAPI
from pydantic import SecretStr
from sqlalchemy import Engine
from sqlalchemy.exc import ProgrammingError
from sqlalchemy.exc import TimeoutError as PoolTimeoutError

from event_pipeline.api import aggregates
from event_pipeline.config import Settings
from event_pipeline.data.aggregates import WindowAggregate
from event_pipeline.domain.queries import AggregateQuery
from event_pipeline.main import create_app_from_settings
from tests.api.helpers import NO_DATABASE_URL, FakePublisher, error_of, make_app, no_database, send

PATH = "/stores/store-7/aggregates"
VALID = {"from": "2026-11-22T12:00:00Z", "to": "2026-11-22T13:00:00Z"}
# Appears only in rejected input or in a database message: never in a response or a log line.
MARKER = "zz-MARKER"


class FakeService:
    """Stands in for `services.queries.store_aggregates`; records what the route asked for."""

    def __init__(self, result: list[WindowAggregate] | Exception) -> None:
        self.result = result
        self.queries: list[AggregateQuery] = []

    def __call__(self, engine: Engine, query: AggregateQuery) -> list[WindowAggregate]:
        self.queries.append(query)
        if isinstance(self.result, Exception):
            raise self.result
        return self.result


@pytest.fixture
def service(monkeypatch: pytest.MonkeyPatch) -> FakeService:
    fake = FakeService([])
    monkeypatch.setattr(aggregates, "store_aggregates", fake)
    return fake


def app(**kwargs: Any) -> FastAPI:
    return make_app(FakePublisher(), engine=no_database(), **kwargs)


def get(application: FastAPI, path: str = PATH, **params: str) -> Any:
    return send(application, method="GET", path=path, params=params)


# --- Normal path ---------------------------------------------------------------------------------


def test_rows_are_returned_as_a_list_with_utc_windows(service: FakeService) -> None:
    window = datetime(2026, 11, 22, 12, 30, tzinfo=UTC)
    service.result = [
        WindowAggregate(window, "EUR", 2, 1250),
        WindowAggregate(window, "USD", 1, 2**63 - 1),
    ]

    response = get(app(), **VALID, currency="EUR")

    assert response.status_code == 200
    assert response.json() == [
        {
            "window_start": "2026-11-22T12:30:00Z",
            "currency": "EUR",
            "order_count": 2,
            "revenue_minor": 1250,
        },
        {
            "window_start": "2026-11-22T12:30:00Z",
            "currency": "USD",
            "order_count": 1,
            "revenue_minor": 2**63 - 1,
        },
    ]
    assert service.queries == [
        AggregateQuery(
            store_id="store-7",
            start=datetime(2026, 11, 22, 12, tzinfo=UTC),
            end=datetime(2026, 11, 22, 13, tzinfo=UTC),
            currency="EUR",
        )
    ]


def test_no_rows_is_an_empty_list(service: FakeService) -> None:
    response = get(app(), path="/stores/never-seen/aggregates", **VALID)

    assert response.status_code == 200
    assert response.json() == []
    assert service.queries[0].currency is None


def test_offsets_in_the_parameters_are_converted_to_utc(service: FakeService) -> None:
    get(app(), **{"from": "2026-11-22T14:00:00+02:00", "to": "2026-11-22T13:00:00Z"})

    assert service.queries[0].start == datetime(2026, 11, 22, 12, tzinfo=UTC)


# --- Invalid parameters --------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("path", "params", "violations"),
    [
        ("/stores/Store-7/aggregates", VALID, [("path.store_id", "invalid_format")]),
        (PATH, {}, [("query.from", "missing_field"), ("query.to", "missing_field")]),
        (PATH, {**VALID, "from": MARKER}, [("query.from", "invalid_format")]),
        (PATH, {**VALID, "to": "2026-11-22T13:00:00"}, [("query.to", "naive_datetime")]),
        (PATH, {**VALID, "currency": MARKER}, [("query.currency", "invalid_format")]),
        (PATH, {**VALID, "to": VALID["from"]}, [("query.to", "out_of_range")]),
        (PATH, {**VALID, "to": "2026-11-23T12:00:01Z"}, [("query.to", "out_of_range")]),
    ],
    ids=[
        "bad_store_id",
        "no_range",
        "bad_from",
        "naive_to",
        "bad_currency",
        "empty_range",
        "range_over_24h",
    ],
)
def test_invalid_parameters_answer_422_without_echoing_them_or_querying(
    path: str, params: dict[str, str], violations: list[tuple[str, str]], service: FakeService
) -> None:
    response = get(app(), path=path, **params)

    assert response.status_code == 422
    error = error_of(response)
    assert error["code"] == "invalid_request"
    assert [(v["path"], v["code"]) for v in error["violations"]] == violations
    assert MARKER not in response.text
    assert service.queries == []


@pytest.mark.parametrize(
    ("query_string", "violations"),
    [
        # A typo would otherwise drop the filter silently and return every currency.
        ("curency=EUR", [("query", "unknown_field")]),
        (f"{MARKER}=1", [("query", "unknown_field")]),
        # Repeated: which value counts would be the framework's choice, not the client's.
        ("from=garbage", [("query.from", "invalid_format")]),
        ("currency=EUR&currency=USD", [("query.currency", "invalid_format")]),
        ("to=2026-11-22T13:00:00Z", [("query.to", "invalid_format")]),
        (
            "currency=EUR&currency=EUR&bogus=1",
            [("query.currency", "invalid_format"), ("query", "unknown_field")],
        ),
    ],
    ids=[
        "typo_in_currency",
        "unknown_name_not_echoed",
        "repeated_from",
        "repeated_currency",
        "repeated_identical_to",
        "repeated_and_unknown",
    ],
)
def test_unknown_or_repeated_parameters_answer_422(
    query_string: str, violations: list[tuple[str, str]], service: FakeService
) -> None:
    # Appended after valid from/to: the valid values come first, so "last value wins" would pass.
    url = f"{PATH}?from={VALID['from']}&to={VALID['to']}&{query_string}"

    response = send(app(), method="GET", path=url)

    assert response.status_code == 422
    error = error_of(response)
    assert error["code"] == "invalid_request"
    assert [(v["path"], v["code"]) for v in error["violations"]] == violations
    assert MARKER not in response.text
    assert service.queries == []


def test_the_maximum_range_comes_from_create_app(service: FakeService) -> None:
    short = app(query_max_range=timedelta(minutes=30))

    response = get(short, **VALID)

    assert response.status_code == 422
    assert error_of(response)["violations"] == [{"path": "query.to", "code": "out_of_range"}]


# --- Database failures ---------------------------------------------------------------------------


def test_unreachable_database_answers_503_and_logs_only_the_error_class(
    caplog: pytest.LogCaptureFixture,
) -> None:
    # The real service and a real engine, with nothing listening behind it.
    with caplog.at_level(logging.WARNING, logger="event_pipeline.api.errors"):
        response = get(app(), **VALID)

    assert response.status_code == 503
    error = error_of(response)
    assert error["code"] == "database_unavailable"
    assert error["violations"] == []
    (record,) = caplog.records
    assert record.levelno == logging.WARNING
    assert record.getMessage().startswith("database unavailable: OperationalError ")
    # psycopg's message names the host, port and user; only the classes are logged.
    assert "127.0.0.1" not in caplog.text
    assert "nobody" not in caplog.text


def test_no_free_pooled_connection_answers_503(
    service: FakeService, caplog: pytest.LogCaptureFixture
) -> None:
    service.result = PoolTimeoutError(f"QueuePool limit reached {MARKER}")

    with caplog.at_level(logging.WARNING, logger="event_pipeline.api.errors"):
        response = get(app(), **VALID)

    assert response.status_code == 503
    assert error_of(response)["code"] == "database_unavailable"
    assert "database unavailable: TimeoutError" in caplog.text
    assert MARKER not in caplog.text


class FakeDriverError(Exception):
    sqlstate = "42P01"

    class diag:  # noqa: N801  (mirrors psycopg's attribute name)
        constraint_name = None


def test_any_other_database_error_answers_500_and_logs_no_message(
    service: FakeService, caplog: pytest.LogCaptureFixture
) -> None:
    service.result = ProgrammingError(
        "SELECT ...", None, FakeDriverError(f"relation does not exist, DETAIL: {MARKER}")
    )

    with caplog.at_level(logging.WARNING, logger="event_pipeline.api.errors"):
        response = get(app(), **VALID)

    assert response.status_code == 500
    assert error_of(response)["code"] == "internal_error"
    (record,) = caplog.records
    assert record.levelno == logging.ERROR
    assert record.getMessage() == (
        "database error: ProgrammingError FakeDriverError sqlstate=42P01"
    )
    assert MARKER not in response.text
    assert MARKER not in caplog.text


# --- The app `make run` serves -------------------------------------------------------------------


def test_the_app_from_settings_queries_through_its_own_engine_and_range() -> None:
    settings = Settings(  # type: ignore[call-arg]
        _env_file=None,
        kafka_bootstrap_servers="127.0.0.1:1",
        database_url=SecretStr(NO_DATABASE_URL),
        query_max_range_s=1800,
        publisher_close_timeout_s=0.5,
    )
    application = create_app_from_settings(settings)

    async def scenario() -> tuple[httpx.Response, httpx.Response]:
        # The lifespan as uvicorn runs it; httpx's ASGITransport does not run it itself.
        async with application.router.lifespan_context(application):
            transport = httpx.ASGITransport(app=application)
            async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
                too_long = await client.get(PATH, params=VALID)
                in_range = await client.get(PATH, params={**VALID, "to": "2026-11-22T12:30:00Z"})
                return too_long, in_range

    too_long, in_range = asyncio.run(scenario())

    # QUERY_MAX_RANGE_S reached the route, and the lifespan's engine (nothing behind it) was used.
    assert too_long.status_code == 422
    assert in_range.status_code == 503
    assert error_of(in_range)["code"] == "database_unavailable"
