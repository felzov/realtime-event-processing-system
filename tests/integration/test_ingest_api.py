"""`POST /events` end to end against the real broker (phase 2, criteria 3 and 5)."""

import asyncio
import logging
import time
from collections.abc import Callable, Iterator
from datetime import UTC, datetime

import httpx
import pytest
from confluent_kafka.admin import AdminClient
from fastapi import FastAPI
from pydantic import SecretStr
from sqlalchemy import Engine, text

from event_pipeline.broker.publisher import RECEIVED_AT_HEADER, Delivered, Delivery, EventPublisher
from event_pipeline.broker.topics import TopicSpec, ensure_topic
from event_pipeline.config import Settings
from event_pipeline.main import create_app_from_settings
from tests.api.helpers import NO_DATABASE_URL, make_app
from tests.integration.kafka_helpers import read_at
from tests.samples import EVENT_ID, STORE_ID, encode, valid_event

TIMEOUT_S = 10.0
CLOSE_TIMEOUT_S = 5.0


class RecordingPublisher:
    """The real publisher, keeping each outcome so the test can read back the reported position."""

    def __init__(self, inner: EventPublisher) -> None:
        self.inner = inner
        self.outcomes: list[Delivery] = []

    async def publish(self, *, key: str, value: bytes, received_at: datetime) -> Delivery:
        outcome = await self.inner.publish(key=key, value=value, received_at=received_at)
        self.outcomes.append(outcome)
        return outcome

    def unready_cause(self) -> str | None:
        return self.inner.unready_cause()


def post_event(app: FastAPI, raw: bytes) -> httpx.Response:
    async def request() -> httpx.Response:
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            return await client.post("/events", content=raw)

    return asyncio.run(request())


@pytest.fixture
def topic(admin: AdminClient, unique_topic: Callable[[], str]) -> str:
    spec = TopicSpec(unique_topic(), partitions=3, replication_factor=1)
    ensure_topic(admin, spec, TIMEOUT_S)
    return spec.name


@pytest.fixture
def publisher(topic: str, settings_for: Callable[..., Settings]) -> Iterator[RecordingPublisher]:
    inner = EventPublisher.from_settings(settings_for(events_topic=topic))
    yield RecordingPublisher(inner)
    inner.close(CLOSE_TIMEOUT_S)


def test_accepted_event_is_on_the_topic_byte_for_byte(
    publisher: RecordingPublisher, topic: str, kafka_bootstrap: str
) -> None:
    raw = encode(valid_event())

    before = datetime.now(UTC)
    response = post_event(make_app(publisher), raw)
    after = datetime.now(UTC)

    assert response.status_code == 202
    assert response.json() == {"event_id": EVENT_ID}
    [delivered] = publisher.outcomes
    assert isinstance(delivered, Delivered)
    message = read_at(kafka_bootstrap, topic, delivered.partition, delivered.offset)
    assert message.value() == raw
    assert message.key() == STORE_ID.encode()
    headers = message.headers()
    # The stub types this as the producer's input (a dict or a list); a consumed message has a list.
    assert isinstance(headers, list)
    [(name, value)] = headers
    assert name == RECEIVED_AT_HEADER
    assert isinstance(value, bytes)
    received_at = datetime.fromisoformat(value.decode("ascii"))
    assert received_at.tzinfo is not None
    assert received_at.utcoffset() == UTC.utcoffset(None)
    assert before <= received_at <= after


def test_unreachable_broker_answers_503_in_bounded_time(unique_topic: Callable[[], str]) -> None:
    # Port 1 on 127.0.0.1: nothing listens, so every connection is refused.
    settings = Settings(  # type: ignore[call-arg]
        _env_file=None,
        kafka_bootstrap_servers="127.0.0.1:1",
        events_topic=unique_topic(),
        kafka_delivery_timeout_ms=2000,
        # Well past the bound asserted below, so the answer is librdkafka's, not the backstop's.
        publish_backstop_s=6.0,
    )
    publisher = EventPublisher.from_settings(settings)
    try:
        started = time.monotonic()
        response = post_event(make_app(publisher), encode(valid_event()))
        elapsed = time.monotonic() - started
    finally:
        publisher.close(timeout_s=1.0)

    assert response.status_code == 503
    assert response.json()["error"]["code"] == "broker_unavailable"
    assert elapsed < settings.kafka_delivery_timeout_ms / 1000 + 2.5


def test_app_from_settings_is_ready_against_the_broker_and_accepts_an_event(
    topic: str,
    kafka_bootstrap: str,
    migrated_engine: Engine,
    db_settings_for: Callable[..., Settings],
) -> None:
    # The app `make run` serves: its lifespan creates and closes the real publisher and engines.
    # The test database (at head), never the development one: `/ready` reads its revision.
    app = create_app_from_settings(
        db_settings_for(kafka_bootstrap_servers=kafka_bootstrap, events_topic=topic)
    )

    async def scenario() -> tuple[httpx.Response, httpx.Response]:
        async with app.router.lifespan_context(app):
            transport = httpx.ASGITransport(app=app)
            async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
                ready = await client.get("/ready")
                accepted = await client.post("/events", content=encode(valid_event()))
                return ready, accepted

    ready, accepted = asyncio.run(scenario())

    assert ready.status_code == 200
    assert accepted.status_code == 202
    assert not app.state.publisher.is_polling()  # closed by the lifespan


def test_app_from_settings_is_not_ready_while_the_database_is_down(
    topic: str,
    settings_for: Callable[..., Settings],
    caplog: pytest.LogCaptureFixture,
) -> None:
    # The broker check passes against the real broker, so the database check runs and fails.
    app = create_app_from_settings(
        settings_for(events_topic=topic, database_url=SecretStr(NO_DATABASE_URL))
    )

    async def scenario() -> httpx.Response:
        async with app.router.lifespan_context(app):
            transport = httpx.ASGITransport(app=app)
            async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
                return await client.get("/ready")

    with caplog.at_level(logging.WARNING, logger="event_pipeline.api.health"):
        ready = asyncio.run(scenario())

    assert ready.status_code == 503
    assert ready.json()["error"]["code"] == "not_ready"
    (record,) = [r for r in caplog.records if r.name == "event_pipeline.api.health"]
    assert record.getMessage() == "not ready: db_unavailable"


def test_the_app_from_settings_bounds_a_blocked_database_check_by_the_ready_timeout(
    topic: str,
    kafka_bootstrap: str,
    migrated_engine: Engine,
    db_settings_for: Callable[..., Settings],
    caplog: pytest.LogCaptureFixture,
) -> None:
    # `/ready` must use the readiness engine (statement timeout READY_TIMEOUT_S), not the query
    # engine (DB_STATEMENT_TIMEOUT_MS, 5 s by default): a lock makes the difference visible.
    ready_timeout_s = 0.5
    app = create_app_from_settings(
        db_settings_for(
            kafka_bootstrap_servers=kafka_bootstrap,
            events_topic=topic,
            ready_timeout_s=ready_timeout_s,
        )
    )

    async def scenario() -> tuple[httpx.Response, float]:
        async with app.router.lifespan_context(app):
            transport = httpx.ASGITransport(app=app)
            async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
                started = time.monotonic()
                response = await client.get("/ready")
                return response, time.monotonic() - started

    locker = migrated_engine.connect()
    try:
        locker.begin()
        # If no statement timeout applied, the check would wait on this lock forever and the job
        # would hang: PostgreSQL ends this session after 5 s instead, and the test fails.
        locker.execute(text("SET idle_in_transaction_session_timeout = '5s'"))
        locker.execute(text("LOCK TABLE alembic_version IN ACCESS EXCLUSIVE MODE"))
        with caplog.at_level(logging.WARNING, logger="event_pipeline.api.health"):
            ready, elapsed = asyncio.run(scenario())
    finally:
        # Released before anything else runs: the next test's migration needs the table.
        locker.rollback()
        locker.close()

    assert ready.status_code == 503
    (record,) = [r for r in caplog.records if r.name == "event_pipeline.api.health"]
    assert record.getMessage() == "not ready: db_unavailable"
    # Broker check plus the database statement timeout; the query engine's would take 5 s.
    assert elapsed < 2.5
