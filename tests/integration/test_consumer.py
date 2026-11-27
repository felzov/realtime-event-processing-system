"""The consumer against real Kafka and PostgreSQL (phase 3 criteria 5 and 6, phase 5 criterion 2,
phase 6 criteria 3 to 6).

Each test has its own topic, consumer group and store ids, so tests never share offsets or rows.
"""

import itertools
import logging
import os
import signal
import subprocess
import sys
import threading
import time
import uuid
from collections.abc import Callable, Iterator, Sequence
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Literal

import pytest
from confluent_kafka import Consumer, KafkaError, KafkaException, Message, Producer, TopicPartition
from confluent_kafka.admin import AdminClient
from pydantic import SecretStr
from sqlalchemy import Connection, Engine, event, func, select, text
from sqlalchemy.exc import OperationalError, ProgrammingError

from event_pipeline.broker.config import consumer_config, producer_config
from event_pipeline.broker.publisher import RECEIVED_AT_HEADER, EventPublisher, format_received_at
from event_pipeline.broker.topics import TopicSpec, ensure_topic
from event_pipeline.config import Settings
from event_pipeline.consumer.__main__ import subscribe
from event_pipeline.consumer.loop import CommittedPartition, Headers, run
from event_pipeline.data.engine import make_engine
from event_pipeline.data.errors import RetryPolicy, sqlstate_of
from event_pipeline.data.tables import dead_letters, order_aggregates, processed_events
from event_pipeline.domain.dead_letters import DeadLetter
from event_pipeline.domain.events import OrderPlacedEvent
from event_pipeline.domain.windows import window_start
from event_pipeline.services.aggregation import ApplyOutcome, apply_order_placed
from event_pipeline.services.dead_letters import store_dead_letter
from tests.api.helpers import make_app
from tests.integration.test_ingest_api import post_event
from tests.samples import encode, valid_event

PARTITIONS = 3
TIMEOUT_S = 20.0
POLL_TIMEOUT_S = 0.2
CLOSE_TIMEOUT_S = 10.0
PAYLOAD_MARKER = "ord-PAYLOAD-MARKER"
WINDOW = datetime(2026, 11, 22, 12, 30, tzinfo=UTC)
# The received_at header of messages produced directly: 30 s after event_body's default
# occurred_at, so on time (ADR 0005). Fixed, so these tests do not depend on the clock.
RECEIVED_AT = WINDOW + timedelta(seconds=45)


@pytest.fixture
def topic(admin: AdminClient, unique_topic: Callable[[], str]) -> str:
    spec = TopicSpec(unique_topic(), partitions=PARTITIONS, replication_factor=1)
    ensure_topic(admin, spec, TIMEOUT_S)
    return spec.name


@pytest.fixture
def settings(
    topic: str, kafka_bootstrap: str, db_settings_for: Callable[..., Settings]
) -> Settings:
    return db_settings_for(
        kafka_bootstrap_servers=kafka_bootstrap,
        events_topic=topic,
        consumer_group_id=f"test.{uuid.uuid4().hex}",
        consumer_poll_timeout_s=POLL_TIMEOUT_S,
    )


def event_body(
    store_id: str,
    *,
    amount_minor: int,
    occurred_at: str = "2026-11-22T12:30:15Z",
    event_id: str | None = None,
) -> dict[str, Any]:
    body = valid_event()
    body["event_id"] = event_id or str(uuid.uuid4())
    body["occurred_at"] = occurred_at
    body["payload"].update(
        {"store_id": store_id, "amount_minor": amount_minor, "order_id": PAYLOAD_MARKER}
    )
    return body


def rfc3339(instant: datetime) -> str:
    return instant.astimezone(UTC).isoformat().replace("+00:00", "Z")


def produce(
    settings: Settings,
    *values: bytes,
    key: bytes = b"store",
    received_at: datetime | None = RECEIVED_AT,
    partition: int | None = None,
) -> None:
    """Produce directly, as the ingest API would: with a received_at header, unless None.

    `partition` overrides the key's partition, for tests that need messages on several.
    """
    headers: Headers = (
        [] if received_at is None else [(RECEIVED_AT_HEADER, format_received_at(received_at))]
    )
    producer = Producer(producer_config(settings))
    for value in values:
        if partition is None:
            producer.produce(settings.events_topic, value=value, key=key, headers=headers)
        else:
            producer.produce(
                settings.events_topic, value=value, key=key, headers=headers, partition=partition
            )
    assert producer.flush(TIMEOUT_S) == 0


# On a fresh broker the first group request creates the offsets topic and loads its coordinator;
# until that is done, coordinator queries fail with one of these (seen in CI). They mean "not yet".
COORDINATOR_NOT_READY = frozenset(
    {
        KafkaError.NOT_COORDINATOR,
        KafkaError.COORDINATOR_LOAD_IN_PROGRESS,
        KafkaError.COORDINATOR_NOT_AVAILABLE,
    }
)


def ask_coordinator[T](query: Callable[[], T]) -> T:
    """Run `query`, retrying while the group coordinator is not ready, for up to TIMEOUT_S.

    Any other error, or the same one past the deadline, is raised.
    """
    deadline = time.monotonic() + TIMEOUT_S
    while True:
        try:
            return query()
        except KafkaException as exc:
            error = exc.args[0] if exc.args else None
            if (
                not isinstance(error, KafkaError)
                or error.code() not in COORDINATOR_NOT_READY
                or time.monotonic() > deadline
            ):
                raise
        time.sleep(0.2)


def committed_offsets(settings: Settings, partitions: int = PARTITIONS) -> dict[int, int]:
    """The group's committed offset per partition; partitions with none are left out."""
    reader = Consumer(
        {
            "bootstrap.servers": settings.kafka_bootstrap_servers,
            "group.id": settings.consumer_group_id,
            "enable.auto.commit": False,
        }
    )
    try:
        wanted = [TopicPartition(settings.events_topic, p) for p in range(partitions)]
        return {
            tp.partition: tp.offset
            for tp in ask_coordinator(lambda: reader.committed(wanted, timeout=TIMEOUT_S))
            if tp.offset >= 0
        }
    finally:
        reader.close()


def end_offsets(settings: Settings, partitions: int = PARTITIONS) -> dict[int, int]:
    """Next offset to be written per partition, for partitions that hold messages."""
    reader = Consumer(
        {
            "bootstrap.servers": settings.kafka_bootstrap_servers,
            "group.id": "unused",
            "enable.auto.commit": False,
        }
    )
    try:
        ends = {}
        for p in range(partitions):
            _, high = reader.get_watermark_offsets(
                TopicPartition(settings.events_topic, p), timeout=TIMEOUT_S
            )
            if high > 0:
                ends[p] = high
        return ends
    finally:
        reader.close()


def counts(engine: Engine, store_id: str) -> list[tuple[str, int, int]]:
    query = (
        select(
            order_aggregates.c.currency,
            order_aggregates.c.order_count,
            order_aggregates.c.revenue_minor,
        )
        .where(order_aggregates.c.store_id == store_id)
        .order_by(order_aggregates.c.window_start, order_aggregates.c.currency)
    )
    with engine.connect() as connection:
        return [tuple(row) for row in connection.execute(query)]


def dead_lettered(engine: Engine, topic: str) -> list[tuple[int, str, str | None, str | None]]:
    """(offset, reason, detail, event_id) of every dead letter read from `topic`, by offset.

    Each test has its own topic, so this sees only that test's dead letters.
    """
    query = (
        select(
            dead_letters.c.source_offset,
            dead_letters.c.reason_code,
            dead_letters.c.detail,
            dead_letters.c.event_id,
        )
        .where(dead_letters.c.source_topic == topic)
        .order_by(dead_letters.c.source_partition, dead_letters.c.source_offset)
    )
    with engine.connect() as connection:
        return [
            (row.source_offset, row.reason_code, row.detail, row.event_id and str(row.event_id))
            for row in connection.execute(query)
        ]


def wait_for(
    condition: Callable[[], bool], what: str, *, watching: "Sequence[RunningConsumer]" = ()
) -> None:
    """Wait for `condition`; fail at once, with the cause, if a watched loop died meanwhile."""
    deadline = time.monotonic() + TIMEOUT_S
    while not condition():
        for consumer in watching:
            if consumer.error is not None:
                cause = consumer.error.__cause__ or consumer.error
                pytest.fail(f"the consumer loop died while waiting for {what}: {cause!r}")
        if time.monotonic() > deadline:
            pytest.fail(f"timed out waiting for {what}")
        time.sleep(0.1)


class RunningConsumer:
    """The real loop on a real consumer, in a thread, until `stop()`."""

    def __init__(
        self,
        settings: Settings,
        engine: Engine,
        wrap: Callable[[Consumer], Any] = lambda consumer: consumer,
        stop_flag: threading.Event | None = None,
        retry: RetryPolicy | None = None,
    ) -> None:
        self.consumer = Consumer(consumer_config(settings))
        # With the process's rebalance callbacks (consumer/__main__.py), so they are exercised too.
        subscribe(self.consumer, settings.events_topic)
        self.stop_flag = stop_flag or threading.Event()
        self.error: BaseException | None = None
        self.outcomes: list[ApplyOutcome] = []

        def apply(event: OrderPlacedEvent) -> ApplyOutcome:
            outcome = apply_order_placed(engine, event)
            self.outcomes.append(outcome)
            return outcome

        def dead_letter(letter: DeadLetter) -> bool:
            return store_dead_letter(engine, letter)

        def target() -> None:
            try:
                run(
                    wrap(self.consumer),
                    apply,
                    dead_letter=dead_letter,
                    stop=self.stop_flag,
                    poll_timeout_s=settings.consumer_poll_timeout_s,
                    lateness=settings.lateness_policy(),
                    retry=retry or settings.db_retry_policy(),
                )
            except BaseException as exc:  # read by the test after join
                self.error = exc

        self.thread = threading.Thread(target=target, daemon=True)
        self.thread.start()

    def stop(self) -> None:
        self.stop_flag.set()
        self.thread.join(TIMEOUT_S)
        assert not self.thread.is_alive(), "the loop did not stop"
        if not self.closed:
            self.closed = True
            self.consumer.close()

    closed = False


@pytest.fixture
def running(settings: Settings, migrated_engine: Engine) -> Iterator[list[RunningConsumer]]:
    started: list[RunningConsumer] = []
    yield started
    # Every consumer, running or already dead, leaves its group.
    for consumer in started:
        consumer.stop()


# --- End to end: POST /events → topic → consumer → PostgreSQL -------------------------------------


def test_events_posted_to_the_api_end_up_in_the_aggregates_once(
    settings: Settings, migrated_engine: Engine, running: list[RunningConsumer]
) -> None:
    store_a, store_b = f"store-{uuid.uuid4().hex[:8]}", f"store-{uuid.uuid4().hex[:8]}"
    # The API stamps received_at with the current time, so the events must be recent to count
    # (ADR 0005): two windows that ended a minute or two ago.
    earlier = window_start(datetime.now(UTC)) - timedelta(minutes=2)
    first, second = (
        rfc3339(earlier + timedelta(seconds=15)),
        rfc3339(earlier + timedelta(minutes=1)),
    )
    retried = event_body(store_a, amount_minor=1000, occurred_at=first)
    bodies = [
        retried,
        event_body(store_a, amount_minor=250, occurred_at=first),
        event_body(store_a, amount_minor=99, occurred_at=second),
        event_body(store_b, amount_minor=500, occurred_at=first),
        retried,  # a client retry: the same event_id, a second message on the topic
    ]
    publisher = EventPublisher.from_settings(settings)
    try:
        app = make_app(publisher, engine=migrated_engine)
        for body in bodies:
            assert post_event(app, encode(body)).status_code == 202
    finally:
        publisher.close(CLOSE_TIMEOUT_S)

    consumer = RunningConsumer(settings, migrated_engine)
    running.append(consumer)
    wait_for(
        lambda: committed_offsets(settings) == end_offsets(settings),
        "every offset committed",
        watching=[consumer],
    )
    consumer.stop()

    assert consumer.error is None
    assert counts(migrated_engine, store_a) == [("EUR", 2, 1250), ("EUR", 1, 99)]
    assert counts(migrated_engine, store_b) == [("EUR", 1, 500)]
    assert sorted(o.value for o in consumer.outcomes) == ["applied"] * 4 + ["duplicate"]
    assert sum(end_offsets(settings).values()) == len(bodies)


def test_invalid_message_is_skipped_and_its_offset_committed(
    settings: Settings,
    migrated_engine: Engine,
    running: list[RunningConsumer],
    caplog: pytest.LogCaptureFixture,
) -> None:
    store_id = f"store-{uuid.uuid4().hex[:8]}"
    # Same key, so both land on one partition in this order.
    produce(
        settings,
        b'{"payload": "' + PAYLOAD_MARKER.encode() + b'"',
        encode(event_body(store_id, amount_minor=700)),
        key=store_id.encode(),
    )

    with caplog.at_level(logging.WARNING, logger="event_pipeline.consumer.loop"):
        consumer = RunningConsumer(settings, migrated_engine)
        running.append(consumer)
        wait_for(
            lambda: committed_offsets(settings) == end_offsets(settings),
            "both committed",
            watching=[consumer],
        )
        consumer.stop()

    assert consumer.error is None
    assert counts(migrated_engine, store_id) == [("EUR", 1, 700)]
    [warning] = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert "invalid_json" in warning.getMessage()
    assert PAYLOAD_MARKER not in caplog.text
    # Kept as it was read, with its key and the received_at header the producer set.
    assert dead_lettered(migrated_engine, settings.events_topic) == [
        (0, "invalid_event", "invalid_json", None)
    ]
    with migrated_engine.connect() as connection:
        row = connection.execute(
            select(dead_letters).where(dead_letters.c.source_topic == settings.events_topic)
        ).one()
    assert row.message_value == b'{"payload": "' + PAYLOAD_MARKER.encode() + b'"'
    assert row.message_key == store_id.encode()
    assert row.received_at_header == format_received_at(RECEIVED_AT)


def test_a_reused_event_id_with_other_content_is_logged_and_not_counted(
    settings: Settings,
    migrated_engine: Engine,
    running: list[RunningConsumer],
    caplog: pytest.LogCaptureFixture,
) -> None:
    store_id = f"store-{uuid.uuid4().hex[:8]}"
    first = event_body(store_id, amount_minor=700)
    reused = event_body(store_id, amount_minor=9, event_id=first["event_id"])
    partition_key = store_id.encode()
    produce(settings, encode(first), encode(reused), key=partition_key)
    # The same content again, as a client retry through the API would arrive: stamped later.
    # received_at is not part of the event, so this is a plain duplicate.
    retried_at = RECEIVED_AT + timedelta(seconds=5)
    produce(settings, encode(first), key=partition_key, received_at=retried_at)

    with caplog.at_level(logging.WARNING, logger="event_pipeline.consumer.loop"):
        consumer = RunningConsumer(settings, migrated_engine)
        running.append(consumer)
        wait_for(
            lambda: committed_offsets(settings) == end_offsets(settings),
            "every offset committed",
            watching=[consumer],
        )
        consumer.stop()

    assert consumer.error is None
    assert consumer.outcomes == [
        ApplyOutcome.APPLIED,
        ApplyOutcome.CONFLICTING_DUPLICATE,
        ApplyOutcome.DUPLICATE,
    ]
    assert counts(migrated_engine, store_id) == [("EUR", 1, 700)]
    [warning] = [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING]
    assert warning.startswith(f"event {first['event_id']} skipped at partition ")
    assert " offset 1: conflicting_duplicate," in warning
    assert PAYLOAD_MARKER not in caplog.text
    assert store_id not in caplog.text
    # The second copy is kept for inspection; the first stays counted.
    assert dead_lettered(migrated_engine, settings.events_topic) == [
        (1, "conflicting_duplicate", None, first["event_id"])
    ]


# --- Late, future and unreadable received_at (ADR 0005) -----------------------------------------


def test_events_that_do_not_count_are_skipped_without_blocking_and_replay_the_same(
    settings: Settings,
    migrated_engine: Engine,
    running: list[RunningConsumer],
    caplog: pytest.LogCaptureFixture,
) -> None:
    store_id = f"store-{uuid.uuid4().hex[:8]}"
    key = store_id.encode()
    occurred = WINDOW + timedelta(seconds=15)  # event_body's default occurred_at
    # One partition (same key), in this order: on time, late, from the future, no header, on time.
    produce(settings, encode(event_body(store_id, amount_minor=1)), key=key)
    late = event_body(store_id, amount_minor=10)
    produce(settings, encode(late), key=key, received_at=occurred + timedelta(minutes=10))
    future = event_body(store_id, amount_minor=100)
    produce(settings, encode(future), key=key, received_at=occurred - timedelta(minutes=2))
    unstamped = event_body(store_id, amount_minor=1000)
    produce(settings, encode(unstamped), key=key, received_at=None)
    produce(settings, encode(event_body(store_id, amount_minor=10_000)), key=key)

    with caplog.at_level(logging.WARNING, logger="event_pipeline.consumer.loop"):
        first = RunningConsumer(settings, migrated_engine)
        running.append(first)
        wait_for(
            lambda: committed_offsets(settings) == end_offsets(settings),
            "every offset committed",
            watching=[first],
        )
        first.stop()

    assert first.error is None
    assert counts(migrated_engine, store_id) == [("EUR", 2, 10_001)]
    assert [o.value for o in first.outcomes] == ["applied", "applied"]
    skipped = [r.getMessage() for r in caplog.records if " skipped at " in r.getMessage()]
    # The key decides the partition; all five messages share it.
    partition = skipped[0].split(" partition ")[1].split()[0]
    assert skipped == [
        f"event {late['event_id']} skipped at partition {partition} offset 1: "
        "late_event, received 600.000 s after occurred_at",
        f"event {future['event_id']} skipped at partition {partition} offset 2: "
        "future_event, received 120.000 s before occurred_at",
        f"event {unstamped['event_id']} skipped at partition {partition} offset 3: "
        "invalid_received_at",
    ]
    assert PAYLOAD_MARKER not in caplog.text
    expected_dead_letters = [
        (1, "late_event", "received 600.000 s after occurred_at", late["event_id"]),
        (2, "future_event", "received 120.000 s before occurred_at", future["event_id"]),
        (3, "invalid_received_at", None, unstamped["event_id"]),
    ]
    assert dead_lettered(migrated_engine, settings.events_topic) == expected_dead_letters
    # Byte for byte, with the received_at each was stamped with: a replay sends these (ADR 0006).
    with migrated_engine.connect() as connection:
        rows = connection.execute(
            select(dead_letters.c.message_value, dead_letters.c.received_at_header)
            .where(dead_letters.c.source_topic == settings.events_topic)
            .order_by(dead_letters.c.source_offset)
        ).all()
    assert [tuple(row) for row in rows] == [
        (encode(late), format_received_at(occurred + timedelta(minutes=10))),
        (encode(future), format_received_at(occurred - timedelta(minutes=2))),
        (encode(unstamped), None),
    ]

    # A replay (a new group reads the topic from the start) classifies every message the same way:
    # the two on-time events are duplicates now, and the other three are skipped again.
    replay_settings = settings.model_copy(update={"consumer_group_id": f"test.{uuid.uuid4().hex}"})
    replay = RunningConsumer(replay_settings, migrated_engine)
    running.append(replay)
    wait_for(
        lambda: committed_offsets(replay_settings) == end_offsets(replay_settings),
        "every offset committed again",
        watching=[replay],
    )
    replay.stop()

    assert replay.error is None
    assert counts(migrated_engine, store_id) == [("EUR", 2, 10_001)]
    assert [o.value for o in replay.outcomes] == ["duplicate", "duplicate"]
    # The same positions: no second dead letter for any of them.
    assert dead_lettered(migrated_engine, settings.events_topic) == expected_dead_letters


# --- Crash point B: database committed, offset not ------------------------------------------------


class SimulatedCrashError(Exception):
    pass


class CrashBeforeFirstCommit:
    """The real consumer, except that the first offset commit fails: the consumer stops after the
    database transaction committed (crash point B, docs/architecture.md). A process kill at that
    point is phase 7; for the offsets the outcome is the same, nothing committed."""

    def __init__(self, consumer: Consumer) -> None:
        self.consumer = consumer

    def poll(self, timeout: float) -> Message | None:
        return self.consumer.poll(timeout)

    def commit(
        self, *, message: Message, asynchronous: Literal[False]
    ) -> Sequence[CommittedPartition] | None:
        raise SimulatedCrashError


def test_restart_after_crash_point_b_redelivers_and_changes_nothing(
    settings: Settings, migrated_engine: Engine, running: list[RunningConsumer]
) -> None:
    store_id = f"store-{uuid.uuid4().hex[:8]}"
    produce(
        settings,
        *(encode(event_body(store_id, amount_minor=100 * n)) for n in (1, 2, 3)),
        key=store_id.encode(),
    )

    crashed = RunningConsumer(settings, migrated_engine, wrap=CrashBeforeFirstCommit)
    running.append(crashed)
    crashed.thread.join(TIMEOUT_S)
    crashed.stop()

    assert crashed.error is not None
    assert isinstance(crashed.error.__cause__, SimulatedCrashError)
    assert crashed.outcomes == [ApplyOutcome.APPLIED]  # the first event reached the database
    assert counts(migrated_engine, store_id) == [("EUR", 1, 100)]
    assert committed_offsets(settings) == {}

    # A new consumer in the same group starts from the last committed offset: the beginning.
    restarted = RunningConsumer(settings, migrated_engine)
    running.append(restarted)
    wait_for(
        lambda: committed_offsets(settings) == end_offsets(settings),
        "every offset committed",
        watching=[restarted],
    )
    restarted.stop()

    assert restarted.error is None
    assert restarted.outcomes == [
        ApplyOutcome.DUPLICATE,
        ApplyOutcome.APPLIED,
        ApplyOutcome.APPLIED,
    ]
    assert counts(migrated_engine, store_id) == [("EUR", 3, 600)]


# --- Restarts: where a consumer resumes (phase 5) -------------------------------------------------


def recorded(engine: Engine, *bodies: dict[str, Any]) -> int:
    """How many of these events processed_events holds."""
    ids = [uuid.UUID(body["event_id"]) for body in bodies]
    query = select(func.count()).where(processed_events.c.event_id.in_(ids))
    with engine.connect() as connection:
        return connection.execute(query).scalar_one()


class StopAfterMessages:
    """The real consumer, except that the stop flag is set when the n-th message is polled: the
    loop finishes that message and stops, while later messages may already sit in the client's
    prefetch queue, read from the broker but not handled."""

    def __init__(self, consumer: Consumer, n: int, stop: threading.Event) -> None:
        self.consumer, self.remaining, self.stop = consumer, n, stop

    def poll(self, timeout: float) -> Message | None:
        message = self.consumer.poll(timeout)
        if message is not None:
            self.remaining -= 1
            if self.remaining == 0:
                self.stop.set()
        return message

    def commit(
        self, *, message: Message, asynchronous: Literal[False]
    ) -> Sequence[CommittedPartition] | None:
        return self.consumer.commit(message=message, asynchronous=asynchronous)


def test_clean_restart_reads_only_the_messages_after_the_committed_offset(
    settings: Settings, migrated_engine: Engine, running: list[RunningConsumer]
) -> None:
    store_id = f"store-{uuid.uuid4().hex[:8]}"
    amounts = (1, 2, 4, 8, 16)
    produce(
        settings,
        *(encode(event_body(store_id, amount_minor=n)) for n in amounts),
        key=store_id.encode(),
    )

    # Stopped mid-stream: after the second of five messages.
    stop = threading.Event()
    first = RunningConsumer(
        settings,
        migrated_engine,
        wrap=lambda consumer: StopAfterMessages(consumer, 2, stop),
        stop_flag=stop,
    )
    running.append(first)
    first.thread.join(TIMEOUT_S)
    first.stop()
    assert first.error is None
    assert first.outcomes == [ApplyOutcome.APPLIED] * 2
    # Only what the loop handled is committed: neither close() nor prefetching commits more.
    [(partition, offset)] = committed_offsets(settings).items()
    assert offset == 2

    restarted = RunningConsumer(settings, migrated_engine)
    running.append(restarted)
    wait_for(
        lambda: committed_offsets(settings) == end_offsets(settings),
        "every offset committed",
        watching=[restarted],
    )
    restarted.stop()

    assert restarted.error is None
    # Three apply calls: had the restart re-read offsets 0 and 1, they would show up as duplicates.
    assert restarted.outcomes == [ApplyOutcome.APPLIED] * 3
    assert committed_offsets(settings) == {partition: len(amounts)}
    assert counts(migrated_engine, store_id) == [("EUR", len(amounts), sum(amounts))]


def fail_next_commit(engine: Engine) -> None:
    """Make the engine's next COMMIT raise before it is sent to PostgreSQL: crash point A at its
    latest moment (docs/architecture.md). The event is applied in one transaction, so that is the
    event's COMMIT; that both of its statements share it is test_aggregation's to prove. A process
    kill is phase 7; for the database the outcome is the same, the transaction never commits."""
    fired = False

    def fail(connection: Connection) -> None:
        nonlocal fired
        if not fired:
            fired = True
            raise SimulatedCrashError

    event.listen(engine, "commit", fail)


def test_restart_after_crash_point_a_applies_the_event_exactly_once(
    settings: Settings, migrated_engine: Engine, running: list[RunningConsumer]
) -> None:
    store_id = f"store-{uuid.uuid4().hex[:8]}"
    bodies = [event_body(store_id, amount_minor=100 * n) for n in (1, 2, 3)]
    produce(settings, *(encode(body) for body in bodies), key=store_id.encode())

    fail_next_commit(migrated_engine)
    crashed = RunningConsumer(settings, migrated_engine)
    running.append(crashed)
    crashed.thread.join(TIMEOUT_S)
    crashed.stop()

    assert crashed.error is not None
    assert isinstance(crashed.error.__cause__, SimulatedCrashError)
    assert crashed.outcomes == []  # the first event's apply raised
    # Nothing of the first event reached either system.
    assert recorded(migrated_engine, bodies[0]) == 0
    assert counts(migrated_engine, store_id) == []
    assert committed_offsets(settings) == {}

    restarted = RunningConsumer(settings, migrated_engine)
    running.append(restarted)
    wait_for(
        lambda: committed_offsets(settings) == end_offsets(settings),
        "every offset committed",
        watching=[restarted],
    )
    restarted.stop()

    assert restarted.error is None
    # The first event is redelivered and applied now, once: not a duplicate of anything.
    assert restarted.outcomes == [ApplyOutcome.APPLIED] * 3
    assert recorded(migrated_engine, *bodies) == 3
    assert counts(migrated_engine, store_id) == [("EUR", 3, 600)]


def test_a_dead_letter_that_fails_to_commit_stops_the_consumer_and_is_written_on_restart(
    settings: Settings, migrated_engine: Engine, running: list[RunningConsumer]
) -> None:
    # The invalid message comes first, so the engine's first COMMIT is its dead letter's.
    store_id = f"store-{uuid.uuid4().hex[:8]}"
    produce(
        settings,
        b"{not json",
        encode(event_body(store_id, amount_minor=700)),
        key=store_id.encode(),
    )

    fail_next_commit(migrated_engine)
    crashed = RunningConsumer(settings, migrated_engine)
    running.append(crashed)
    crashed.thread.join(TIMEOUT_S)
    crashed.stop()

    assert crashed.error is not None
    assert isinstance(crashed.error.__cause__, SimulatedCrashError)
    # Neither the dead letter nor the offset: the message is not lost, it will be read again.
    assert dead_lettered(migrated_engine, settings.events_topic) == []
    assert committed_offsets(settings) == {}
    assert crashed.outcomes == []

    restarted = RunningConsumer(settings, migrated_engine)
    running.append(restarted)
    wait_for(
        lambda: committed_offsets(settings) == end_offsets(settings),
        "every offset committed",
        watching=[restarted],
    )
    restarted.stop()

    assert restarted.error is None
    assert dead_lettered(migrated_engine, settings.events_topic) == [
        (0, "invalid_event", "invalid_json", None)
    ]
    assert restarted.outcomes == [ApplyOutcome.APPLIED]
    assert counts(migrated_engine, store_id) == [("EUR", 1, 700)]


def test_a_new_group_rereads_the_topic_and_changes_no_aggregate(
    settings: Settings, migrated_engine: Engine, running: list[RunningConsumer]
) -> None:
    # Store n on partition n - 1, with n messages: every partition holds a different count.
    stores = [f"store-{uuid.uuid4().hex[:8]}" for _ in range(PARTITIONS)]
    for n, store_id in enumerate(stores, start=1):
        produce(
            settings,
            *(encode(event_body(store_id, amount_minor=10 * n)) for _ in range(n)),
            key=store_id.encode(),
            partition=n - 1,
        )
    log_end = {n - 1: n for n in range(1, PARTITIONS + 1)}
    assert end_offsets(settings) == log_end
    total = sum(log_end.values())

    first = RunningConsumer(settings, migrated_engine)
    running.append(first)
    wait_for(
        lambda: committed_offsets(settings) == log_end,
        "every offset committed",
        watching=[first],
    )
    first.stop()
    assert first.error is None
    assert first.outcomes == [ApplyOutcome.APPLIED] * total
    before = {store_id: counts(migrated_engine, store_id) for store_id in stores}
    assert before == {
        store_id: [("EUR", n, 10 * n * n)] for n, store_id in enumerate(stores, start=1)
    }

    # A new group has no committed offsets, so it starts from the beginning (earliest).
    replay_settings = settings.model_copy(update={"consumer_group_id": f"test.{uuid.uuid4().hex}"})
    replay = RunningConsumer(replay_settings, migrated_engine)
    running.append(replay)
    wait_for(
        lambda: committed_offsets(replay_settings) == log_end,
        "every offset committed by the new group",
        watching=[replay],
    )
    replay.stop()

    assert replay.error is None
    assert replay.outcomes == [ApplyOutcome.DUPLICATE] * total
    assert {store_id: counts(migrated_engine, store_id) for store_id in stores} == before
    # Each group keeps its own offsets: the replay did not move the first group's.
    assert committed_offsets(settings) == log_end


# --- Database errors: rejected events and transient failures (ADR 0006, decisions 4 to 6) -----


def store_at_the_bigint_limit(engine: Engine) -> str:
    """A store whose WINDOW row in EUR already holds the largest `bigint` revenue."""
    store_id = f"store-{uuid.uuid4().hex[:8]}"
    with engine.begin() as connection:
        connection.execute(
            order_aggregates.insert().values(
                store_id=store_id,
                window_start=WINDOW,
                currency="EUR",
                order_count=1,
                revenue_minor=2**63 - 1,
            )
        )
    return store_id


def test_an_event_the_database_rejects_is_dead_lettered_and_the_next_is_applied(
    settings: Settings,
    migrated_engine: Engine,
    running: list[RunningConsumer],
    caplog: pytest.LogCaptureFixture,
) -> None:
    # Until phase 6, slice 3 this overflow stopped the consumer on every restart: its partition
    # was blocked. Now it is a dead letter, and the next event on the same partition counts.
    store_id = store_at_the_bigint_limit(migrated_engine)
    overflowing = event_body(store_id, amount_minor=1)
    next_minute = event_body(store_id, amount_minor=7, occurred_at="2026-11-22T12:31:15Z")
    produce(settings, encode(overflowing), encode(next_minute), key=store_id.encode())
    log_end = end_offsets(settings)

    with caplog.at_level(logging.WARNING, logger="event_pipeline.consumer.loop"):
        consumer = RunningConsumer(settings, migrated_engine)
        running.append(consumer)
        wait_for(
            lambda: committed_offsets(settings) == log_end, "both offsets", watching=[consumer]
        )

    assert consumer.outcomes == [ApplyOutcome.APPLIED]
    [(_, reason, detail, event_id)] = dead_lettered(migrated_engine, settings.events_topic)
    assert reason == "database_rejected"
    assert event_id == overflowing["event_id"]
    # numeric_value_out_of_range; classes and SQLSTATE only.
    assert detail == "DataError NumericValueOutOfRange sqlstate=22003"
    # The rejected apply was rolled back: the full row is unchanged, the event not recorded.
    assert counts(migrated_engine, store_id) == [("EUR", 1, 2**63 - 1), ("EUR", 1, 7)]
    assert recorded(migrated_engine, overflowing) == 0
    assert [r.levelno for r in caplog.records] == [logging.WARNING]
    assert "database_rejected" in caplog.records[0].getMessage()
    assert store_id not in caplog.text
    assert PAYLOAD_MARKER not in caplog.text


def impatient_engine(db_settings_for: Callable[..., Settings]) -> Engine:
    """The application's engine with a 300 ms statement timeout, so a held lock times out fast."""
    return make_engine(db_settings_for(db_statement_timeout_ms=300))


def retry_warnings(caplog: pytest.LogCaptureFixture) -> list[str]:
    return [
        r.getMessage()
        for r in caplog.records
        if r.levelno == logging.WARNING and "retrying in" in r.getMessage()
    ]


def test_a_lock_that_clears_within_the_attempts_applies_the_event_once(
    settings: Settings,
    migrated_engine: Engine,
    db_settings_for: Callable[..., Settings],
    running: list[RunningConsumer],
    row_lock: Callable[[str], tuple[Connection, int]],
    caplog: pytest.LogCaptureFixture,
) -> None:
    # A real transient error: the upsert waits on a row another transaction holds until the
    # statement timeout cancels it (57014). The lock goes away before the attempts run out.
    store_id = f"store-{uuid.uuid4().hex[:8]}"
    blocker, _ = row_lock(store_id)
    produce(settings, encode(event_body(store_id, amount_minor=4)), key=store_id.encode())
    engine = impatient_engine(db_settings_for)
    try:
        with caplog.at_level(logging.WARNING, logger="event_pipeline.consumer.loop"):
            consumer = RunningConsumer(settings, engine)
            running.append(consumer)
            wait_for(lambda: retry_warnings(caplog) != [], "a retry", watching=[consumer])
            blocker.rollback()
            wait_for(
                lambda: committed_offsets(settings) == end_offsets(settings),
                "the offset",
                watching=[consumer],
            )
    finally:
        engine.dispose()

    assert consumer.outcomes == [ApplyOutcome.APPLIED]
    assert counts(migrated_engine, store_id) == [("EUR", 1, 4)]
    assert dead_lettered(migrated_engine, settings.events_topic) == []
    warnings = retry_warnings(caplog)
    assert 1 <= len(warnings) <= 2
    assert "(attempt 1 of 3): OperationalError QueryCanceled sqlstate=57014" in warnings[0]
    assert [r for r in caplog.records if r.levelno >= logging.ERROR] == []
    assert PAYLOAD_MARKER not in caplog.text


def test_a_lock_held_past_every_attempt_stops_the_consumer_without_committing(
    settings: Settings,
    migrated_engine: Engine,
    db_settings_for: Callable[..., Settings],
    running: list[RunningConsumer],
    row_lock: Callable[[str], tuple[Connection, int]],
    caplog: pytest.LogCaptureFixture,
) -> None:
    store_id = f"store-{uuid.uuid4().hex[:8]}"
    row_lock(store_id)
    produce(settings, encode(event_body(store_id, amount_minor=4)), key=store_id.encode())
    engine = impatient_engine(db_settings_for)
    try:
        with caplog.at_level(logging.WARNING, logger="event_pipeline.consumer.loop"):
            consumer = RunningConsumer(settings, engine)
            running.append(consumer)
            consumer.thread.join(TIMEOUT_S)
    finally:
        engine.dispose()

    assert consumer.error is not None
    cause = consumer.error.__cause__
    assert isinstance(cause, OperationalError)
    assert sqlstate_of(cause) == "57014"
    # Not dead-lettered, not committed: a restart once the database is healthy applies it.
    assert committed_offsets(settings) == {}
    assert dead_lettered(migrated_engine, settings.events_topic) == []
    assert len(retry_warnings(caplog)) == 2
    [stopping] = [r for r in caplog.records if r.levelno == logging.ERROR]
    assert stopping.getMessage().endswith("; apply failed 3 of 3 attempts")
    assert store_id not in caplog.text
    assert PAYLOAD_MARKER not in caplog.text


def without_the_fingerprint_column(engine: Engine) -> None:
    """Make the engine's processed_events insert name a column the table does not have, so
    PostgreSQL itself answers 42703, as for code deployed before its migration ran. The schema
    is not touched: other tests share it."""

    def rename(
        conn: Connection,
        cursor: Any,
        statement: str,
        parameters: Any,
        context: Any,
        executemany: bool,
    ) -> tuple[str, Any]:
        if statement.startswith("INSERT INTO processed_events"):
            # Only the first occurrence, the column list: the bind parameter keeps its name.
            statement = statement.replace("body_sha256", "body_sha256_not_migrated", 1)
        return statement, parameters

    event.listen(engine, "before_cursor_execute", rename, retval=True)


def test_a_missing_column_stops_the_consumer_instead_of_dead_lettering(
    settings: Settings,
    db_settings_for: Callable[..., Settings],
    migrated_engine: Engine,
    running: list[RunningConsumer],
    caplog: pytest.LogCaptureFixture,
) -> None:
    # Class 42 is transient on purpose (ADR 0006, decision 5): a schema behind the code must stop
    # the consumer, not move every event into dead_letters.
    store_id = f"store-{uuid.uuid4().hex[:8]}"
    produce(settings, encode(event_body(store_id, amount_minor=4)), key=store_id.encode())
    engine = make_engine(db_settings_for())
    without_the_fingerprint_column(engine)
    try:
        with caplog.at_level(logging.WARNING, logger="event_pipeline.consumer.loop"):
            consumer = RunningConsumer(
                settings,
                engine,
                retry=RetryPolicy(attempts=2, first_backoff_s=0.1, max_backoff_s=0.1),
            )
            running.append(consumer)
            consumer.thread.join(TIMEOUT_S)
    finally:
        engine.dispose()

    assert consumer.error is not None
    cause = consumer.error.__cause__
    assert isinstance(cause, ProgrammingError)
    assert sqlstate_of(cause) == "42703"  # undefined_column
    assert len(retry_warnings(caplog)) == 1
    [stopping] = [r for r in caplog.records if r.levelno == logging.ERROR]
    assert "ProgrammingError UndefinedColumn sqlstate=42703" in stopping.getMessage()
    assert stopping.getMessage().endswith("; apply failed 2 of 2 attempts")
    assert committed_offsets(settings) == {}
    assert dead_lettered(migrated_engine, settings.events_topic) == []
    assert counts(migrated_engine, store_id) == []
    assert store_id not in caplog.text
    assert PAYLOAD_MARKER not in caplog.text


# --- The process: signals and exit status ---------------------------------------------------------


class ConsumerProcess:
    """`python -m event_pipeline.consumer` with this test's topic, group and database.

    Started in an empty directory and without the developer's setting variables, so a local
    `.env` or exported setting cannot change what the test runs.
    """

    def __init__(
        self, settings: Settings, database_url: SecretStr, cwd: Path, **overrides: str
    ) -> None:
        env = {k: v for k, v in os.environ.items() if k.lower() not in Settings.model_fields}
        env.update(
            DATABASE_URL=database_url.get_secret_value(),
            KAFKA_BOOTSTRAP_SERVERS=settings.kafka_bootstrap_servers,
            EVENTS_TOPIC=settings.events_topic,
            CONSUMER_GROUP_ID=settings.consumer_group_id,
            CONSUMER_POLL_TIMEOUT_S=str(POLL_TIMEOUT_S),
            CONSUMER_CLOSE_TIMEOUT_S=str(CLOSE_TIMEOUT_S),
        )
        env.update(overrides)
        self.process = subprocess.Popen(
            [sys.executable, "-m", "event_pipeline.consumer"],
            env=env,
            cwd=cwd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )
        self.output = ""

    def alive(self) -> bool:
        return self.process.poll() is None

    def kill(self) -> None:
        """SIGKILL: no handler, no `finally`, no close(); the group still counts the member until
        its session timeout runs out."""
        self.process.send_signal(signal.SIGKILL)
        self.output, _ = self.process.communicate(timeout=TIMEOUT_S)
        assert self.process.returncode == -signal.SIGKILL, self.output

    def finish(self, timeout_s: float = CLOSE_TIMEOUT_S + 5) -> int:
        """Wait for the exit (killing it after `timeout_s`) and return its status."""
        try:
            self.output, _ = self.process.communicate(timeout=timeout_s)
        except subprocess.TimeoutExpired:
            self.process.kill()
            self.output, _ = self.process.communicate()
            pytest.fail(f"the consumer process did not exit:\n{self.output}")
        return self.process.returncode

    def wait_for(
        self, condition: Callable[[], bool], what: str, *, timeout_s: float = TIMEOUT_S
    ) -> None:
        deadline = time.monotonic() + timeout_s
        while not condition():
            if not self.alive() or time.monotonic() > deadline:
                status = self.finish(timeout_s=1)
                pytest.fail(f"waiting for {what}; process status {status}:\n{self.output}")
            time.sleep(0.1)


@pytest.fixture
def processes(tmp_path: Path) -> Iterator[list[ConsumerProcess]]:
    started: list[ConsumerProcess] = []
    yield started
    for consumer in started:
        if consumer.alive():
            consumer.process.kill()
            consumer.process.communicate()


def group_members(settings: Settings) -> int:
    admin = AdminClient({"bootstrap.servers": settings.kafka_bootstrap_servers})

    def describe() -> int:
        futures = admin.describe_consumer_groups([settings.consumer_group_id])
        return len(futures[settings.consumer_group_id].result(timeout=TIMEOUT_S).members)

    return ask_coordinator(describe)


def blocked_by(engine: Engine, pid: int) -> bool:
    return blocked_pids(engine, pid) != []


def blocked_pids(engine: Engine, pid: int) -> list[int]:
    """The backends waiting on a lock that backend `pid` holds."""
    query = text("SELECT pid FROM pg_stat_activity WHERE :pid = ANY(pg_blocking_pids(pid))")
    with engine.connect() as connection:
        return list(connection.execute(query, {"pid": pid}).scalars())


def backend_exists(engine: Engine, pid: int) -> bool:
    query = text("SELECT count(*) FROM pg_stat_activity WHERE pid = :pid")
    with engine.connect() as connection:
        return bool(connection.execute(query, {"pid": pid}).scalar_one())


@pytest.fixture
def row_lock(migrated_engine: Engine) -> Iterator[Callable[[str], tuple[Connection, int]]]:
    """Holds an uncommitted insert of a store's aggregate row, so the consumer's upsert of that
    row waits for it: an event caught in the middle of its transaction."""
    held: list[Connection] = []

    def hold(store_id: str) -> tuple[Connection, int]:
        connection = migrated_engine.connect()
        held.append(connection)
        connection.begin()
        connection.execute(
            order_aggregates.insert().values(
                store_id=store_id,
                window_start=WINDOW,
                currency="EUR",
                order_count=1,
                revenue_minor=2,
            )
        )
        pid = connection.execute(text("SELECT pg_backend_pid()")).scalar_one()
        return connection, pid

    yield hold
    for connection in held:
        connection.close()


@pytest.mark.parametrize("signum", [signal.SIGTERM, signal.SIGINT], ids=["sigterm", "sigint"])
def test_signal_stops_the_idle_process_cleanly(
    signum: signal.Signals,
    settings: Settings,
    migrated_engine: Engine,
    test_database_url: SecretStr,
    tmp_path: Path,
    processes: list[ConsumerProcess],
) -> None:
    store_id = f"store-{uuid.uuid4().hex[:8]}"
    produce(settings, encode(event_body(store_id, amount_minor=4200)), key=store_id.encode())
    consumer = ConsumerProcess(settings, test_database_url, tmp_path)
    processes.append(consumer)
    consumer.wait_for(lambda: committed_offsets(settings) == end_offsets(settings), "the offset")

    started = time.monotonic()
    consumer.process.send_signal(signum)
    status = consumer.finish()
    elapsed = time.monotonic() - started

    assert status == 0, consumer.output
    assert elapsed < CLOSE_TIMEOUT_S, consumer.output
    assert f"{signum.name} received" in consumer.output
    assert "event_pipeline.consumer stopped\n" in consumer.output
    assert counts(migrated_engine, store_id) == [("EUR", 1, 4200)]
    # close() ran: the consumer left its group instead of lingering until the session timeout.
    assert group_members(settings) == 0
    assert PAYLOAD_MARKER not in consumer.output


def test_sigterm_during_an_event_finishes_and_commits_it_first(
    settings: Settings,
    migrated_engine: Engine,
    test_database_url: SecretStr,
    tmp_path: Path,
    processes: list[ConsumerProcess],
    row_lock: Callable[[str], tuple[Connection, int]],
) -> None:
    store_id = f"store-{uuid.uuid4().hex[:8]}"
    blocker, blocker_pid = row_lock(store_id)
    produce(settings, encode(event_body(store_id, amount_minor=4)), key=store_id.encode())
    consumer = ConsumerProcess(settings, test_database_url, tmp_path)
    processes.append(consumer)
    consumer.wait_for(lambda: blocked_by(migrated_engine, blocker_pid), "the upsert to wait")

    consumer.process.send_signal(signal.SIGTERM)
    time.sleep(0.5)  # the signal has arrived; the event is still mid-transaction
    assert consumer.alive()
    blocker.commit()
    status = consumer.finish()

    assert status == 0, consumer.output
    assert counts(migrated_engine, store_id) == [("EUR", 2, 6)]
    assert committed_offsets(settings) == end_offsets(settings)
    assert group_members(settings) == 0


def test_shutdown_past_the_close_timeout_ends_the_process_with_status_2(
    settings: Settings,
    migrated_engine: Engine,
    test_database_url: SecretStr,
    tmp_path: Path,
    processes: list[ConsumerProcess],
    row_lock: Callable[[str], tuple[Connection, int]],
) -> None:
    store_id = f"store-{uuid.uuid4().hex[:8]}"
    blocker, blocker_pid = row_lock(store_id)
    produce(settings, encode(event_body(store_id, amount_minor=4)), key=store_id.encode())
    # A statement timeout longer than the close timeout, so the blocked upsert outlasts it.
    consumer = ConsumerProcess(
        settings,
        test_database_url,
        tmp_path,
        CONSUMER_CLOSE_TIMEOUT_S="1.5",
        DB_STATEMENT_TIMEOUT_MS="30000",
        # One attempt, so the 30 s timeout stays within the default max.poll.interval's bound.
        DB_APPLY_ATTEMPTS="1",
    )
    processes.append(consumer)
    consumer.wait_for(lambda: blocked_by(migrated_engine, blocker_pid), "the upsert to wait")

    consumer.process.send_signal(signal.SIGTERM)
    status = consumer.finish(timeout_s=10)
    blocker.rollback()

    assert status == 2, consumer.output
    assert "did not stop within 1.5 s" in consumer.output
    assert committed_offsets(settings) == {}
    # The killed transaction left nothing: neither the event nor the blocker's row.
    assert counts(migrated_engine, store_id) == []


def test_failure_ends_the_process_with_status_1_and_one_line_without_the_payload(
    settings: Settings,
    migrated_engine: Engine,
    test_database_url: SecretStr,
    tmp_path: Path,
    processes: list[ConsumerProcess],
    row_lock: Callable[[str], tuple[Connection, int]],
) -> None:
    # Until phase 6, slice 3 a bigint overflow stopped the process; it is a dead letter now. A
    # lock held past every attempt is a failure that still stops it.
    store_id = f"store-{uuid.uuid4().hex[:8]}"
    row_lock(store_id)
    produce(settings, encode(event_body(store_id, amount_minor=1)), key=store_id.encode())
    consumer = ConsumerProcess(
        settings, test_database_url, tmp_path, DB_STATEMENT_TIMEOUT_MS="300", DB_APPLY_ATTEMPTS="2"
    )
    processes.append(consumer)

    status = consumer.finish()

    assert status == 1, consumer.output
    errors = [line for line in consumer.output.splitlines() if " ERROR " in line]
    assert len(errors) == 1, consumer.output
    assert "sqlstate=57014" in errors[0]  # query_canceled: the statement timeout
    # Which statement failed: the upsert, not the processed_events insert.
    assert "event_pipeline/data/aggregates.py:" in errors[0]
    assert " in add_order_to_aggregate)" in errors[0]
    assert errors[0].endswith("; apply failed 2 of 2 attempts")
    retries = [line for line in consumer.output.splitlines() if "retrying in 0.5 s" in line]
    assert len(retries) == 1, consumer.output
    # No traceback, no DETAIL, no statement parameters: nothing of the event but its id.
    assert "Traceback" not in consumer.output
    assert store_id not in consumer.output
    assert PAYLOAD_MARKER not in consumer.output
    assert committed_offsets(settings) == {}
    assert group_members(settings) == 0


def test_sigterm_during_a_retry_pause_stops_at_once_with_status_1(
    settings: Settings,
    migrated_engine: Engine,
    test_database_url: SecretStr,
    tmp_path: Path,
    processes: list[ConsumerProcess],
    row_lock: Callable[[str], tuple[Connection, int]],
) -> None:
    # Five attempts: 0.5 + 1 + 2 + 2 s of pauses still ahead after the first failure, so a
    # prompt exit shows the stop request cut the pause short instead of waiting the attempts out.
    store_id = f"store-{uuid.uuid4().hex[:8]}"
    row_lock(store_id)
    produce(settings, encode(event_body(store_id, amount_minor=1)), key=store_id.encode())
    consumer = ConsumerProcess(
        settings, test_database_url, tmp_path, DB_STATEMENT_TIMEOUT_MS="300", DB_APPLY_ATTEMPTS="5"
    )
    processes.append(consumer)
    # A reader thread collects the output, so waiting for a line never blocks on a silent
    # process and the deadline below always applies.
    lines: list[str] = []

    def read_output() -> None:
        assert consumer.process.stdout is not None
        lines.extend(consumer.process.stdout)

    reader = threading.Thread(target=read_output, name="consumer-output", daemon=True)
    reader.start()

    def output() -> str:
        return "".join(lines)

    deadline = time.monotonic() + TIMEOUT_S
    while not any("retrying in 0.5 s" in line for line in lines):
        if not consumer.alive() or time.monotonic() > deadline:
            consumer.process.kill()
            reader.join(timeout=5)
            exited = consumer.process.poll()
            pytest.fail(f"waiting for the first retry; process status {exited}:\n{output()}")
        time.sleep(0.05)
    signalled_at = time.monotonic()
    consumer.process.send_signal(signal.SIGTERM)
    try:
        status = consumer.process.wait(timeout=CLOSE_TIMEOUT_S + 5)
    except subprocess.TimeoutExpired:
        consumer.process.kill()
        reader.join(timeout=5)
        pytest.fail(f"the consumer process did not exit:\n{output()}")
    elapsed_s = time.monotonic() - signalled_at
    reader.join(timeout=5)
    consumer.output = output()

    assert status == 1, consumer.output
    # Well under the 4.5 s of pauses left (plus 300 ms per attempt); leaving the group included.
    assert elapsed_s < 3.0, consumer.output
    errors = [line for line in consumer.output.splitlines() if " ERROR " in line]
    assert len(errors) == 1, consumer.output
    assert "sqlstate=57014" in errors[0]
    # The signal usually lands in the first pause ("failed 1 of 5"); on a slow machine it can
    # land during attempt 2, which is then the one cut short. Either way the pause is not waited.
    assert "; apply failed " in errors[0]
    assert errors[0].endswith(" of 5 attempts, and a stop was requested before the next"), (
        consumer.output
    )
    assert committed_offsets(settings) == {}
    assert group_members(settings) == 0
    assert store_id not in consumer.output
    assert PAYLOAD_MARKER not in consumer.output


# --- The process killed: SIGKILL (phase 7 criteria 3 and 4) ---------------------------------------

# The broker's minimum. A killed process's partitions go to its successor once this runs out:
# 6.0 s here against 45.0 s with the default (phase 7 spike).
KILL_SESSION_TIMEOUT_MS = "6000"
# A process started after a kill first waits for the killed member's session to run out, so its
# waits get that on top of the usual budget.
KILL_TIMEOUT_S = TIMEOUT_S + int(KILL_SESSION_TIMEOUT_MS) / 1000
KILLS = 3
EVENTS_PER_ROUND = 200
# How long a frozen process is given for requests it sent before the freeze to be answered, so the
# broker's and the database's state no longer move while the test reads them. A commit answered
# later still is caught by settled_commit() after the kill.
SETTLE_S = 0.2
# Two reads of a dead group's committed offset this far apart that agree: nothing is in flight.
COMMIT_SETTLE_S = 0.5


def applied(engine: Engine, event_ids: Sequence[uuid.UUID]) -> int:
    """How many of `event_ids` are in processed_events: applied, each exactly once (its PK)."""
    query = (
        select(func.count())
        .select_from(processed_events)
        .where(processed_events.c.event_id.in_(event_ids))
    )
    with engine.connect() as connection:
        return int(connection.execute(query).scalar_one())


def settled_commit(settings: Settings) -> int:
    """Partition 0's committed offset once it stopped moving: after a kill, a commit request the
    process sent before it died may still be answered."""
    deadline = time.monotonic() + TIMEOUT_S
    last = committed_offsets(settings).get(0, 0)
    while True:
        time.sleep(COMMIT_SETTLE_S)
        current = committed_offsets(settings).get(0, 0)
        if current == last:
            return current
        if time.monotonic() > deadline:
            pytest.fail("the committed offset kept moving after the kill")
        last = current


def freeze_at_crash_point_b(
    consumer: ConsumerProcess,
    settings: Settings,
    engine: Engine,
    event_ids: Sequence[uuid.UUID],
) -> None:
    """Leave `consumer` stopped (SIGSTOP) at crash point B: an event's transaction committed, its
    offset not. Every event is on partition 0, so that is "more applied than committed".

    The window is one offset-commit round trip per event, too short to hit with a kill at a random
    moment (a version of this test that did so passed with deduplication switched off). So the
    process is frozen at varying moments and the two systems compared until the state is found.
    Probing starts once the process is applying events: frozen most of the time, it would barely
    get through its start-up and group join.
    """
    before = applied(engine, event_ids)
    consumer.wait_for(
        lambda: applied(engine, event_ids) > before,
        "the process to apply events",
        timeout_s=KILL_TIMEOUT_S,
    )
    jitter_s = itertools.cycle([0.0, 0.003, 0.007, 0.011, 0.017])
    deadline = time.monotonic() + TIMEOUT_S
    while True:
        if not consumer.alive() or time.monotonic() > deadline:
            status = consumer.finish(timeout_s=1)
            pytest.fail(f"crash point B not reached; process status {status}:\n{consumer.output}")
        time.sleep(next(jitter_s))
        consumer.process.send_signal(signal.SIGSTOP)
        time.sleep(SETTLE_S)
        done, committed = applied(engine, event_ids), committed_offsets(settings).get(0, 0)
        if committed > done:
            # Every message here is a valid, on-time event, so each committed offset needs its
            # applied event: a kill now would lose one.
            pytest.fail(f"offset ahead of the database: {committed} committed, {done} applied")
        if done > committed:
            return
        if done == len(event_ids):
            pytest.fail("the stream was used up before crash point B was found")
        consumer.process.send_signal(signal.SIGCONT)


def test_sigkill_at_crash_point_b_redelivers_and_every_aggregate_stays_exact(
    settings: Settings,
    migrated_engine: Engine,
    test_database_url: SecretStr,
    tmp_path: Path,
    processes: list[ConsumerProcess],
) -> None:
    # Each round adds events, starts a process and kills it at crash point B. The event applied
    # but not committed is redelivered to the next process, and the duplicates table keeps it from
    # counting twice. The last process runs to the end of the topic.
    stores = [f"store-{uuid.uuid4().hex[:8]}" for _ in range(6)]
    expected = dict.fromkeys(stores, (0, 0))
    event_ids: list[uuid.UUID] = []
    producer = Producer(producer_config(settings))
    headers: Headers = [(RECEIVED_AT_HEADER, format_received_at(RECEIVED_AT))]

    for round_ in range(KILLS + 1):
        for i in range(EVENTS_PER_ROUND):
            store_id = stores[i % len(stores)]
            amount = round_ * 1000 + i + 1
            body = event_body(store_id, amount_minor=amount)
            event_ids.append(uuid.UUID(body["event_id"]))
            count, revenue = expected[store_id]
            expected[store_id] = (count + 1, revenue + amount)
            producer.produce(
                settings.events_topic,
                value=encode(body),
                key=store_id.encode(),
                headers=headers,
                partition=0,
            )
        assert producer.flush(TIMEOUT_S) == 0

        consumer = ConsumerProcess(
            settings,
            test_database_url,
            tmp_path,
            CONSUMER_SESSION_TIMEOUT_MS=KILL_SESSION_TIMEOUT_MS,
        )
        processes.append(consumer)
        if round_ == KILLS:
            break
        freeze_at_crash_point_b(consumer, settings, migrated_engine, event_ids)
        consumer.kill()
        # Still true after the kill: the next process starts from the uncommitted offset.
        assert applied(migrated_engine, event_ids) > settled_commit(settings)

    consumer.wait_for(
        lambda: committed_offsets(settings) == end_offsets(settings),
        "every offset committed",
        timeout_s=KILL_TIMEOUT_S,
    )
    consumer.process.send_signal(signal.SIGTERM)
    assert consumer.finish() == 0, consumer.output

    assert applied(migrated_engine, event_ids) == len(event_ids)
    for store_id in stores:
        count, revenue = expected[store_id]
        assert counts(migrated_engine, store_id) == [("EUR", count, revenue)], store_id
    assert dead_lettered(migrated_engine, settings.events_topic) == []
    assert PAYLOAD_MARKER not in consumer.output


def test_sigkill_inside_the_transaction_rolls_it_back_and_the_restart_applies_it_once(
    settings: Settings,
    migrated_engine: Engine,
    test_database_url: SecretStr,
    tmp_path: Path,
    processes: list[ConsumerProcess],
    row_lock: Callable[[str], tuple[Connection, int]],
) -> None:
    # The process dies with its transaction open: processed_events row inserted, the upsert waiting
    # on the test's row lock. Nothing was committed, so nothing of it may remain.
    store_id = f"store-{uuid.uuid4().hex[:8]}"
    blocker, blocker_pid = row_lock(store_id)
    body = event_body(store_id, amount_minor=4)
    produce(settings, encode(body), key=store_id.encode())
    first = ConsumerProcess(
        settings,
        test_database_url,
        tmp_path,
        CONSUMER_SESSION_TIMEOUT_MS=KILL_SESSION_TIMEOUT_MS,
        # The orphan below waits on the lock until its statement timeout cancels the upsert; 30 s
        # keeps it waiting through the test's steps on a slow machine. One attempt, so the 30 s
        # stays within the default max.poll.interval's bound.
        DB_STATEMENT_TIMEOUT_MS="30000",
        DB_APPLY_ATTEMPTS="1",
    )
    processes.append(first)
    first.wait_for(lambda: blocked_by(migrated_engine, blocker_pid), "the upsert to wait")

    first.kill()
    # PostgreSQL does not notice a dead client while its backend waits on a lock: the orphaned
    # transaction keeps waiting (bounded by its statement timeout) and holds its uncommitted
    # processed_events row meanwhile.
    [orphan_pid] = blocked_pids(migrated_engine, blocker_pid)
    blocker.commit()
    # The orphan runs its upsert once the lock is gone, then finds its client gone and ends; it
    # never received a COMMIT. Checked after it ended: before, its rows would be invisible anyway.
    wait_for(lambda: not backend_exists(migrated_engine, orphan_pid), "the orphan to end")
    assert applied(migrated_engine, [uuid.UUID(body["event_id"])]) == 0
    assert counts(migrated_engine, store_id) == [("EUR", 1, 2)]  # the blocker's row only
    assert committed_offsets(settings) == {}

    second = ConsumerProcess(
        settings, test_database_url, tmp_path, CONSUMER_SESSION_TIMEOUT_MS=KILL_SESSION_TIMEOUT_MS
    )
    processes.append(second)
    second.wait_for(
        lambda: committed_offsets(settings) == end_offsets(settings),
        "the offset",
        timeout_s=KILL_TIMEOUT_S,
    )
    second.process.send_signal(signal.SIGTERM)
    assert second.finish() == 0, second.output

    assert applied(migrated_engine, [uuid.UUID(body["event_id"])]) == 1
    assert counts(migrated_engine, store_id) == [("EUR", 2, 6)]
    assert dead_lettered(migrated_engine, settings.events_topic) == []
