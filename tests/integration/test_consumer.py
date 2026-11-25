"""The consumer against real Kafka and PostgreSQL (phase 3 criteria 5 and 6, phase 5 criterion 2).

Each test has its own topic, consumer group and store ids, so tests never share offsets or rows.
"""

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
from sqlalchemy.exc import DBAPIError

from event_pipeline.broker.config import consumer_config, producer_config
from event_pipeline.broker.publisher import RECEIVED_AT_HEADER, EventPublisher, format_received_at
from event_pipeline.broker.topics import TopicSpec, ensure_topic
from event_pipeline.config import Settings
from event_pipeline.consumer.__main__ import subscribe
from event_pipeline.consumer.loop import CommittedPartition, Headers, run
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


# --- A database failure stops the consumer -------------------------------------------------------


def test_database_failure_stops_the_consumer_without_committing(
    settings: Settings,
    migrated_engine: Engine,
    running: list[RunningConsumer],
    caplog: pytest.LogCaptureFixture,
) -> None:
    store_id = f"store-{uuid.uuid4().hex[:8]}"
    with migrated_engine.begin() as connection:
        connection.execute(
            order_aggregates.insert().values(
                store_id=store_id,
                window_start=WINDOW,
                currency="EUR",
                order_count=1,
                revenue_minor=2**63 - 1,
            )
        )
    produce(settings, encode(event_body(store_id, amount_minor=1)), key=store_id.encode())

    with caplog.at_level(logging.ERROR, logger="event_pipeline.consumer.loop"):
        consumer = RunningConsumer(settings, migrated_engine)
        running.append(consumer)
        consumer.thread.join(TIMEOUT_S)
        consumer.stop()

    assert consumer.error is not None
    assert isinstance(consumer.error.__cause__, DBAPIError)
    assert committed_offsets(settings) == {}
    [record] = [r for r in caplog.records if r.name == "event_pipeline.consumer.loop"]
    assert "sqlstate=22003" in record.getMessage()  # numeric_value_out_of_range
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

    def finish(self, timeout_s: float = CLOSE_TIMEOUT_S + 5) -> int:
        """Wait for the exit (killing it after `timeout_s`) and return its status."""
        try:
            self.output, _ = self.process.communicate(timeout=timeout_s)
        except subprocess.TimeoutExpired:
            self.process.kill()
            self.output, _ = self.process.communicate()
            pytest.fail(f"the consumer process did not exit:\n{self.output}")
        return self.process.returncode

    def wait_for(self, condition: Callable[[], bool], what: str) -> None:
        deadline = time.monotonic() + TIMEOUT_S
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
    query = text("SELECT count(*) FROM pg_stat_activity WHERE :pid = ANY(pg_blocking_pids(pid))")
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
) -> None:
    store_id = f"store-{uuid.uuid4().hex[:8]}"
    with migrated_engine.begin() as connection:
        connection.execute(
            order_aggregates.insert().values(
                store_id=store_id,
                window_start=WINDOW,
                currency="EUR",
                order_count=1,
                revenue_minor=2**63 - 1,
            )
        )
    produce(settings, encode(event_body(store_id, amount_minor=1)), key=store_id.encode())
    consumer = ConsumerProcess(settings, test_database_url, tmp_path)
    processes.append(consumer)

    status = consumer.finish()

    assert status == 1, consumer.output
    errors = [line for line in consumer.output.splitlines() if " ERROR " in line]
    assert len(errors) == 1, consumer.output
    assert "sqlstate=22003" in errors[0]
    # Which statement failed: the upsert, not the processed_events insert.
    assert "event_pipeline/data/aggregates.py:" in errors[0]
    assert " in add_order_to_aggregate)" in errors[0]
    # No traceback, no DETAIL, no statement parameters: nothing of the event but its id.
    assert "Traceback" not in consumer.output
    assert store_id not in consumer.output
    assert PAYLOAD_MARKER not in consumer.output
    assert committed_offsets(settings) == {}
    assert group_members(settings) == 0
