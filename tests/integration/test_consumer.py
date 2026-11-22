"""The consumer against real Kafka and PostgreSQL (phase 3 acceptance criteria 5 and 6).

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
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal

import pytest
from confluent_kafka import Consumer, Message, Producer, TopicPartition
from confluent_kafka.admin import AdminClient
from pydantic import SecretStr
from sqlalchemy import Connection, Engine, select, text
from sqlalchemy.exc import DBAPIError

from event_pipeline.broker.config import consumer_config, producer_config
from event_pipeline.broker.publisher import EventPublisher
from event_pipeline.broker.topics import TopicSpec, ensure_topic
from event_pipeline.config import Settings
from event_pipeline.consumer.loop import CommittedPartition, run
from event_pipeline.data.tables import order_aggregates
from event_pipeline.domain.events import OrderPlacedEvent
from event_pipeline.main import create_app
from event_pipeline.services.aggregation import ApplyOutcome, apply_order_placed
from tests.integration.test_ingest_api import post_event
from tests.samples import encode, valid_event

PARTITIONS = 3
TIMEOUT_S = 20.0
POLL_TIMEOUT_S = 0.2
CLOSE_TIMEOUT_S = 10.0
PAYLOAD_MARKER = "ord-PAYLOAD-MARKER"
WINDOW = datetime(2026, 11, 22, 12, 30, tzinfo=UTC)


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


def produce(settings: Settings, *values: bytes, key: bytes = b"store") -> None:
    producer = Producer(producer_config(settings))
    for value in values:
        producer.produce(settings.events_topic, value=value, key=key)
    assert producer.flush(TIMEOUT_S) == 0


def committed_offsets(settings: Settings) -> dict[int, int]:
    """The group's committed offset per partition; partitions with none are left out."""
    reader = Consumer(
        {
            "bootstrap.servers": settings.kafka_bootstrap_servers,
            "group.id": settings.consumer_group_id,
            "enable.auto.commit": False,
        }
    )
    try:
        partitions = [TopicPartition(settings.events_topic, p) for p in range(PARTITIONS)]
        return {
            tp.partition: tp.offset
            for tp in reader.committed(partitions, timeout=TIMEOUT_S)
            if tp.offset >= 0
        }
    finally:
        reader.close()


def end_offsets(settings: Settings) -> dict[int, int]:
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
        for p in range(PARTITIONS):
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
    ) -> None:
        self.consumer = Consumer(consumer_config(settings))
        self.consumer.subscribe([settings.events_topic])
        self.stop_flag = threading.Event()
        self.error: BaseException | None = None
        self.outcomes: list[ApplyOutcome] = []

        def apply(event: OrderPlacedEvent) -> ApplyOutcome:
            outcome = apply_order_placed(engine, event)
            self.outcomes.append(outcome)
            return outcome

        def target() -> None:
            try:
                run(
                    wrap(self.consumer),
                    apply,
                    stop=self.stop_flag,
                    poll_timeout_s=settings.consumer_poll_timeout_s,
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
    retried = event_body(store_a, amount_minor=1000)
    bodies = [
        retried,
        event_body(store_a, amount_minor=250),
        event_body(store_a, amount_minor=99, occurred_at="2026-11-22T12:31:00Z"),
        event_body(store_b, amount_minor=500),
        retried,  # a client retry: the same event_id, a second message on the topic
    ]
    publisher = EventPublisher.from_settings(settings)
    try:
        app = create_app(publisher)
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
    futures = admin.describe_consumer_groups([settings.consumer_group_id])
    return len(futures[settings.consumer_group_id].result(timeout=TIMEOUT_S).members)


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
