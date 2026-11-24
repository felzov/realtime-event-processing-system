"""EventPublisher without a broker: outcome mapping, the poll thread's failure modes, the backstop.

The producers here point at 127.0.0.1:1, where nothing listens, so nothing is ever delivered.
"""

import asyncio
import logging
import time
from datetime import UTC, datetime, timedelta, timezone
from typing import Any

import pytest
from confluent_kafka import KafkaError, Message, Producer
from confluent_kafka.admin import (  # type: ignore[attr-defined]
    ClusterMetadata,
    PartitionMetadata,
    TopicMetadata,
)

from event_pipeline.broker.publisher import (
    CAUSE_BACKSTOP,
    CAUSE_CALLBACK_ERROR,
    CAUSE_CLOSED,
    CAUSE_NO_OFFSET,
    CAUSE_POLL_THREAD_DEAD,
    CAUSE_QUEUE_FULL,
    Delivered,
    Delivery,
    EventPublisher,
    NotDelivered,
    PublishFailure,
    _outcome,
    _resolve,
    classify,
    format_received_at,
    log_client_error,
    parse_received_at,
)
from event_pipeline.config import Settings

UNREACHABLE: dict[str, Any] = {"bootstrap.servers": "127.0.0.1:1"}
RECEIVED_AT = datetime(2026, 11, 19, 15, 36, 33, 191000, tzinfo=UTC)
READY_TIMEOUT_S = 0.5


@pytest.mark.parametrize(
    ("error", "expected"),
    [
        (KafkaError(KafkaError._MSG_TIMED_OUT), PublishFailure.BROKER_UNAVAILABLE),
        (
            KafkaError(KafkaError.NOT_ENOUGH_REPLICAS, retriable=True),
            PublishFailure.BROKER_UNAVAILABLE,
        ),
        (KafkaError(KafkaError.MSG_SIZE_TOO_LARGE), PublishFailure.REJECTED),
        (KafkaError(KafkaError.TOPIC_AUTHORIZATION_FAILED), PublishFailure.REJECTED),
    ],
    ids=["timed_out", "retriable", "too_large", "not_authorized"],
)
def test_classify_maps_kafka_errors(error: KafkaError, expected: PublishFailure) -> None:
    assert classify(error) == NotDelivered(expected, error.name())


def test_received_at_is_rfc3339_utc_with_microseconds() -> None:
    assert format_received_at(RECEIVED_AT) == b"2026-11-19T15:36:33.191000Z"


def test_received_at_with_another_offset_is_converted_to_utc() -> None:
    plus_two = RECEIVED_AT.astimezone(timezone(timedelta(hours=2)))

    assert format_received_at(plus_two) == b"2026-11-19T15:36:33.191000Z"


def test_naive_received_at_is_refused() -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        format_received_at(datetime(2026, 11, 19, 15, 36, 33))


@pytest.mark.parametrize(
    "received_at",
    [
        datetime(2026, 11, 19, 15, 36, 33, 191_000, tzinfo=UTC),
        datetime(2026, 11, 19, 15, 36, 33, tzinfo=UTC),
        datetime(2026, 1, 2, 3, 4, 5, 6, tzinfo=UTC),
    ],
    ids=["milliseconds", "whole_second", "single_digit_fields"],
)
def test_received_at_round_trips(received_at: datetime) -> None:
    assert parse_received_at(format_received_at(received_at)) == received_at


@pytest.mark.parametrize(
    "value",
    [
        None,
        b"",
        b"garbage",
        "2026-11-19T15:36:33.191000Z".encode("utf-16"),
        b"2026-11-19T15:36:33.191000\xc3\xa9",
        b"2026-11-19T15:36:33.191000",
        b"2026-11-19T15:36:33.191000+00:00",
        b"2026-11-19T15:36:33Z",
        b"2026-11-19T15:36:33.191Z",
        b"2026-1-19T15:36:33.191000Z",
        b"2026-11-19 15:36:33.191000Z",
        b"2026-11-19T15:36:33.191000z",
        b" 2026-11-19T15:36:33.191000Z",
        b"2026-13-19T15:36:33.191000Z",
        b"1795390127",
    ],
    ids=[
        "missing",
        "empty",
        "garbage",
        "utf16",
        "non_ascii",
        "naive",
        "numeric_offset",
        "no_fraction",
        "short_fraction",
        "one_digit_month",
        "space_separator",
        "lowercase_z",
        "leading_space",
        "month_13",
        "unix_seconds",
    ],
)
def test_received_at_in_any_other_form_is_refused(value: bytes | None) -> None:
    with pytest.raises(ValueError):
        parse_received_at(value)


def test_successful_report_becomes_delivered() -> None:
    message = Message(topic="t", partition=2, offset=7)

    assert _outcome(None, message) == Delivered(partition=2, offset=7)


def test_report_without_an_offset_is_not_called_a_success(
    caplog: pytest.LogCaptureFixture,
) -> None:
    message = Message(topic="t", partition=2, offset=-1001)

    with caplog.at_level(logging.ERROR):
        outcome = _outcome(None, message)

    assert outcome == NotDelivered(PublishFailure.PUBLISHER_FAILED, CAUSE_NO_OFFSET)
    assert "delivery report without an offset" in caplog.text


def test_failed_report_is_classified() -> None:
    error = KafkaError(KafkaError._MSG_TIMED_OUT)

    assert _outcome(error, Message(topic="t")) == classify(error)


def test_late_report_for_a_finished_request_is_ignored() -> None:
    async def scenario() -> Delivery:
        future: asyncio.Future[Delivery] = asyncio.get_running_loop().create_future()
        future.set_result(Delivered(0, 1))
        _resolve(future, Delivered(0, 2))  # must not raise InvalidStateError
        return future.result()

    assert asyncio.run(scenario()) == Delivered(0, 1)


def test_report_after_the_event_loop_closed_does_not_raise(
    caplog: pytest.LogCaptureFixture,
) -> None:
    # Raising would kill the poll thread and leave every later request without an answer.
    loop = asyncio.new_event_loop()
    future: asyncio.Future[Delivery] = loop.create_future()
    loop.close()
    callback = EventPublisher._on_delivery(loop, future)

    with caplog.at_level(logging.WARNING):
        callback(None, Message(topic="t", partition=0, offset=1))

    assert "after its event loop closed" in caplog.text
    assert not future.done()


class _BrokenMessage(Message):
    """A report whose handling raises, standing in for a bug in the callback."""

    def partition(self) -> int | None:
        raise ValueError("simulated bug while reading the report")


def test_error_in_the_callback_answers_the_request_instead_of_raising(
    caplog: pytest.LogCaptureFixture,
) -> None:
    # Raising would escape through poll() and end the poll thread for every later request.
    async def scenario() -> Delivery:
        loop = asyncio.get_running_loop()
        future: asyncio.Future[Delivery] = loop.create_future()
        EventPublisher._on_delivery(loop, future)(None, _BrokenMessage(topic="t", offset=1))
        return await asyncio.wait_for(future, timeout=1.0)

    with caplog.at_level(logging.ERROR):
        outcome = asyncio.run(scenario())

    assert outcome == NotDelivered(PublishFailure.PUBLISHER_FAILED, CAUSE_CALLBACK_ERROR)
    assert "delivery callback failed" in caplog.text


def test_message_over_the_size_limit_is_rejected_by_produce() -> None:
    # librdkafka refuses it inside produce(), before any broker is involved.
    producer = Producer({**UNREACHABLE, "message.max.bytes": 1000})
    publisher = EventPublisher(producer, topic="t", backstop_s=5.0, ready_timeout_s=READY_TIMEOUT_S)
    try:
        outcome = asyncio.run(
            publisher.publish(key="s", value=b"x" * 2000, received_at=RECEIVED_AT)
        )
    finally:
        publisher.close(timeout_s=0.1)

    assert outcome == NotDelivered(PublishFailure.REJECTED, "MSG_SIZE_TOO_LARGE")


def test_full_local_queue_answers_broker_unavailable() -> None:
    producer = Producer(
        {**UNREACHABLE, "queue.buffering.max.messages": 1, "delivery.timeout.ms": 60_000}
    )
    publisher = EventPublisher(producer, topic="t", backstop_s=5.0, ready_timeout_s=READY_TIMEOUT_S)

    async def two_publishes() -> Delivery:
        # The first message fills the queue and stays there: nothing can deliver it.
        first = asyncio.create_task(
            publisher.publish(key="s", value=b"{}", received_at=RECEIVED_AT)
        )
        await asyncio.sleep(0)
        second = await publisher.publish(key="s", value=b"{}", received_at=RECEIVED_AT)
        first.cancel()
        return second

    try:
        outcome = asyncio.run(two_publishes())
    finally:
        publisher.close(timeout_s=0.1)

    assert outcome == NotDelivered(PublishFailure.BROKER_UNAVAILABLE, CAUSE_QUEUE_FULL)


class _DeadPollProducer(Producer):
    """A real producer whose `poll()` fails, as an exception escaping a callback would."""

    def __init__(self, config: dict[str, Any]) -> None:
        super().__init__(config)
        self.produced = 0
        self.metadata_requests = 0

    def poll(self, timeout: float = -1) -> int:
        raise RuntimeError("simulated failure in poll")

    def produce(self, *args: Any, **kwargs: Any) -> None:
        self.produced += 1

    def list_topics(self, *args: Any, **kwargs: Any) -> Any:
        self.metadata_requests += 1
        return super().list_topics(*args, **kwargs)


def _wait_until_not_polling(publisher: EventPublisher) -> None:
    deadline = time.monotonic() + 2.0
    while publisher.is_polling() and time.monotonic() < deadline:
        time.sleep(0.01)


def test_dead_poll_thread_is_detected_and_nothing_is_produced(
    caplog: pytest.LogCaptureFixture,
) -> None:
    producer = _DeadPollProducer(UNREACHABLE)
    with caplog.at_level(logging.ERROR):
        publisher = EventPublisher(
            producer, topic="t", backstop_s=5.0, ready_timeout_s=READY_TIMEOUT_S
        )
        _wait_until_not_polling(publisher)

    assert not publisher.is_polling()
    assert "poll thread died" in caplog.text

    started = time.monotonic()
    outcome = asyncio.run(publisher.publish(key="s", value=b"{}", received_at=RECEIVED_AT))

    assert outcome == NotDelivered(PublishFailure.PUBLISHER_FAILED, CAUSE_POLL_THREAD_DEAD)
    assert time.monotonic() - started < 1.0  # at once, not after the backstop
    assert producer.produced == 0


def test_dead_poll_thread_makes_the_publisher_unready_without_asking_the_broker() -> None:
    producer = _DeadPollProducer(UNREACHABLE)
    publisher = EventPublisher(producer, topic="t", backstop_s=5.0, ready_timeout_s=READY_TIMEOUT_S)
    _wait_until_not_polling(publisher)

    assert publisher.unready_cause() == CAUSE_POLL_THREAD_DEAD
    assert producer.metadata_requests == 0


def test_unreachable_broker_makes_the_publisher_unready_within_the_ready_timeout() -> None:
    publisher = EventPublisher(
        Producer(UNREACHABLE), topic="t", backstop_s=5.0, ready_timeout_s=READY_TIMEOUT_S
    )
    try:
        started = time.monotonic()
        cause = publisher.unready_cause()
        elapsed = time.monotonic() - started
    finally:
        publisher.close(timeout_s=0.1)

    # librdkafka's name for "no broker could be reached" (measured against 127.0.0.1:1).
    assert cause == "_TRANSPORT"
    # Both bounds: librdkafka waits exactly the timeout it is given, so this also proves the
    # setting (not some other timeout) reaches the metadata request.
    assert READY_TIMEOUT_S - 0.1 <= elapsed < READY_TIMEOUT_S + 1.0


def test_closed_publisher_is_unready() -> None:
    publisher = EventPublisher(
        Producer(UNREACHABLE), topic="t", backstop_s=5.0, ready_timeout_s=READY_TIMEOUT_S
    )
    publisher.close(timeout_s=0.1)

    # A normal shutdown, not a crashed thread: the log line must not read like one.
    assert publisher.unready_cause() == CAUSE_CLOSED


class _MetadataProducer(Producer):
    """A real producer (its poll thread works) whose metadata answer is set by the test."""

    def __init__(self, metadata: ClusterMetadata) -> None:
        super().__init__(UNREACHABLE)
        self.metadata = metadata

    def list_topics(self, *args: Any, **kwargs: Any) -> ClusterMetadata:
        return self.metadata


def _partition(partition_id: int, leader: int = 1, error: KafkaError | None = None) -> Any:
    partition = PartitionMetadata()
    partition.id, partition.leader, partition.error = partition_id, leader, error
    return partition


def _metadata(topic: str, partitions: list[Any] | None, error: KafkaError | None = None) -> Any:
    metadata = ClusterMetadata()
    if partitions is not None:
        topic_metadata = TopicMetadata()
        topic_metadata.topic, topic_metadata.error = topic, error
        topic_metadata.partitions = {p.id: p for p in partitions}
        metadata.topics = {topic: topic_metadata}
    return metadata


@pytest.mark.parametrize(
    ("partitions", "topic_error", "expected"),
    [
        ([_partition(0), _partition(1)], None, None),
        (None, None, "UNKNOWN_TOPIC_OR_PART"),
        ([], KafkaError(KafkaError.UNKNOWN_TOPIC_OR_PART), "UNKNOWN_TOPIC_OR_PART"),
        ([], None, "UNKNOWN_TOPIC_OR_PART"),
        (
            [_partition(0), _partition(1, error=KafkaError(KafkaError.LEADER_NOT_AVAILABLE))],
            None,
            "LEADER_NOT_AVAILABLE",
        ),
        ([_partition(0), _partition(1, leader=-1)], None, "LEADER_NOT_AVAILABLE"),
    ],
    ids=[
        "all_partitions_led",
        "topic_absent",
        "topic_error",
        "no_partitions",
        "partition_error",
        "partition_without_leader",
    ],
)
def test_unready_unless_every_partition_has_a_leader(
    partitions: list[Any] | None, topic_error: KafkaError | None, expected: str | None
) -> None:
    # A message for a partition without a leader waits for delivery.timeout.ms and answers 503,
    # so `/ready` must not say 200 while one exists.
    producer = _MetadataProducer(_metadata("t", partitions, topic_error))
    publisher = EventPublisher(producer, topic="t", backstop_s=5.0, ready_timeout_s=READY_TIMEOUT_S)
    try:
        assert publisher.unready_cause() == expected
    finally:
        publisher.close(timeout_s=0.1)


def test_backstop_answers_when_no_report_arrives(caplog: pytest.LogCaptureFixture) -> None:
    # delivery.timeout.ms far beyond the backstop stands in for "librdkafka never reports",
    # which the settings validator prevents in a real configuration.
    producer = Producer({**UNREACHABLE, "delivery.timeout.ms": 60_000})
    publisher = EventPublisher(producer, topic="t", backstop_s=0.3, ready_timeout_s=READY_TIMEOUT_S)

    started = time.monotonic()
    with caplog.at_level(logging.ERROR):
        outcome = asyncio.run(publisher.publish(key="s", value=b"{}", received_at=RECEIVED_AT))
    elapsed = time.monotonic() - started

    assert outcome == NotDelivered(PublishFailure.PUBLISHER_FAILED, CAUSE_BACKSTOP)
    assert 0.3 <= elapsed < 1.5
    assert "no delivery report within the 0.3 s backstop" in caplog.text
    # The message is still queued; close() gives up after its timeout and says how many are left.
    assert publisher.close(timeout_s=0.2) == 1


def test_close_stops_polling_and_is_idempotent() -> None:
    publisher = EventPublisher(
        Producer(UNREACHABLE), topic="t", backstop_s=5.0, ready_timeout_s=READY_TIMEOUT_S
    )
    assert publisher.is_polling()

    assert publisher.close(timeout_s=1.0) == 0
    assert publisher.close(timeout_s=1.0) == 0
    assert not publisher.is_polling()


def test_publish_after_close_is_a_programming_error() -> None:
    publisher = EventPublisher(
        Producer(UNREACHABLE), topic="t", backstop_s=5.0, ready_timeout_s=READY_TIMEOUT_S
    )
    publisher.close(timeout_s=1.0)

    with pytest.raises(RuntimeError, match="after close"):
        asyncio.run(publisher.publish(key="s", value=b"{}", received_at=RECEIVED_AT))


def test_fatal_client_error_is_logged_as_an_error(caplog: pytest.LogCaptureFixture) -> None:
    # A fatal error leaves the producer unusable; it must stand out from ordinary rejections.
    with caplog.at_level(logging.WARNING):
        log_client_error(KafkaError(KafkaError._FATAL, "simulated", fatal=True))

    (record,) = caplog.records
    assert record.levelno == logging.ERROR
    assert "fatal" in record.getMessage()
    assert "_FATAL" in record.getMessage()


def test_non_fatal_client_error_is_a_warning(caplog: pytest.LogCaptureFixture) -> None:
    with caplog.at_level(logging.WARNING):
        log_client_error(KafkaError(KafkaError._ALL_BROKERS_DOWN))

    (record,) = caplog.records
    assert record.levelno == logging.WARNING
    assert "_ALL_BROKERS_DOWN" in record.getMessage()


def test_publisher_from_settings_reports_client_errors(caplog: pytest.LogCaptureFixture) -> None:
    # The poll thread serves error callbacks too; an unreachable broker produces one within about
    # a second, which shows the callback is wired into the producer's configuration.
    settings = Settings(_env_file=None, kafka_bootstrap_servers="127.0.0.1:1")  # type: ignore[call-arg]
    with caplog.at_level(logging.WARNING, logger="event_pipeline.broker.publisher"):
        publisher = EventPublisher.from_settings(settings)
        try:
            deadline = time.monotonic() + 5.0
            while "kafka client error" not in caplog.text and time.monotonic() < deadline:
                time.sleep(0.05)
        finally:
            publisher.close(timeout_s=0.1)

    assert "kafka client error" in caplog.text
