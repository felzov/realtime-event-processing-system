"""The poll loop with a fake consumer: commit order, invalid messages, failures, stopping
(phase 3 acceptance criteria 4 and 6)."""

import logging
import threading
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Literal

import pytest
from confluent_kafka import KafkaError, KafkaException
from sqlalchemy.exc import IntegrityError

import event_pipeline
from event_pipeline.broker.publisher import RECEIVED_AT_HEADER, format_received_at
from event_pipeline.consumer.loop import (
    ConsumerFailedError,
    Headers,
    OffsetCommitError,
    describe_error,
    raised_at,
    run,
)
from event_pipeline.domain.events import OrderPlacedEvent
from event_pipeline.domain.lateness import LatenessPolicy
from event_pipeline.services.aggregation import ApplyOutcome
from tests.samples import EVENT_ID, STORE_ID, encode, valid_event

POLL_TIMEOUT_S = 0.01
LATENESS = LatenessPolicy(
    allowed_lateness=timedelta(minutes=5), max_future_skew=timedelta(minutes=1)
)
# The sample event's occurred_at (tests/samples.py) and a received_at ten seconds later: on time.
OCCURRED_AT = datetime(2026, 11, 18, 10, 0, 30, tzinfo=UTC)
RECEIVED_ON_TIME = OCCURRED_AT + timedelta(seconds=10)


def received_at_headers(received_at: datetime) -> list[tuple[str, str | bytes | None]]:
    return [(RECEIVED_AT_HEADER, format_received_at(received_at))]


# Appears only in the payload of the test events: no log line may contain it.
PAYLOAD_MARKER = "ord-PAYLOAD-MARKER"


def event_bytes(**payload: Any) -> bytes:
    body = valid_event()
    body["payload"].update({"order_id": PAYLOAD_MARKER, **payload})
    return encode(body)


@dataclass
class FakeMessage:
    raw: bytes | None
    offset_: int
    partition_: int = 2
    error_: KafkaError | None = None
    headers_: Headers | None = field(default_factory=lambda: received_at_headers(RECEIVED_ON_TIME))

    def error(self) -> KafkaError | None:
        return self.error_

    def value(self) -> bytes | None:
        return self.raw

    def partition(self) -> int | None:
        return self.partition_

    def offset(self) -> int | None:
        return self.offset_

    def headers(self) -> Headers | None:
        return self.headers_


@dataclass
class FakePartition:
    error: KafkaError | None = None


@dataclass
class FakeConsumer:
    """Hands out `messages` in order, then sets `stop`; records every call in `calls`."""

    messages: list[FakeMessage | None]
    stop: threading.Event
    calls: list[tuple[str, int | None]]
    commit_result: list[FakePartition] | None = field(default_factory=lambda: [FakePartition()])
    commit_raises: BaseException | None = None
    poll_raises: BaseException | None = None

    def poll(self, timeout: float) -> FakeMessage | None:
        assert timeout == POLL_TIMEOUT_S
        if self.poll_raises is not None:
            raise self.poll_raises
        if not self.messages:
            self.stop.set()
            return None
        return self.messages.pop(0)

    def commit(
        self, *, message: FakeMessage, asynchronous: Literal[False]
    ) -> list[FakePartition] | None:
        assert asynchronous is False
        self.calls.append(("commit", message.offset()))
        if self.commit_raises is not None:
            raise self.commit_raises
        return self.commit_result


class Harness:
    def __init__(self, *messages: FakeMessage | None) -> None:
        self.stop = threading.Event()
        self.calls: list[tuple[str, int | None]] = []
        self.consumer = FakeConsumer(list(messages), self.stop, self.calls)
        self.applied: list[OrderPlacedEvent] = []
        self.apply_result: Callable[[OrderPlacedEvent], ApplyOutcome] = lambda event: (
            ApplyOutcome.APPLIED
        )

    def apply(self, event: OrderPlacedEvent) -> ApplyOutcome:
        self.calls.append(("apply", None))
        self.applied.append(event)
        return self.apply_result(event)

    def run(self) -> None:
        run(
            self.consumer,
            self.apply,
            stop=self.stop,
            poll_timeout_s=POLL_TIMEOUT_S,
            lateness=LATENESS,
        )


# --- Normal path ---------------------------------------------------------------------------------


def test_offset_is_committed_after_the_apply_returns() -> None:
    harness = Harness(FakeMessage(event_bytes(), offset_=41))

    harness.run()

    assert harness.calls == [("apply", None), ("commit", 41)]
    [event] = harness.applied
    assert str(event.event_id) == EVENT_ID
    assert event.payload.store_id == STORE_ID


def test_a_duplicate_is_committed_like_an_applied_event() -> None:
    harness = Harness(FakeMessage(event_bytes(), offset_=1), FakeMessage(event_bytes(), offset_=2))
    harness.apply_result = lambda event: ApplyOutcome.DUPLICATE

    harness.run()

    assert harness.calls == [("apply", None), ("commit", 1), ("apply", None), ("commit", 2)]


def test_empty_polls_keep_the_loop_going_until_stopped() -> None:
    harness = Harness(None, None, FakeMessage(event_bytes(), offset_=7), None)

    harness.run()

    assert harness.calls == [("apply", None), ("commit", 7)]


# --- Invalid messages ----------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("raw", "reason"),
    [
        (b"{not json", "invalid_json"),
        (None, "invalid_json"),
        (event_bytes(amount_minor=-1), "out_of_range"),
        (event_bytes(currency="euro"), "invalid_format"),
    ],
    ids=["not_json", "tombstone", "negative_amount", "bad_currency"],
)
def test_invalid_message_is_logged_and_committed_without_touching_the_database(
    raw: bytes | None, reason: str, caplog: pytest.LogCaptureFixture
) -> None:
    harness = Harness(FakeMessage(raw, offset_=9, partition_=4))

    with caplog.at_level(logging.WARNING, logger="event_pipeline.consumer.loop"):
        harness.run()

    assert harness.calls == [("commit", 9)]
    [record] = caplog.records
    assert record.levelno == logging.WARNING
    assert "partition 4 offset 9" in record.getMessage()
    assert reason in record.getMessage()
    assert PAYLOAD_MARKER not in caplog.text
    assert "euro" not in caplog.text


# --- Late, future and unreadable received_at (ADR 0005) -----------------------------------------


@pytest.mark.parametrize(
    ("headers", "reason"),
    [
        (
            received_at_headers(OCCURRED_AT + timedelta(minutes=5, microseconds=1)),
            "late_event, received 300.000 s after occurred_at",
        ),
        (
            received_at_headers(OCCURRED_AT + timedelta(days=2)),
            "late_event, received 172800.000 s after occurred_at",
        ),
        (
            received_at_headers(OCCURRED_AT - timedelta(minutes=1, microseconds=1)),
            "future_event, received 60.000 s before occurred_at",
        ),
        (None, "invalid_received_at"),
        ([], "invalid_received_at"),
        ([("other", b"x")], "invalid_received_at"),
        ([(RECEIVED_AT_HEADER, None)], "invalid_received_at"),
        ([(RECEIVED_AT_HEADER, b"2026-11-18T10:00:40Z")], "invalid_received_at"),
        (
            [(RECEIVED_AT_HEADER, format_received_at(RECEIVED_ON_TIME).decode())],
            "invalid_received_at",
        ),
        (received_at_headers(RECEIVED_ON_TIME) * 2, "invalid_received_at"),
    ],
    ids=[
        "late_by_a_microsecond",
        "days_late",
        "from_the_future",
        "no_headers",
        "empty_headers",
        "other_header_only",
        "header_without_value",
        "malformed",
        "str_value",
        "repeated",
    ],
)
def test_an_event_that_does_not_count_is_logged_and_committed_without_the_database(
    headers: Headers | None, reason: str, caplog: pytest.LogCaptureFixture
) -> None:
    harness = Harness(FakeMessage(event_bytes(), offset_=7, partition_=5, headers_=headers))

    with caplog.at_level(logging.WARNING, logger="event_pipeline.consumer.loop"):
        harness.run()

    assert harness.calls == [("commit", 7)]
    [record] = caplog.records
    assert record.levelno == logging.WARNING
    assert record.getMessage() == f"event {EVENT_ID} skipped at partition 5 offset 7: {reason}"
    assert PAYLOAD_MARKER not in caplog.text


@pytest.mark.parametrize(
    "received_at",
    [
        OCCURRED_AT + timedelta(minutes=5),
        OCCURRED_AT - timedelta(minutes=1),
        OCCURRED_AT,
    ],
    ids=["exactly_the_allowed_lateness", "exactly_the_allowed_skew", "same_instant"],
)
def test_an_event_on_time_at_the_bounds_is_applied(received_at: datetime) -> None:
    harness = Harness(
        FakeMessage(event_bytes(), offset_=3, headers_=received_at_headers(received_at))
    )

    harness.run()

    assert harness.calls == [("apply", None), ("commit", 3)]


def test_a_bug_reading_the_headers_stops_the_loop_instead_of_skipping(
    caplog: pytest.LogCaptureFixture,
) -> None:
    # A header item that is not a (name, value) pair is not bad input but a broken client or bug:
    # skipping would silently drop every event, so the loop fails without committing.
    malformed: Any = [(RECEIVED_AT_HEADER, b"x", "extra")]
    harness = Harness(FakeMessage(event_bytes(), offset_=7, headers_=malformed))

    with caplog.at_level(logging.ERROR), pytest.raises(ConsumerFailedError):
        harness.run()

    assert harness.calls == []
    assert "skipped" not in caplog.text


def test_headers_as_a_dict_are_read_too() -> None:
    # confluent-kafka types headers as a list or a dict; a consumed message has a list.
    headers: Headers = {RECEIVED_AT_HEADER: format_received_at(RECEIVED_ON_TIME)}
    harness = Harness(FakeMessage(event_bytes(), offset_=3, headers_=headers))

    harness.run()

    assert harness.calls == [("apply", None), ("commit", 3)]


def test_a_skipped_event_does_not_hold_up_the_next() -> None:
    late = received_at_headers(OCCURRED_AT + timedelta(hours=1))
    harness = Harness(
        FakeMessage(event_bytes(), offset_=1),
        FakeMessage(event_bytes(), offset_=2, headers_=late),
        FakeMessage(event_bytes(), offset_=3),
    )

    harness.run()

    assert harness.calls == [
        ("apply", None),
        ("commit", 1),
        ("commit", 2),
        ("apply", None),
        ("commit", 3),
    ]


# --- Failures: no commit, the loop raises --------------------------------------------------------


def run_until_failure(harness: "Harness") -> BaseException:
    """Run the loop, expect it to stop on a failure, and return the original error."""
    with pytest.raises(ConsumerFailedError) as exc_info:
        harness.run()
    assert exc_info.value.__cause__ is not None
    return exc_info.value.__cause__


def stopping_record(caplog: pytest.LogCaptureFixture) -> logging.LogRecord:
    [record] = [r for r in caplog.records if r.levelno == logging.ERROR]
    assert record.getMessage().startswith("consumer stopping: ")
    assert record.exc_info is None
    return record


def test_apply_error_stops_the_loop_without_committing(caplog: pytest.LogCaptureFixture) -> None:
    harness = Harness(FakeMessage(event_bytes(), offset_=5), FakeMessage(event_bytes(), offset_=6))

    def fail(event: OrderPlacedEvent) -> ApplyOutcome:
        raise RuntimeError(f"failed on {PAYLOAD_MARKER}")

    harness.apply_result = fail

    with caplog.at_level(logging.ERROR):
        assert isinstance(run_until_failure(harness), RuntimeError)

    # Stopped at the first message: no commit, the second message never read.
    assert harness.calls == [("apply", None)]
    message = stopping_record(caplog).getMessage()
    assert f"RuntimeError at partition 2 offset 5 event {EVENT_ID}" in message
    assert "offset commit not confirmed" in message
    # The deepest frame in our own code, relative to src/: the apply call in the loop.
    assert "(event_pipeline/consumer/loop.py:" in message
    assert " in _process)" in message
    assert PAYLOAD_MARKER not in caplog.text


class FakeDriverError(Exception):
    sqlstate = "23514"

    class diag:  # noqa: N801  (mirrors psycopg's attribute name)
        constraint_name = "ck_order_aggregates_currency_format"


def test_database_error_is_logged_by_class_sqlstate_and_constraint_only(
    caplog: pytest.LogCaptureFixture,
) -> None:
    harness = Harness(FakeMessage(event_bytes(), offset_=5))
    # As PostgreSQL reports a CHECK violation: the DETAIL repeats the row.
    error = IntegrityError(
        "INSERT ...", {"store_id": STORE_ID}, FakeDriverError(f"DETAIL: ({PAYLOAD_MARKER})")
    )

    def fail(event: OrderPlacedEvent) -> ApplyOutcome:
        raise error

    harness.apply_result = fail

    with caplog.at_level(logging.ERROR):
        assert run_until_failure(harness) is error

    message = stopping_record(caplog).getMessage()
    assert (
        "IntegrityError FakeDriverError sqlstate=23514 "
        "constraint=ck_order_aggregates_currency_format" in message
    )
    assert PAYLOAD_MARKER not in caplog.text
    assert STORE_ID not in caplog.text


def test_offset_commit_that_raises_stops_the_loop(caplog: pytest.LogCaptureFixture) -> None:
    harness = Harness(FakeMessage(event_bytes(), offset_=5), FakeMessage(event_bytes(), offset_=6))
    harness.consumer.commit_raises = KafkaException(KafkaError(KafkaError.REQUEST_TIMED_OUT))

    with caplog.at_level(logging.ERROR):
        assert isinstance(run_until_failure(harness), KafkaException)

    assert harness.calls == [("apply", None), ("commit", 5)]
    message = stopping_record(caplog).getMessage()
    assert "KafkaException REQUEST_TIMED_OUT (Broker: Request timed out)" in message
    assert f"event {EVENT_ID}" in message


def test_offset_commit_failed_for_the_partition_stops_the_loop(
    caplog: pytest.LogCaptureFixture,
) -> None:
    # A synchronous commit can report a per-partition failure without raising.
    harness = Harness(FakeMessage(event_bytes(), offset_=5), FakeMessage(event_bytes(), offset_=6))
    harness.consumer.commit_result = [FakePartition(KafkaError(KafkaError.REBALANCE_IN_PROGRESS))]

    with caplog.at_level(logging.ERROR):
        assert isinstance(run_until_failure(harness), OffsetCommitError)

    assert harness.calls == [("apply", None), ("commit", 5)]
    assert "OffsetCommitError REBALANCE_IN_PROGRESS" in stopping_record(caplog).getMessage()


def test_offset_commit_without_an_answer_stops_the_loop(caplog: pytest.LogCaptureFixture) -> None:
    harness = Harness(FakeMessage(event_bytes(), offset_=5), FakeMessage(event_bytes(), offset_=6))
    harness.consumer.commit_result = None

    with caplog.at_level(logging.ERROR):
        assert isinstance(run_until_failure(harness), OffsetCommitError)

    assert harness.calls == [("apply", None), ("commit", 5)]
    assert "the commit returned no partitions" in stopping_record(caplog).getMessage()


def test_poll_error_stops_the_loop_and_is_logged(caplog: pytest.LogCaptureFixture) -> None:
    # poll() raises on a closed consumer and re-raises exceptions from client callbacks.
    harness = Harness(FakeMessage(event_bytes(), offset_=5))
    harness.consumer.poll_raises = RuntimeError(f"callback failed on {PAYLOAD_MARKER}")

    with caplog.at_level(logging.ERROR):
        assert isinstance(run_until_failure(harness), RuntimeError)

    assert harness.calls == []
    message = stopping_record(caplog).getMessage()
    assert "RuntimeError at partition None offset None event None" in message
    assert PAYLOAD_MARKER not in caplog.text


def test_broker_error_message_stops_the_loop_before_the_database(
    caplog: pytest.LogCaptureFixture,
) -> None:
    error = KafkaError(KafkaError.UNKNOWN_TOPIC_OR_PART)
    harness = Harness(FakeMessage(None, offset_=0, error_=error))

    with caplog.at_level(logging.ERROR):
        assert isinstance(run_until_failure(harness), KafkaException)

    assert harness.calls == []
    message = stopping_record(caplog).getMessage()
    assert "KafkaException UNKNOWN_TOPIC_OR_PART" in message
    assert "at partition 2 offset 0 event None" in message


# --- Stopping ------------------------------------------------------------------------------------


def test_stop_during_an_event_finishes_it_first() -> None:
    # The stop request arrives while the event is being applied: its offset is still committed,
    # and the next message is never read.
    harness = Harness(FakeMessage(event_bytes(), offset_=1), FakeMessage(event_bytes(), offset_=2))

    def apply_then_stop(event: OrderPlacedEvent) -> ApplyOutcome:
        harness.stop.set()
        return ApplyOutcome.APPLIED

    harness.apply_result = apply_then_stop

    harness.run()

    assert harness.calls == [("apply", None), ("commit", 1)]


def test_stop_before_the_first_poll_reads_nothing() -> None:
    harness = Harness(FakeMessage(event_bytes(), offset_=1))
    harness.stop.set()

    harness.run()

    assert harness.calls == []


def test_describe_error_of_an_unknown_exception_is_its_class() -> None:
    assert describe_error(ValueError(PAYLOAD_MARKER)) == "ValueError"


# --- Where the error was raised -------------------------------------------------------------------

SRC_DIR = Path(event_pipeline.__file__).resolve().parent.parent


def define(path: Path, source: str, namespace: dict[str, Any]) -> dict[str, Any]:
    """Run `source` as if it were the file at `path`, so its frames carry that file name."""
    exec(compile(source, str(path), "exec"), namespace)
    return namespace


def test_raised_at_skips_frames_of_other_packages_installed_next_to_ours() -> None:
    # In a non-editable install the package sits in site-packages next to SQLAlchemy and
    # psycopg: a library frame there is not ours, even though it shares our parent directory.
    library = define(
        SRC_DIR / "some_driver" / "cursor.py", "def execute():\n    raise ValueError\n", {}
    )
    ours = define(
        SRC_DIR / "event_pipeline" / "data" / "fake.py",
        "def add_order():\n    execute()\n",
        {"execute": library["execute"]},
    )

    with pytest.raises(ValueError) as exc_info:
        ours["add_order"]()

    assert raised_at(exc_info.value) == "event_pipeline/data/fake.py:2 in add_order"


def test_raised_at_outside_the_package_says_so() -> None:
    library = define(
        SRC_DIR / "some_driver" / "cursor.py", "def execute():\n    raise ValueError\n", {}
    )

    with pytest.raises(ValueError) as exc_info:
        library["execute"]()

    assert raised_at(exc_info.value) == "outside event_pipeline"
