"""The poll loop with a fake consumer: commit order, invalid messages, failures, stopping
(phase 3 acceptance criteria 4 and 6), commits refused by a rebalance (phase 5 criterion 3)."""

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
    MEMBERSHIP_CHANGED_COMMIT_ERRORS,
    ConsumerFailedError,
    Headers,
    OffsetCommitError,
    describe_error,
    raised_at,
    received_at_header,
    run,
)
from event_pipeline.domain.dead_letters import DeadLetter, DeadLetterReason
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


TOPIC = "orders.placed.v1"
KEY = b"store-key"


@dataclass
class FakeMessage:
    raw: bytes | None
    offset_: int
    partition_: int = 2
    error_: KafkaError | None = None
    headers_: Headers | None = field(default_factory=lambda: received_at_headers(RECEIVED_ON_TIME))

    def error(self) -> KafkaError | None:
        return self.error_

    def topic(self) -> str | None:
        return TOPIC

    def key(self) -> bytes | None:
        return KEY

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
    # Answers the first commit only (raised if an exception); later commits get `commit_result`.
    first_commit: BaseException | list[FakePartition] | None = None

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
        if self.first_commit is not None:
            first, self.first_commit = self.first_commit, None
            if isinstance(first, BaseException):
                raise first
            return first
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
        self.dead_letters: list[DeadLetter] = []
        self.dead_letter_result: Callable[[DeadLetter], bool] = lambda letter: True

    def apply(self, event: OrderPlacedEvent) -> ApplyOutcome:
        self.calls.append(("apply", None))
        self.applied.append(event)
        return self.apply_result(event)

    def dead_letter(self, letter: DeadLetter) -> bool:
        self.calls.append(("dead_letter", letter.offset))
        self.dead_letters.append(letter)
        return self.dead_letter_result(letter)

    def run(self) -> None:
        run(
            self.consumer,
            self.apply,
            dead_letter=self.dead_letter,
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


def test_a_conflicting_duplicate_is_logged_without_values_and_committed(
    caplog: pytest.LogCaptureFixture,
) -> None:
    harness = Harness(FakeMessage(event_bytes(), offset_=5), FakeMessage(event_bytes(), offset_=6))
    outcomes = iter([ApplyOutcome.CONFLICTING_DUPLICATE, ApplyOutcome.APPLIED])
    harness.apply_result = lambda event: next(outcomes)

    with caplog.at_level(logging.WARNING):
        harness.run()

    # Dead-lettered, then committed; the next message is handled as usual.
    assert harness.calls == [
        ("apply", None),
        ("dead_letter", 5),
        ("commit", 5),
        ("apply", None),
        ("commit", 6),
    ]
    [letter] = harness.dead_letters
    assert letter.reason is DeadLetterReason.CONFLICTING_DUPLICATE
    assert (letter.detail, str(letter.event_id)) == (None, EVENT_ID)
    [record] = caplog.records
    assert record.levelno == logging.WARNING
    assert record.getMessage() == (
        f"event {EVENT_ID} skipped at partition 2 offset 5: conflicting_duplicate, its event_id "
        "was already counted with different content"
    )
    assert PAYLOAD_MARKER not in caplog.text
    assert STORE_ID not in caplog.text


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
def test_invalid_message_is_dead_lettered_logged_and_committed_without_applying(
    raw: bytes | None, reason: str, caplog: pytest.LogCaptureFixture
) -> None:
    harness = Harness(FakeMessage(raw, offset_=9, partition_=4))

    with caplog.at_level(logging.WARNING, logger="event_pipeline.consumer.loop"):
        harness.run()

    assert harness.calls == [("dead_letter", 9), ("commit", 9)]
    # Stored as it was read, whatever it is.
    assert harness.dead_letters == [
        DeadLetter(
            topic=TOPIC,
            partition=4,
            offset=9,
            key=KEY,
            value=raw,
            received_at_header=format_received_at(RECEIVED_ON_TIME),
            reason=DeadLetterReason.INVALID_EVENT,
            detail=reason,
            event_id=None,
        )
    ]
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
def test_an_event_that_does_not_count_is_dead_lettered_logged_and_committed_without_applying(
    headers: Headers | None, reason: str, caplog: pytest.LogCaptureFixture
) -> None:
    harness = Harness(FakeMessage(event_bytes(), offset_=7, partition_=5, headers_=headers))

    with caplog.at_level(logging.WARNING, logger="event_pipeline.consumer.loop"):
        harness.run()

    assert harness.calls == [("dead_letter", 7), ("commit", 7)]
    [letter] = harness.dead_letters
    # The log's reason is the dead letter's code, then its detail (the delay), if any.
    code, _, detail = reason.partition(", ")
    assert (letter.reason.value, letter.detail) == (code, detail or None)
    assert str(letter.event_id) == EVENT_ID
    assert (letter.topic, letter.partition, letter.offset) == (TOPIC, 5, 7)
    assert letter.value == event_bytes()
    assert letter.received_at_header == received_at_header(headers)
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
        ("dead_letter", 2),
        ("commit", 2),
        ("apply", None),
        ("commit", 3),
    ]


def test_a_dead_letter_already_recorded_is_noted_and_committed(
    caplog: pytest.LogCaptureFixture,
) -> None:
    # A redelivery after a crash between the dead letter's commit and the offset commit.
    harness = Harness(FakeMessage(b"{not json", offset_=9, partition_=4))
    harness.dead_letter_result = lambda letter: False

    with caplog.at_level(logging.INFO, logger="event_pipeline.consumer.loop"):
        harness.run()

    assert harness.calls == [("dead_letter", 9), ("commit", 9)]
    assert (
        "dead letter at partition 4 offset 9 was already recorded at this position: a "
        "redelivery, or an offset reused after the topic was recreated" in caplog.text
    )


# --- Failures: no commit, the loop raises --------------------------------------------------------


@pytest.mark.parametrize(
    ("message", "outcome", "reason"),
    [
        (FakeMessage(b"{not json", offset_=9), None, "invalid_event (invalid_json)"),
        (FakeMessage(event_bytes(), offset_=9, headers_=[]), None, "invalid_received_at"),
        # The apply has committed (nothing changed), then the dead letter fails: still no commit.
        (
            FakeMessage(event_bytes(), offset_=9),
            ApplyOutcome.CONFLICTING_DUPLICATE,
            "conflicting_duplicate",
        ),
    ],
    ids=["invalid", "invalid_received_at", "conflicting_duplicate"],
)
def test_a_failed_dead_letter_write_stops_the_loop_without_committing(
    message: FakeMessage,
    outcome: ApplyOutcome | None,
    reason: str,
    caplog: pytest.LogCaptureFixture,
) -> None:
    harness = Harness(message)
    if outcome is not None:
        harness.apply_result = lambda event: outcome

    def fail(letter: DeadLetter) -> bool:
        raise RuntimeError("database unavailable")

    harness.dead_letter_result = fail

    with caplog.at_level(logging.WARNING), pytest.raises(ConsumerFailedError):
        harness.run()

    # The offset stays uncommitted, so the message is redelivered; nothing says it was skipped.
    expected_calls = (
        [("dead_letter", 9)] if outcome is None else [("apply", None), ("dead_letter", 9)]
    )
    assert harness.calls == expected_calls
    assert "skipped" not in caplog.text
    # The stop line keeps why the message was being dead-lettered.
    assert (
        stopping_record(caplog)
        .getMessage()
        .startswith("consumer stopping: RuntimeError at partition 2 offset 9")
    )
    assert (
        stopping_record(caplog).getMessage().endswith(f"; it was being dead-lettered as {reason}")
    )


def test_an_offset_commit_failing_after_a_stored_dead_letter_does_not_blame_the_dead_letter(
    caplog: pytest.LogCaptureFixture,
) -> None:
    harness = Harness(FakeMessage(b"{not json", offset_=9))
    harness.consumer.commit_raises = KafkaException(KafkaError(KafkaError._TRANSPORT))

    with caplog.at_level(logging.ERROR):
        run_until_failure(harness)

    assert harness.calls == [("dead_letter", 9), ("commit", 9)]
    message = stopping_record(caplog).getMessage()
    assert "_TRANSPORT" in message
    assert "dead-lettered as" not in message


def test_a_database_error_writing_a_dead_letter_is_logged_without_the_message(
    caplog: pytest.LogCaptureFixture,
) -> None:
    # The dead letter's INSERT carries the message bytes; PostgreSQL's DETAIL would repeat them.
    harness = Harness(FakeMessage(b'{"order": "' + PAYLOAD_MARKER.encode() + b'"', offset_=9))
    error = IntegrityError(
        "INSERT INTO dead_letters ...",
        {"message_value": PAYLOAD_MARKER},
        FakeDriverError(f"DETAIL: ({PAYLOAD_MARKER})"),
    )

    def fail(letter: DeadLetter) -> bool:
        raise error

    harness.dead_letter_result = fail

    with caplog.at_level(logging.WARNING):
        assert run_until_failure(harness) is error

    assert harness.calls == [("dead_letter", 9)]
    assert "sqlstate=23514" in stopping_record(caplog).getMessage()
    assert PAYLOAD_MARKER not in caplog.text


def test_a_bug_reading_the_headers_of_an_invalid_message_stops_the_loop() -> None:
    # The headers are kept with every dead letter, so a malformed header item stops the loop on an
    # invalid message too, instead of storing it without them.
    malformed: Any = [(RECEIVED_AT_HEADER, b"x", "extra")]
    harness = Harness(FakeMessage(b"{not json", offset_=7, headers_=malformed))

    with pytest.raises(ConsumerFailedError):
        harness.run()

    assert harness.calls == []


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
    harness.consumer.commit_result = [FakePartition(KafkaError(KafkaError.UNKNOWN_TOPIC_OR_PART))]

    with caplog.at_level(logging.ERROR):
        assert isinstance(run_until_failure(harness), OffsetCommitError)

    assert harness.calls == [("apply", None), ("commit", 5)]
    assert "OffsetCommitError UNKNOWN_TOPIC_OR_PART" in stopping_record(caplog).getMessage()


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


# --- Commits refused because the group membership changed (ADR 0004, decisions 10 and 12) ---------

MEMBERSHIP_CHANGED = [
    KafkaError.ILLEGAL_GENERATION,
    KafkaError.UNKNOWN_MEMBER_ID,
    KafkaError.REBALANCE_IN_PROGRESS,
]
# Commit errors that look related but say something else: they still stop the loop.
OTHER_COMMIT_ERRORS = [
    KafkaError.NOT_COORDINATOR,
    KafkaError.FENCED_INSTANCE_ID,
    KafkaError.INCONSISTENT_GROUP_PROTOCOL,
]


def test_the_allow_list_is_exactly_the_three_membership_codes() -> None:
    # A code added to the list becomes non-fatal; that is an ADR 0004 decision, not an edit.
    assert frozenset(MEMBERSHIP_CHANGED) == MEMBERSHIP_CHANGED_COMMIT_ERRORS


def refused(code: int, how: str) -> BaseException | list[FakePartition]:
    """A commit failure with `code`, raised or reported for the partition."""
    if how == "raised":
        return KafkaException(KafkaError(code))
    return [FakePartition(KafkaError(code))]


@pytest.mark.parametrize("how", ["raised", "for the partition"])
@pytest.mark.parametrize("code", MEMBERSHIP_CHANGED)
def test_commit_refused_by_a_membership_change_is_a_warning_and_the_loop_goes_on(
    code: int, how: str, caplog: pytest.LogCaptureFixture
) -> None:
    harness = Harness(FakeMessage(event_bytes(), offset_=5), FakeMessage(event_bytes(), offset_=6))
    harness.consumer.first_commit = refused(code, how)

    with caplog.at_level(logging.WARNING):
        harness.run()

    # The next message is applied and committed as usual.
    assert harness.calls == [("apply", None), ("commit", 5), ("apply", None), ("commit", 6)]
    [record] = caplog.records
    assert record.levelno == logging.WARNING
    message = record.getMessage()
    assert message.startswith(
        f"offset not committed at partition 2 offset 5 event {EVENT_ID}: "
        f"{KafkaError(code).name()} ("
    )
    assert message.endswith("the event may be redelivered")
    assert PAYLOAD_MARKER not in caplog.text


@pytest.mark.parametrize("how", ["raised", "for the partition"])
@pytest.mark.parametrize("code", OTHER_COMMIT_ERRORS)
def test_other_commit_errors_still_stop_the_loop(
    code: int, how: str, caplog: pytest.LogCaptureFixture
) -> None:
    harness = Harness(FakeMessage(event_bytes(), offset_=5), FakeMessage(event_bytes(), offset_=6))
    harness.consumer.first_commit = refused(code, how)

    with caplog.at_level(logging.WARNING):
        run_until_failure(harness)

    assert harness.calls == [("apply", None), ("commit", 5)]
    assert KafkaError(code).name() in stopping_record(caplog).getMessage()
    assert not [r for r in caplog.records if r.levelno == logging.WARNING]


@pytest.mark.parametrize("membership_first", [True, False], ids=["allowed-first", "other-first"])
def test_a_membership_change_does_not_hide_another_partition_error(
    membership_first: bool, caplog: pytest.LogCaptureFixture
) -> None:
    harness = Harness(FakeMessage(event_bytes(), offset_=5), FakeMessage(event_bytes(), offset_=6))
    errors = [
        FakePartition(KafkaError(KafkaError.ILLEGAL_GENERATION)),
        FakePartition(KafkaError(KafkaError.UNKNOWN_TOPIC_OR_PART)),
    ]
    harness.consumer.first_commit = errors if membership_first else errors[::-1]

    with caplog.at_level(logging.WARNING):
        assert isinstance(run_until_failure(harness), OffsetCommitError)

    assert harness.calls == [("apply", None), ("commit", 5)]
    assert "UNKNOWN_TOPIC_OR_PART" in stopping_record(caplog).getMessage()
    # No "may be redelivered" line before the stop: it would contradict it.
    assert not [r for r in caplog.records if r.levelno == logging.WARNING]


@pytest.mark.parametrize("how", ["raised", "for the partition"])
def test_a_fatal_error_with_an_allowed_code_still_stops_the_loop(
    how: str, caplog: pytest.LogCaptureFixture
) -> None:
    harness = Harness(FakeMessage(event_bytes(), offset_=5), FakeMessage(event_bytes(), offset_=6))
    error = KafkaError(KafkaError.UNKNOWN_MEMBER_ID, fatal=True)
    harness.consumer.first_commit = (
        KafkaException(error) if how == "raised" else [FakePartition(error)]
    )

    with caplog.at_level(logging.WARNING):
        run_until_failure(harness)

    assert harness.calls == [("apply", None), ("commit", 5)]
    assert "UNKNOWN_MEMBER_ID" in stopping_record(caplog).getMessage()
    assert not [r for r in caplog.records if r.levelno == logging.WARNING]


def test_a_kafka_exception_without_an_error_code_still_stops_the_loop(
    caplog: pytest.LogCaptureFixture,
) -> None:
    harness = Harness(FakeMessage(event_bytes(), offset_=5))
    harness.consumer.first_commit = KafkaException("no KafkaError inside")

    with caplog.at_level(logging.ERROR):
        assert isinstance(run_until_failure(harness), KafkaException)

    assert harness.calls == [("apply", None), ("commit", 5)]


def test_after_an_eviction_the_refused_commit_is_let_through_and_max_poll_stops_the_loop(
    caplog: pytest.LogCaptureFixture,
) -> None:
    # The sequence the phase 5 spike saw: the evicted consumer's commit fails with
    # UNKNOWN_MEMBER_ID (decision 10: a WARNING), and its next poll returns _MAX_POLL_EXCEEDED,
    # which stops the loop: the bound on time between polls was broken (decision 12).
    harness = Harness(
        FakeMessage(event_bytes(), offset_=5),
        FakeMessage(None, offset_=6, error_=KafkaError(KafkaError._MAX_POLL_EXCEEDED)),
        FakeMessage(event_bytes(), offset_=7),
    )
    harness.consumer.first_commit = refused(KafkaError.UNKNOWN_MEMBER_ID, "raised")

    with caplog.at_level(logging.WARNING):
        assert isinstance(run_until_failure(harness), KafkaException)

    assert harness.calls == [("apply", None), ("commit", 5)]
    [warning] = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert "offset 5" in warning.getMessage()
    assert "UNKNOWN_MEMBER_ID" in warning.getMessage()
    assert "KafkaException _MAX_POLL_EXCEEDED" in stopping_record(caplog).getMessage()


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
