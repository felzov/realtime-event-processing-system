"""The consumer's poll loop: one message at a time, offset committed after its effect (ADR 0004).

For each message:

1. A broker error delivered as a message stops the loop (decision 6).
2. A value that is not a valid event is written to the dead-letter table with its reason codes
   (ADR 0006), then logged with them, its partition and offset, never its content, and its offset
   is committed. Its headers are read too, for the dead letter, so a malformed header item stops
   the loop here as it does for a valid event.
3. A valid event whose `received_at` header is missing or malformed, or that is late or from the
   future (ADR 0005), is dead-lettered the same way, with its event id, and never applied.
4. Any other valid event is applied in one database transaction; then its offset + 1 is committed
   synchronously (decisions 1 and 3). A duplicate is committed the same way; a conflicting
   duplicate (its event_id already counted with different content, decision 13) is also
   dead-lettered and logged. An event the database refuses (SQLSTATE class 22 or 23: a value out
   of range, a constraint violated) is dead-lettered as `database_rejected` with the error's
   classes and SQLSTATE, and logged; its apply was rolled back, so it changed nothing.

A dead letter is written in its own committed transaction before the offset commit, as an applied
event is (ADR 0006, decision 2). A redelivered message is not dead-lettered twice.

Every transaction, an apply or a dead letter, that fails with a transient database error (no
connection, a timeout, a missing column: anything but class 22 or 23) is run again, up to the retry
policy's attempts, with a pause between them that a stop request cuts short (ADR 0006,
decision 5). If the last attempt fails, or a stop is requested while waiting, the loop stops
without committing the offset, and the message is redelivered after a restart. A class 22 or 23
error from writing a dead letter is a bug, like any error that is not from the database: it stops
the loop at once, without a retry (decision 6).

A commit refused because the consumer's group membership changed under it (a rebalance, or an
eviction) is logged at WARNING and the loop carries on (decision 10): the event's effect is already
in the database, so at worst it is redelivered and counted as a duplicate. Any other error from
polling, applying or committing ends the loop without committing that message's offset: the error
is logged and raised as `ConsumerFailedError`, and the process exits non-zero. A restart redelivers
from the last committed offset, and the duplicates table absorbs what was already applied
(decision 6).

The stop flag is checked between messages, so a stop request never interrupts an event between
its database commit and its offset commit.
"""

import logging
import threading
import traceback
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Literal, Protocol, assert_never
from uuid import UUID

from confluent_kafka import KafkaError, KafkaException
from sqlalchemy.exc import DBAPIError

from event_pipeline.broker.publisher import RECEIVED_AT_HEADER, parse_received_at
from event_pipeline.data.errors import (
    RetryPolicy,
    describe_database_error,
    is_transient,
    rejects_the_values,
)
from event_pipeline.domain.dead_letters import DeadLetter, DeadLetterReason
from event_pipeline.domain.errors import EventRejectedError, ReasonCode
from event_pipeline.domain.events import OrderPlacedEvent, parse_event
from event_pipeline.domain.lateness import LatenessPolicy, Timing, classify
from event_pipeline.services.aggregation import ApplyOutcome

logger = logging.getLogger(__name__)

# What confluent-kafka's `Message.headers()` returns (its `HeadersType`, which is private).
type HeaderValue = str | bytes | None
type Headers = dict[str, HeaderValue] | list[tuple[str, HeaderValue]]


class Message(Protocol):
    """What the loop reads from a `confluent_kafka.Message`."""

    def error(self) -> KafkaError | None: ...
    def topic(self) -> str | None: ...
    def key(self) -> bytes | None: ...
    def value(self) -> bytes | None: ...
    def partition(self) -> int | None: ...
    def offset(self) -> int | None: ...
    def headers(self) -> Headers | None: ...


class CommittedPartition(Protocol):
    """What the loop reads from each `TopicPartition` a synchronous commit returns."""

    @property
    def error(self) -> KafkaError | None: ...


class MessageSource[M: Message](Protocol):
    """What the loop needs from `confluent_kafka.Consumer`; the unit tests pass a fake.

    Generic in the message type, so the real consumer matches with its own `Message` class.
    """

    def poll(self, timeout: float) -> M | None: ...
    def commit(
        self, *, message: M, asynchronous: Literal[False]
    ) -> Sequence[CommittedPartition] | None: ...


# Commit errors that only say the group's membership changed under the commit (ADR 0004,
# decision 10). ILLEGAL_GENERATION (a rebalance) and UNKNOWN_MEMBER_ID (after an eviction) were seen
# in the phase 5 spike; REBALANCE_IN_PROGRESS was not, but Kafka defines it for the same situation.
MEMBERSHIP_CHANGED_COMMIT_ERRORS = frozenset(
    {
        KafkaError.ILLEGAL_GENERATION,
        KafkaError.UNKNOWN_MEMBER_ID,
        KafkaError.REBALANCE_IN_PROGRESS,
    }
)


class OffsetCommitError(Exception):
    """The broker did not confirm that it stored the offset of the committed message."""


# Writes one dead letter in its own committed transaction; False if its position was already there.
type RecordDeadLetter = Callable[[DeadLetter], bool]


class ConsumerFailedError(Exception):
    """The loop stopped on a failure and has logged it; the cause is `__cause__`."""


@dataclass
class _Position:
    """Where the loop is, for the log line if it has to stop."""

    partition: int | None = None
    offset: int | None = None
    event_id: UUID | None = None
    # Set only while its dead letter is written: if that fails, the stop line says why it was being
    # kept. Cleared once it is stored, so a later failure (the offset commit) is not blamed on it.
    dead_letter_reason: str | None = None
    # Why a transient database error ended the loop instead of another attempt.
    gave_up: str | None = None


def run[M: Message](
    consumer: MessageSource[M],
    apply: Callable[[OrderPlacedEvent], ApplyOutcome],
    *,
    dead_letter: RecordDeadLetter,
    stop: threading.Event,
    poll_timeout_s: float,
    lateness: LatenessPolicy,
    retry: RetryPolicy,
) -> None:
    """Process messages until `stop` is set.

    On the first failure, from polling, applying or committing, log it and raise
    `ConsumerFailedError` from it: the caller knows it is already logged. A transient database
    error is a failure only once `retry` gives up on it.
    """
    steps = _Steps(apply, dead_letter, lateness, retry, stop)
    while not stop.is_set():
        position = _Position()
        try:
            message = consumer.poll(poll_timeout_s)
            if message is None:
                continue
            position.partition, position.offset = message.partition(), message.offset()
            _process(consumer, steps, message, position)
        except Exception as exc:
            # No exc_info: the traceback would print str(exc), which for a database error carries
            # the event's values (PostgreSQL's DETAIL repeats the failing row).
            logger.error(
                "consumer stopping: %s at partition %s offset %s event %s (%s); "
                "offset commit not confirmed%s%s",
                describe_error(exc),
                position.partition,
                position.offset,
                position.event_id,
                raised_at(exc),
                "" if position.gave_up is None else f"; {position.gave_up}",
                ""
                if position.dead_letter_reason is None
                else f"; it was being dead-lettered as {position.dead_letter_reason}",
            )
            raise ConsumerFailedError from exc


@dataclass(frozen=True)
class _Steps:
    """What `run` was given to process one message with."""

    apply: Callable[[OrderPlacedEvent], ApplyOutcome]
    dead_letter: RecordDeadLetter
    lateness: LatenessPolicy
    retry: RetryPolicy
    stop: threading.Event

    def attempt[T](self, call: Callable[[], T], position: _Position, what: str) -> T:
        """`call()`, run again after a transient database error (ADR 0006, decision 5).

        Any other error propagates at once. After the last attempt, or when a stop is requested
        during a pause, the transient error propagates too, with `position.gave_up` saying which.
        """
        failed = 0
        while True:
            try:
                return call()
            except Exception as exc:
                if not is_transient(exc):
                    raise
                failed += 1
                if failed >= self.retry.attempts:
                    position.gave_up = f"{what} failed {failed} of {self.retry.attempts} attempts"
                    raise
                pause_s = self.retry.backoff_s(failed)
                # Not an error yet: the next attempt may pass. Logged by class and SQLSTATE, as
                # every database error is.
                logger.warning(
                    "%s failed at partition %s offset %s event %s (attempt %s of %s): %s; "
                    "retrying in %s s",
                    what,
                    position.partition,
                    position.offset,
                    position.event_id,
                    failed,
                    self.retry.attempts,
                    describe_error(exc),
                    pause_s,
                )
                if self.stop.wait(pause_s):
                    position.gave_up = (
                        f"{what} failed {failed} of {self.retry.attempts} attempts, and a stop "
                        "was requested before the next"
                    )
                    raise


def _process[M: Message](
    consumer: MessageSource[M], steps: _Steps, message: M, position: _Position
) -> None:
    error = message.error()
    if error is not None:
        raise KafkaException(error)

    try:
        event = parse_event(message.value() or b"")
    except EventRejectedError as rejected:
        codes = ", ".join(sorted({violation.code.value for violation in rejected.violations}))
        _dead_letter(steps, message, position, DeadLetterReason.INVALID_EVENT, codes, event_id=None)
        logger.warning(
            "invalid message skipped at partition %s offset %s: %s",
            message.partition(),
            message.offset(),
            codes,
        )
    else:
        position.event_id = event.event_id
        skipped = _not_counted_because(event, message.headers(), steps.lateness)
        if skipped is None:
            skipped = _apply(steps, event, message, position)
        if skipped is not None:
            reason, detail = skipped
            described = reason.value if detail is None else f"{reason.value}, {detail}"
            _dead_letter(steps, message, position, reason, detail, event_id=event.event_id)
            logger.warning(
                "event %s skipped at partition %s offset %s: %s",
                event.event_id,
                message.partition(),
                message.offset(),
                described,
            )

    _commit(consumer, message, position)


def _apply[M: Message](
    steps: _Steps, event: OrderPlacedEvent, message: M, position: _Position
) -> tuple[DeadLetterReason, str | None] | None:
    """Apply `event`; None if that is all, or why it must be dead-lettered instead."""

    # Named, not a lambda: the stop line names the deepest frame of this package (`raised_at`).
    def apply_event() -> ApplyOutcome:
        return steps.apply(event)

    try:
        outcome = steps.attempt(apply_event, position, "apply")
    except Exception as exc:
        # The database refused the event's values (ADR 0006, decision 4): another attempt would
        # fail the same way. The transaction was rolled back, so no table changed.
        if not rejects_the_values(exc):
            raise
        # A DBAPIError: rejects_the_values checked it. Classes, SQLSTATE and constraint only.
        return DeadLetterReason.DATABASE_REJECTED, describe_error(exc)
    # Exhaustive: a new ApplyOutcome fails type-checking here until the loop handles it, so no
    # outcome can be committed past without a decision on how it is reported.
    match outcome:
        case ApplyOutcome.APPLIED | ApplyOutcome.DUPLICATE:
            pass
        case ApplyOutcome.CONFLICTING_DUPLICATE:
            _dead_letter(
                steps,
                message,
                position,
                DeadLetterReason.CONFLICTING_DUPLICATE,
                None,
                event_id=event.event_id,
            )
            logger.warning(
                "event %s skipped at partition %s offset %s: %s, its event_id was already "
                "counted with different content",
                event.event_id,
                message.partition(),
                message.offset(),
                ReasonCode.CONFLICTING_DUPLICATE.value,
            )
        case _:
            assert_never(outcome)
    logger.debug(
        "event %s %s at partition %s offset %s",
        event.event_id,
        outcome.value,
        message.partition(),
        message.offset(),
    )
    return None


def _dead_letter[M: Message](
    steps: _Steps,
    message: M,
    position: _Position,
    reason: DeadLetterReason,
    detail: str | None,
    *,
    event_id: UUID | None,
) -> None:
    """Write `message` to the dead-letter table as it was read (ADR 0006, decisions 2 and 3).

    Key and value are kept byte for byte. Of the headers only `received_at` is kept, and only when
    there is exactly one with a bytes value, as `received_at_header` reads it: a repeated header is
    stored as none. Keeping one copy would let a replay turn a message with two headers, which never
    counts, into one that does.
    """
    position.dead_letter_reason = reason.value if detail is None else f"{reason.value} ({detail})"
    topic, partition, offset = message.topic(), message.partition(), message.offset()
    if topic is None or partition is None or offset is None:
        # Only an error message lacks them, and the loop stops on those before reaching here.
        raise ValueError("a consumed message without a topic, partition or offset")
    letter = DeadLetter(
        topic=topic,
        partition=partition,
        offset=offset,
        key=message.key(),
        value=message.value(),
        received_at_header=received_at_header(message.headers()),
        reason=reason,
        detail=detail,
        event_id=event_id,
    )

    def write_dead_letter() -> bool:
        return steps.dead_letter(letter)

    new = steps.attempt(write_dead_letter, position, "dead letter")
    position.dead_letter_reason = None
    if not new:
        logger.info(
            "dead letter at partition %s offset %s was already recorded at this position: a "
            "redelivery, a retry after an unconfirmed commit, or an offset reused after the topic "
            "was recreated",
            partition,
            offset,
        )


def _not_counted_because(
    event: OrderPlacedEvent, headers: Headers | None, lateness: LatenessPolicy
) -> tuple[DeadLetterReason, str | None] | None:
    """None if `event` counts; otherwise its reason and, for timing, by how much (ADR 0005)."""
    value = received_at_header(headers)
    try:
        received_at = parse_received_at(value)
    except ValueError:
        # Only the header's value is bad input. An error while finding it (headers shaped other
        # than confluent-kafka documents) is a bug: it stops the loop like any other.
        return DeadLetterReason.INVALID_RECEIVED_AT, None
    timing = classify(event.occurred_at, received_at, lateness)
    delay_s = (received_at - event.occurred_at).total_seconds()
    if timing is Timing.LATE:
        return DeadLetterReason.LATE_EVENT, f"received {delay_s:.3f} s after occurred_at"
    if timing is Timing.FUTURE:
        return DeadLetterReason.FUTURE_EVENT, f"received {-delay_s:.3f} s before occurred_at"
    return None


def received_at_header(headers: Headers | None) -> bytes | None:
    """The value of the one `received_at` header; None if there is none, or more than one.

    Kafka allows a header name to repeat. Which copy would count is not defined, so two copies are
    treated like none (ADR 0005, decision 5). A value that is not bytes is not the API's either.
    """
    if headers is None:
        return None
    pairs = headers.items() if isinstance(headers, dict) else headers
    values = [value for name, value in pairs if name == RECEIVED_AT_HEADER]
    if len(values) != 1 or not isinstance(values[0], bytes):
        return None
    return values[0]


def _commit[M: Message](consumer: MessageSource[M], message: M, position: _Position) -> None:
    # Synchronous: the next message is not read before the broker stored this offset. A commit can
    # fail per partition without raising, so each returned partition's error is checked; a
    # synchronous commit always returns the partitions, so no answer counts as a failure.
    try:
        committed = consumer.commit(message=message, asynchronous=False)
    except KafkaException as exc:
        error = _kafka_error_of(exc)
        if error is None or not _membership_changed(error):
            raise
        _log_not_committed(error, position)
        return
    if committed is None:
        raise OffsetCommitError("the commit returned no partitions")
    errors = [partition.error for partition in committed if partition.error is not None]
    # Every error is checked before any is logged as harmless: one that is not on the list stops
    # the loop, without a misleading "may be redelivered" line first.
    for error in errors:
        if not _membership_changed(error):
            raise OffsetCommitError(_kafka_error(error))
    for error in errors:
        _log_not_committed(error, position)


def _membership_changed(error: KafkaError) -> bool:
    """Decision 10's allow-list. A fatal error never qualifies, whatever its code: the client is
    unusable then. librdkafka reports fatal errors as _FATAL, so this is a guard, not a case seen.
    """
    return error.code() in MEMBERSHIP_CHANGED_COMMIT_ERRORS and not error.fatal()


def _log_not_committed(error: KafkaError, position: _Position) -> None:
    """Decision 10: not fatal. If the partition stays here, the next commit on it covers this
    offset; if it moved, or the process stops first, the event is redelivered as a duplicate."""
    logger.warning(
        "offset not committed at partition %s offset %s event %s: %s; the group membership "
        "changed, so the event may be redelivered",
        position.partition,
        position.offset,
        position.event_id,
        _kafka_error(error),
    )


def describe_error(exc: BaseException) -> str:
    """The error's class and codes, never a database message: those can contain event values.

    Kafka error strings are included: librdkafka describes brokers, topics and partitions, never
    message contents.
    """
    if isinstance(exc, DBAPIError):
        return describe_database_error(exc)
    if isinstance(exc, KafkaException) and (error := _kafka_error_of(exc)) is not None:
        return f"KafkaException {_kafka_error(error)}"
    if isinstance(exc, OffsetCommitError):
        return f"OffsetCommitError {exc}"
    return type(exc).__name__


def _kafka_error_of(exc: KafkaException) -> KafkaError | None:
    return exc.args[0] if exc.args and isinstance(exc.args[0], KafkaError) else None


def _kafka_error(error: KafkaError) -> str:
    return f"{error.name()} ({error.str()})"


# This package's directory. Not its parent: in a non-editable install that is site-packages,
# which also holds SQLAlchemy and psycopg.
_PACKAGE_DIR = Path(__file__).resolve().parents[1]


def raised_at(exc: BaseException) -> str:
    """Where in this project's code `exc` was raised, without its message.

    The deepest frame inside `event_pipeline`: for a database error the innermost frame is in
    SQLAlchemy or psycopg, which does not say which of our statements failed.
    """
    frames = traceback.extract_tb(exc.__traceback__)
    ours = [frame for frame in frames if Path(frame.filename).is_relative_to(_PACKAGE_DIR)]
    if not ours:
        return "outside event_pipeline" if frames else "no traceback"
    frame = ours[-1]
    return f"{Path(frame.filename).relative_to(_PACKAGE_DIR.parent)}:{frame.lineno} in {frame.name}"
