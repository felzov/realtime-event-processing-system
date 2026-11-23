"""The consumer's poll loop: one message at a time, offset committed after its effect (ADR 0004).

For each message:

1. A broker error delivered as a message stops the loop (decision 6).
2. A value that is not a valid event is logged with its reason codes, partition and offset, never
   its content, and its offset is committed (decision 7, interim until the dead-letter path).
3. A valid event is applied in one database transaction; then its offset + 1 is committed
   synchronously (decisions 1 and 3). A duplicate is committed the same way.

Any error from polling, applying or committing ends the loop without committing that message's
offset: the error is logged and raised as `ConsumerFailedError`, and the process exits non-zero.
A restart redelivers from the last committed offset, and the duplicates table absorbs what was
already applied (decision 6).

The stop flag is checked between messages, so a stop request never interrupts an event between
its database commit and its offset commit.
"""

import logging
import threading
import traceback
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Literal, Protocol
from uuid import UUID

from confluent_kafka import KafkaError, KafkaException
from sqlalchemy.exc import DBAPIError

from event_pipeline.data.errors import describe_database_error
from event_pipeline.domain.errors import EventRejectedError
from event_pipeline.domain.events import OrderPlacedEvent, parse_event
from event_pipeline.services.aggregation import ApplyOutcome

logger = logging.getLogger(__name__)


class Message(Protocol):
    """What the loop reads from a `confluent_kafka.Message`."""

    def error(self) -> KafkaError | None: ...
    def value(self) -> bytes | None: ...
    def partition(self) -> int | None: ...
    def offset(self) -> int | None: ...


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


class OffsetCommitError(Exception):
    """The broker did not confirm that it stored the offset of the committed message."""


class ConsumerFailedError(Exception):
    """The loop stopped on a failure and has logged it; the cause is `__cause__`."""


@dataclass
class _Position:
    """Where the loop is, for the log line if it has to stop."""

    partition: int | None = None
    offset: int | None = None
    event_id: UUID | None = None


def run[M: Message](
    consumer: MessageSource[M],
    apply: Callable[[OrderPlacedEvent], ApplyOutcome],
    *,
    stop: threading.Event,
    poll_timeout_s: float,
) -> None:
    """Process messages until `stop` is set.

    On the first failure, from polling, applying or committing, log it and raise
    `ConsumerFailedError` from it: the caller knows it is already logged.
    """
    while not stop.is_set():
        position = _Position()
        try:
            message = consumer.poll(poll_timeout_s)
            if message is None:
                continue
            position.partition, position.offset = message.partition(), message.offset()
            _process(consumer, apply, message, position)
        except Exception as exc:
            # No exc_info: the traceback would print str(exc), which for a database error carries
            # the event's values (PostgreSQL's DETAIL repeats the failing row).
            logger.error(
                "consumer stopping: %s at partition %s offset %s event %s (%s); "
                "offset commit not confirmed",
                describe_error(exc),
                position.partition,
                position.offset,
                position.event_id,
                raised_at(exc),
            )
            raise ConsumerFailedError from exc


def _process[M: Message](
    consumer: MessageSource[M],
    apply: Callable[[OrderPlacedEvent], ApplyOutcome],
    message: M,
    position: _Position,
) -> None:
    error = message.error()
    if error is not None:
        raise KafkaException(error)

    try:
        event = parse_event(message.value() or b"")
    except EventRejectedError as rejected:
        logger.warning(
            "invalid message skipped at partition %s offset %s: %s",
            message.partition(),
            message.offset(),
            ", ".join(sorted({violation.code.value for violation in rejected.violations})),
        )
    else:
        position.event_id = event.event_id
        outcome = apply(event)
        logger.debug(
            "event %s %s at partition %s offset %s",
            event.event_id,
            outcome.value,
            message.partition(),
            message.offset(),
        )

    _commit(consumer, message)


def _commit[M: Message](consumer: MessageSource[M], message: M) -> None:
    # Synchronous: the next message is not read before the broker stored this offset. A commit can
    # fail per partition without raising, so each returned partition's error is checked; a
    # synchronous commit always returns the partitions, so no answer counts as a failure.
    # A commit that lands while the group rebalances fails too (REBALANCE_IN_PROGRESS,
    # ILLEGAL_GENERATION) and stops the consumer like any other failure; committing on partition
    # revoke is phase 5.
    committed = consumer.commit(message=message, asynchronous=False)
    if committed is None:
        raise OffsetCommitError("the commit returned no partitions")
    for partition in committed:
        if partition.error is not None:
            raise OffsetCommitError(_kafka_error(partition.error))


def describe_error(exc: BaseException) -> str:
    """The error's class and codes, never a database message: those can contain event values.

    Kafka error strings are included: librdkafka describes brokers, topics and partitions, never
    message contents.
    """
    if isinstance(exc, DBAPIError):
        return describe_database_error(exc)
    if isinstance(exc, KafkaException) and exc.args and isinstance(exc.args[0], KafkaError):
        return f"KafkaException {_kafka_error(exc.args[0])}"
    if isinstance(exc, OffsetCommitError):
        return f"OffsetCommitError {exc}"
    return type(exc).__name__


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
