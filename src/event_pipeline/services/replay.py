"""Replay dead letters through the events topic (ADR 0006, decision 7).

A replay never writes to the database: it reads dead letters and publishes each one's key, value
and original `received_at` header back to the topic, where the consumer treats it like any other
message (deduplication by `event_id`, the lateness rule, per-store order). A dead letter stays in
the table after it is replayed; if the replayed copy fails again, the consumer adds a new one at
its new offset.

Nothing here prints or logs a key or a value: the key is the store id, and the value is the
client's event.
"""

import logging
from collections.abc import Callable, Sequence
from typing import Final, Protocol

from confluent_kafka import KafkaError, KafkaException, Message
from sqlalchemy import Engine

from event_pipeline.broker.publisher import (
    CAUSE_BACKSTOP,
    CAUSE_CALLBACK_ERROR,
    CAUSE_QUEUE_FULL,
    RECEIVED_AT_HEADER,
    Delivery,
    NotDelivered,
    PublishFailure,
    classify,
    delivery_outcome,
)
from event_pipeline.data.dead_letters import select_for_replay
from event_pipeline.domain.dead_letters import ReplayCandidates, ReplaySelection, StoredDeadLetter

logger = logging.getLogger(__name__)

type DeliveryCallback = Callable[[KafkaError | None, Message], None]

# The producer reported a fatal error (possible with idempotence): it cannot send anything more,
# so the letters after it are not produced. A new run uses a new producer.
CAUSE_PRODUCER_FATAL: Final = "producer_fatal"


class ReplayProducer(Protocol):
    """The part of `confluent_kafka.Producer` a replay uses."""

    def produce(
        self,
        topic: str,
        *,
        value: bytes | None,
        key: bytes | None,
        headers: list[tuple[str, str | bytes | None]],
        on_delivery: DeliveryCallback,
    ) -> None: ...

    def poll(self, timeout: float) -> int: ...

    def flush(self, timeout: float) -> int: ...


def find_dead_letters_to_replay(
    engine: Engine, topic: str, selection: ReplaySelection
) -> ReplayCandidates:
    """Read-only. One snapshot, so the counts agree with the letters while the consumer runs."""
    with engine.connect().execution_options(isolation_level="REPEATABLE READ") as connection:
        return select_for_replay(connection, topic, selection)


def publish_dead_letters(
    producer: ReplayProducer,
    topic: str,
    letters: Sequence[StoredDeadLetter],
    timeout_s: float,
) -> list[Delivery]:
    """Publish each dead letter to `topic` and wait for every delivery report.

    Returns one outcome per letter, in order. The final wait is `timeout_s`, and each message that
    meets a full local queue adds one more wait of up to `timeout_s` before it is produced again.
    A letter without a report by then is `PUBLISHER_FAILED` (`no_delivery_report`): it may or may
    not have been written, which is harmless, since a replayed duplicate is dropped by the
    consumer. After a fatal producer error nothing more is produced, and every letter not
    confirmed is `PUBLISHER_FAILED` (`producer_fatal`).
    """
    outcomes: list[Delivery | None] = [None] * len(letters)
    fatal = False

    def report_for(index: int) -> DeliveryCallback:
        def on_delivery(error: KafkaError | None, message: Message) -> None:
            nonlocal fatal
            try:
                if error is not None and error.fatal():
                    fatal = True
                    outcomes[index] = NotDelivered(
                        PublishFailure.PUBLISHER_FAILED, CAUSE_PRODUCER_FATAL
                    )
                else:
                    outcomes[index] = delivery_outcome(error, message)
            except Exception:
                # Raising here would escape from poll() or flush() and end the replay without its
                # report; this letter is counted as unconfirmed instead.
                logger.exception("delivery report could not be read; its message is unconfirmed")
                outcomes[index] = NotDelivered(
                    PublishFailure.PUBLISHER_FAILED, CAUSE_CALLBACK_ERROR
                )

        return on_delivery

    for index, stored in enumerate(letters):
        if fatal:
            break
        try:
            try:
                _produce(producer, topic, stored, report_for(index))
            except BufferError:
                # librdkafka's local queue is full: wait for what is queued, then try once more.
                producer.flush(timeout_s)
                if fatal:
                    break
                _produce(producer, topic, stored, report_for(index))
        except BufferError:
            outcomes[index] = NotDelivered(PublishFailure.BROKER_UNAVAILABLE, CAUSE_QUEUE_FULL)
        except KafkaException as exc:
            error: KafkaError = exc.args[0]
            if error.fatal():
                fatal = True
                outcomes[index] = NotDelivered(
                    PublishFailure.PUBLISHER_FAILED, CAUSE_PRODUCER_FATAL
                )
            else:
                outcomes[index] = classify(error)
        # Serves the reports that have arrived, so they do not pile up during a long replay.
        producer.poll(0)
    producer.flush(timeout_s)
    missing = CAUSE_PRODUCER_FATAL if fatal else CAUSE_BACKSTOP
    return [
        outcome if outcome is not None else NotDelivered(PublishFailure.PUBLISHER_FAILED, missing)
        for outcome in outcomes
    ]


def _produce(
    producer: ReplayProducer, topic: str, stored: StoredDeadLetter, on_delivery: DeliveryCallback
) -> None:
    letter = stored.letter
    # No header if none was stored: the consumer then dead-letters the message as
    # invalid_received_at again, as it did the first time.
    header = letter.received_at_header
    headers: list[tuple[str, str | bytes | None]] = (
        [] if header is None else [(RECEIVED_AT_HEADER, header)]
    )
    producer.produce(
        topic, value=letter.value, key=letter.key, headers=headers, on_delivery=on_delivery
    )
