"""Publishing accepted events to the topic and waiting for the broker's answer (ADR 0003).

One synchronous `confluent_kafka.Producer` per process and one thread that calls `poll()` on it.
librdkafka runs a delivery callback only from `poll()`, so without that thread no request would
ever learn its outcome. The callback runs on the poll thread and hands the outcome to the waiting
request's event loop with `call_soon_threadsafe`; asyncio futures are not thread-safe.

Nothing here logs or returns the message value: it is the client's event.
"""

import asyncio
import logging
import threading
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from typing import Final

from confluent_kafka import KafkaError, KafkaException, Message, Producer

from event_pipeline.broker.config import producer_config
from event_pipeline.config import Settings

logger = logging.getLogger(__name__)

RECEIVED_AT_HEADER: Final = "received_at"

# How long one `poll()` blocks when there is nothing to serve. It bounds how quickly the thread
# notices `close()`; delivery reports are served as soon as they arrive, not every interval.
_POLL_INTERVAL_S: Final = 0.1


@dataclass(frozen=True, slots=True)
class Delivered:
    """The broker acknowledged the write with `acks=all`."""

    partition: int
    offset: int


class PublishFailure(StrEnum):
    # The broker did not confirm in time or answered with a retriable error, or librdkafka's local
    # queue is full. A retry with the same `event_id` may succeed.
    BROKER_UNAVAILABLE = "broker_unavailable"
    # A non-retriable error: sending the same message again would fail the same way.
    REJECTED = "rejected"
    # A fault in this process, not the broker: the poll thread is dead, or no delivery report
    # arrived before the backstop. The write may or may not have happened.
    PUBLISHER_FAILED = "publisher_failed"


@dataclass(frozen=True, slots=True)
class NotDelivered:
    """The write was not confirmed. That does not mean it was not written (ADR 0003)."""

    failure: PublishFailure
    # A Kafka error name such as `_MSG_TIMED_OUT`, or one of this module's own causes. Safe to
    # log: it never contains the message.
    cause: str


type Delivery = Delivered | NotDelivered
type DeliveryCallback = Callable[[KafkaError | None, Message], None]

CAUSE_POLL_THREAD_DEAD: Final = "poll_thread_dead"
CAUSE_CLOSED: Final = "publisher_closed"
CAUSE_BACKSTOP: Final = "no_delivery_report"
CAUSE_QUEUE_FULL: Final = "local_queue_full"
CAUSE_CALLBACK_ERROR: Final = "delivery_callback_error"
CAUSE_NO_OFFSET: Final = "no_offset_in_report"


def classify(error: KafkaError) -> NotDelivered:
    """Map a Kafka error from `produce()` or a delivery report to an outcome.

    `_MSG_TIMED_OUT` is not flagged retriable by librdkafka, but it is what an unreachable or
    unresponsive broker produces once `delivery.timeout.ms` has passed (ADR 0002 spike).
    """
    if error.code() == KafkaError._MSG_TIMED_OUT or error.retriable():
        return NotDelivered(PublishFailure.BROKER_UNAVAILABLE, error.name())
    return NotDelivered(PublishFailure.REJECTED, error.name())


def log_client_error(error: KafkaError) -> None:
    """librdkafka's `error_cb`, served by the poll thread like the delivery reports.

    Most of these errors are transient (a broker connection dropped) and librdkafka recovers on its
    own. A fatal one (possible with idempotence) leaves the producer unusable: every later
    message fails, so it is logged as an error to stand out from ordinary rejections.
    """
    if error.fatal():
        logger.error(
            "fatal kafka client error, the producer is unusable: %s: %s", error.name(), error.str()
        )
    else:
        logger.warning("kafka client error: %s: %s", error.name(), error.str())


def format_received_at(received_at: datetime) -> bytes:
    """RFC 3339 in UTC with microseconds and `Z`, for example `2026-11-19T15:36:33.191000Z`."""
    if received_at.tzinfo is None:
        # `astimezone` would read a naive value as local time and shift it silently.
        raise ValueError("received_at must be timezone-aware")
    return received_at.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%S.%fZ").encode("ascii")


def parse_received_at(value: bytes | None) -> datetime:
    """The inverse of `format_received_at`; anything else raises `ValueError`.

    Only the exact form the ingest API writes is accepted, so a header written by another producer
    in some other form is refused rather than guessed at (ADR 0005).
    """
    if value is None:
        raise ValueError("received_at is missing")
    # UnicodeDecodeError is a ValueError.
    text = value.decode("ascii")
    parsed = datetime.strptime(text, "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=UTC)
    # strptime also accepts shorter forms (one-digit months, fewer microsecond digits): writing
    # the value back must give the same bytes.
    if format_received_at(parsed) != value:
        raise ValueError("received_at is not in the ingest API's format")
    return parsed


class EventPublisher:
    """Produces to one topic and awaits each delivery report, bounded by a backstop."""

    def __init__(
        self, producer: Producer, topic: str, backstop_s: float, ready_timeout_s: float
    ) -> None:
        self._producer = producer
        self._topic = topic
        self._backstop_s = backstop_s
        self._ready_timeout_s = ready_timeout_s
        self._closed = False
        self._stop = threading.Event()
        self._poll_thread = threading.Thread(
            target=self._poll_loop, name="kafka-producer-poll", daemon=True
        )
        self._poll_thread.start()

    @classmethod
    def from_settings(cls, settings: Settings) -> "EventPublisher":
        return cls(
            Producer({**producer_config(settings), "error_cb": log_client_error}),
            topic=settings.events_topic,
            backstop_s=settings.publish_backstop_s,
            ready_timeout_s=settings.ready_timeout_s,
        )

    def is_polling(self) -> bool:
        """False once the poll thread has died or the publisher was closed."""
        return self._poll_thread.is_alive()

    def unready_cause(self) -> str | None:
        """Why `publish()` cannot succeed right now, or None if it can. `/ready` calls it.

        Checks, in order: the publisher is open and its poll thread alive (no network call), the
        broker answers a metadata request within the ready timeout, the topic exists there, and
        every partition has a leader (a message for a leaderless partition would wait for
        `delivery.timeout.ms` and answer 503). The cause is `publisher_closed`,
        `poll_thread_dead` or a Kafka error name such as `_TRANSPORT` (no broker answered),
        `UNKNOWN_TOPIC_OR_PART` or `LEADER_NOT_AVAILABLE`; it is safe to log. Blocks for up to
        the ready timeout.

        A fatal producer error is not detected (ADR 0003, limitations): metadata requests still
        succeed after one.
        """
        if self._closed:
            return CAUSE_CLOSED
        if not self._poll_thread.is_alive():
            return CAUSE_POLL_THREAD_DEAD
        try:
            # The producer's own handle is thread-safe, so this can run beside the poll thread.
            # Asking for one topic keeps the request small. A producer's metadata request may ask
            # the broker to create a missing topic (librdkafka documents allow.auto.create.topics
            # as on for producers; not verified for this call), so a missing topic reads as
            # UNKNOWN_TOPIC_OR_PART because the broker refuses: `auto.create.topics.enable=false`
            # in compose.yaml (phase 2, D3). With auto-create on, this check could create the topic
            # with the broker's defaults.
            metadata = self._producer.list_topics(topic=self._topic, timeout=self._ready_timeout_s)
        except KafkaException as exc:
            error: KafkaError = exc.args[0]
            return error.name()
        unknown = KafkaError(KafkaError.UNKNOWN_TOPIC_OR_PART).name()
        topic = metadata.topics.get(self._topic)
        if topic is None:
            # Not seen from librdkafka (a missing topic comes back with an error); kept so an
            # answer without the topic can never read as ready.
            return unknown
        topic_error: KafkaError | None = topic.error
        if topic_error is not None:
            return topic_error.name()
        if not topic.partitions:
            return unknown
        for partition in topic.partitions.values():
            partition_error: KafkaError | None = partition.error
            if partition_error is not None:
                return partition_error.name()
            if partition.leader < 0:
                return KafkaError(KafkaError.LEADER_NOT_AVAILABLE).name()
        return None

    async def publish(self, *, key: str, value: bytes, received_at: datetime) -> Delivery:
        """Produce one message and wait for the broker's answer.

        Returns within about `delivery.timeout.ms` (+ about 1 s) when librdkafka reports, and
        within the backstop otherwise. Raises `RuntimeError` after `close()` (a programming error:
        the API stops taking requests before it closes the publisher).
        """
        if self._closed:
            raise RuntimeError("publish() after close()")
        if not self._poll_thread.is_alive():
            # Without the poll thread no delivery report would ever be served; producing would only
            # leave a message whose outcome nobody learns.
            return NotDelivered(PublishFailure.PUBLISHER_FAILED, CAUSE_POLL_THREAD_DEAD)

        loop = asyncio.get_running_loop()
        future: asyncio.Future[Delivery] = loop.create_future()
        try:
            self._producer.produce(
                self._topic,
                value=value,
                key=key.encode("utf-8"),
                headers=[(RECEIVED_AT_HEADER, format_received_at(received_at))],
                on_delivery=self._on_delivery(loop, future),
            )
        except BufferError:
            # librdkafka's local queue is full: the broker is not keeping up or not reachable.
            return NotDelivered(PublishFailure.BROKER_UNAVAILABLE, CAUSE_QUEUE_FULL)
        except KafkaException as exc:
            return classify(exc.args[0])

        try:
            return await asyncio.wait_for(future, timeout=self._backstop_s)
        except TimeoutError:
            # librdkafka always reports within delivery.timeout.ms (+ about 1 s), and the backstop
            # is longer than that (config.py). Reaching here means a bug, not a slow broker.
            logger.error(
                "no delivery report within the %.1f s backstop (topic %s); answering unconfirmed",
                self._backstop_s,
                self._topic,
            )
            return NotDelivered(PublishFailure.PUBLISHER_FAILED, CAUSE_BACKSTOP)

    def close(self, timeout_s: float) -> int:
        """Stop polling, then wait up to `timeout_s` for outstanding delivery reports.

        Call it only once the API has stopped taking requests: a `publish()` still in flight may
        produce after the flush and then wait for the backstop, logged as an error. It blocks the
        calling thread for up to `timeout_s` (plus one poll interval), so an event loop should run
        it in a worker thread.

        Returns the number of messages still without a report; they may or may not reach the
        broker. Idempotent.
        """
        self._closed = True
        self._stop.set()
        self._poll_thread.join()
        # `flush()` serves the remaining reports on this thread. Their requests may already be
        # gone; the callback copes with that.
        remaining = self._producer.flush(timeout_s)
        if remaining:
            logger.warning(
                "%d message(s) without a delivery report after a %.1f s flush (topic %s)",
                remaining,
                timeout_s,
                self._topic,
            )
        return remaining

    def _poll_loop(self) -> None:
        try:
            while not self._stop.is_set():
                self._producer.poll(_POLL_INTERVAL_S)
        except Exception:
            # An exception escaping `poll()` (the delivery callback guards its own). The thread
            # ends, `is_polling()` turns False, and every later `publish()` answers
            # PUBLISHER_FAILED at once.
            logger.exception("producer poll thread died; delivery reports are no longer served")

    @staticmethod
    def _on_delivery(
        loop: asyncio.AbstractEventLoop, future: asyncio.Future[Delivery]
    ) -> DeliveryCallback:
        def on_delivery(error: KafkaError | None, message: Message) -> None:
            try:
                outcome = delivery_outcome(error, message)
            except Exception:
                # A bug in handling one report. Raising would end the poll thread, and every later
                # request would fail; this request gets an unconfirmed answer instead.
                logger.exception("delivery callback failed; answering unconfirmed")
                outcome = NotDelivered(PublishFailure.PUBLISHER_FAILED, CAUSE_CALLBACK_ERROR)
            try:
                loop.call_soon_threadsafe(_resolve, future, outcome)
            except RuntimeError:
                # The event loop is closed (a report served after shutdown): nobody is waiting.
                # Raising here would kill the poll thread.
                logger.warning("delivery report arrived after its event loop closed (%s)", outcome)

        return on_delivery


def delivery_outcome(error: KafkaError | None, message: Message) -> Delivery:
    """What one delivery report says; the replay (services/replay.py) reads its reports with it."""
    if error is not None:
        return classify(error)
    partition, offset = message.partition(), message.offset()
    if partition is None or offset is None or offset < 0:
        # Not expected with acks=all and idempotence; reported as unconfirmed rather than as a
        # success with a made-up position.
        logger.error(
            "delivery report without an offset (partition %s); answering unconfirmed", partition
        )
        return NotDelivered(PublishFailure.PUBLISHER_FAILED, CAUSE_NO_OFFSET)
    return Delivered(partition=partition, offset=offset)


def _resolve(future: asyncio.Future[Delivery], outcome: Delivery) -> None:
    # Runs on the event loop. The future is already done if the backstop fired or the client went
    # away (the request was cancelled); the message may still have been written, which is fine
    # under at-least-once delivery.
    if not future.done():
        future.set_result(outcome)
