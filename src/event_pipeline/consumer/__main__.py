"""`python -m event_pipeline.consumer` (`make consumer`): run the aggregating consumer.

SIGTERM or SIGINT asks the loop to stop after the current event; a second signal is ignored, the
close timeout bounds the wait. Exit status:

- 0: stopped on request, every applied event's offset committed, group left;
- 1: stopped by a failure, which is logged once, without the error's message; the failed event's
  offset is not committed, so a restart redelivers it;
- 2: did not finish within CONSUMER_CLOSE_TIMEOUT_S of the stop request or of the failure (for
  example, leaving the group with the broker unreachable) and was ended. Safe for the same reason
  as a crash: an event whose offset was not committed is redelivered, and the duplicates table
  absorbs what was applied (ADR 0004).

"The current event" is finished only if it ends within the close timeout. One event's database
work may legitimately take longer (with the defaults up to 30 s: pool + connect timeouts and four
statements, config.py), so a stop request during a slow event can end with status 2 and that
event's transaction rolled back. It is redelivered on restart; nothing is lost or counted twice.
"""

import logging
import os
import signal
import sys
import threading
from types import FrameType

from confluent_kafka import Consumer, KafkaError, TopicPartition

from event_pipeline.broker.config import consumer_config
from event_pipeline.config import Settings
from event_pipeline.consumer.loop import ConsumerFailedError, describe_error, raised_at, run
from event_pipeline.data.engine import make_engine
from event_pipeline.domain.events import OrderPlacedEvent
from event_pipeline.services.aggregation import ApplyOutcome, apply_order_placed

logger = logging.getLogger("event_pipeline.consumer")

EXIT_FAILED = 1
EXIT_CLOSE_TIMEOUT = 2
# How long the watchdog waits for its log line before ending the process anyway.
EXPIRE_REPORT_WAIT_S = 1.0


class CloseWatchdog:
    """Ends the process if shutdown takes longer than `timeout_s` from the first `arm()`.

    No lock: `arm()` and `cancel()` are called only from the main thread, and the signal handler
    that also calls `arm()` runs in the main thread too, between bytecodes, so it can interrupt
    the main thread's own `arm()` or `cancel()`. A lock held there would deadlock the handler. The
    one race left: a handler landing between the check and the assignment in `arm()` starts a
    second timer that `cancel()` does not stop. It has the same deadline and only does what the
    watchdog is for, so it is harmless.
    """

    def __init__(self, timeout_s: float) -> None:
        self._timeout_s = timeout_s
        self._timer: threading.Timer | None = None

    def arm(self) -> None:
        if self._timer is not None:
            return
        # Assigned before start(), so a handler arriving during start() sees it and returns.
        self._timer = threading.Timer(self._timeout_s, self._expire)
        # A daemon thread, so it never keeps a finished process alive (threading.Timer is not a
        # daemon by default).
        self._timer.daemon = True
        self._timer.start()

    def cancel(self) -> None:
        if self._timer is not None:
            self._timer.cancel()

    def _expire(self) -> None:
        # The log line is written from a further thread: the main thread may be stuck writing to a
        # full stderr pipe while holding the handler's lock, and the exit must not wait for it.
        # StreamHandler flushes every record, so a line that is written is not lost by os._exit.
        report = threading.Thread(target=self._report, daemon=True)
        report.start()
        report.join(EXPIRE_REPORT_WAIT_S)
        # os._exit, not sys.exit: this runs in the timer thread, and the main thread may be blocked
        # in the client library.
        os._exit(EXIT_CLOSE_TIMEOUT)

    def _report(self) -> None:
        logger.error("did not stop within %s s; exiting", self._timeout_s)


def log_client_error(error: KafkaError) -> None:
    """librdkafka's `error_cb`, called from `poll()`.

    Most of these errors are transient (a broker connection dropped) and librdkafka recovers on its
    own; while the broker is unreachable the loop just receives no messages. A fatal one leaves the
    consumer unusable, so it is logged as an error. Kafka error strings describe brokers, topics
    and partitions, never message contents.
    """
    if error.fatal():
        logger.error(
            "fatal kafka client error, the consumer is unusable: %s: %s", error.name(), error.str()
        )
    else:
        logger.warning("kafka client error: %s: %s", error.name(), error.str())


def log_assigned(consumer: Consumer, partitions: list[TopicPartition]) -> None:
    """`on_assign`. Logs only: the client applies the assignment itself after the callback
    returns (ADR 0004, decision 11; seen in the phase 5 spike)."""
    logger.info("partitions assigned: %s", describe_partitions(partitions))


def log_revoked(consumer: Consumer, partitions: list[TopicPartition]) -> None:
    """`on_revoke`. Nothing to commit: the loop commits each event's offset before it polls again,
    and partitions are revoked only inside `poll` (decision 11)."""
    logger.info("partitions revoked: %s", describe_partitions(partitions))


def log_lost(consumer: Consumer, partitions: list[TopicPartition]) -> None:
    """`on_lost`: the group dropped this consumer (for example, `max.poll.interval.ms` exceeded),
    so other members may already own these partitions."""
    logger.warning("partitions lost: %s", describe_partitions(partitions))


def describe_partitions(partitions: list[TopicPartition]) -> str:
    """`topic [0, 3]` per topic, or `none`. A cooperative rebalance can call back with no
    partitions for a member whose assignment does not change."""
    by_topic: dict[str, list[int]] = {}
    for partition in partitions:
        by_topic.setdefault(partition.topic, []).append(partition.partition)
    if not by_topic:
        return "none"
    return ", ".join(f"{topic} {sorted(numbers)}" for topic, numbers in sorted(by_topic.items()))


def subscribe(consumer: Consumer, topic: str) -> None:
    """Subscribe with the rebalance callbacks; the integration tests subscribe the same way."""
    consumer.subscribe([topic], on_assign=log_assigned, on_revoke=log_revoked, on_lost=log_lost)


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    settings = Settings()
    stop = threading.Event()
    watchdog = CloseWatchdog(settings.consumer_close_timeout_s)
    _stop_on_signals(stop, watchdog)

    engine = make_engine(settings)
    consumer = Consumer({**consumer_config(settings), "error_cb": log_client_error})
    status = 0
    try:
        subscribe(consumer, settings.events_topic)
        logger.info("consuming %s as group %s", settings.events_topic, settings.consumer_group_id)

        def apply(event: OrderPlacedEvent) -> ApplyOutcome:
            return apply_order_placed(engine, event)

        run(
            consumer,
            apply,
            stop=stop,
            poll_timeout_s=settings.consumer_poll_timeout_s,
            lateness=settings.lateness_policy(),
        )
    except ConsumerFailedError:
        status = EXIT_FAILED  # the loop logged it
    except Exception as exc:
        # Outside the loop (subscribing, for example). Logged the same way: no message.
        logger.error("consumer failed: %s (%s)", describe_error(exc), raised_at(exc))
        status = EXIT_FAILED
    finally:
        # Bounded on every path, not only after a signal: leaving the group can block while the
        # broker is unreachable, and a failed consumer must still exit for its supervisor to see.
        watchdog.arm()
        # Leaves the group, so its partitions are reassigned at once instead of after the session
        # timeout. With auto-commit off it commits nothing.
        consumer.close()
        engine.dispose()
        watchdog.cancel()
    logger.info("stopped" if status == 0 else "stopped after a failure")
    return status


def _stop_on_signals(stop: threading.Event, watchdog: CloseWatchdog) -> None:
    def request_stop(signum: int, frame: FrameType | None) -> None:
        if stop.is_set():
            return
        logger.info("%s received; stopping after the current event", signal.Signals(signum).name)
        stop.set()
        watchdog.arm()

    signal.signal(signal.SIGTERM, request_stop)
    signal.signal(signal.SIGINT, request_stop)


if __name__ == "__main__":
    sys.exit(main())
