"""The consumer process's watchdog, client error callback and rebalance callbacks."""

import logging
import os
import signal
import threading
from datetime import timedelta
from pathlib import Path
from typing import Any

import pytest
from confluent_kafka import KafkaError, TopicPartition

from event_pipeline.consumer import __main__ as consumer_main
from event_pipeline.consumer.__main__ import (
    EXIT_CLOSE_TIMEOUT,
    CloseWatchdog,
    describe_partitions,
    log_assigned,
    log_client_error,
    log_lost,
    log_revoked,
)
from event_pipeline.data.errors import RetryPolicy
from event_pipeline.domain.lateness import LatenessPolicy


def test_watchdog_exits_even_when_logging_is_blocked(monkeypatch: pytest.MonkeyPatch) -> None:
    # The main thread can be stuck writing a log line to a full stderr pipe, holding the handler's
    # lock: the watchdog must not wait for that lock before ending the process.
    exits: list[int] = []
    monkeypatch.setattr(os, "_exit", exits.append)
    handler = logging.StreamHandler()
    consumer_main.logger.addHandler(handler)
    handler.acquire()
    try:
        expire = threading.Thread(target=CloseWatchdog(0.1)._expire, daemon=True)
        expire.start()
        expire.join(consumer_main.EXPIRE_REPORT_WAIT_S + 2)

        assert not expire.is_alive(), "the watchdog waited for the logging lock"
        assert exits == [EXIT_CLOSE_TIMEOUT]
    finally:
        handler.release()
        consumer_main.logger.removeHandler(handler)


def test_non_fatal_client_error_is_a_warning(caplog: pytest.LogCaptureFixture) -> None:
    with caplog.at_level(logging.WARNING, logger="event_pipeline.consumer"):
        log_client_error(KafkaError(KafkaError._TRANSPORT))

    [record] = caplog.records
    assert record.levelno == logging.WARNING
    assert "_TRANSPORT" in record.getMessage()


def test_fatal_client_error_is_an_error(caplog: pytest.LogCaptureFixture) -> None:
    with caplog.at_level(logging.WARNING, logger="event_pipeline.consumer"):
        log_client_error(KafkaError(KafkaError._FENCED, fatal=True))

    [record] = caplog.records
    assert record.levelno == logging.ERROR
    assert "fatal" in record.getMessage()


@pytest.mark.parametrize("reentered_in", ["start", "cancel"])
def test_arm_and_cancel_survive_a_signal_handler_arming_in_the_middle(
    reentered_in: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Python runs signal handlers in the main thread between bytecodes, so the SIGTERM handler's
    # arm() can run inside the main thread's own arm() or cancel() (after a failure, in `finally`).
    watchdog = CloseWatchdog(60.0)

    class SignalArrivesTimer(threading.Timer):
        def start(self) -> None:
            if reentered_in == "start":
                watchdog.arm()
            super().start()

        def cancel(self) -> None:
            if reentered_in == "cancel":
                watchdog.arm()
            super().cancel()

    monkeypatch.setattr(threading, "Timer", SignalArrivesTimer)

    def shut_down() -> None:
        watchdog.arm()
        watchdog.cancel()

    main_thread = threading.Thread(target=shut_down, daemon=True)
    main_thread.start()
    main_thread.join(2)

    assert not main_thread.is_alive(), "arm() or cancel() deadlocked on its own lock"
    watchdog.cancel()


def installed_stop_handler(
    monkeypatch: pytest.MonkeyPatch, stop: threading.Event, watchdog: CloseWatchdog
) -> Any:
    """The SIGTERM handler `_stop_on_signals` installs, without installing it in pytest."""
    handlers: dict[int, Any] = {}
    monkeypatch.setattr(
        signal, "signal", lambda signum, handler: handlers.update({signum: handler})
    )
    consumer_main._stop_on_signals(stop, watchdog)
    assert handlers[signal.SIGINT] is handlers[signal.SIGTERM]
    return handlers[signal.SIGTERM]


def test_a_signal_during_the_loops_stop_wait_neither_deadlocks_nor_is_lost(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # The loop pauses between retries with stop.wait() in the main thread, and Event.wait holds the
    # Event's lock (a plain Lock) for a few bytecodes on entry and on wake-up. A handler that
    # took that lock there would wait for its own thread forever: no stop, no watchdog.
    stop = threading.Event()
    armed: list[bool] = []
    watchdog = CloseWatchdog(60.0)
    monkeypatch.setattr(watchdog, "arm", lambda: armed.append(True))
    handler = installed_stop_handler(monkeypatch, stop, watchdog)

    def signal_while_holding_the_lock() -> None:
        with stop._cond:  # type: ignore[attr-defined]  # what Event.wait holds
            handler(signal.SIGTERM, None)

    main_thread = threading.Thread(target=signal_while_holding_the_lock, daemon=True)
    main_thread.start()
    main_thread.join(2)

    assert not main_thread.is_alive(), "the handler deadlocked on the stop flag's lock"
    assert armed == [True]
    # Set once the lock is free, so the loop still sees the request.
    assert stop.wait(2)


def test_a_second_signal_is_ignored(monkeypatch: pytest.MonkeyPatch) -> None:
    stop = threading.Event()
    armed: list[bool] = []
    watchdog = CloseWatchdog(60.0)
    monkeypatch.setattr(watchdog, "arm", lambda: armed.append(True))
    handler = installed_stop_handler(monkeypatch, stop, watchdog)

    handler(signal.SIGTERM, None)
    assert stop.wait(2)
    handler(signal.SIGINT, None)

    assert armed == [True]


class _StubConsumer:
    def __init__(self, config: dict[str, Any]) -> None:
        self.config = config
        self.callbacks: dict[str, Any] = {}

    def subscribe(self, topics: list[str], **callbacks: Any) -> None:
        self.callbacks = callbacks

    def close(self) -> None: ...


class _StubEngine:
    def dispose(self) -> None: ...


def test_main_writes_dead_letters_through_the_service_on_its_engine(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.chdir(tmp_path)  # no .env
    engine = _StubEngine()
    seen: dict[str, Any] = {}
    stored: list[tuple[object, object]] = []
    monkeypatch.setattr(consumer_main, "Consumer", _StubConsumer)
    monkeypatch.setattr(consumer_main, "make_engine", lambda settings: engine)
    monkeypatch.setattr(consumer_main, "_stop_on_signals", lambda stop, watchdog: None)
    monkeypatch.setattr(consumer_main, "run", lambda *args, **kwargs: seen.update(kwargs))

    def store(on: object, letter: object) -> bool:
        stored.append((on, letter))
        return True

    monkeypatch.setattr(consumer_main, "store_dead_letter", store)

    assert consumer_main.main() == 0
    letter = object()
    assert seen["dead_letter"](letter) is True
    assert stored == [(engine, letter)]


def test_main_runs_the_loop_with_the_configured_lateness_policy(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    # Not the defaults (300 / 60): a hard-coded or default policy would not match.
    monkeypatch.chdir(tmp_path)  # no .env
    monkeypatch.setenv("ALLOWED_LATENESS_S", "17")
    monkeypatch.setenv("MAX_FUTURE_SKEW_S", "3")
    seen: dict[str, Any] = {}
    monkeypatch.setattr(consumer_main, "Consumer", _StubConsumer)
    monkeypatch.setattr(consumer_main, "make_engine", lambda settings: _StubEngine())
    # No signal handlers installed in the pytest process.
    monkeypatch.setattr(consumer_main, "_stop_on_signals", lambda stop, watchdog: None)
    monkeypatch.setattr(consumer_main, "run", lambda *args, **kwargs: seen.update(kwargs))

    assert consumer_main.main() == 0
    assert seen["lateness"] == LatenessPolicy(
        allowed_lateness=timedelta(seconds=17), max_future_skew=timedelta(seconds=3)
    )


def test_main_runs_the_loop_with_the_configured_retry_policy(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    # Not the default (3): a hard-coded or default policy would not match.
    monkeypatch.chdir(tmp_path)  # no .env
    monkeypatch.setenv("DB_APPLY_ATTEMPTS", "2")
    seen: dict[str, Any] = {}
    monkeypatch.setattr(consumer_main, "Consumer", _StubConsumer)
    monkeypatch.setattr(consumer_main, "make_engine", lambda settings: _StubEngine())
    monkeypatch.setattr(consumer_main, "_stop_on_signals", lambda stop, watchdog: None)
    monkeypatch.setattr(consumer_main, "run", lambda *args, **kwargs: seen.update(kwargs))

    assert consumer_main.main() == 0
    assert seen["retry"] == RetryPolicy(attempts=2, first_backoff_s=0.5, max_backoff_s=2.0)


def test_main_subscribes_with_the_rebalance_callbacks_and_the_cooperative_strategy(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.chdir(tmp_path)  # no .env
    made: list[_StubConsumer] = []

    def make_consumer(config: dict[str, Any]) -> _StubConsumer:
        made.append(_StubConsumer(config))
        return made[-1]

    monkeypatch.setattr(consumer_main, "Consumer", make_consumer)
    monkeypatch.setattr(consumer_main, "make_engine", lambda settings: _StubEngine())
    monkeypatch.setattr(consumer_main, "_stop_on_signals", lambda stop, watchdog: None)
    monkeypatch.setattr(consumer_main, "run", lambda *args, **kwargs: None)

    assert consumer_main.main() == 0
    [consumer] = made
    assert consumer.callbacks == {
        "on_assign": log_assigned,
        "on_revoke": log_revoked,
        "on_lost": log_lost,
    }
    assert consumer.config["partition.assignment.strategy"] == "cooperative-sticky"


@pytest.mark.parametrize(
    ("callback", "level", "text"),
    [
        (log_assigned, logging.INFO, "partitions assigned: "),
        (log_revoked, logging.INFO, "partitions revoked: "),
        (log_lost, logging.WARNING, "partitions lost: "),
    ],
)
def test_rebalance_callbacks_only_log(
    callback: Any, level: int, text: str, caplog: pytest.LogCaptureFixture
) -> None:
    partitions = [TopicPartition("events", 3), TopicPartition("events", 0)]
    consumer = object()  # any call on it would fail: the callbacks must not touch the consumer

    with caplog.at_level(logging.INFO, logger="event_pipeline.consumer"):
        callback(consumer, partitions)

    [record] = caplog.records
    assert record.levelno == level
    assert record.getMessage() == text + "events [0, 3]"


def test_describe_partitions_groups_by_topic_and_says_none_for_an_empty_list() -> None:
    assert describe_partitions([]) == "none"
    assert (
        describe_partitions(
            [TopicPartition("b", 1), TopicPartition("a", 2), TopicPartition("b", 0)]
        )
        == "a [2], b [0, 1]"
    )
