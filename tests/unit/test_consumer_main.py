"""The consumer process's watchdog and client error callback."""

import logging
import os
import threading

import pytest
from confluent_kafka import KafkaError

from event_pipeline.consumer import __main__ as consumer_main
from event_pipeline.consumer.__main__ import EXIT_CLOSE_TIMEOUT, CloseWatchdog, log_client_error


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
