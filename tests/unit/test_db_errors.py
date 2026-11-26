"""Telling a rejected event from a database outage (ADR 0006, decisions 4 and 5), and the retry
pauses, with the classes the ADR 0006 spike saw for each case."""

import psycopg
import psycopg.errors
import pytest
from sqlalchemy.exc import (
    DataError,
    DBAPIError,
    IntegrityError,
    InterfaceError,
    NotSupportedError,
    OperationalError,
    ProgrammingError,
)
from sqlalchemy.exc import TimeoutError as PoolTimeoutError

from event_pipeline.data.errors import (
    RetryPolicy,
    describe_database_error,
    is_transient,
    rejects_the_values,
    sqlstate_of,
)


def wrapped(wrapper: type[DBAPIError], driver_error: Exception) -> DBAPIError:
    return wrapper("SQL ...", None, driver_error)


REJECTED = [
    wrapped(DataError, psycopg.errors.NumericValueOutOfRange()),
    wrapped(DataError, psycopg.errors.CharacterNotInRepertoire()),
    wrapped(IntegrityError, psycopg.errors.CheckViolation()),
    wrapped(IntegrityError, psycopg.errors.NotNullViolation()),
    wrapped(IntegrityError, psycopg.errors.UniqueViolation()),
]
TRANSIENT = [
    wrapped(OperationalError, psycopg.OperationalError("connection refused")),
    wrapped(InterfaceError, psycopg.InterfaceError("connection already closed")),
    wrapped(OperationalError, psycopg.errors.QueryCanceled()),
    wrapped(OperationalError, psycopg.errors.DeadlockDetected()),
    wrapped(OperationalError, psycopg.errors.SerializationFailure()),
    wrapped(OperationalError, psycopg.errors.AdminShutdown()),
    wrapped(OperationalError, psycopg.errors.TooManyConnections()),
    wrapped(ProgrammingError, psycopg.errors.UndefinedColumn()),
    wrapped(ProgrammingError, psycopg.errors.UndefinedTable()),
]


@pytest.mark.parametrize("error", REJECTED, ids=lambda e: sqlstate_of(e))
def test_class_22_and_23_reject_the_values_and_are_not_retried(error: DBAPIError) -> None:
    assert rejects_the_values(error)
    assert not is_transient(error)


@pytest.mark.parametrize("error", TRANSIENT, ids=lambda e: sqlstate_of(e) or type(e.orig).__name__)
def test_every_other_database_error_is_transient(error: DBAPIError) -> None:
    assert is_transient(error)
    assert not rejects_the_values(error)


def test_the_pools_timeout_is_transient() -> None:
    error = PoolTimeoutError("QueuePool limit of size 5 overflow 10 reached")

    assert is_transient(error)
    assert not rejects_the_values(error)


@pytest.mark.parametrize(
    "error",
    [RuntimeError("bug"), ValueError("bug"), TimeoutError("not the pool's"), KeyError("x")],
    ids=["runtime", "value", "builtin_timeout", "key"],
)
def test_an_error_that_is_not_from_the_database_is_neither(error: Exception) -> None:
    # A bug: the loop stops at once (decision 6).
    assert not is_transient(error)
    assert not rejects_the_values(error)


@pytest.mark.parametrize(
    "error",
    [
        wrapped(ProgrammingError, psycopg.ProgrammingError("cannot adapt type 'Thing'")),
        wrapped(NotSupportedError, psycopg.NotSupportedError("not supported")),
        wrapped(DBAPIError, psycopg.DatabaseError("client-side")),
    ],
    ids=["programming", "not_supported", "generic"],
)
def test_a_client_side_error_without_a_sqlstate_is_a_bug_not_an_outage(error: DBAPIError) -> None:
    # psycopg raises these itself, before or without asking the server: our code passed something
    # it cannot send. Retrying would only report a bug as an outage (ADR 0006, update to
    # decision 5).
    assert sqlstate_of(error) is None
    assert not is_transient(error)
    assert not rejects_the_values(error)


def test_a_driver_error_without_a_sqlstate_is_not_a_rejection() -> None:
    # Errors without a connection carry none (the spike: refused, wrong password, missing DB).
    error = wrapped(OperationalError, psycopg.OperationalError("no route"))

    assert sqlstate_of(error) is None
    assert describe_database_error(error) == "OperationalError OperationalError"


def test_the_pauses_double_from_half_a_second_up_to_two() -> None:
    policy = RetryPolicy(attempts=6, first_backoff_s=0.5, max_backoff_s=2.0)

    assert [policy.backoff_s(n) for n in range(1, 6)] == [0.5, 1.0, 2.0, 2.0, 2.0]
    assert policy.total_backoff_s() == 7.5


@pytest.mark.parametrize(("attempts", "total"), [(1, 0.0), (2, 0.5), (3, 1.5)])
def test_one_attempt_has_no_pause_and_each_more_adds_one(attempts: int, total: float) -> None:
    policy = RetryPolicy(attempts=attempts, first_backoff_s=0.5, max_backoff_s=2.0)

    assert policy.total_backoff_s() == total


def test_a_huge_attempt_count_does_not_overflow_the_pause() -> None:
    policy = RetryPolicy(attempts=10_000, first_backoff_s=0.5, max_backoff_s=2.0)

    assert policy.backoff_s(5_000) == 2.0


@pytest.mark.parametrize(
    "arguments",
    [(0, 0.5, 2.0), (3, -0.1, 2.0), (3, 2.0, 0.5)],
    ids=["no_attempts", "negative_pause", "max_below_first"],
)
def test_an_impossible_policy_is_refused(arguments: tuple[int, float, float]) -> None:
    attempts, first, maximum = arguments
    with pytest.raises(ValueError):
        RetryPolicy(attempts=attempts, first_backoff_s=first, max_backoff_s=maximum)
