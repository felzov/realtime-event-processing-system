"""Database errors: describing one for a log line, and telling a rejected event from an outage.

The message of a database error can contain event values: PostgreSQL's DETAIL for a constraint
violation repeats the failing row, even with the engine hiding statement parameters. So only the
classes, the SQLSTATE and the constraint name are logged, never `str(exc)`.

ADR 0006, decisions 4 and 5: an error whose SQLSTATE class is 22 (data exception) or 23 (integrity
constraint violation) describes the values of the statement, so running it again fails the same
way. Every other database error is transient: it may pass if the statement is run again.
"""

from dataclasses import dataclass

from sqlalchemy.exc import DBAPIError, InterfaceError, OperationalError
from sqlalchemy.exc import TimeoutError as PoolTimeoutError

# SQLSTATE classes that describe the statement's values (the spike in ADR 0006: 22003 bigint
# overflow, 22021 text not valid UTF-8, 23514 CHECK, 23502 NOT NULL).
REJECTING_SQLSTATE_CLASSES = frozenset({"22", "23"})


def describe_database_error(exc: DBAPIError) -> str:
    """For example `IntegrityError CheckViolation sqlstate=23514 constraint=ck_...`."""
    driver_error = exc.orig
    parts = [type(exc).__name__, type(driver_error).__name__]
    sqlstate = sqlstate_of(exc)
    if sqlstate:
        parts.append(f"sqlstate={sqlstate}")
    constraint = getattr(getattr(driver_error, "diag", None), "constraint_name", None)
    if constraint:
        parts.append(f"constraint={constraint}")
    return " ".join(parts)


def sqlstate_of(exc: DBAPIError) -> str | None:
    """The SQLSTATE PostgreSQL sent; None for an error psycopg raised itself, such as a refused
    connection, a closed connection, or a value it cannot send."""
    sqlstate = getattr(exc.orig, "sqlstate", None)
    return sqlstate if isinstance(sqlstate, str) and sqlstate else None


def rejects_the_values(exc: BaseException) -> bool:
    """True if the database refused the statement's values: SQLSTATE class 22 or 23 (decision 4).

    Running the same statement again fails the same way. What that means depends on the statement:
    for an event's apply, the event is dead-lettered; for a dead letter's own insert, it is a bug.
    """
    if not isinstance(exc, DBAPIError):
        return False
    sqlstate = sqlstate_of(exc)
    return sqlstate is not None and sqlstate[:2] in REJECTING_SQLSTATE_CLASSES


def is_transient(exc: BaseException) -> bool:
    """True for a database error that may pass on a later attempt (decision 5).

    Every `DBAPIError` with a SQLSTATE that does not reject the values: classes 08, 40, 53, 57, and
    also 42 (a missing column or table: a migration not run must stop the consumer after the
    attempts, not dead-letter every event). Without a SQLSTATE psycopg raised the error itself:
    transient only as an `OperationalError` or `InterfaceError` (no connection, a connection
    lost); any other, such as a value it cannot adapt, is a bug in our code (update 2026-11-25).
    Also the pool's timeout waiting for a free connection, which is not a `DBAPIError`. Anything
    else is not a database error, so a bug.
    """
    if isinstance(exc, PoolTimeoutError):
        return True
    if not isinstance(exc, DBAPIError) or rejects_the_values(exc):
        return False
    if sqlstate_of(exc) is None:
        return isinstance(exc, OperationalError | InterfaceError)
    return True


@dataclass(frozen=True, slots=True)
class RetryPolicy:
    """How often a transient database error is retried in place, and how long to pause between.

    The pause after the n-th failed attempt is `first_backoff_s * 2 ** (n - 1)`, at most
    `max_backoff_s`: 0.5 s, 1 s, 2 s, 2 s, ... with the defaults (config.py).
    """

    attempts: int
    first_backoff_s: float
    max_backoff_s: float

    def __post_init__(self) -> None:
        if self.attempts < 1:
            raise ValueError("attempts must be at least 1")
        if self.first_backoff_s < 0 or self.max_backoff_s < self.first_backoff_s:
            raise ValueError("backoffs must satisfy 0 <= first_backoff_s <= max_backoff_s")

    def backoff_s(self, failed_attempts: int) -> float:
        """The pause after `failed_attempts` attempts have failed (1 for the first pause)."""
        # The exponent is capped so a large attempt count cannot overflow a float; 2 ** 30 times
        # any positive first pause is past every sensible maximum anyway.
        exponent = min(failed_attempts - 1, 30)
        return min(self.first_backoff_s * 2.0**exponent, self.max_backoff_s)

    def total_backoff_s(self) -> float:
        """All pauses of one call that fails every attempt: one fewer than the attempts."""
        return sum(self.backoff_s(n) for n in range(1, self.attempts))
