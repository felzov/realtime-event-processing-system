"""Describing a database error for a log line without its message.

The message of a database error can contain event values: PostgreSQL's DETAIL for a constraint
violation repeats the failing row, even with the engine hiding statement parameters. So only the
classes, the SQLSTATE and the constraint name are logged, never `str(exc)`.
"""

from sqlalchemy.exc import DBAPIError


def describe_database_error(exc: DBAPIError) -> str:
    """For example `IntegrityError CheckViolation sqlstate=23514 constraint=ck_...`."""
    driver_error = exc.orig
    parts = [type(exc).__name__, type(driver_error).__name__]
    sqlstate = getattr(driver_error, "sqlstate", None)
    if sqlstate:
        parts.append(f"sqlstate={sqlstate}")
    constraint = getattr(getattr(driver_error, "diag", None), "constraint_name", None)
    if constraint:
        parts.append(f"constraint={constraint}")
    return " ".join(parts)
