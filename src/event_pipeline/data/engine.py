"""The SQLAlchemy engine: synchronous, psycopg 3, with explicit timeouts (ADR 0002, decision 6).

Creating the engine does not connect; the first connection is opened on first use. Every
connection the engine opens carries the same limits:

- `connect_timeout` (libpq): how long opening a connection may take;
- `statement_timeout` (PostgreSQL): how long one statement may run before the server cancels it;
- `pool_timeout` (SQLAlchemy): how long to wait for a free connection from the pool.

Only `services/` opens transactions on it (architecture: layer rules).

`make_readiness_engine` is a second, smaller engine for `/ready` alone: its limits follow
`READY_TIMEOUT_S` instead, so a probe answers in bounded time and never waits behind queries for a
pooled connection.
"""

import math
from typing import Any

from sqlalchemy import Engine, create_engine
from sqlalchemy.pool import NullPool

from event_pipeline.config import Settings

# libpq's connect_timeout is whole seconds, and values below 2 are raised to 2.
LIBPQ_MIN_CONNECT_TIMEOUT_S = 2


def make_engine(settings: Settings) -> Engine:
    return create_engine(
        settings.database_url.get_secret_value(),
        connect_args={
            "connect_timeout": settings.db_connect_timeout_s,
            # Sent as a startup parameter, so it holds from the first statement of every connection,
            # including ones the pool replaces after a failure.
            "options": f"-c statement_timeout={settings.db_statement_timeout_ms}",
        },
        pool_timeout=settings.db_pool_timeout_s,
        # A pooled connection the server closed (restart, idle timeout) is replaced before use
        # instead of failing the caller's first statement.
        pool_pre_ping=True,
        # Statement parameters are event values; without this SQLAlchemy quotes them in every
        # error message, and from there in logs.
        hide_parameters=True,
    )


def readiness_connect_args(settings: Settings) -> dict[str, Any]:
    """The connection limits of the readiness engine, derived from `READY_TIMEOUT_S`.

    The connect timeout is rounded up to whole seconds and is at least libpq's minimum, so with the
    default 2 s it is 2 s; the statement timeout is the ready timeout itself.
    """
    return {
        "connect_timeout": max(LIBPQ_MIN_CONNECT_TIMEOUT_S, math.ceil(settings.ready_timeout_s)),
        "options": f"-c statement_timeout={math.ceil(settings.ready_timeout_s * 1000)}",
    }


def make_readiness_engine(settings: Settings) -> Engine:
    """The engine behind `/ready`'s database check (one short query per probe).

    `NullPool`: every probe opens a fresh connection and closes it. There is no pool to wait for,
    no pooled connection that a PostgreSQL restart left stale, and query traffic on the main
    engine cannot use up the probe's connections. A probe every few seconds makes the cost of a
    new connection negligible.
    """
    return create_engine(
        settings.database_url.get_secret_value(),
        connect_args=readiness_connect_args(settings),
        poolclass=NullPool,
        hide_parameters=True,
    )
