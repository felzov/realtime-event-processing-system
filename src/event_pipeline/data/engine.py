"""The SQLAlchemy engine: synchronous, psycopg 3, with explicit timeouts (ADR 0002, decision 6).

Creating the engine does not connect; the first connection is opened on first use. Every
connection the engine opens carries the same limits:

- `connect_timeout` (libpq): how long opening a connection may take;
- `statement_timeout` (PostgreSQL): how long one statement may run before the server cancels it;
- `pool_timeout` (SQLAlchemy): how long to wait for a free connection from the pool.

Only `services/` opens transactions on it (architecture: layer rules).
"""

from sqlalchemy import Engine, create_engine

from event_pipeline.config import Settings


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
    )
