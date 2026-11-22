"""Alembic environment: runs migrations online, on the application's own engine.

The URL and timeouts come from `Settings` (DATABASE_URL), so `make db-migrate` migrates the database
the application uses. A caller that already holds a connection (the integration tests) passes it
as `config.attributes["connection"]`; the migration then runs on it, inside its transaction.
"""

from logging.config import fileConfig

from alembic import context
from sqlalchemy import Connection

from event_pipeline.config import Settings
from event_pipeline.data.engine import make_engine

config = context.config


def run_migrations(connection: Connection) -> None:
    context.configure(connection=connection)
    with context.begin_transaction():
        context.run_migrations()


def main() -> None:
    connection = config.attributes.get("connection")
    if connection is not None:
        # No fileConfig here: it would replace the caller's logging setup, and its default
        # `disable_existing_loggers=True` silences every logger created before it.
        run_migrations(connection)
        return
    if config.config_file_name is not None:
        fileConfig(config.config_file_name)
    engine = make_engine(Settings())
    try:
        with engine.connect() as connection:
            run_migrations(connection)
    finally:
        engine.dispose()


if context.is_offline_mode():
    # Offline mode prints SQL instead of running it; this project applies migrations only online.
    raise SystemExit("offline migrations (--sql) are not supported")
main()
