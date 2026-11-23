"""The migration revision the database is at, as Alembic records it."""

from sqlalchemy import Connection, text


def select_migration_revisions(connection: Connection) -> list[str]:
    """Every row of Alembic's `alembic_version` table: one revision after a normal migration.

    Alembic owns that table, so it is not in `data/tables.py`. A database that was never
    migrated has no such table, and this raises PostgreSQL's undefined_table (42P01).
    """
    return list(connection.execute(text("SELECT version_num FROM alembic_version")).scalars())
