"""The database tables, as SQLAlchemy Core `Table`s, for the queries in `data/` and `services/`.

The schema itself is created by the Alembic migrations in `migrations/`, never by
`metadata.create_all()`. These definitions describe the same columns, types, defaults and keys,
so queries are built from typed columns; an integration test compares them with the migrated
database. The CHECK constraints live only in the migration: the database enforces them, and
nothing here creates tables.
"""

from typing import Final

from sqlalchemy import (
    BigInteger,
    Column,
    DateTime,
    LargeBinary,
    MetaData,
    PrimaryKeyConstraint,
    Table,
    Text,
    func,
)
from sqlalchemy.dialects.postgresql import UUID

# The migration revision this code is written against. A unit test checks that it is the Alembic
# head, so adding a migration without updating it fails the tests; `/ready` is to report not ready
# while the database is at any other revision.
EXPECTED_REVISION: Final = "0002"

metadata = MetaData()

processed_events = Table(
    "processed_events",
    metadata,
    Column("event_id", UUID(as_uuid=True), nullable=False),
    Column("processed_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
    # The first applied copy's fingerprint (domain/fingerprint.py); NULL: from before revision 0002.
    Column("body_sha256", LargeBinary, nullable=True),
    PrimaryKeyConstraint("event_id", name="pk_processed_events"),
)

order_aggregates = Table(
    "order_aggregates",
    metadata,
    Column("store_id", Text, nullable=False),
    Column("window_start", DateTime(timezone=True), nullable=False),
    Column("currency", Text, nullable=False),
    Column("order_count", BigInteger, nullable=False),
    Column("revenue_minor", BigInteger, nullable=False),
    Column("updated_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
    PrimaryKeyConstraint("store_id", "window_start", "currency", name="pk_order_aggregates"),
)
