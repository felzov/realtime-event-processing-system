"""Create processed_events and order_aggregates

Revision ID: 0001
Revises:
Create Date: 2026-11-22

Both tables are new and empty, so there is no existing data to carry over. The downgrade drops
them with their rows: it is for development and the migration tests, never for a database whose
aggregates matter.

The CHECK constraints repeat the event contract (ADR 0001) so that a write path that skips
`parse_event` still cannot store a value the contract rejects. They are named, so a violation
can be identified by name. Hand-written, frozen: this file does not import application code,
so a later change to `event_pipeline` cannot change what revision 0001 creates.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0001"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    # One row per applied event (ADR 0004, decision 2): the primary key decides which delivery
    # of an event_id applies it. Kept indefinitely in phase 3 (ADR 0004, decision 8).
    op.create_table(
        "processed_events",
        sa.Column("event_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column(
            "processed_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.PrimaryKeyConstraint("event_id", name="pk_processed_events"),
    )

    # One row per store, currency and one-minute event-time window (ADR 0001, decision 1). Key
    # order (store_id, window_start, currency) matches the query API: one store, a window_start
    # range, sorted by window_start then currency, read as one index range scan.
    op.create_table(
        "order_aggregates",
        sa.Column("store_id", sa.Text(), nullable=False),
        sa.Column("window_start", sa.DateTime(timezone=True), nullable=False),
        sa.Column("currency", sa.Text(), nullable=False),
        sa.Column("order_count", sa.BigInteger(), nullable=False),
        sa.Column("revenue_minor", sa.BigInteger(), nullable=False),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.PrimaryKeyConstraint("store_id", "window_start", "currency", name="pk_order_aggregates"),
        # The contract's StoreId and CurrencyCode. PostgreSQL's `$` is the end of the string, and
        # `[a-z]` compares code points, so "Ab", "é" and a trailing newline are all rejected.
        sa.CheckConstraint(
            "store_id ~ '^[a-z0-9_-]{1,64}$'", name="ck_order_aggregates_store_id_format"
        ),
        sa.CheckConstraint("currency ~ '^[A-Z]{3}$'", name="ck_order_aggregates_currency_format"),
        # A window starts on a whole UTC minute. date_trunc with an explicit zone is immutable;
        # without it, the result would depend on the session's TimeZone setting.
        sa.CheckConstraint(
            "date_trunc('minute', window_start, 'UTC') = window_start",
            name="ck_order_aggregates_window_start_minute",
        ),
        # A row exists only once an order has been applied to it; amounts are never negative.
        sa.CheckConstraint("order_count > 0", name="ck_order_aggregates_order_count_positive"),
        sa.CheckConstraint(
            "revenue_minor >= 0", name="ck_order_aggregates_revenue_minor_non_negative"
        ),
    )


def downgrade() -> None:
    op.drop_table("order_aggregates")
    op.drop_table("processed_events")
