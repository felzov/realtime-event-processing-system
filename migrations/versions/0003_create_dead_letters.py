"""Create dead_letters

Revision ID: 0003
Revises: 0002
Create Date: 2026-11-25

The dead-letter table of ADR 0006: one row per message the consumer did not count, with the
message as it was read and the reason. A new table, so no existing row changes and no existing
table is locked beyond the catalogue update. Deploy order as for 0002: migrate first; code from
before this revision does not use the table, but its `/ready` answers migration_mismatch until it
is replaced.

The downgrade drops the table and the dead letters in it: the messages are still on the topic until
its retention removes them, but the record of why they were not counted is gone. It is for
development and the migration tests.

The reason codes are listed here, not imported: hand-written and frozen, this file does not import
application code. A unit test (tests/unit/test_dead_letters.py) checks the list against
`DeadLetterReason`.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0003"
down_revision: str | None = "0002"
# Branches are not used: every revision has exactly one parent.
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

REASON_CODES = (
    "invalid_event",
    "invalid_received_at",
    "late_event",
    "future_event",
    "conflicting_duplicate",
    "database_rejected",
)


def upgrade() -> None:
    reasons = ", ".join(f"'{code}'" for code in REASON_CODES)
    op.create_table(
        "dead_letters",
        # Where the message was read. "source_" also keeps clear of OFFSET, a reserved word.
        sa.Column("source_topic", sa.Text(), nullable=False),
        sa.Column("source_partition", sa.Integer(), nullable=False),
        sa.Column("source_offset", sa.BigInteger(), nullable=False),
        # The message as it was read; Kafka allows both to be absent.
        sa.Column("message_key", postgresql.BYTEA(), nullable=True),
        sa.Column("message_value", postgresql.BYTEA(), nullable=True),
        # The one header the ingest API sets, as raw bytes: the reason may be that it is malformed.
        sa.Column("received_at_header", postgresql.BYTEA(), nullable=True),
        sa.Column("reason_code", sa.Text(), nullable=False),
        sa.Column("detail", sa.Text(), nullable=True),
        sa.Column("event_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column(
            "dead_lettered_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        # A redelivery of the same message is the same dead letter (ADR 0006, decision 2).
        sa.PrimaryKeyConstraint(
            "source_topic", "source_partition", "source_offset", name="pk_dead_letters"
        ),
        sa.CheckConstraint(f"reason_code IN ({reasons})", name="ck_dead_letters_reason_code"),
        sa.CheckConstraint("source_partition >= 0", name="ck_dead_letters_source_partition"),
        sa.CheckConstraint("source_offset >= 0", name="ck_dead_letters_source_offset"),
    )


def downgrade() -> None:
    op.drop_table("dead_letters")
