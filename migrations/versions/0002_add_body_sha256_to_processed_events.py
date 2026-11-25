"""Add processed_events.body_sha256

Revision ID: 0002
Revises: 0001
Create Date: 2026-11-25

The fingerprint of the first copy of each event (ADR 0004, decision 13), so that a later copy with
the same event_id and different content is detected instead of silently counted as a duplicate.

Existing rows keep NULL: the content of events recorded before this revision is not stored
anywhere, so there is nothing to backfill from, and a later copy of such an event is treated as a
plain duplicate. Nullable without a default, so adding the column changes only the catalogue and
rewrites no rows. Adding the CHECK reads every existing row once to validate it, while holding the
table's ACCESS EXCLUSIVE lock: cheap at this project's size, but it grows with processed_events
(`NOT VALID` plus a separate `VALIDATE CONSTRAINT` would avoid holding that lock for the scan).
Deploy order: migrate first. Code from before this revision keeps consuming, ingesting and
answering queries on it and writes NULL, but its `/ready` answers migration_mismatch (it expects
exactly 0001) until it is replaced. Code from this revision on an 0001 database stops on the first
event it applies; that event is redelivered when it is restarted after the migration.

The downgrade drops the column and the fingerprints in it; event_ids and aggregates stay. After an
upgrade again, the rows recorded meanwhile have NULL, like rows from before this revision.
Hand-written, frozen: does not import application code.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0002"
down_revision: str | None = "0001"
# Branches are not used: every revision has exactly one parent.
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column("processed_events", sa.Column("body_sha256", postgresql.BYTEA(), nullable=True))
    # A SHA-256 is 32 bytes. NULL passes the check (rows from before this revision).
    op.create_check_constraint(
        "ck_processed_events_body_sha256_length",
        "processed_events",
        "octet_length(body_sha256) = 32",
    )


def downgrade() -> None:
    # Dropping the column drops its CHECK constraint with it.
    op.drop_column("processed_events", "body_sha256")
