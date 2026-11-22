"""${message}

Revision ID: ${up_revision}
Revises: ${down_revision | comma,n}
Create Date: ${create_date}
"""

from collections.abc import Sequence

# Add `from alembic import op` (and `import sqlalchemy as sa`) once upgrade() uses them.

revision: str = "${up_revision}"
down_revision: str | None = ${f'"{down_revision}"' if down_revision else "None"}
# Branches are not used: every revision has exactly one parent.
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    ${upgrades if upgrades else "raise NotImplementedError"}


def downgrade() -> None:
    ${downgrades if downgrades else "raise NotImplementedError"}
