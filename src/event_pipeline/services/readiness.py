"""Whether the database is ready for the API: reachable, and migrated to the expected revision.

`/ready` asks this after the broker check (api/health.py). The answer is a cause, safe to log and
never returned to the client, or None when ready. The database is the source of truth for the
query API; a database at another revision than the code expects would answer queries with the
wrong schema, so it counts as not ready as much as an unreachable one (phase 3, E6).
"""

from typing import Final

from sqlalchemy import Engine
from sqlalchemy.exc import DBAPIError, OperationalError

from event_pipeline.data.revision import select_migration_revisions
from event_pipeline.data.tables import EXPECTED_REVISION

CAUSE_DB_UNAVAILABLE: Final = "db_unavailable"
CAUSE_MIGRATION_MISSING: Final = "migration_missing"
CAUSE_MIGRATION_MISMATCH: Final = "migration_mismatch"
# PostgreSQL's undefined_table: no alembic_version table, so never migrated.
_UNDEFINED_TABLE: Final = "42P01"


class DatabaseReadiness:
    """The database half of `/ready`, on its own engine (`data.engine.make_readiness_engine`)."""

    def __init__(self, engine: Engine, expected_revision: str = EXPECTED_REVISION) -> None:
        self._engine = engine
        self._expected_revision = expected_revision

    def unready_cause(self) -> str | None:
        """None if ready; otherwise `db_unavailable`, `migration_missing`, `migration_mismatch`
        or `db_error:<SQLSTATE or class>`.

        Bounded by the readiness engine's connect timeout (per address the host name resolves to)
        plus its statement timeout, while PostgreSQL responds: the statement timeout is enforced
        by the server, so a server that hangs after the connection holds this call until the OS
        gives up on the socket. A statement timeout is an OperationalError (QueryCanceled), so it
        reads as unavailable.
        """
        try:
            with self._engine.connect() as connection:
                revisions = select_migration_revisions(connection)
        except OperationalError:
            return CAUSE_DB_UNAVAILABLE
        except DBAPIError as exc:
            sqlstate = getattr(exc.orig, "sqlstate", None)
            if sqlstate == _UNDEFINED_TABLE:
                return CAUSE_MIGRATION_MISSING
            return f"db_error:{sqlstate or type(exc.orig).__name__}"
        if revisions != [self._expected_revision]:
            return CAUSE_MIGRATION_MISMATCH
        return None
