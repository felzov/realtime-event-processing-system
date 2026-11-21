"""Guards for tests that use a real PostgreSQL database."""

from sqlalchemy.engine import make_url
from sqlalchemy.exc import ArgumentError

TEST_DATABASE_SUFFIX = "_test"
# Query parameters libpq would use to pick another database than the URL's path names. The fixture
# also asks the server which database it got (`current_database()`), which catches every other way.
_DATABASE_OVERRIDING_PARAMETERS = frozenset({"dbname", "service"})


class NotATestDatabaseError(Exception):
    """The URL names a database tests must not touch; the message never contains the URL."""


def require_test_database(url: str) -> str:
    """Return `url` if its database name ends in `_test`, so tests can never write elsewhere.

    Tests create, change and (from phase 3, slice 2) migrate up and down whatever database they
    get; a `TEST_DATABASE_URL` pointing at a development database by mistake must stop the run.
    """
    try:
        parsed = make_url(url)
    except (ArgumentError, ValueError):
        # A ValueError's text can quote the password (a bad port); the message never does.
        raise NotATestDatabaseError("TEST_DATABASE_URL is not a valid database URL") from None
    if _DATABASE_OVERRIDING_PARAMETERS & parsed.query.keys():
        raise NotATestDatabaseError(
            "TEST_DATABASE_URL must not choose the database in its query string (dbname, service)"
        )
    database = parsed.database
    if not database or not database.endswith(TEST_DATABASE_SUFFIX):
        raise NotATestDatabaseError(
            f"TEST_DATABASE_URL must name a database ending in {TEST_DATABASE_SUFFIX!r}"
        )
    return url
