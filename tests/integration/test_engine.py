"""The engine's timeouts reach PostgreSQL, and connection errors do not reveal the password."""

import time
from collections.abc import Callable

import psycopg.errors
import pytest
from pydantic import SecretStr
from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.exc import OperationalError

from event_pipeline.config import Settings
from event_pipeline.data.engine import make_engine


def test_engine_connects_with_the_configured_statement_timeout(
    db_settings_for: Callable[..., Settings],
) -> None:
    engine = make_engine(db_settings_for(db_statement_timeout_ms=1234))
    try:
        with engine.connect() as connection:
            # pg_settings reports the value in its unit (ms), unlike SHOW, which may print "1234ms".
            setting = connection.execute(
                text("SELECT setting FROM pg_settings WHERE name = 'statement_timeout'")
            ).scalar_one()
            database = connection.execute(text("SELECT current_database()")).scalar_one()
    finally:
        engine.dispose()

    assert setting == "1234"
    assert database.endswith("_test")


def test_engine_passes_the_connect_timeout_to_libpq(
    db_settings_for: Callable[..., Settings],
) -> None:
    engine = make_engine(db_settings_for(db_connect_timeout_s=3))
    try:
        with engine.connect() as connection:
            # What libpq actually used for this connection, not what was passed in.
            parameters = connection.connection.dbapi_connection.info.get_parameters()  # type: ignore[union-attr]
    finally:
        engine.dispose()

    assert parameters["connect_timeout"] == "3"


def test_pooled_connection_ended_by_the_server_is_replaced_before_use(
    db_settings_for: Callable[..., Settings],
) -> None:
    # A PostgreSQL restart or an idle timeout ends the server side of pooled connections. With
    # pool_pre_ping the next checkout notices and opens a new one; without it the caller's first
    # statement would fail with OperationalError.
    engine = make_engine(db_settings_for(db_statement_timeout_ms=1234))
    try:
        with engine.connect() as connection:
            first_pid = connection.execute(text("SELECT pg_backend_pid()")).scalar_one()
        # Back in the pool. End its server process from another, unpooled connection.
        other_engine = make_engine(db_settings_for())
        try:
            with other_engine.connect() as other:
                ended = other.execute(
                    text("SELECT pg_terminate_backend(:pid)"), {"pid": first_pid}
                ).scalar_one()
        finally:
            other_engine.dispose()

        with engine.connect() as connection:
            second_pid = connection.execute(text("SELECT pg_backend_pid()")).scalar_one()
            setting = connection.execute(
                text("SELECT setting FROM pg_settings WHERE name = 'statement_timeout'")
            ).scalar_one()
    finally:
        engine.dispose()

    assert ended is True
    assert second_pid != first_pid
    # The replacement got the same startup parameters as the first connection.
    assert setting == "1234"


def test_statement_over_the_timeout_is_cancelled_by_the_server(
    db_settings_for: Callable[..., Settings],
) -> None:
    timeout_ms = 300
    engine = make_engine(db_settings_for(db_statement_timeout_ms=timeout_ms))
    try:
        with engine.connect() as connection:
            started = time.monotonic()
            with pytest.raises(OperationalError) as exc_info:
                connection.execute(text("SELECT pg_sleep(5)"))
            elapsed = time.monotonic() - started
    finally:
        engine.dispose()

    assert isinstance(exc_info.value.orig, psycopg.errors.QueryCanceled)
    # Cancelled at the timeout, not after the 5 s the statement asked for.
    assert timeout_ms / 1000 <= elapsed < timeout_ms / 1000 + 1.5


def test_wrong_password_fails_without_revealing_it(test_database_url: SecretStr) -> None:
    wrong = "pw-MARKER-wrong-3b7d"
    url = make_url(test_database_url.get_secret_value()).set(password=wrong)
    engine = make_engine(
        Settings(  # type: ignore[call-arg]
            _env_file=None, database_url=SecretStr(url.render_as_string(hide_password=False))
        )
    )
    try:
        with pytest.raises(OperationalError) as exc_info, engine.connect():
            pass
    finally:
        engine.dispose()

    assert "password authentication failed" in str(exc_info.value)
    assert wrong not in str(exc_info.value)
