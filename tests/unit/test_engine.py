"""The engine is configured from settings without connecting; tests/integration covers behaviour."""

from typing import Any

from sqlalchemy.pool import QueuePool

from event_pipeline.config import Settings
from event_pipeline.data.engine import make_engine

DB_PASSWORD = "pw-MARKER-5e0a"  # gitleaks:allow


def make_settings(**overrides: Any) -> Settings:
    return Settings(_env_file=None, **overrides)  # type: ignore[call-arg]


def test_engine_waits_for_a_pooled_connection_only_as_long_as_configured() -> None:
    # Port 1: nothing listens. Creating the engine must not try to connect.
    engine = make_engine(
        make_settings(
            database_url="postgresql+psycopg://u@127.0.0.1:1/x_test", db_pool_timeout_s=2.5
        )
    )

    assert isinstance(engine.pool, QueuePool)
    assert engine.pool.timeout() == 2.5
    engine.dispose()


def test_engine_repr_masks_the_password() -> None:
    engine = make_engine(
        make_settings(database_url=f"postgresql+psycopg://app:{DB_PASSWORD}@127.0.0.1:1/events")
    )

    assert DB_PASSWORD not in repr(engine)
    assert DB_PASSWORD not in str(engine.url)
    engine.dispose()


def test_engine_hides_statement_parameters_in_errors() -> None:
    # Parameters are event values (store_id, amounts); errors must not quote them.
    engine = make_engine(make_settings())

    assert engine.hide_parameters is True
