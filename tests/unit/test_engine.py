"""The engine is configured from settings without connecting; tests/integration covers behaviour."""

from typing import Any

import pytest
from sqlalchemy.pool import NullPool, QueuePool

from event_pipeline.config import Settings
from event_pipeline.data.engine import make_engine, make_readiness_engine, readiness_connect_args

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


@pytest.mark.parametrize(
    ("ready_timeout_s", "connect_timeout_s", "statement_timeout_ms"),
    [(0.5, 2, 500), (2.0, 2, 2000), (2.5, 3, 2500), (0.0011, 2, 2)],
    ids=["below_libpq_minimum", "default", "rounded_up", "sub_millisecond_rounded_up"],
)
def test_readiness_limits_follow_the_ready_timeout(
    ready_timeout_s: float, connect_timeout_s: int, statement_timeout_ms: int
) -> None:
    arguments = readiness_connect_args(make_settings(ready_timeout_s=ready_timeout_s))

    assert arguments == {
        "connect_timeout": connect_timeout_s,
        "options": f"-c statement_timeout={statement_timeout_ms}",
    }


def test_readiness_engine_pools_nothing_and_hides_parameters() -> None:
    engine = make_readiness_engine(
        make_settings(database_url=f"postgresql+psycopg://app:{DB_PASSWORD}@127.0.0.1:1/x_test")
    )

    assert isinstance(engine.pool, NullPool)
    assert engine.hide_parameters
    assert DB_PASSWORD not in repr(engine)
    engine.dispose()
