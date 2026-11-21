import re
from typing import Any

import pytest
from pydantic import ValidationError

from event_pipeline.broker.config import producer_config
from event_pipeline.broker.topics import TopicSpec, events_topic
from event_pipeline.config import Settings


def make_settings(**overrides: Any) -> Settings:
    # No `.env` file, so a developer's local file cannot change what the test sees.
    return Settings(_env_file=None, **overrides)  # type: ignore[call-arg]


def test_defaults_describe_the_local_topic() -> None:
    assert events_topic(make_settings()) == TopicSpec("orders.placed.v1", 6, 1)


def test_settings_come_from_the_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "kafka-1:9092,kafka-2:9092")
    monkeypatch.setenv("KAFKA_DELIVERY_TIMEOUT_MS", "2000")

    settings = make_settings()

    assert settings.kafka_bootstrap_servers == "kafka-1:9092,kafka-2:9092"
    assert settings.kafka_delivery_timeout_ms == 2000


def test_producer_config_sets_the_adr_0002_guarantees() -> None:
    config = producer_config(make_settings(kafka_delivery_timeout_ms=4000))

    assert config["acks"] == "all"
    assert config["enable.idempotence"] is True
    assert config["partitioner"] == "murmur2_random"
    assert config["delivery.timeout.ms"] == 4000
    assert config["bootstrap.servers"] == "127.0.0.1:9092"


@pytest.mark.parametrize(
    ("delivery_ms", "backstop_s"),
    [(5000, 5.0), (5000, 4.9), (5000, 6.999)],
    ids=["equal", "shorter", "under_2s_margin"],
)
def test_backstop_must_outlast_the_delivery_timeout(delivery_ms: int, backstop_s: float) -> None:
    with pytest.raises(ValidationError, match="publish_backstop_s must be longer"):
        make_settings(kafka_delivery_timeout_ms=delivery_ms, publish_backstop_s=backstop_s)


def test_backstop_with_2s_margin_is_accepted() -> None:
    # librdkafka reports _MSG_TIMED_OUT up to about a second after delivery.timeout.ms.
    settings = make_settings(kafka_delivery_timeout_ms=5000, publish_backstop_s=7.0)

    assert settings.publish_backstop_s == 7.0


@pytest.mark.parametrize(
    "overrides",
    [
        {"events_topic": "orders placed"},
        {"events_topic": ""},
        {"events_topic": "x" * 250},
        {"events_topic_partitions": 0},
        {"events_topic_replication_factor": 0},
        {"kafka_delivery_timeout_ms": 0},
        {"kafka_admin_timeout_s": 0},
        {"kafka_bootstrap_servers": ""},
        {"ready_timeout_s": 0},
        {"publisher_close_timeout_s": 0},
        {"db_connect_timeout_s": 1},
        {"db_statement_timeout_ms": 0},
        {"db_pool_timeout_s": 0},
    ],
    ids=[
        "topic_with_space",
        "topic_empty",
        "topic_too_long",
        "zero_partitions",
        "zero_replication",
        "zero_delivery_timeout",
        "zero_admin_timeout",
        "empty_bootstrap",
        "zero_ready_timeout",
        "zero_close_timeout",
        "connect_timeout_below_libpq_minimum",
        "zero_statement_timeout",
        "zero_pool_timeout",
    ],
)
def test_invalid_settings_are_rejected(overrides: dict[str, Any]) -> None:
    with pytest.raises(ValidationError):
        make_settings(**overrides)


def test_validation_errors_do_not_echo_values(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("KAFKA_DELIVERY_TIMEOUT_MS", "not-a-number-SECRET")

    with pytest.raises(ValidationError) as exc_info:
        make_settings()

    assert "SECRET" not in str(exc_info.value)


# A distinctive password: no error message or repr may contain it.
DB_PASSWORD = "pw-MARKER-91c4"  # gitleaks:allow


def test_database_url_default_is_the_local_compose_database() -> None:
    url = make_settings().database_url.get_secret_value()

    assert url.startswith("postgresql+psycopg://")
    assert url.endswith("@127.0.0.1:5432/event_pipeline")


def test_database_url_is_hidden_in_the_settings_repr() -> None:
    settings = make_settings(
        database_url=f"postgresql+psycopg://app:{DB_PASSWORD}@db.example:5432/events"
    )

    assert DB_PASSWORD not in repr(settings)
    assert DB_PASSWORD not in str(settings)


@pytest.mark.parametrize(
    ("url", "message"),
    [
        # The driver must be named, not left to SQLAlchemy's default.
        (f"postgresql://app:{DB_PASSWORD}@db:5432/events", "must start with postgresql+psycopg"),
        (f"mysql+pymysql://app:{DB_PASSWORD}@db:3306/events", "must start with postgresql+psycopg"),
        (f"postgresql+psycopg://app:{DB_PASSWORD}@db:5432", "must name a database"),
        (f"not a url {DB_PASSWORD}", "not a valid database URL"),
        # SQLAlchemy's own error for these quotes the text it read as a port: the password.
        (f"postgresql+psycopg://app:{DB_PASSWORD}", "not a valid database URL"),
        (f"postgresql+psycopg://app:p@ss:{DB_PASSWORD}@db:5432/events", "not a valid database URL"),
    ],
    ids=[
        "no_driver",
        "other_dialect",
        "no_database",
        "unparseable",
        "no_host",
        "unencoded_at_in_password",
    ],
)
def test_invalid_database_url_is_rejected_without_echoing_it(url: str, message: str) -> None:
    with pytest.raises(ValidationError, match=re.escape(message)) as exc_info:
        make_settings(database_url=url)

    assert DB_PASSWORD not in str(exc_info.value)
