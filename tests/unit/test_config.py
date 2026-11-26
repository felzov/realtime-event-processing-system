import re
from datetime import timedelta
from pathlib import Path
from typing import Any

import pytest
from confluent_kafka import Consumer
from pydantic import ValidationError

from event_pipeline.broker.config import consumer_config, producer_config
from event_pipeline.broker.topics import TopicSpec, events_topic
from event_pipeline.config import Settings
from event_pipeline.data.errors import RetryPolicy


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
        {"db_apply_attempts": 0},
        {"consumer_group_id": ""},
        {"consumer_poll_timeout_s": 0},
        {"consumer_max_poll_interval_ms": 0},
        {"consumer_close_timeout_s": 0},
        {"query_max_range_s": 0},
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
        "zero_apply_attempts",
        "empty_group_id",
        "zero_poll_timeout",
        "zero_max_poll_interval",
        "zero_consumer_close_timeout",
        "zero_query_max_range",
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


def test_consumer_config_sets_the_adr_0004_offset_policy_and_strategy() -> None:
    config = consumer_config(
        make_settings(consumer_group_id="aggregator-test", consumer_max_poll_interval_ms=480_000)
    )

    assert config == {
        "bootstrap.servers": "127.0.0.1:9092",
        "group.id": "aggregator-test",
        "enable.auto.commit": False,
        "auto.offset.reset": "earliest",
        "max.poll.interval.ms": 480_000,
        "partition.assignment.strategy": "cooperative-sticky",
    }


# Worst-case database time of one message with these: one transaction is (5 + 5) s + 4 x 10 s =
# 50 s; three attempts and 0.5 + 1 s of pauses make 151.5 s; two transactions 303 s; twice is 606 s.
LONG_STATEMENTS = {"db_statement_timeout_ms": 10_000}
# The same with the default timeouts: 30 s per transaction, 2 x (3 x 30 + 1.5) = 183 s; twice is
# 366 s.
DEFAULT_FLOOR_MS = 366_000


@pytest.mark.parametrize(
    "overrides",
    [
        {**LONG_STATEMENTS, "consumer_max_poll_interval_ms": 606_000},
        {"consumer_max_poll_interval_ms": DEFAULT_FLOOR_MS},
        # librdkafka's lower bound, with timeouts small enough not to matter (one attempt: 2 x 7 s,
        # twice is 28 s).
        {
            "db_pool_timeout_s": 1.0,
            "db_connect_timeout_s": 2,
            "db_statement_timeout_ms": 1000,
            "db_apply_attempts": 1,
            "consumer_max_poll_interval_ms": 45_000,
        },
    ],
    ids=["twice_the_worst_case_message", "default_timeouts_floor", "session_timeout"],
)
def test_accepted_max_poll_interval_makes_a_consumer_librdkafka_accepts(
    overrides: dict[str, Any],
) -> None:
    settings = make_settings(**overrides)

    # Creating the client validates the configuration; it does not connect.
    Consumer(consumer_config(settings)).close()


@pytest.mark.parametrize(
    "overrides",
    [
        {**LONG_STATEMENTS, "consumer_max_poll_interval_ms": 605_999},
        {"consumer_max_poll_interval_ms": DEFAULT_FLOOR_MS - 1},
        # One more attempt: 4 x 30 s + 0.5 + 1 + 2 s = 123.5 s per transaction, 494 s floor.
        {"db_apply_attempts": 4, "consumer_max_poll_interval_ms": 493_999},
    ],
    ids=["long_statements", "default_timeouts", "four_attempts"],
)
def test_max_poll_interval_must_be_well_over_the_worst_case_message(
    overrides: dict[str, Any],
) -> None:
    with pytest.raises(ValidationError, match="consumer_max_poll_interval_ms must be at least 2"):
        make_settings(**overrides)


def test_four_attempts_fit_at_their_floor() -> None:
    assert make_settings(db_apply_attempts=4, consumer_max_poll_interval_ms=494_000)


def test_the_default_max_poll_interval_covers_the_default_worst_case() -> None:
    # Phase 6 raised it from 300 s (librdkafka's default), which is below the 366 s floor.
    assert make_settings().consumer_max_poll_interval_ms == 600_000
    with pytest.raises(ValidationError, match="consumer_max_poll_interval_ms must be at least 2"):
        make_settings(consumer_max_poll_interval_ms=300_000)


def test_long_statement_timeout_needs_a_longer_max_poll_interval_than_the_default() -> None:
    # The default max.poll.interval (600 s) against a 10 s statement timeout: 2 x 303 s.
    with pytest.raises(ValidationError, match="consumer_max_poll_interval_ms must be at least 2"):
        make_settings(**LONG_STATEMENTS)


def test_max_poll_interval_below_the_session_timeout_is_rejected() -> None:
    # librdkafka refuses it: "max.poll.interval.ms must be >= session.timeout.ms".
    with pytest.raises(ValidationError, match="at least 45000"):
        make_settings(
            db_pool_timeout_s=1.0,
            db_connect_timeout_s=2,
            db_statement_timeout_ms=1000,
            consumer_max_poll_interval_ms=44_999,
        )


@pytest.mark.parametrize("close_timeout_s", [1.0, 0.5], ids=["equal", "shorter"])
def test_close_timeout_must_outlast_a_poll(close_timeout_s: float) -> None:
    with pytest.raises(ValidationError, match="consumer_close_timeout_s must be longer"):
        make_settings(consumer_poll_timeout_s=1.0, consumer_close_timeout_s=close_timeout_s)


def test_env_example_lists_every_setting_with_its_default() -> None:
    # .env.example documents the settings; a setting added without it, or a changed default not
    # copied there, fails here.
    path = Path(__file__).resolve().parents[2] / ".env.example"
    from_example = Settings(_env_file=path)  # type: ignore[call-arg]
    names = {
        line.split("=", 1)[0].lower()
        for line in path.read_text().splitlines()
        if line and not line.startswith("#")
    }

    assert names == set(Settings.model_fields)
    assert from_example == make_settings()


def test_the_lateness_policy_comes_from_the_settings() -> None:
    settings = make_settings(allowed_lateness_s=90.5, max_future_skew_s=0)

    policy = settings.lateness_policy()

    assert policy.allowed_lateness == timedelta(seconds=90.5)
    assert policy.max_future_skew == timedelta(0)


def test_the_default_lateness_policy() -> None:
    policy = make_settings().lateness_policy()

    assert policy.allowed_lateness == timedelta(minutes=5)
    assert policy.max_future_skew == timedelta(minutes=1)


@pytest.mark.parametrize(
    "overrides",
    [{"allowed_lateness_s": 0}, {"allowed_lateness_s": -1}, {"max_future_skew_s": -1}],
    ids=["zero_lateness", "negative_lateness", "negative_skew"],
)
def test_a_lateness_setting_that_would_drop_every_event_is_refused(
    overrides: dict[str, float],
) -> None:
    with pytest.raises(ValidationError):
        make_settings(**overrides)


@pytest.mark.parametrize(
    "overrides",
    [
        {"allowed_lateness_s": 1e-7},
        {"allowed_lateness_s": 1e14},
        {"allowed_lateness_s": float("inf")},
        {"max_future_skew_s": float("inf")},
    ],
    ids=["rounds_to_zero", "overflows_timedelta", "inf_lateness", "inf_skew"],
)
def test_a_lateness_setting_that_cannot_become_a_policy_is_refused_at_load(
    overrides: dict[str, float],
) -> None:
    # Refused when the settings load, not later when the consumer or the API builds the policy.
    with pytest.raises(ValidationError):
        make_settings(**overrides)


def test_the_retry_policy_comes_from_the_settings() -> None:
    policy = make_settings(
        db_apply_attempts=5, consumer_max_poll_interval_ms=900_000
    ).db_retry_policy()

    assert policy == RetryPolicy(attempts=5, first_backoff_s=0.5, max_backoff_s=2.0)
