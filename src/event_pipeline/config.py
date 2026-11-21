"""Settings from environment variables, or from a local `.env` file.

All limits and timeouts are explicit here. The defaults are placeholders for a local single-broker
setup, not measured values.
"""

from typing import Self

from pydantic import Field, SecretStr, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict
from sqlalchemy.engine import make_url
from sqlalchemy.exc import ArgumentError

# librdkafka checks message timeouts about once a second, so `_MSG_TIMED_OUT` can arrive up to about
# a second after `delivery.timeout.ms` (5.6 s for 5000 ms in the ADR 0002 spike); waiting for the
# poll that delivers the report adds a little more.
BACKSTOP_MARGIN_MS = 2_000

# The SQLAlchemy dialect and driver every DATABASE_URL must name: psycopg 3, synchronous (ADR 0002).
# Named explicitly rather than left to SQLAlchemy's default for `postgresql://`, which has changed
# between versions (psycopg2 in 2.0, psycopg in 2.1).
DATABASE_DRIVER = "postgresql+psycopg"


class Settings(BaseSettings):
    # Unknown keys in `.env` are ignored. Validation errors would otherwise print the values read,
    # so they are hidden: DATABASE_URL contains the database password.
    model_config = SettingsConfigDict(env_file=".env", extra="ignore", hide_input_in_errors=True)

    # Comma-separated host:port list. Plaintext and unauthenticated: local development only.
    # 127.0.0.1 rather than localhost, matching the published port (compose.yaml).
    kafka_bootstrap_servers: str = Field(default="127.0.0.1:9092", min_length=1)

    # Kafka allows up to 249 characters from this set. The partition count and the replication
    # factor are part of the topic's contract: changing the count moves stores to other
    # partitions, so it means a new `.v2` topic (ADR 0001, decision 6).
    events_topic: str = Field(
        default="orders.placed.v1", min_length=1, max_length=249, pattern=r"^[A-Za-z0-9._-]+$"
    )
    events_topic_partitions: int = Field(default=6, ge=1)
    # 1 because there is one local broker; a real deployment needs at least 3 (ADR 0002).
    events_topic_replication_factor: int = Field(default=1, ge=1)

    # Bounds topic creation and, separately, the metadata wait that checks an existing topic, so
    # `make topics` can take about twice this in the worst case.
    kafka_admin_timeout_s: float = Field(default=10.0, gt=0)

    # librdkafka's limit for one message, retries included, after which the delivery report
    # carries `_MSG_TIMED_OUT` (ADR 0002, decision 4). The ingest API answers 503 then.
    kafka_delivery_timeout_ms: int = Field(default=5_000, gt=0)
    # Outer limit on waiting for a delivery report. Only a bug (a report that never arrives)
    # should reach it, so it must outlast the delivery timeout by BACKSTOP_MARGIN_MS.
    publish_backstop_s: float = Field(default=10.0, gt=0)

    # Bound on the broker metadata request behind `/ready`, so a probe against an unreachable
    # broker answers 503 in about this long instead of hanging.
    ready_timeout_s: float = Field(default=2.0, gt=0)
    # On shutdown, how long to wait for delivery reports still outstanding (ADR 0003,
    # decision 10). Messages still without a report after it may or may not reach the broker.
    publisher_close_timeout_s: float = Field(default=5.0, gt=0)

    # The default is the local Compose database (compose.yaml); its password is a fixed development
    # value, not a secret. A SecretStr keeps the URL, password included, out of reprs and logs.
    database_url: SecretStr = SecretStr(
        f"{DATABASE_DRIVER}://event_pipeline:event_pipeline_local@127.0.0.1:5432/event_pipeline"
    )
    # libpq's connect_timeout, in whole seconds; libpq treats anything below 2 as 2.
    db_connect_timeout_s: int = Field(default=5, ge=2)
    # PostgreSQL's statement_timeout for every connection of the engine: a statement running longer
    # is cancelled by the server, so no query can hang a request or the consumer loop.
    db_statement_timeout_ms: int = Field(default=5_000, gt=0)
    # How long a caller waits for a free connection from the engine's pool before an error.
    db_pool_timeout_s: float = Field(default=5.0, gt=0)

    @field_validator("database_url")
    @classmethod
    def _database_url_names_the_driver(cls, value: SecretStr) -> SecretStr:
        # Checked at startup so a wrong URL fails with a clear message instead of on first use. The
        # message never contains the URL: SQLAlchemy's own ValueError for a bad port quotes the
        # text it read as the port, which is the password when the host is missing or the password
        # holds an unencoded `@`.
        try:
            url = make_url(value.get_secret_value())
        except (ArgumentError, ValueError):
            raise ValueError("database_url is not a valid database URL") from None
        if url.drivername != DATABASE_DRIVER:
            raise ValueError(f"database_url must start with {DATABASE_DRIVER}://")
        if not url.database:
            raise ValueError("database_url must name a database")
        return value

    @model_validator(mode="after")
    def _backstop_outlasts_delivery_timeout(self) -> Self:
        # Otherwise the backstop would fire before librdkafka reports, turning an ordinary
        # broker timeout into the "should never happen" path.
        if self.publish_backstop_s * 1000 < self.kafka_delivery_timeout_ms + BACKSTOP_MARGIN_MS:
            raise ValueError(
                "publish_backstop_s must be longer than kafka_delivery_timeout_ms by at least "
                f"{BACKSTOP_MARGIN_MS} ms"
            )
        return self
