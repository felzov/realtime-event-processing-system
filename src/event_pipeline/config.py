"""Settings from environment variables, or from a local `.env` file.

All limits and timeouts are explicit here. The defaults are placeholders for a local single-broker
setup, not measured values.
"""

from datetime import timedelta
from typing import Self

from pydantic import Field, SecretStr, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict
from sqlalchemy.engine import make_url
from sqlalchemy.exc import ArgumentError

from event_pipeline.domain.lateness import LatenessPolicy

# librdkafka checks message timeouts about once a second, so `_MSG_TIMED_OUT` can arrive up to about
# a second after `delivery.timeout.ms` (5.6 s for 5000 ms in the ADR 0002 spike); waiting for the
# poll that delivers the report adds a little more.
BACKSTOP_MARGIN_MS = 2_000

# The SQLAlchemy dialect and driver every DATABASE_URL must name: psycopg 3, synchronous (ADR 0002).
# Named explicitly rather than left to SQLAlchemy's default for `postgresql://`, which has changed
# between versions (psycopg2 in 2.0, psycopg in 2.1).
DATABASE_DRIVER = "postgresql+psycopg"

# 24 h; also the default of `create_app`, which the tests call without settings.
DEFAULT_QUERY_MAX_RANGE_S = 86_400

# The late-event policy's defaults (ADR 0005), shared with `create_app` like the one above.
DEFAULT_ALLOWED_LATENESS_S = 300.0
DEFAULT_MAX_FUTURE_SKEW_S = 60.0
DEFAULT_LATENESS = LatenessPolicy(
    allowed_lateness=timedelta(seconds=DEFAULT_ALLOWED_LATENESS_S),
    max_future_skew=timedelta(seconds=DEFAULT_MAX_FUTURE_SKEW_S),
)

# Statements one applied event can run: the pool's pre-ping, the processed_events insert, the
# aggregate upsert and COMMIT (services/aggregation.py).
DB_STATEMENTS_PER_EVENT = 4
# How many times that worst case must fit into max.poll.interval.ms, so it stays well under it.
POLL_INTERVAL_MARGIN = 2
# librdkafka's default session.timeout.ms, which max.poll.interval.ms may not be below.
LIBRDKAFKA_SESSION_TIMEOUT_MS = 45_000


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

    # Bound on each check behind `/ready`, so a probe against an unreachable broker or database
    # answers 503 instead of hanging: the broker metadata request, and the readiness engine's
    # statement timeout and connect timeout (rounded up to whole seconds, at least libpq's 2 s).
    ready_timeout_s: float = Field(default=2.0, gt=0)
    # On shutdown, how long to wait for delivery reports still outstanding (ADR 0003,
    # decision 10). Messages still without a report after it may or may not reach the broker.
    publisher_close_timeout_s: float = Field(default=5.0, gt=0)
    # The longest range `[from, to)` one aggregate query may ask for. It bounds the rows of one
    # response: one per minute window and currency, so 1440 windows per currency by default.
    query_max_range_s: int = Field(default=DEFAULT_QUERY_MAX_RANGE_S, gt=0)

    # The late-event policy (ADR 0005). An event that reached ingest more than ALLOWED_LATENESS_S
    # after its occurred_at, or claims an occurred_at more than MAX_FUTURE_SKEW_S after it reached
    # ingest, changes no aggregate. Both defaults are assumptions, not measured delays.
    allowed_lateness_s: float = Field(default=DEFAULT_ALLOWED_LATENESS_S, gt=0)
    max_future_skew_s: float = Field(default=DEFAULT_MAX_FUTURE_SKEW_S, ge=0)

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

    # The consumer group of `make consumer`. Every consumer with this id shares the topic's
    # partitions and the committed offsets; a new id re-reads the topic from the start (ADR 0004,
    # decision 4), which the duplicates table makes harmless.
    consumer_group_id: str = Field(
        default="event-pipeline-aggregator", min_length=1, max_length=255
    )
    # How long one poll waits for a message. It also bounds how long a stop request waits when no
    # messages arrive.
    consumer_poll_timeout_s: float = Field(default=1.0, gt=0)
    # librdkafka's max.poll.interval.ms: a consumer that does not poll again within it is removed
    # from the group, and its partitions move to another consumer.
    consumer_max_poll_interval_ms: int = Field(default=300_000, gt=0)
    # From a stop request to the process exiting: finishing the current event, leaving the group.
    # Past it the process exits anyway, with an error status (consumer/__main__.py).
    consumer_close_timeout_s: float = Field(default=10.0, gt=0)

    def lateness_policy(self) -> LatenessPolicy:
        """The late-event policy for the consumer and the ingest API (ADR 0005)."""
        return LatenessPolicy(
            allowed_lateness=timedelta(seconds=self.allowed_lateness_s),
            max_future_skew=timedelta(seconds=self.max_future_skew_s),
        )

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
    def _one_event_fits_well_within_the_poll_interval(self) -> Self:
        # The consumer polls again only after the event's transaction and offset commit. Their
        # database part is bounded by the timeouts: waiting for a pooled connection, opening one,
        # and up to DB_STATEMENTS_PER_EVENT statements. If that bound came near max.poll.interval,
        # a slow but successful apply would get the consumer removed from its group mid-event.
        # The offset commit after it is not in the bound: its time is set by librdkafka's own
        # request and coordinator timeouts, not by a setting here; the margin is meant to absorb it.
        apply_bound_ms = (
            self.db_pool_timeout_s + self.db_connect_timeout_s
        ) * 1000 + DB_STATEMENTS_PER_EVENT * self.db_statement_timeout_ms
        if self.consumer_max_poll_interval_ms < LIBRDKAFKA_SESSION_TIMEOUT_MS:
            # librdkafka refuses to create a consumer otherwise (session.timeout.ms is left at its
            # default).
            raise ValueError(
                f"consumer_max_poll_interval_ms must be at least {LIBRDKAFKA_SESSION_TIMEOUT_MS} "
                "(librdkafka's session.timeout.ms)"
            )
        if self.consumer_max_poll_interval_ms < POLL_INTERVAL_MARGIN * apply_bound_ms:
            raise ValueError(
                f"consumer_max_poll_interval_ms must be at least {POLL_INTERVAL_MARGIN} times the "
                "worst-case database time of one event (pool + connect timeouts + "
                f"{DB_STATEMENTS_PER_EVENT} x db_statement_timeout_ms)"
            )
        return self

    @model_validator(mode="after")
    def _lateness_settings_make_a_policy(self) -> Self:
        # Each field can be valid while the policy is not: inf or a huge value overflows
        # timedelta, and a value below a microsecond rounds to zero. Refused here, at load, rather
        # than when the consumer or the API first builds the policy.
        try:
            self.lateness_policy()
        except (ValueError, OverflowError) as exc:
            raise ValueError(f"allowed_lateness_s / max_future_skew_s: {exc}") from None
        return self

    @model_validator(mode="after")
    def _close_timeout_outlasts_a_poll(self) -> Self:
        # Otherwise a stop request while waiting for messages could hit the close timeout.
        if self.consumer_close_timeout_s <= self.consumer_poll_timeout_s:
            raise ValueError("consumer_close_timeout_s must be longer than consumer_poll_timeout_s")
        return self

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
