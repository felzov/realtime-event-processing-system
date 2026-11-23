"""The only places Kafka client configuration is built (ADR 0002)."""

from event_pipeline.config import Settings

# The value types librdkafka accepts in a configuration dict.
type ClientConfig = dict[str, str | int | float | bool]


def producer_config(settings: Settings) -> ClientConfig:
    """Configuration for every producer that writes events.

    - `acks=all` and idempotence: a delivery report means the broker stored the message, and
      librdkafka's own retries neither duplicate nor reorder messages within a partition.
    - `murmur2_random`: keys land on the same partitions as with Kafka's Java tools. librdkafka's
      default partitioner maps them differently, which would break per-store order on a replay
      (ADR 0002, decision 3).
    """
    return {
        "bootstrap.servers": settings.kafka_bootstrap_servers,
        "acks": "all",
        "enable.idempotence": True,
        "partitioner": "murmur2_random",
        "delivery.timeout.ms": settings.kafka_delivery_timeout_ms,
    }


def admin_config(settings: Settings) -> ClientConfig:
    return {"bootstrap.servers": settings.kafka_bootstrap_servers}


def consumer_config(settings: Settings) -> ClientConfig:
    """Configuration for the aggregating consumer (ADR 0002, decision 5; ADR 0004).

    - `enable.auto.commit=false`: the loop commits each message's offset itself, after the
      database transaction holding its effect has committed (ADR 0004, decisions 1 and 3).
    - `auto.offset.reset=earliest`: a group with no committed offset starts at the beginning of
      each partition, so events produced before the consumer first ran are counted (decision 4).
    - The partition assignment strategy is librdkafka's default (decision 5).
    """
    return {
        "bootstrap.servers": settings.kafka_bootstrap_servers,
        "group.id": settings.consumer_group_id,
        "enable.auto.commit": False,
        "auto.offset.reset": "earliest",
        "max.poll.interval.ms": settings.consumer_max_poll_interval_ms,
    }
