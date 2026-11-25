"""Two consumers in one group while events keep arriving (phase 5 criterion 4, ADR 0004 decisions
9-11).

A producer thread writes events to every partition for the whole test. A second consumer joins,
the group rebalances (cooperative-sticky: only the moving partitions stop), then the first
consumer leaves and the second takes over its partitions. Whatever moved and whenever, every event
must be counted exactly once, and every partition's committed offset must reach the end of its log.
"""

import contextlib
import logging
import threading
import time
import uuid
from collections import defaultdict
from collections.abc import Callable, Iterator

import pytest
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient
from sqlalchemy import Engine

from event_pipeline.broker.config import producer_config
from event_pipeline.broker.publisher import RECEIVED_AT_HEADER, format_received_at
from event_pipeline.broker.topics import TopicSpec, ensure_topic
from event_pipeline.config import Settings
from event_pipeline.consumer.loop import Headers
from event_pipeline.services.aggregation import ApplyOutcome
from tests.integration.test_consumer import (
    PAYLOAD_MARKER,
    POLL_TIMEOUT_S,
    RECEIVED_AT,
    TIMEOUT_S,
    RunningConsumer,
    ask_coordinator,
    committed_offsets,
    counts,
    end_offsets,
    event_body,
    wait_for,
)
from tests.samples import encode

PARTITIONS = 6
STORES = 12  # two per partition
PRODUCE_INTERVAL_S = 0.02


class SteadyProducer:
    """Produces one event every PRODUCE_INTERVAL_S, cycling through the stores, until `stop()`.

    Store i always goes to partition i % PARTITIONS, so every partition receives events.
    Records what it produced, per store, for the expected aggregates.
    """

    def __init__(self, settings: Settings, stores: list[str]) -> None:
        self.settings, self.stores = settings, stores
        self.expected: dict[str, list[int]] = defaultdict(list)  # store -> amounts
        self.stop_flag = threading.Event()
        self.error: BaseException | None = None
        self.thread = threading.Thread(target=self._run, daemon=True)
        self.thread.start()

    def _run(self) -> None:
        producer = Producer(producer_config(self.settings))
        headers: Headers = [(RECEIVED_AT_HEADER, format_received_at(RECEIVED_AT))]
        sent: list[tuple[str, int]] = []
        try:
            n = 0
            while not self.stop_flag.is_set():
                index = n % len(self.stores)
                store_id, amount = self.stores[index], n + 1
                producer.produce(
                    self.settings.events_topic,
                    value=encode(event_body(store_id, amount_minor=amount)),
                    key=store_id.encode(),
                    partition=index % PARTITIONS,
                    headers=headers,
                )
                sent.append((store_id, amount))
                producer.poll(0)
                n += 1
                time.sleep(PRODUCE_INTERVAL_S)
            assert producer.flush(TIMEOUT_S) == 0, "not every event was delivered"
            for store_id, amount in sent:
                self.expected[store_id].append(amount)
        except BaseException as exc:  # read by the test after stop()
            self.error = exc

    def produced(self) -> int:
        return sum(len(amounts) for amounts in self.expected.values())

    def stop(self) -> None:
        self.stop_flag.set()
        self.thread.join(TIMEOUT_S)
        assert not self.thread.is_alive(), "the producer did not stop"
        assert self.error is None, f"the producer failed: {self.error!r}"


def member_assignments(admin: AdminClient, settings: Settings) -> list[frozenset[int]]:
    """The partitions each member of the group currently owns, as the coordinator reports them."""

    def describe() -> list[frozenset[int]]:
        futures = admin.describe_consumer_groups([settings.consumer_group_id])
        group = futures[settings.consumer_group_id].result(timeout=TIMEOUT_S)
        return [
            frozenset(tp.partition for tp in member.assignment.topic_partitions)
            for member in group.members
        ]

    return ask_coordinator(describe)


def split_between_two(assignments: list[frozenset[int]]) -> bool:
    return (
        len(assignments) == 2
        and all(assignments)
        and assignments[0] | assignments[1] == frozenset(range(PARTITIONS))
    )


@pytest.fixture
def settings(
    admin: AdminClient,
    unique_topic: Callable[[], str],
    kafka_bootstrap: str,
    db_settings_for: Callable[..., Settings],
) -> Settings:
    spec = TopicSpec(unique_topic(), partitions=PARTITIONS, replication_factor=1)
    ensure_topic(admin, spec, TIMEOUT_S)
    return db_settings_for(
        kafka_bootstrap_servers=kafka_bootstrap,
        events_topic=spec.name,
        consumer_group_id=f"test.{uuid.uuid4().hex}",
        consumer_poll_timeout_s=POLL_TIMEOUT_S,
    )


@pytest.fixture
def started(migrated_engine: Engine) -> Iterator[list[RunningConsumer | SteadyProducer]]:
    """Everything the test started is stopped, even if stopping one of them fails."""
    things: list[RunningConsumer | SteadyProducer] = []
    yield things
    with contextlib.ExitStack() as stack:
        for thing in things:
            stack.callback(thing.stop)


def test_a_consumer_joining_and_one_leaving_mid_stream_counts_every_event_once(
    admin: AdminClient,
    settings: Settings,
    migrated_engine: Engine,
    started: list[RunningConsumer | SteadyProducer],
    caplog: pytest.LogCaptureFixture,
) -> None:
    stores = [f"store-{uuid.uuid4().hex[:8]}" for _ in range(STORES)]
    caplog.set_level(logging.INFO, logger="event_pipeline.consumer")

    producer = SteadyProducer(settings, stores)
    started.append(producer)
    first = RunningConsumer(settings, migrated_engine)
    started.append(first)
    wait_for(lambda: len(first.outcomes) >= 10, "the first consumer to apply", watching=[first])

    # A second consumer joins: the group splits the partitions between the two.
    second = RunningConsumer(settings, migrated_engine)
    started.append(second)
    wait_for(
        lambda: split_between_two(member_assignments(admin, settings)),
        "the partitions to be split between two consumers",
        watching=[first, second],
    )
    applied_by_second = len(second.outcomes)
    wait_for(
        lambda: len(second.outcomes) > applied_by_second,
        "the second consumer to apply",
        watching=[first, second],
    )

    # The first leaves: the second takes over every partition.
    first.stop()
    wait_for(
        lambda: member_assignments(admin, settings) == [frozenset(range(PARTITIONS))],
        "the second consumer to own every partition",
        watching=[second],
    )
    # Keep producing until every partition has received a message since the takeover. A commit the
    # rebalance refused is not retried; the next message's commit on that partition covers it
    # (ADR 0004, decision 10), so stopping earlier could leave an offset behind for good.
    at_takeover = end_offsets(settings, PARTITIONS)
    wait_for(
        lambda: all(
            end_offsets(settings, PARTITIONS).get(p, 0) > at_takeover.get(p, 0)
            for p in range(PARTITIONS)
        ),
        "a message on every partition after the takeover",
        watching=[second],
    )

    producer.stop()
    refused = [r for r in caplog.records if r.getMessage().startswith("offset not committed")]
    wait_for(
        lambda: committed_offsets(settings, PARTITIONS) == end_offsets(settings, PARTITIONS),
        f"every offset committed ({len(refused)} commits refused by the rebalances)",
        watching=[second],
    )
    second.stop()

    assert first.error is None
    assert second.error is None
    # Every partition received events and has its offset at the end of its log.
    ends = end_offsets(settings, PARTITIONS)
    assert sorted(ends) == list(range(PARTITIONS))
    assert committed_offsets(settings, PARTITIONS) == ends
    assert sum(ends.values()) == producer.produced()
    # Each event applied exactly once across both consumers; a redelivery is a duplicate.
    outcomes = first.outcomes + second.outcomes
    assert outcomes.count(ApplyOutcome.APPLIED) == producer.produced()
    for store_id in stores:
        amounts = producer.expected[store_id]
        assert counts(migrated_engine, store_id) == [("EUR", len(amounts), sum(amounts))]

    messages = [(r.levelno, r.getMessage()) for r in caplog.records]
    topic = settings.events_topic
    assigned = [m for level, m in messages if m.startswith(f"partitions assigned: {topic} ")]
    revoked = [m for level, m in messages if m.startswith(f"partitions revoked: {topic} ")]
    # First consumer: everything; second: its share, then the first one's; both revoked on leaving.
    assert len(assigned) >= 3, messages
    assert len(revoked) >= 2, messages
    assert all(
        level == logging.INFO for level, m in messages if m.startswith("partitions assigned")
    )
    assert all(level == logging.INFO for level, m in messages if m.startswith("partitions revoked"))
    assert not [m for level, m in messages if level >= logging.ERROR]
    assert PAYLOAD_MARKER not in caplog.text
