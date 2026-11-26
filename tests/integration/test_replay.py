"""Replay against a real broker and PostgreSQL (ADR 0006, decision 7).

The replay runs as the real process (`python -m event_pipeline.replay`) where its output or exit
status matters, and through its service functions where only what it publishes does. Every test has
its own topic, so it sees only its own dead letters.
"""

import os
import re
import subprocess
import sys
import uuid
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient
from pydantic import SecretStr
from sqlalchemy import Engine, select, update

from event_pipeline.broker.config import producer_config
from event_pipeline.broker.publisher import RECEIVED_AT_HEADER, Delivered, format_received_at
from event_pipeline.broker.topics import TopicSpec, ensure_topic
from event_pipeline.config import Settings
from event_pipeline.data.tables import dead_letters
from event_pipeline.domain.dead_letters import DeadLetter, DeadLetterReason, ReplaySelection
from event_pipeline.services.aggregation import ApplyOutcome
from event_pipeline.services.dead_letters import store_dead_letter
from event_pipeline.services.replay import find_dead_letters_to_replay, publish_dead_letters
from tests.integration.kafka_helpers import read_at
from tests.integration.test_consumer import (
    PARTITIONS,
    PAYLOAD_MARKER,
    POLL_TIMEOUT_S,
    RECEIVED_AT,
    TIMEOUT_S,
    WINDOW,
    RunningConsumer,
    committed_offsets,
    counts,
    dead_lettered,
    end_offsets,
    event_body,
    produce,
    wait_for,
)
from tests.samples import encode

pytestmark = pytest.mark.integration

OCCURRED = WINDOW + timedelta(seconds=15)  # event_body's default occurred_at
# Ten minutes after it: late under the default ALLOWED_LATENESS_S (300 s).
LATE_RECEIVED_AT = OCCURRED + timedelta(minutes=10)
LONGER_LATENESS_S = 3600.0


@pytest.fixture
def topic(admin: AdminClient, unique_topic: Callable[[], str]) -> str:
    spec = TopicSpec(unique_topic(), partitions=PARTITIONS, replication_factor=1)
    ensure_topic(admin, spec, TIMEOUT_S)
    return spec.name


@pytest.fixture
def settings(
    topic: str, kafka_bootstrap: str, db_settings_for: Callable[..., Settings]
) -> Settings:
    return db_settings_for(
        kafka_bootstrap_servers=kafka_bootstrap,
        events_topic=topic,
        consumer_group_id=f"test.{uuid.uuid4().hex}",
        consumer_poll_timeout_s=POLL_TIMEOUT_S,
    )


@pytest.fixture
def running() -> Iterator[list[RunningConsumer]]:
    started: list[RunningConsumer] = []
    yield started
    for consumer in started:
        consumer.stop()


@dataclass
class Finished:
    status: int
    out: str
    err: str

    @property
    def shown(self) -> str:
        return self.out + self.err


def run_replay(
    settings: Settings, database_url: SecretStr, cwd: Path, *argv: str, **overrides: str
) -> Finished:
    """The real process, in an empty directory and without the developer's setting variables."""
    env = {k: v for k, v in os.environ.items() if k.lower() not in Settings.model_fields}
    env.update(
        DATABASE_URL=database_url.get_secret_value(),
        KAFKA_BOOTSTRAP_SERVERS=settings.kafka_bootstrap_servers,
        EVENTS_TOPIC=settings.events_topic,
    )
    env.update(overrides)
    done = subprocess.run(
        [sys.executable, "-m", "event_pipeline.replay", *argv],
        env=env,
        cwd=cwd,
        capture_output=True,
        text=True,
        timeout=TIMEOUT_S * 2,
        check=False,
    )
    return Finished(done.returncode, done.stdout, done.stderr)


def published_positions(out: str) -> list[tuple[int, int]]:
    return [
        (int(partition), int(offset))
        for partition, offset in re.findall(r"published at partition (\d+) offset (\d+)", out)
    ]


def consume_everything(
    settings: Settings, engine: Engine, running: list[RunningConsumer]
) -> RunningConsumer:
    """One consumer of `settings`' group until every offset is committed; stopped afterwards."""
    consumer = RunningConsumer(settings, engine)
    running.append(consumer)
    wait_for(
        lambda: committed_offsets(settings) == end_offsets(settings),
        "every offset committed",
        watching=[consumer],
    )
    consumer.stop()
    assert consumer.error is None
    return consumer


def replay(settings: Settings, engine: Engine, selection: ReplaySelection) -> list[object]:
    """Through the service functions, as `--execute` does; returns the delivery outcomes."""
    candidates = find_dead_letters_to_replay(engine, settings.events_topic, selection)
    producer = Producer(producer_config(settings))
    outcomes = publish_dead_letters(
        producer, settings.events_topic, candidates.letters, settings.publish_backstop_s
    )
    assert all(isinstance(outcome, Delivered) for outcome in outcomes), outcomes
    return list(outcomes)


# --- A late event, replayed --------------------------------------------------------------------


def test_a_late_event_counts_after_raising_the_limit_and_a_second_replay_changes_nothing(
    settings: Settings,
    migrated_engine: Engine,
    test_database_url: SecretStr,
    tmp_path: Path,
    running: list[RunningConsumer],
) -> None:
    store_id = f"store-{uuid.uuid4().hex[:8]}"
    key = store_id.encode()
    late = event_body(store_id, amount_minor=10)
    produce(settings, encode(event_body(store_id, amount_minor=1)), key=key)
    produce(settings, encode(late), key=key, received_at=LATE_RECEIVED_AT)
    consume_everything(settings, migrated_engine, running)
    assert counts(migrated_engine, store_id) == [("EUR", 1, 1)]
    [(late_offset, reason, _, event_id)] = dead_lettered(migrated_engine, settings.events_topic)
    assert (reason, event_id) == ("late_event", late["event_id"])

    # A dry run lists it and publishes nothing.
    before = end_offsets(settings)
    dry = run_replay(settings, test_database_url, tmp_path)
    assert dry.status == 0, dry.shown
    assert end_offsets(settings) == before
    lines = dry.out.splitlines()
    assert lines[0] == f"1 dead letter(s) from {settings.events_topic} would be published:"
    assert f" offset {late_offset} late_event, event {event_id}, dead-lettered " in lines[1]
    assert lines[-1] == "dry run: nothing published; add --execute to publish them"

    # Published as it was stored, with its original received_at.
    executed = run_replay(settings, test_database_url, tmp_path, "--execute")
    assert executed.status == 0, executed.shown
    assert executed.out.splitlines()[-1] == (
        f"published 1 of 1 dead letter(s) to {settings.events_topic}"
    )
    [(partition, offset)] = published_positions(executed.out)
    # The same partitioner as the API (murmur2_random), so the copy joins its store's partition.
    with migrated_engine.connect() as connection:
        source_partition = connection.execute(
            select(dead_letters.c.source_partition).where(
                dead_letters.c.source_topic == settings.events_topic
            )
        ).scalar_one()
    assert partition == source_partition
    message = read_at(settings.kafka_bootstrap_servers, settings.events_topic, partition, offset)
    assert message.key() == key
    assert message.value() == encode(late)
    assert message.headers() == [(RECEIVED_AT_HEADER, format_received_at(LATE_RECEIVED_AT))]

    # With a longer allowed lateness the consumer counts it once.
    patient = settings.model_copy(update={"allowed_lateness_s": LONGER_LATENESS_S})
    counted = consume_everything(patient, migrated_engine, running)
    assert counted.outcomes == [ApplyOutcome.APPLIED]
    assert counts(migrated_engine, store_id) == [("EUR", 2, 11)]

    # The dead letter is still there; replaying it again only adds a duplicate.
    again = run_replay(settings, test_database_url, tmp_path, "--execute")
    assert again.status == 0, again.shown
    duplicate = consume_everything(patient, migrated_engine, running)
    assert duplicate.outcomes == [ApplyOutcome.DUPLICATE]
    assert counts(migrated_engine, store_id) == [("EUR", 2, 11)]
    assert len(dead_lettered(migrated_engine, settings.events_topic)) == 1

    for finished in (dry, executed, again):
        assert store_id not in finished.shown
        assert PAYLOAD_MARKER not in finished.shown


def test_a_replay_that_fails_again_adds_one_dead_letter_per_run_not_double(
    settings: Settings, migrated_engine: Engine, running: list[RunningConsumer]
) -> None:
    store_id = f"store-{uuid.uuid4().hex[:8]}"
    late = event_body(store_id, amount_minor=10)
    produce(settings, encode(late), key=store_id.encode(), received_at=LATE_RECEIVED_AT)
    consume_everything(settings, migrated_engine, running)

    # Still late under the same limit: each replay is dead-lettered again at its new offset.
    assert len(replay(settings, migrated_engine, ReplaySelection())) == 1
    consume_everything(settings, migrated_engine, running)
    assert len(dead_lettered(migrated_engine, settings.events_topic)) == 2

    candidates = find_dead_letters_to_replay(
        migrated_engine, settings.events_topic, ReplaySelection()
    )
    assert (len(candidates.letters), candidates.superseded) == (1, 1)
    newest = max(row[0] for row in dead_lettered(migrated_engine, settings.events_topic))
    assert candidates.letters[0].letter.offset == newest
    assert len(replay(settings, migrated_engine, ReplaySelection())) == 1
    consume_everything(settings, migrated_engine, running)

    rows = dead_lettered(migrated_engine, settings.events_topic)
    assert [(reason, event_id) for _, reason, _, event_id in rows] == [
        ("late_event", late["event_id"])
    ] * 3
    assert counts(migrated_engine, store_id) == []


# --- What a replay refuses or sends as it is ---------------------------------------------------


def test_a_conflicting_duplicate_is_refused(
    settings: Settings,
    migrated_engine: Engine,
    test_database_url: SecretStr,
    tmp_path: Path,
    running: list[RunningConsumer],
) -> None:
    store_id = f"store-{uuid.uuid4().hex[:8]}"
    first = event_body(store_id, amount_minor=700)
    reused = event_body(store_id, amount_minor=9, event_id=first["event_id"])
    produce(settings, encode(first), encode(reused), key=store_id.encode())
    consume_everything(settings, migrated_engine, running)
    assert [row[1] for row in dead_lettered(migrated_engine, settings.events_topic)] == [
        "conflicting_duplicate"
    ]
    before = end_offsets(settings)

    listed = run_replay(settings, test_database_url, tmp_path, "--execute")
    asked = run_replay(
        settings, test_database_url, tmp_path, "--reason", "conflicting_duplicate", "--execute"
    )

    assert listed.status == 0, listed.shown
    assert "refused: 1 dead letter(s) with reason conflicting_duplicate" in listed.out
    assert listed.out.splitlines()[-1] == (
        f"published 0 of 0 dead letter(s) to {settings.events_topic}"
    )
    assert asked.status == 2, asked.shown
    assert "conflicting_duplicate cannot be replayed" in asked.err
    assert end_offsets(settings) == before
    assert counts(migrated_engine, store_id) == [("EUR", 1, 700)]


def test_an_invalid_or_unstamped_message_is_published_as_stored_and_dead_lettered_again(
    settings: Settings, migrated_engine: Engine, running: list[RunningConsumer]
) -> None:
    store_id = f"store-{uuid.uuid4().hex[:8]}"
    key = store_id.encode()
    invalid = b'{"order_id": "' + PAYLOAD_MARKER.encode() + b'"'
    unstamped = event_body(store_id, amount_minor=5)
    produce(settings, invalid, key=key)
    produce(settings, encode(unstamped), key=key, received_at=None)
    consume_everything(settings, migrated_engine, running)

    outcomes = replay(settings, migrated_engine, ReplaySelection())

    assert len(outcomes) == 2
    # The invalid one was stamped and keeps its header; the unstamped one is sent without one.
    sent = [
        (invalid, [(RECEIVED_AT_HEADER, format_received_at(RECEIVED_AT))]),
        (encode(unstamped), None),
    ]
    for outcome, (value, headers) in zip(outcomes, sent, strict=True):
        assert isinstance(outcome, Delivered)
        message = read_at(
            settings.kafka_bootstrap_servers,
            settings.events_topic,
            outcome.partition,
            outcome.offset,
        )
        assert (message.key(), message.value(), message.headers()) == (key, value, headers)
    consume_everything(settings, migrated_engine, running)
    rows = dead_lettered(migrated_engine, settings.events_topic)
    assert [(reason, event_id) for _, reason, _, event_id in rows] == [
        ("invalid_event", None),
        ("invalid_received_at", unstamped["event_id"]),
    ] * 2
    assert counts(migrated_engine, store_id) == []


# --- The selection, on stored rows -------------------------------------------------------------


def test_the_selection_filters_and_keeps_only_the_newest_dead_letter_of_each_event(
    migrated_engine: Engine,
) -> None:
    topic_name = f"replay-test-{uuid.uuid4().hex[:8]}"
    event_a, event_b, event_c = uuid.uuid4(), uuid.uuid4(), uuid.uuid4()
    day = datetime(2026, 11, 25, tzinfo=UTC)

    def letter(
        offset: int,
        reason: DeadLetterReason,
        event_id: uuid.UUID | None,
        value: bytes = b"value",
    ) -> None:
        store_dead_letter(
            migrated_engine,
            DeadLetter(
                topic=topic_name,
                partition=offset % 2,
                offset=offset,
                key=b"store",
                value=value,
                received_at_header=None,
                reason=reason,
                detail=None,
                event_id=event_id,
            ),
        )
        with migrated_engine.begin() as connection:
            connection.execute(
                update(dead_letters)
                .where(dead_letters.c.source_topic == topic_name)
                .where(dead_letters.c.source_offset == offset)
                .values(dead_lettered_at=day + timedelta(hours=offset))
            )

    letter(1, DeadLetterReason.LATE_EVENT, event_a)
    letter(2, DeadLetterReason.INVALID_EVENT, None)
    letter(3, DeadLetterReason.LATE_EVENT, event_a)  # a replay of 1, late again
    letter(4, DeadLetterReason.INVALID_EVENT, None)  # a replay of 2, invalid again: same bytes
    letter(5, DeadLetterReason.FUTURE_EVENT, event_b)
    letter(6, DeadLetterReason.CONFLICTING_DUPLICATE, event_b)
    # Two copies of one event with different content, both late: neither was ever applied, so
    # both are published, and the consumer decides between them as it did the first time.
    letter(7, DeadLetterReason.LATE_EVENT, event_c, value=b"first copy")
    letter(8, DeadLetterReason.LATE_EVENT, event_c, value=b"second copy")
    # Another invalid message: other bytes, so not a replay of 2.
    letter(9, DeadLetterReason.INVALID_EVENT, None, value=b"other invalid")
    # Another topic's dead letter is never selected.
    store_dead_letter(
        migrated_engine,
        DeadLetter(
            topic=f"{topic_name}-other",
            partition=0,
            offset=1,
            key=None,
            value=None,
            received_at_header=None,
            reason=DeadLetterReason.LATE_EVENT,
            detail=None,
            event_id=event_a,
        ),
    )

    def offsets(**selection: object) -> tuple[list[int], int, int]:
        found = find_dead_letters_to_replay(
            migrated_engine,
            topic_name,
            ReplaySelection(**selection),  # type: ignore[arg-type]
        )
        return [s.letter.offset for s in found.letters], found.superseded, found.refused

    # Oldest first; 1 is superseded by 3 and 2 by 4; 6 is refused.
    assert offsets() == ([3, 4, 5, 7, 8, 9], 2, 1)
    assert offsets(reasons=frozenset({DeadLetterReason.INVALID_EVENT})) == ([4, 9], 1, 0)
    assert offsets(event_id=event_a) == ([3], 1, 0)
    assert offsets(event_id=event_b) == ([5], 0, 1)
    assert offsets(event_id=event_c) == ([7, 8], 0, 0)
    # [since, until): 3 is dead-lettered exactly at `until`, so the newest copy in range is 1.
    assert offsets(since=day + timedelta(hours=1), until=day + timedelta(hours=3)) == (
        [1, 2],
        0,
        0,
    )
    assert offsets(since=day + timedelta(hours=4)) == ([4, 5, 7, 8, 9], 0, 1)
    assert offsets(until=day) == ([], 0, 0)


# --- Failure -----------------------------------------------------------------------------------


def test_a_replay_the_broker_does_not_confirm_exits_with_status_1(
    settings: Settings, migrated_engine: Engine, test_database_url: SecretStr, tmp_path: Path
) -> None:
    # A topic that does not exist (auto-create is off), so no message can be delivered.
    missing = settings.model_copy(update={"events_topic": f"test.missing.{uuid.uuid4().hex}"})
    store_dead_letter(
        migrated_engine,
        DeadLetter(
            topic=missing.events_topic,
            partition=0,
            offset=3,
            key=b"store",
            value=PAYLOAD_MARKER.encode(),
            received_at_header=format_received_at(RECEIVED_AT),
            reason=DeadLetterReason.LATE_EVENT,
            detail=None,
            event_id=uuid.uuid4(),
        ),
    )

    finished = run_replay(
        missing,
        test_database_url,
        tmp_path,
        "--execute",
        KAFKA_DELIVERY_TIMEOUT_MS="1000",
        PUBLISH_BACKSTOP_S="4",
    )

    assert finished.status == 1, finished.shown
    assert ": NOT published: " in finished.out
    assert finished.out.splitlines()[-1] == (
        f"published 0 of 1 dead letter(s) to {missing.events_topic}"
    )
    assert "running the same replay again is safe" in finished.err
    assert PAYLOAD_MARKER not in finished.shown
