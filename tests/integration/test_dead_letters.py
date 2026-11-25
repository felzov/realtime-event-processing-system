"""record_dead_letter against PostgreSQL (ADR 0006, decision 2).

Every test uses its own topic name, so tests never see each other's rows and need no cleanup.
"""

import dataclasses
import uuid
from typing import Any

import pytest
from sqlalchemy import Engine, Row, select

from event_pipeline.data.dead_letters import record_dead_letter
from event_pipeline.data.tables import dead_letters
from event_pipeline.domain.dead_letters import DeadLetter, DeadLetterReason

pytestmark = pytest.mark.integration


def dead_letter(**overrides: Any) -> DeadLetter:
    letter = DeadLetter(
        topic=f"dead-letter-test-{uuid.uuid4().hex[:8]}",
        partition=2,
        offset=41,
        key=b"store-1",
        # Not UTF-8: the value is kept as bytes, whatever it is.
        value=b'{"event_id": \xff}',
        received_at_header=b"2026-11-25T10:00:00.000000Z",
        reason=DeadLetterReason.INVALID_EVENT,
        detail="invalid_json",
        event_id=None,
    )
    return dataclasses.replace(letter, **overrides)


def record(engine: Engine, letter: DeadLetter) -> bool:
    with engine.begin() as connection:
        return record_dead_letter(connection, letter)


def stored(engine: Engine, topic: str) -> list[Row[Any]]:
    query = (
        select(dead_letters)
        .where(dead_letters.c.source_topic == topic)
        .order_by(dead_letters.c.source_partition, dead_letters.c.source_offset)
    )
    with engine.connect() as connection:
        return list(connection.execute(query))


def test_a_dead_letter_is_stored_as_read(migrated_engine: Engine) -> None:
    event_id = uuid.uuid4()
    letter = dead_letter(
        reason=DeadLetterReason.LATE_EVENT,
        detail="late_event, received 412.000 s after occurred_at",
        event_id=event_id,
    )

    assert record(migrated_engine, letter) is True

    [row] = stored(migrated_engine, letter.topic)
    assert (row.source_topic, row.source_partition, row.source_offset) == (letter.topic, 2, 41)
    assert row.message_key == b"store-1"
    assert row.message_value == b'{"event_id": \xff}'
    assert row.received_at_header == b"2026-11-25T10:00:00.000000Z"
    assert row.reason_code == "late_event"
    assert row.detail == "late_event, received 412.000 s after occurred_at"
    assert row.event_id == event_id
    assert row.dead_lettered_at is not None


def test_a_message_without_key_value_or_header_is_stored(migrated_engine: Engine) -> None:
    letter = dead_letter(key=None, value=None, received_at_header=None, detail=None)

    assert record(migrated_engine, letter) is True

    [row] = stored(migrated_engine, letter.topic)
    assert (row.message_key, row.message_value, row.received_at_header) == (None, None, None)
    assert row.detail is None


def test_a_redelivered_message_is_recorded_once_and_keeps_the_first_row(
    migrated_engine: Engine,
) -> None:
    first = dead_letter()
    assert record(migrated_engine, first) is True

    # Same position: the same message read again. Whatever the second read says, the first stays.
    again = dataclasses.replace(
        first, reason=DeadLetterReason.DATABASE_REJECTED, detail="DataError", value=b"other"
    )
    assert record(migrated_engine, again) is False

    [row] = stored(migrated_engine, first.topic)
    assert (row.reason_code, row.detail, row.message_value) == (
        "invalid_event",
        "invalid_json",
        first.value,
    )


def test_another_position_is_another_dead_letter(migrated_engine: Engine) -> None:
    first = dead_letter()
    others = [
        dataclasses.replace(first, offset=42),
        dataclasses.replace(first, partition=3),
    ]

    assert [record(migrated_engine, letter) for letter in [first, *others]] == [True, True, True]
    assert len(stored(migrated_engine, first.topic)) == 3


def test_an_offset_past_the_32_bit_range_is_stored(migrated_engine: Engine) -> None:
    # Kafka offsets are 64-bit; a partition passes 2**31 - 1 after about 2.1 billion messages.
    letter = dead_letter(offset=2**31 + 7)

    assert record(migrated_engine, letter) is True
    assert stored(migrated_engine, letter.topic)[0].source_offset == 2**31 + 7


@pytest.mark.parametrize("reason", list(DeadLetterReason))
def test_every_reason_is_accepted(migrated_engine: Engine, reason: DeadLetterReason) -> None:
    letter = dead_letter(reason=reason)

    assert record(migrated_engine, letter) is True
    assert stored(migrated_engine, letter.topic)[0].reason_code == reason.value
