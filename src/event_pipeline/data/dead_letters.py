"""The statements on the dead-letter table (ADR 0006).

Each takes a `Connection` that is already in a transaction and neither commits nor rolls back:
`services/` owns the transaction.
"""

from sqlalchemy import ColumnElement, Connection, func, select
from sqlalchemy.dialects.postgresql import insert

from event_pipeline.data.tables import dead_letters
from event_pipeline.domain.dead_letters import (
    NOT_REPLAYABLE,
    DeadLetter,
    DeadLetterReason,
    ReplayCandidates,
    ReplaySelection,
    StoredDeadLetter,
)


def record_dead_letter(connection: Connection, dead_letter: DeadLetter) -> bool:
    """Insert `dead_letter`; True if it was new, False if its position was already recorded.

    A redelivered message has the same topic, partition and offset, so its second insert is a
    conflict and the first row stays as it is (ADR 0006, decision 2).
    """
    statement = (
        insert(dead_letters)
        .values(
            source_topic=dead_letter.topic,
            source_partition=dead_letter.partition,
            source_offset=dead_letter.offset,
            message_key=dead_letter.key,
            message_value=dead_letter.value,
            received_at_header=dead_letter.received_at_header,
            reason_code=dead_letter.reason.value,
            detail=dead_letter.detail,
            event_id=dead_letter.event_id,
        )
        .on_conflict_do_nothing(
            index_elements=[
                dead_letters.c.source_topic,
                dead_letters.c.source_partition,
                dead_letters.c.source_offset,
            ]
        )
        .returning(dead_letters.c.source_offset)
    )
    return connection.execute(statement).first() is not None


def select_for_replay(
    connection: Connection, topic: str, selection: ReplaySelection
) -> ReplayCandidates:
    """The dead letters read from `topic` that `selection` picks (ADR 0006, decision 7).

    Of the dead letters with the same `event_id`, key and value bytes (a message and the replays
    of it that failed again), only the newest is a candidate. Copies of one `event_id` with
    different content are each a candidate: the consumer decides between them as it did the
    first time. Dead letters without an `event_id` (an `invalid_event`) are grouped the same way,
    by key and value: PostgreSQL's PARTITION BY puts NULLs together, and identical bytes fail or
    count identically. Oldest dead letter first, by position within one moment; that is not always
    the order the consumer first read them, since a replay that failed again moves its event later.
    """
    c = dead_letters.c
    matching: list[ColumnElement[bool]] = [c.source_topic == topic]
    if selection.event_id is not None:
        matching.append(c.event_id == selection.event_id)
    if selection.since is not None:
        matching.append(c.dead_lettered_at >= selection.since)
    if selection.until is not None:
        matching.append(c.dead_lettered_at < selection.until)
    refused_values = [reason.value for reason in NOT_REPLAYABLE]
    replayable = [*matching, c.reason_code.not_in(refused_values)]
    if selection.reasons:
        replayable.append(c.reason_code.in_([reason.value for reason in selection.reasons]))

    newest_first = (
        func.row_number()
        .over(
            partition_by=(c.event_id, c.message_key, c.message_value),
            order_by=(c.dead_lettered_at.desc(), c.source_partition.desc(), c.source_offset.desc()),
        )
        .label("newest_first")
    )
    ranked = select(dead_letters, newest_first).where(*replayable).subquery()
    query = (
        select(ranked)
        .where(ranked.c.newest_first == 1)
        .order_by(ranked.c.dead_lettered_at, ranked.c.source_partition, ranked.c.source_offset)
    )
    letters = tuple(
        StoredDeadLetter(
            letter=DeadLetter(
                topic=row.source_topic,
                partition=row.source_partition,
                offset=row.source_offset,
                key=row.message_key,
                value=row.message_value,
                received_at_header=row.received_at_header,
                reason=DeadLetterReason(row.reason_code),
                detail=row.detail,
                event_id=row.event_id,
            ),
            dead_lettered_at=row.dead_lettered_at,
        )
        for row in connection.execute(query)
    )
    matched = connection.execute(
        select(func.count()).select_from(dead_letters).where(*replayable)
    ).scalar_one()
    refused = 0
    if not selection.reasons:
        refused = connection.execute(
            select(func.count())
            .select_from(dead_letters)
            .where(*matching, c.reason_code.in_(refused_values))
        ).scalar_one()
    return ReplayCandidates(letters=letters, superseded=matched - len(letters), refused=refused)
