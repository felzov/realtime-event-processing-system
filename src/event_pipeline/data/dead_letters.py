"""The statements on the dead-letter table (ADR 0006).

Each takes a `Connection` that is already in a transaction and neither commits nor rolls back:
`services/` owns the transaction.
"""

from sqlalchemy import Connection
from sqlalchemy.dialects.postgresql import insert

from event_pipeline.data.tables import dead_letters
from event_pipeline.domain.dead_letters import DeadLetter


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
