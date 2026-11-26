"""Record a message the consumer does not count (ADR 0006, decision 2).

One committed transaction per dead letter, so the consumer commits the message's offset only once
the dead letter is stored. Any database error propagates after the transaction is rolled back; as
with an apply, its text can contain values, so it is logged by class and SQLSTATE only.
"""

from sqlalchemy import Engine

from event_pipeline.data.dead_letters import record_dead_letter
from event_pipeline.domain.dead_letters import DeadLetter


def store_dead_letter(engine: Engine, dead_letter: DeadLetter) -> bool:
    """Store `dead_letter`; True if it was new, False if a redelivery found it already stored."""
    with engine.begin() as connection:
        return record_dead_letter(connection, dead_letter)
