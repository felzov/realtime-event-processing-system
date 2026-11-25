"""Apply one valid `order.placed` event to its aggregate, at most once per `event_id` (ADR 0004).

One transaction: record the event_id in processed_events with the event's fingerprint, and only if
it was new, add the order to its window's row. A duplicate changes nothing. A duplicate whose
fingerprint differs from the stored one is a conflicting duplicate (ADR 0004, decision 13): the
same event_id reused for different content. It changes nothing either; the first applied copy
stays counted, and the caller reports it. Any database error propagates after the transaction
is rolled back, so neither table changes; the consumer then stops without committing the offset
(ADR 0004, decision 6), and the event is redelivered after a restart.

The text of a propagated error can still contain the event's values: the engine hides statement
parameters, but PostgreSQL's own DETAIL for a constraint violation repeats the failing row. Log the
exception class and SQLSTATE, never `str(exc)`.

The window comes from the event's own `occurred_at` (event time). Whether an event counts at all
is decided before this is called: the consumer skips late and future events (ADR 0005), so every
event that reaches this function is applied.
"""

from enum import Enum

from sqlalchemy import Engine

from event_pipeline.data.aggregates import (
    add_order_to_aggregate,
    record_processed_event,
    recorded_fingerprint,
)
from event_pipeline.domain.events import OrderPlacedEvent
from event_pipeline.domain.fingerprint import fingerprint
from event_pipeline.domain.windows import window_start


class ApplyOutcome(Enum):
    APPLIED = "applied"
    # The event_id was already recorded: this delivery is a redelivery or a client retry.
    DUPLICATE = "duplicate"
    # The event_id was already recorded with different content; the first applied copy stays.
    CONFLICTING_DUPLICATE = "conflicting_duplicate"


def apply_order_placed(engine: Engine, event: OrderPlacedEvent) -> ApplyOutcome:
    """Apply `event` in one committed transaction and report whether it changed the aggregate."""
    body_sha256 = fingerprint(event)
    with engine.begin() as connection:
        if not record_processed_event(connection, event.event_id, body_sha256):
            stored = recorded_fingerprint(connection, event.event_id)
            # None: recorded before revision 0002, nothing to compare with.
            if stored is not None and stored != body_sha256:
                return ApplyOutcome.CONFLICTING_DUPLICATE
            return ApplyOutcome.DUPLICATE
        add_order_to_aggregate(
            connection,
            store_id=event.payload.store_id,
            currency=event.payload.currency,
            window_start=window_start(event.occurred_at),
            amount_minor=event.payload.amount_minor,
        )
    return ApplyOutcome.APPLIED
