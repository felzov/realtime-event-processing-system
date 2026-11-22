"""Apply one valid `order.placed` event to its aggregate, at most once per `event_id` (ADR 0004).

One transaction: record the event_id in processed_events, and only if it was new, add the order to
its window's row. A duplicate changes nothing. Any database error propagates after the transaction
is rolled back, so neither table changes; the consumer then stops without committing the offset
(ADR 0004, decision 6), and the event is redelivered after a restart.

The text of a propagated error can still contain the event's values: the engine hides statement
parameters, but PostgreSQL's own DETAIL for a constraint violation repeats the failing row. Log the
exception class and SQLSTATE, never `str(exc)`.

The window comes from the event's own `occurred_at` (event time). Every valid event is applied,
however late: the late-event policy is phase 4 (ADR 0005).
"""

from enum import Enum

from sqlalchemy import Engine

from event_pipeline.data.aggregates import add_order_to_aggregate, record_processed_event
from event_pipeline.domain.events import OrderPlacedEvent
from event_pipeline.domain.windows import window_start


class ApplyOutcome(Enum):
    APPLIED = "applied"
    # The event_id was already recorded: this delivery is a redelivery or a client retry.
    DUPLICATE = "duplicate"


def apply_order_placed(engine: Engine, event: OrderPlacedEvent) -> ApplyOutcome:
    """Apply `event` in one committed transaction and report whether it changed the aggregate."""
    with engine.begin() as connection:
        if not record_processed_event(connection, event.event_id):
            return ApplyOutcome.DUPLICATE
        add_order_to_aggregate(
            connection,
            store_id=event.payload.store_id,
            currency=event.payload.currency,
            window_start=window_start(event.occurred_at),
            amount_minor=event.payload.amount_minor,
        )
    return ApplyOutcome.APPLIED
