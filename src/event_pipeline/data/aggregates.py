"""The two statements of the apply transaction (ADR 0004, decision 2).

Each takes a `Connection` that is already in a transaction and neither commits nor rolls back:
`services/aggregation.py` owns the transaction, so both statements commit or neither does.
"""

from datetime import datetime
from uuid import UUID

from sqlalchemy import Connection, func
from sqlalchemy.dialects.postgresql import insert

from event_pipeline.data.tables import order_aggregates, processed_events


def record_processed_event(connection: Connection, event_id: UUID) -> bool:
    """Insert `event_id` into processed_events; True if it was new, False if already recorded.

    If another transaction has inserted the same id and not yet committed, this waits for it on
    the primary key: its commit makes this a conflict (False), its rollback makes this the first
    insert (True). That is what makes concurrent deliveries apply an event once. The wait lasts
    at most the engine's statement timeout (`DB_STATEMENT_TIMEOUT_MS`); after it the insert is
    cancelled and the error propagates like any other.
    """
    statement = (
        insert(processed_events)
        .values(event_id=event_id)
        .on_conflict_do_nothing(index_elements=[processed_events.c.event_id])
        .returning(processed_events.c.event_id)
    )
    return connection.execute(statement).first() is not None


def add_order_to_aggregate(
    connection: Connection,
    *,
    store_id: str,
    currency: str,
    window_start: datetime,
    amount_minor: int,
) -> None:
    """Count one order of `amount_minor` in its window's row, creating the row if needed."""
    statement = insert(order_aggregates).values(
        store_id=store_id,
        window_start=window_start,
        currency=currency,
        order_count=1,
        revenue_minor=amount_minor,
    )
    statement = statement.on_conflict_do_update(
        index_elements=list(order_aggregates.primary_key.columns),
        set_={
            "order_count": order_aggregates.c.order_count + 1,
            "revenue_minor": order_aggregates.c.revenue_minor + statement.excluded.revenue_minor,
            "updated_at": func.now(),
        },
    )
    connection.execute(statement)
