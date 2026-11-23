"""The statements on the aggregate tables.

The two statements of the apply transaction (ADR 0004, decision 2) and the query API's select.
Each takes a `Connection` that is already in a transaction and neither commits nor rolls back:
`services/` owns the transaction, so both apply statements commit or neither does.
"""

from dataclasses import dataclass
from datetime import UTC, datetime
from uuid import UUID

from sqlalchemy import Connection, func, select
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


@dataclass(frozen=True, slots=True)
class WindowAggregate:
    """One row of order_aggregates, as the query API returns it; `window_start` is in UTC."""

    window_start: datetime
    currency: str
    order_count: int
    revenue_minor: int


def select_store_aggregates(
    connection: Connection,
    *,
    store_id: str,
    start: datetime,
    end: datetime,
    currency: str | None,
) -> list[WindowAggregate]:
    """The store's rows with `start <= window_start < end`, by window_start, then currency.

    The primary key (store_id, window_start, currency) serves both the filter and the order: one
    index range scan, no sort.
    """
    table = order_aggregates.c
    statement = (
        select(table.window_start, table.currency, table.order_count, table.revenue_minor)
        .where(table.store_id == store_id, table.window_start >= start, table.window_start < end)
        .order_by(table.window_start, table.currency)
    )
    if currency is not None:
        statement = statement.where(table.currency == currency)
    return [
        WindowAggregate(
            # timestamptz comes back in the session's time zone; the API speaks UTC.
            window_start=row.window_start.astimezone(UTC),
            currency=row.currency,
            order_count=row.order_count,
            revenue_minor=row.revenue_minor,
        )
        for row in connection.execute(statement)
    ]
