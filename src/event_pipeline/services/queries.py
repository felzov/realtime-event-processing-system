"""Read a store's aggregates from PostgreSQL, the source of truth for them (architecture).

A read-only transaction on a pooled connection: opened here, rolled back when the connection is
returned (nothing was written). Bounded by the engine's pool, connect and statement timeouts; a
database error propagates, and the API maps it (api/errors.py).
"""

from sqlalchemy import Engine

from event_pipeline.data.aggregates import WindowAggregate, select_store_aggregates
from event_pipeline.domain.queries import AggregateQuery


def store_aggregates(engine: Engine, query: AggregateQuery) -> list[WindowAggregate]:
    with engine.connect() as connection:
        return select_store_aggregates(
            connection,
            store_id=query.store_id,
            start=query.start,
            end=query.end,
            currency=query.currency,
        )
