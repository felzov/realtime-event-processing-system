"""`GET /stores/{store_id}/aggregates`: a store's per-minute order counts and revenue.

Read from PostgreSQL only, the source of truth for aggregates; the topic is never read here. The
parameters are taken as raw strings and validated by `domain/queries.py`, so every rejection gets
the error envelope with reason codes; FastAPI's own validation error would echo the input.
"""

from datetime import datetime
from typing import Annotated

from fastapi import APIRouter, Query, Request
from pydantic import BaseModel

from event_pipeline.api.dependencies import AppEngine, QueryMaxRange
from event_pipeline.api.errors import QUERY_ERROR_RESPONSES
from event_pipeline.domain.queries import QUERY_PARAMETERS, parse_aggregate_query
from event_pipeline.services.queries import store_aggregates

router = APIRouter()


class WindowAggregateBody(BaseModel):
    window_start: datetime
    currency: str
    order_count: int
    revenue_minor: int


@router.get(
    "/stores/{store_id}/aggregates",
    responses=QUERY_ERROR_RESPONSES,
    summary="A store's per-minute order aggregates in [from, to)",
)
def get_store_aggregates(
    store_id: str,
    request: Request,
    engine: AppEngine,
    max_range: QueryMaxRange,
    start: Annotated[str | None, Query(alias="from")] = None,
    end: Annotated[str | None, Query(alias="to")] = None,
    currency: str | None = None,
) -> list[WindowAggregateBody]:
    # A plain `def`: the query blocks, so FastAPI runs it in its thread pool, off the event loop
    # that serves `POST /events`.
    # FastAPI hands over one value per parameter and ignores names it does not declare; the
    # domain rejects both cases, so it is told about them here.
    given = request.query_params
    query = parse_aggregate_query(
        store_id=store_id,
        start=start,
        end=end,
        currency=currency,
        max_range=max_range,
        repeated=frozenset(name for name in QUERY_PARAMETERS if len(given.getlist(name)) > 1),
        unknown_parameters=any(name not in QUERY_PARAMETERS for name in given),
    )
    return [
        WindowAggregateBody(
            window_start=row.window_start,
            currency=row.currency,
            order_count=row.order_count,
            revenue_minor=row.revenue_minor,
        )
        for row in store_aggregates(engine, query)
    ]
