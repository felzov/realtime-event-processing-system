"""Application factories.

`create_app(publisher, engine=...)`: the caller owns the publisher and the engine (it creates them
before the app and closes them after). The tests use it with fakes or real ones; creating an engine
does not connect, so tests that never query can pass one for a database that does not exist.

`create_app_from_settings()`: what `make run` serves (`uvicorn --factory`). Its lifespan creates
the `EventPublisher` and the engine from settings on startup and closes both on shutdown, after the
server has stopped taking requests and the ones in flight have finished (ADR 0003, decision 10).
"""

import asyncio
from collections.abc import AsyncIterator, Callable
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from datetime import timedelta
from typing import Protocol

from fastapi import FastAPI
from sqlalchemy import Engine

from event_pipeline.api import aggregates, events, health
from event_pipeline.api.dependencies import ReadinessProbe
from event_pipeline.api.errors import register_error_handlers
from event_pipeline.broker.publisher import EventPublisher
from event_pipeline.config import DEFAULT_QUERY_MAX_RANGE_S, Settings
from event_pipeline.data.engine import make_engine
from event_pipeline.services.ingest import Publisher

type Lifespan = Callable[[FastAPI], AbstractAsyncContextManager[None]]


class PublisherWithReadiness(Publisher, ReadinessProbe, Protocol):
    """What the routes need: `POST /events` publishes, `/ready` asks whether it could."""


def create_app(
    publisher: PublisherWithReadiness,
    *,
    engine: Engine,
    query_max_range: timedelta = timedelta(seconds=DEFAULT_QUERY_MAX_RANGE_S),
) -> FastAPI:
    app = _new_app(lifespan=None)
    app.state.publisher = publisher
    app.state.engine = engine
    app.state.query_max_range = query_max_range
    return app


def create_app_from_settings(settings: Settings | None = None) -> FastAPI:
    # Read when uvicorn calls the factory, not at import, so a bad setting fails the start.
    resolved = settings if settings is not None else Settings()

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        # Created first: it holds nothing to close, so a failure creating the publisher after it
        # leaves nothing open. It does not connect: queries answer 503 while PostgreSQL is down.
        engine = make_engine(resolved)
        app.state.engine = engine
        # Creating the producer does not connect either: the app starts even if the broker is
        # down, and `/ready` answers 503 until it is reachable.
        publisher = EventPublisher.from_settings(resolved)
        app.state.publisher = publisher
        app.state.query_max_range = timedelta(seconds=resolved.query_max_range_s)
        try:
            yield
        finally:
            try:
                # `close()` blocks for up to the flush timeout; a worker thread keeps the loop free.
                await asyncio.to_thread(publisher.close, resolved.publisher_close_timeout_s)
            finally:
                # Closes the pooled connections; the in-flight requests have finished by now.
                await asyncio.to_thread(engine.dispose)

    return _new_app(lifespan)


def _new_app(lifespan: Lifespan | None) -> FastAPI:
    app = FastAPI(title="realtime-event-processing-system", lifespan=lifespan)
    register_error_handlers(app)
    app.include_router(events.router)
    app.include_router(aggregates.router)
    app.include_router(health.router)
    return app
