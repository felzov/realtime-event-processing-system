"""A fake publisher and an in-process client for the API tests.

Requests go through httpx's `ASGITransport`, which does not run the app's lifespan: the app from
`make_app` (below) needs none.
"""

import asyncio
from dataclasses import dataclass
from datetime import datetime
from typing import Any

import httpx
from fastapi import FastAPI
from pydantic import SecretStr
from sqlalchemy import Engine

from event_pipeline.broker.publisher import Delivered, Delivery
from event_pipeline.config import Settings
from event_pipeline.data.engine import make_engine
from event_pipeline.main import PublisherWithReadiness, create_app

DELIVERED = Delivered(partition=3, offset=41)

# Port 1 on 127.0.0.1: nothing listens. Creating an engine does not connect, so tests that never
# query are unaffected, and one that does gets a refused connection at once, never a real database.
NO_DATABASE_URL = "postgresql+psycopg://nobody@127.0.0.1:1/no_database_test"


def no_database() -> Engine:
    """The application's engine (`make_engine`) for a database that does not exist."""
    settings = Settings(_env_file=None, database_url=SecretStr(NO_DATABASE_URL))  # type: ignore[call-arg]
    return make_engine(settings)


class FakeReadiness:
    """A readiness check (`/ready`'s database half) that answers `cause`, or raises `raises`."""

    def __init__(self, cause: str | None = None, raises: Exception | None = None) -> None:
        self.cause = cause
        self.raises = raises
        self.calls = 0

    def unready_cause(self) -> str | None:
        self.calls += 1
        if self.raises is not None:
            raise self.raises
        return self.cause


def make_app(
    publisher: PublisherWithReadiness | None = None,
    *,
    engine: Engine | None = None,
    database_readiness: FakeReadiness | None = None,
    **kwargs: Any,
) -> FastAPI:
    """`create_app` with fakes for what a test does not name: a publisher that delivers, an engine
    for a database that does not exist, and a database check that reports ready."""
    return create_app(
        publisher if publisher is not None else FakePublisher(),
        engine=engine if engine is not None else no_database(),
        database_readiness=database_readiness
        if database_readiness is not None
        else FakeReadiness(),
        **kwargs,
    )


@dataclass(frozen=True, slots=True)
class PublishCall:
    key: str
    value: bytes
    received_at: datetime


class FakePublisher:
    def __init__(
        self, outcome: Delivery | Exception = DELIVERED, unready_cause: str | None = None
    ) -> None:
        self.outcome = outcome
        self.calls: list[PublishCall] = []
        self.cause = unready_cause

    async def publish(self, *, key: str, value: bytes, received_at: datetime) -> Delivery:
        self.calls.append(PublishCall(key, value, received_at))
        if isinstance(self.outcome, Exception):
            raise self.outcome
        return self.outcome

    def unready_cause(self) -> str | None:
        return self.cause


def send(
    app: FastAPI,
    method: str = "POST",
    path: str = "/events",
    raise_app_exceptions: bool = True,
    **kwargs: Any,
) -> httpx.Response:
    async def request() -> httpx.Response:
        transport = httpx.ASGITransport(app=app, raise_app_exceptions=raise_app_exceptions)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            return await client.request(method, path, **kwargs)

    return asyncio.run(request())


def error_of(response: httpx.Response) -> dict[str, Any]:
    body: dict[str, Any] = response.json()
    assert set(body) == {"error"}
    assert set(body["error"]) == {"code", "message", "violations"}
    error: dict[str, Any] = body["error"]
    return error
