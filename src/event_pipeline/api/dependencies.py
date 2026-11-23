"""FastAPI dependencies: what `create_app` stores on `app.state`, and the capped body."""

from datetime import timedelta
from typing import Annotated, Protocol

from fastapi import Depends, Request
from sqlalchemy import Engine

from event_pipeline.api.errors import BodyTooLargeError
from event_pipeline.domain.events import MAX_BODY_BYTES
from event_pipeline.services.ingest import Publisher


class ReadinessProbe(Protocol):
    """What `/ready` needs from each of its checks: `broker.publisher.EventPublisher` and
    `services.readiness.DatabaseReadiness`. The API tests pass fakes."""

    def unready_cause(self) -> str | None: ...


# More digits than this is over any cap this API could have, without parsing the number.
_MAX_LENGTH_DIGITS = 20


def _publisher(request: Request) -> Publisher:
    publisher: Publisher = request.app.state.publisher
    return publisher


def _readiness(request: Request) -> ReadinessProbe:
    # The same object as the publisher: what `/ready` reports is whether it can publish.
    probe: ReadinessProbe = request.app.state.publisher
    return probe


def _database_readiness(request: Request) -> ReadinessProbe:
    probe: ReadinessProbe = request.app.state.database_readiness
    return probe


def _engine(request: Request) -> Engine:
    engine: Engine = request.app.state.engine
    return engine


def _query_max_range(request: Request) -> timedelta:
    max_range: timedelta = request.app.state.query_max_range
    return max_range


async def _capped_body(request: Request) -> bytes:
    """The request body, refused with 413 as soon as it is known to exceed `MAX_BODY_BYTES`.

    A declared `Content-Length` over the cap is refused before reading anything. The body is
    counted while it streams in any case: the header is absent for a chunked body, and the server
    (not this code) is what holds a client to the length it declared.
    """
    declared = request.headers.get("content-length")
    # A malformed header never reaches the app behind uvicorn (its HTTP parser refuses it). If one
    # does, it is ignored here and the streamed count below enforces the cap. `isascii()` because
    # `isdigit()` alone accepts characters such as "²" that `int()` refuses; the length limit
    # because `int()` refuses more than 4300 digits, and any value that long is over the cap.
    if (
        declared is not None
        and declared.isascii()
        and declared.isdigit()
        and (len(declared) > _MAX_LENGTH_DIGITS or int(declared) > MAX_BODY_BYTES)
    ):
        raise BodyTooLargeError

    chunks: list[bytes] = []
    size = 0
    async for chunk in request.stream():
        size += len(chunk)
        if size > MAX_BODY_BYTES:
            # Stop consuming: the rest of the body is never read into memory.
            raise BodyTooLargeError
        chunks.append(chunk)
    return b"".join(chunks)


AppPublisher = Annotated[Publisher, Depends(_publisher)]
AppReadiness = Annotated[ReadinessProbe, Depends(_readiness)]
AppDatabaseReadiness = Annotated[ReadinessProbe, Depends(_database_readiness)]
CappedBody = Annotated[bytes, Depends(_capped_body)]
AppEngine = Annotated[Engine, Depends(_engine)]
QueryMaxRange = Annotated[timedelta, Depends(_query_max_range)]
