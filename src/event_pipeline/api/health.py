"""`/health` (liveness) and `/ready` (readiness).

`/health` answers 200 whenever the process can serve HTTP; it checks nothing else, so a broker or
database outage never makes an orchestrator restart a healthy process. `/ready` answers 200 only
when both the ingest and the query API could succeed now:

1. the publisher: its poll thread is alive, the broker answers and the topic exists;
2. then the database: reachable on the readiness engine, and at `EXPECTED_REVISION` (phase 3, E6).

The first cause found is logged, never returned: a failing check at WARNING, an exception from a
check (a bug, not an outage) at ERROR. Each check is bounded while the other side responds: the
broker by `READY_TIMEOUT_S`, the database by the readiness engine's connect and statement timeouts,
so with the defaults a probe takes up to about 3 x `READY_TIMEOUT_S`. The statement timeout is the
server's: a PostgreSQL that hangs after accepting the connection holds the probe until the OS gives
up on the socket. A database outage also takes ingest out of rotation: one process serves both.
"""

import logging
from typing import Literal

from fastapi import APIRouter, status
from pydantic import BaseModel

from event_pipeline.api.dependencies import AppDatabaseReadiness, AppReadiness, ReadinessProbe
from event_pipeline.api.errors import ErrorResponse, NotReadyError

logger = logging.getLogger(__name__)

# The prefix of the cause for an exception raised by a check.
_UNEXPECTED = "unexpected:"

router = APIRouter()


class Healthy(BaseModel):
    status: Literal["ok"] = "ok"


class Ready(BaseModel):
    status: Literal["ready"] = "ready"


@router.get("/health", summary="Liveness: the process serves HTTP")
async def health() -> Healthy:
    return Healthy()


@router.get(
    "/ready",
    responses={status.HTTP_503_SERVICE_UNAVAILABLE: {"model": ErrorResponse}},
    summary="Readiness: events can be published now",
)
def ready(publisher: AppReadiness, database: AppDatabaseReadiness) -> Ready:
    # A plain `def`: FastAPI runs it in its thread pool, because the checks block for up to their
    # timeouts and must not block the event loop that serves `POST /events`.
    for probe in (publisher, database):
        cause = _cause_of(probe)
        if cause is not None:
            # Probes repeat every few seconds, so an outage logs one line per probe.
            level = logging.ERROR if cause.startswith(_UNEXPECTED) else logging.WARNING
            logger.log(level, "not ready: %s", cause)
            raise NotReadyError
    return Ready()


def _cause_of(probe: ReadinessProbe) -> str | None:
    try:
        return probe.unready_cause()
    except Exception as exc:
        # Any failure of a check means not ready (503), not a server error (500). Its message is
        # not logged: it could come from the database driver and carry stored values.
        return f"{_UNEXPECTED}{type(exc).__name__}"
