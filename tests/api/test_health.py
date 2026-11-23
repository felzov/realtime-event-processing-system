"""`/health`, `/ready` and the lifespan of the app that `make run` serves."""

import asyncio
import logging
import time

import httpx
import pytest

from event_pipeline.config import Settings
from event_pipeline.main import create_app, create_app_from_settings
from tests.api.helpers import FakePublisher, error_of, no_database, send

CAUSE = "UNKNOWN_TOPIC_OR_PART"


def test_health_answers_200() -> None:
    response = send(create_app(FakePublisher(), engine=no_database()), method="GET", path="/health")

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_health_answers_200_while_not_ready() -> None:
    # Liveness must not depend on the broker, or an outage would get a healthy process restarted.
    response = send(
        create_app(FakePublisher(unready_cause=CAUSE), engine=no_database()),
        method="GET",
        path="/health",
    )

    assert response.status_code == 200


def test_ready_answers_200_when_the_publisher_can_publish() -> None:
    response = send(create_app(FakePublisher(), engine=no_database()), method="GET", path="/ready")

    assert response.status_code == 200
    assert response.json() == {"status": "ready"}


def test_not_ready_answers_503_and_logs_the_cause_without_returning_it(
    caplog: pytest.LogCaptureFixture,
) -> None:
    with caplog.at_level(logging.WARNING, logger="event_pipeline.api.health"):
        response = send(
            create_app(FakePublisher(unready_cause=CAUSE), engine=no_database()),
            method="GET",
            path="/ready",
        )

    assert response.status_code == 503
    error = error_of(response)
    assert error["code"] == "not_ready"
    assert CAUSE not in response.text
    (record,) = caplog.records
    assert record.levelno == logging.WARNING
    assert CAUSE in record.getMessage()


class _SlowProbe(FakePublisher):
    """A probe that blocks like a metadata request to an unreachable broker."""

    def unready_cause(self) -> str | None:
        time.sleep(1.0)
        return "_TRANSPORT"


def test_a_slow_ready_check_does_not_block_other_requests() -> None:
    app = create_app(_SlowProbe(), engine=no_database())

    async def both() -> tuple[float, float]:
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            started = time.monotonic()

            async def timed(path: str) -> float:
                await client.get(path)
                return time.monotonic() - started

            ready_task = asyncio.create_task(timed("/ready"))
            await asyncio.sleep(0.1)  # let /ready start its blocking check
            health_s = await timed("/health")
            return health_s, await ready_task

    health_s, ready_s = asyncio.run(both())

    # /ready runs in the thread pool; on the event loop it would hold /health back for 1 s.
    assert health_s < 0.5
    assert ready_s >= 1.0


def test_lifespan_starts_without_a_broker_and_closes_the_publisher_on_shutdown(
    caplog: pytest.LogCaptureFixture,
) -> None:
    # Port 1 on 127.0.0.1: nothing listens. Creating the producer does not connect, so startup
    # succeeds and readiness reports the outage instead.
    settings = Settings(  # type: ignore[call-arg]
        _env_file=None,
        kafka_bootstrap_servers="127.0.0.1:1",
        ready_timeout_s=0.5,
        publisher_close_timeout_s=0.5,
    )
    app = create_app_from_settings(settings)

    async def scenario() -> tuple[httpx.Response, httpx.Response, float, bool]:
        # The lifespan as uvicorn runs it: startup before the first request, shutdown after the
        # last. httpx's ASGITransport does not run it itself.
        async with app.router.lifespan_context(app):
            transport = httpx.ASGITransport(app=app)
            async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
                was_polling = app.state.publisher.is_polling()
                health = await client.get("/health")
                started = time.monotonic()
                ready = await client.get("/ready")
                return health, ready, time.monotonic() - started, was_polling

    with caplog.at_level(logging.WARNING, logger="event_pipeline.api.health"):
        health, ready, ready_s, was_polling = asyncio.run(scenario())

    assert was_polling
    assert health.status_code == 200
    assert ready.status_code == 503
    assert error_of(ready)["code"] == "not_ready"
    assert ready_s < settings.ready_timeout_s + 1.0
    # The broker check failed, not the poll thread: a thread that died at startup would also 503.
    # The publisher's own client-error warnings are captured too; only `/ready`'s line matters.
    (record,) = [r for r in caplog.records if r.name == "event_pipeline.api.health"]
    assert "_TRANSPORT" in record.getMessage()
    assert not app.state.publisher.is_polling()
