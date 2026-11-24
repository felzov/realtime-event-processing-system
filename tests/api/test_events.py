"""`POST /events` with a fake publisher: every row of ADR 0003's mapping table, and the body cap.

Requests go through httpx's `ASGITransport`, which hands the app the request body one chunk per
`receive()`, so a test can count how much of a streamed body the app actually read.
"""

import asyncio
import json
import logging
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta, timezone
from typing import Any, cast

import httpx
import pytest
from pydantic import SecretStr
from starlette.types import Message, Scope

from event_pipeline.broker.publisher import Delivery, NotDelivered, PublishFailure
from event_pipeline.config import Settings
from event_pipeline.domain.events import MAX_BODY_BYTES
from event_pipeline.domain.lateness import LatenessPolicy
from event_pipeline.main import create_app_from_settings
from tests.api.helpers import NO_DATABASE_URL, FakePublisher, error_of, make_app, send
from tests.samples import EVENT_ID, STORE_ID, encode, valid_event

# Distinctive text placed in rejected input; no response may contain it.
MARKER = "ECHO-MARKER-7f3a"


# --- 202: the broker acknowledged ----------------------------------------------------------------


def test_valid_event_is_published_as_sent_and_answered_202() -> None:
    publisher = FakePublisher()
    raw = encode(valid_event())

    before = datetime.now(UTC)
    response = send(make_app(publisher), content=raw)
    after = datetime.now(UTC)

    assert response.status_code == 202
    assert response.json() == {"event_id": EVENT_ID}
    [call] = publisher.calls
    assert call.key == STORE_ID
    assert call.value == raw
    assert call.received_at.tzinfo is UTC
    assert before <= call.received_at <= after


def test_body_of_exactly_the_cap_is_accepted() -> None:
    # Trailing whitespace is valid JSON, so the event can be padded to the exact limit.
    raw = encode(valid_event())
    raw += b" " * (MAX_BODY_BYTES - len(raw))
    publisher = FakePublisher()

    response = send(make_app(publisher), content=raw)

    assert response.status_code == 202
    assert publisher.calls[0].value == raw


def test_body_is_published_byte_for_byte_not_reserialised() -> None:
    # Compact separators and reversed key order: a route that re-encoded the parsed event would
    # produce different bytes (ADR 0003, decision 2).
    event = valid_event()
    raw = json.dumps(dict(reversed(event.items())), separators=(",", ":")).encode()
    assert raw != encode(event)
    publisher = FakePublisher()

    response = send(make_app(publisher), content=raw)

    assert response.status_code == 202
    assert publisher.calls[0].value == raw


# --- 422: the event fails the contract -----------------------------------------------------------


def _without_store_id() -> dict[str, Any]:
    event = valid_event()
    del event["payload"]["store_id"]
    return event


def _negative_amount() -> dict[str, Any]:
    event = valid_event()
    event["payload"]["amount_minor"] = -1
    return event


def _unknown_type() -> dict[str, Any]:
    return {**valid_event(), "event_type": "order.cancelled"}


@pytest.mark.parametrize(
    ("raw", "violations"),
    [
        (encode(_without_store_id()), [{"path": "payload.store_id", "code": "missing_field"}]),
        (encode(_negative_amount()), [{"path": "payload.amount_minor", "code": "out_of_range"}]),
        (encode(_unknown_type()), [{"path": "event_type", "code": "unknown_event_type"}]),
        (b"{not json", [{"path": "<body>", "code": "invalid_json"}]),
        (b"", [{"path": "<body>", "code": "invalid_json"}]),
    ],
    ids=["missing_field", "out_of_range", "unknown_event_type", "not_json", "empty_body"],
)
def test_invalid_event_is_422_with_domain_codes_and_not_published(
    raw: bytes, violations: list[dict[str, str]]
) -> None:
    publisher = FakePublisher()

    response = send(make_app(publisher), content=raw)

    assert response.status_code == 422
    error = error_of(response)
    assert error["code"] == "event_rejected"
    assert error["violations"] == violations
    assert publisher.calls == []


def test_rejection_never_echoes_input() -> None:
    event = valid_event()
    event["payload"]["currency"] = MARKER
    event["source"] = MARKER
    event[MARKER] = MARKER

    response = send(make_app(FakePublisher()), content=encode(event))

    assert response.status_code == 422
    assert MARKER not in response.text
    assert {v["code"] for v in error_of(response)["violations"]} == {
        "invalid_format",
        "unknown_field",
    }


# --- 422 for an event from the future; a late event is accepted (ADR 0005, decision 6) ----------


def event_occurring_at(occurred_at: datetime, offset: timedelta = timedelta(0)) -> bytes:
    """The sample event with `occurred_at` written in the UTC offset `offset`."""
    body = valid_event()
    body["occurred_at"] = occurred_at.astimezone(timezone(offset)).isoformat()
    return encode(body)


REJECTED_FROM_THE_FUTURE = {
    "error": {
        "code": "event_rejected",
        "message": "the event does not satisfy the contract",
        "violations": [{"path": "occurred_at", "code": "out_of_range"}],
    }
}


def test_an_event_from_the_future_is_422_and_not_published() -> None:
    publisher = FakePublisher()
    tomorrow = datetime.now(UTC) + timedelta(days=1)

    response = send(make_app(publisher), content=event_occurring_at(tomorrow))

    assert response.status_code == 422
    # The whole body: a fixed envelope, so no form of the submitted time can be echoed.
    assert response.json() == REJECTED_FROM_THE_FUTURE
    assert publisher.calls == []


@pytest.mark.parametrize(
    ("ahead", "offset", "status"),
    [
        (timedelta(0), timedelta(hours=14), 202),
        (timedelta(hours=2), timedelta(hours=-12), 422),
    ],
    ids=["now_written_in_plus_14", "two_hours_ahead_written_in_minus_12"],
)
def test_occurred_at_is_compared_as_an_instant_whatever_its_offset(
    ahead: timedelta, offset: timedelta, status: int
) -> None:
    # In -12:00, an instant two hours ahead reads as a wall-clock time ten hours behind UTC.
    publisher = FakePublisher()

    response = send(
        make_app(publisher), content=event_occurring_at(datetime.now(UTC) + ahead, offset)
    )

    assert response.status_code == status
    assert len(publisher.calls) == (1 if status == 202 else 0)


@pytest.mark.parametrize(
    "offset",
    [timedelta(seconds=30), timedelta(0), -timedelta(days=30)],
    ids=["slightly_ahead_within_the_skew", "now", "a_month_late"],
)
def test_an_event_within_the_skew_or_late_is_accepted(offset: timedelta) -> None:
    # Late events are accepted: the topic keeps them, and the consumer skips them (ADR 0005).
    publisher = FakePublisher()

    response = send(make_app(publisher), content=event_occurring_at(datetime.now(UTC) + offset))

    assert response.status_code == 202
    assert len(publisher.calls) == 1


def test_the_skew_comes_from_create_app() -> None:
    strict = LatenessPolicy(allowed_lateness=timedelta(minutes=5), max_future_skew=timedelta(0))
    publisher = FakePublisher()
    soon = datetime.now(UTC) + timedelta(seconds=30)

    response = send(make_app(publisher, lateness=strict), content=event_occurring_at(soon))

    assert response.status_code == 422
    assert publisher.calls == []


@pytest.mark.parametrize(
    ("max_future_skew_s", "status", "code"),
    [(0, 422, "event_rejected"), (3600, 503, "broker_unavailable")],
    ids=["no_skew_rejects", "an_hour_of_skew_passes_to_the_unreachable_broker"],
)
def test_the_app_from_settings_uses_the_configured_skew(
    max_future_skew_s: int, status: int, code: str
) -> None:
    # Nothing listens on the broker port: an event that passes the check is not confirmed (503),
    # one that fails it is rejected before publishing (422).
    settings = Settings(  # type: ignore[call-arg]
        _env_file=None,
        kafka_bootstrap_servers="127.0.0.1:1",
        kafka_delivery_timeout_ms=500,
        publish_backstop_s=3.0,
        publisher_close_timeout_s=0.5,
        database_url=SecretStr(NO_DATABASE_URL),
        max_future_skew_s=max_future_skew_s,
    )
    application = create_app_from_settings(settings)
    raw = event_occurring_at(datetime.now(UTC) + timedelta(minutes=30))

    async def scenario() -> httpx.Response:
        async with application.router.lifespan_context(application):
            transport = httpx.ASGITransport(app=application)
            async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
                return await client.post("/events", content=raw)

    response = asyncio.run(scenario())

    assert response.status_code == status
    assert error_of(response)["code"] == code
    if status == 422:
        assert response.json() == REJECTED_FROM_THE_FUTURE


# --- 413: the body is over the cap, refused before parsing ---------------------------------------


def test_declared_length_over_the_cap_is_413_without_reading_the_body() -> None:
    publisher = FakePublisher()
    chunks_read = 0

    async def body() -> AsyncIterator[bytes]:
        nonlocal chunks_read
        for _ in range(MAX_BODY_BYTES + 1):
            chunks_read += 1
            yield b" "

    response = send(
        make_app(publisher),
        content=body(),
        headers={"Content-Length": str(MAX_BODY_BYTES + 1)},
    )

    assert response.status_code == 413
    assert error_of(response)["code"] == "body_too_large"
    assert MARKER not in response.text
    assert publisher.calls == []
    # The transport may prefetch the first chunk; the app asked for none.
    assert chunks_read <= 1


def test_content_length_with_too_many_digits_is_413_not_500() -> None:
    # Over Python's 4300-digit limit for int(); behind uvicorn the HTTP parser refuses it first.
    async def body() -> AsyncIterator[bytes]:
        yield b"{}"

    response = send(
        make_app(FakePublisher()),
        content=body(),
        headers={"Content-Length": "9" * 5000},
    )

    assert response.status_code == 413
    assert error_of(response)["code"] == "body_too_large"


def test_streamed_body_over_the_cap_is_413_and_reading_stops_at_the_cap() -> None:
    # Chunked, no Content-Length: only counting while reading can catch it.
    publisher = FakePublisher()
    chunk = b" " * 1024
    total_chunks = 64
    chunks_read = 0

    async def body() -> AsyncIterator[bytes]:
        nonlocal chunks_read
        for _ in range(total_chunks):
            chunks_read += 1
            yield chunk

    response = send(make_app(publisher), content=body())

    assert response.status_code == 413
    assert error_of(response)["code"] == "body_too_large"
    assert publisher.calls == []
    # The 17th 1 KiB chunk crosses 16 KiB; one more may be prefetched by the transport.
    assert chunks_read <= MAX_BODY_BYTES // len(chunk) + 2


def test_body_one_byte_over_the_cap_is_413() -> None:
    response = send(make_app(FakePublisher()), content=b" " * (MAX_BODY_BYTES + 1))

    assert response.status_code == 413


# --- 503 / 500: the broker did not confirm -------------------------------------------------------


@pytest.mark.parametrize(
    ("outcome", "status", "code"),
    [
        (
            NotDelivered(PublishFailure.BROKER_UNAVAILABLE, "_MSG_TIMED_OUT"),
            503,
            "broker_unavailable",
        ),
        (
            NotDelivered(PublishFailure.BROKER_UNAVAILABLE, "local_queue_full"),
            503,
            "broker_unavailable",
        ),
        (NotDelivered(PublishFailure.REJECTED, "MSG_SIZE_TOO_LARGE"), 500, "internal_error"),
        (NotDelivered(PublishFailure.PUBLISHER_FAILED, "poll_thread_dead"), 500, "internal_error"),
        (
            NotDelivered(PublishFailure.PUBLISHER_FAILED, "no_delivery_report"),
            500,
            "internal_error",
        ),
    ],
    ids=["timed_out", "queue_full", "rejected", "poll_thread_dead", "backstop"],
)
def test_unconfirmed_publish_maps_to_its_status(
    outcome: NotDelivered, status: int, code: str
) -> None:
    publisher = FakePublisher(outcome)

    response = send(make_app(publisher), content=encode(valid_event()))

    assert response.status_code == status
    error = error_of(response)
    assert error["code"] == code
    assert error["violations"] == []
    # The Kafka cause is for the log, not the client.
    assert outcome.cause not in response.text
    assert len(publisher.calls) == 1


def test_unconfirmed_publish_is_logged_with_event_id_and_cause_but_not_the_event(
    caplog: pytest.LogCaptureFixture,
) -> None:
    publisher = FakePublisher(NotDelivered(PublishFailure.BROKER_UNAVAILABLE, "_MSG_TIMED_OUT"))

    with caplog.at_level(logging.WARNING, logger="event_pipeline.services.ingest"):
        send(make_app(publisher), content=encode(valid_event()))

    [record] = caplog.records
    assert record.levelno == logging.WARNING
    assert EVENT_ID in record.getMessage()
    assert "_MSG_TIMED_OUT" in record.getMessage()
    assert "ord-1001" not in record.getMessage()


def test_failure_other_than_unavailable_is_logged_as_error(
    caplog: pytest.LogCaptureFixture,
) -> None:
    publisher = FakePublisher(NotDelivered(PublishFailure.REJECTED, "MSG_SIZE_TOO_LARGE"))

    with caplog.at_level(logging.WARNING, logger="event_pipeline.services.ingest"):
        send(make_app(publisher), content=encode(valid_event()))

    [record] = caplog.records
    assert record.levelno == logging.ERROR


def test_unexpected_exception_is_500_in_the_envelope() -> None:
    publisher = FakePublisher(RuntimeError(MARKER))

    response = send(
        make_app(publisher),
        raise_app_exceptions=False,
        content=encode(valid_event()),
    )

    assert response.status_code == 500
    assert error_of(response)["code"] == "internal_error"
    assert MARKER not in response.text


def test_outcome_that_is_not_a_delivery_is_500_not_202() -> None:
    # 202 must follow only a `Delivered`; anything unrecognised fails closed.
    publisher = FakePublisher(cast(Delivery, object()))

    response = send(
        make_app(publisher),
        raise_app_exceptions=False,
        content=encode(valid_event()),
    )

    assert response.status_code == 500
    assert error_of(response)["code"] == "internal_error"


def test_client_disconnecting_mid_body_is_not_a_server_error(
    caplog: pytest.LogCaptureFixture,
) -> None:
    # Driven as raw ASGI: httpx's transport cannot hang up halfway through a body.
    publisher = FakePublisher()
    incoming: list[Message] = [
        {"type": "http.request", "body": b'{"event_id": ', "more_body": True},
        {"type": "http.disconnect"},
    ]
    scope: Scope = {
        "type": "http",
        "asgi": {"version": "3.0"},
        "http_version": "1.1",
        "method": "POST",
        "scheme": "http",
        "path": "/events",
        "raw_path": b"/events",
        "query_string": b"",
        "root_path": "",
        "headers": [(b"host", b"test")],
        "client": ("127.0.0.1", 1),
        "server": ("test", 80),
    }

    async def receive() -> Message:
        return incoming.pop(0)

    async def send_message(message: Message) -> None:
        # Any response is discarded, as uvicorn does once the client is gone.
        pass

    with caplog.at_level(logging.INFO):
        # An exception escaping the app is what uvicorn logs as an ERROR traceback.
        asyncio.run(make_app(publisher)(scope, receive, send_message))

    assert publisher.calls == []
    assert not [r for r in caplog.records if r.levelno >= logging.ERROR]
    assert [r.name for r in caplog.records] == ["event_pipeline.api.errors"]


# --- Routing errors use the same envelope --------------------------------------------------------


def test_unknown_path_is_404_in_the_envelope() -> None:
    response = send(make_app(FakePublisher()), path=f"/{MARKER}")

    assert response.status_code == 404
    assert error_of(response)["code"] == "not_found"
    assert MARKER not in response.text


def test_wrong_method_is_405_with_allow_header() -> None:
    response = send(make_app(FakePublisher()), method="GET")

    assert response.status_code == 405
    assert error_of(response)["code"] == "method_not_allowed"
    assert response.headers["allow"] == "POST"
