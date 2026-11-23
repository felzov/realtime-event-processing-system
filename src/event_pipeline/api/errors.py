"""The error envelope and the exception handlers that produce it (ADR 0003, decision 7).

Every error response is `{"error": {"code", "message", "violations"}}`. `violations` lists
`{path, code}` pairs for a rejected event or invalid query parameters and is empty otherwise.
Messages are fixed strings: they never echo the request, and the Kafka or database cause of a
failure is logged, not returned.
"""

import logging
from collections.abc import Mapping
from enum import StrEnum
from http import HTTPStatus
from types import MappingProxyType
from typing import Final, cast

from fastapi import FastAPI, Request, status
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from sqlalchemy.exc import DBAPIError, OperationalError
from sqlalchemy.exc import TimeoutError as PoolTimeoutError
from starlette.exceptions import HTTPException
from starlette.requests import ClientDisconnect

from event_pipeline.broker.publisher import PublishFailure
from event_pipeline.data.errors import describe_database_error
from event_pipeline.domain.errors import EventRejectedError, QueryRejectedError
from event_pipeline.domain.events import MAX_BODY_BYTES
from event_pipeline.services.ingest import EventNotConfirmedError

logger = logging.getLogger(__name__)


class ApiErrorCode(StrEnum):
    """Stable codes clients can branch on. Never renamed or reused for another meaning."""

    EVENT_REJECTED = "event_rejected"
    BODY_TOO_LARGE = "body_too_large"
    BROKER_UNAVAILABLE = "broker_unavailable"
    INTERNAL_ERROR = "internal_error"
    NOT_FOUND = "not_found"
    METHOD_NOT_ALLOWED = "method_not_allowed"
    INVALID_REQUEST = "invalid_request"
    NOT_READY = "not_ready"
    DATABASE_UNAVAILABLE = "database_unavailable"


class BodyTooLargeError(Exception):
    """The request body is over `MAX_BODY_BYTES`; raised before any of it is parsed."""


class NotReadyError(Exception):
    """`/ready` found a reason the API cannot publish now; the cause is logged, not returned."""


class ViolationBody(BaseModel):
    path: str
    code: str


class ErrorBody(BaseModel):
    code: ApiErrorCode
    message: str
    violations: list[ViolationBody] = []


class ErrorResponse(BaseModel):
    error: ErrorBody


# OpenAPI entries for `POST /events`; the handlers below produce them.
INGEST_ERROR_RESPONSES: Final[dict[int | str, dict[str, object]]] = {
    status.HTTP_413_CONTENT_TOO_LARGE: {"model": ErrorResponse},
    status.HTTP_422_UNPROCESSABLE_CONTENT: {"model": ErrorResponse},
    status.HTTP_500_INTERNAL_SERVER_ERROR: {"model": ErrorResponse},
    status.HTTP_503_SERVICE_UNAVAILABLE: {
        "model": ErrorResponse,
        "description": "The broker did not confirm the write; retry with the same event_id",
    },
}

# OpenAPI entries for the aggregate query; the handlers below produce them.
QUERY_ERROR_RESPONSES: Final[dict[int | str, dict[str, object]]] = {
    status.HTTP_422_UNPROCESSABLE_CONTENT: {"model": ErrorResponse},
    status.HTTP_500_INTERNAL_SERVER_ERROR: {"model": ErrorResponse},
    status.HTTP_503_SERVICE_UNAVAILABLE: {
        "model": ErrorResponse,
        "description": "The database did not answer in time; retry later",
    },
}

# Routing raises these itself (unknown path, wrong method).
_HTTP_EXCEPTION_CODES: Final[Mapping[int, ApiErrorCode]] = MappingProxyType(
    {
        status.HTTP_404_NOT_FOUND: ApiErrorCode.NOT_FOUND,
        status.HTTP_405_METHOD_NOT_ALLOWED: ApiErrorCode.METHOD_NOT_ALLOWED,
    }
)


def error_response(
    status_code: int,
    code: ApiErrorCode,
    message: str,
    violations: list[ViolationBody] | None = None,
    headers: Mapping[str, str] | None = None,
) -> JSONResponse:
    body = ErrorResponse(error=ErrorBody(code=code, message=message, violations=violations or []))
    return JSONResponse(body.model_dump(mode="json"), status_code=status_code, headers=headers)


# Starlette selects a handler by exception class, so each cast below only narrows the type.


async def _handle_event_rejected(request: Request, exc: Exception) -> JSONResponse:
    error = cast(EventRejectedError, exc)
    return error_response(
        status.HTTP_422_UNPROCESSABLE_CONTENT,
        ApiErrorCode.EVENT_REJECTED,
        "the event does not satisfy the contract",
        [ViolationBody(path=v.path, code=v.code) for v in error.violations],
    )


async def _handle_query_rejected(request: Request, exc: Exception) -> JSONResponse:
    error = cast(QueryRejectedError, exc)
    return error_response(
        status.HTTP_422_UNPROCESSABLE_CONTENT,
        ApiErrorCode.INVALID_REQUEST,
        "the query parameters are invalid",
        [ViolationBody(path=v.path, code=v.code) for v in error.violations],
    )


async def _handle_database_error(request: Request, exc: Exception) -> JSONResponse:
    # Handled here, not by the catch-all: that one lets the server log the traceback, which prints
    # str(exc), and a database message can carry stored values.
    described = describe_database_error(exc) if isinstance(exc, DBAPIError) else type(exc).__name__
    # Unreachable server, broken connection, statement timeout (QueryCanceled is an
    # OperationalError) or no free pooled connection: the database is unavailable for now.
    if isinstance(exc, OperationalError | PoolTimeoutError):
        logger.warning("database unavailable: %s", described)
        return error_response(
            status.HTTP_503_SERVICE_UNAVAILABLE,
            ApiErrorCode.DATABASE_UNAVAILABLE,
            "the database did not answer; retry later",
        )
    logger.error("database error: %s", described)
    return _internal_error()


async def _handle_body_too_large(request: Request, exc: Exception) -> JSONResponse:
    return error_response(
        status.HTTP_413_CONTENT_TOO_LARGE,
        ApiErrorCode.BODY_TOO_LARGE,
        f"the request body is larger than {MAX_BODY_BYTES} bytes",
    )


async def _handle_not_confirmed(request: Request, exc: Exception) -> JSONResponse:
    # The service has logged the cause. A rejected message or a fault in this process is not
    # something the client can fix by retrying soon, so it is a 500 (ADR 0003, decision 7).
    error = cast(EventNotConfirmedError, exc)
    if error.failure is PublishFailure.BROKER_UNAVAILABLE:
        return error_response(
            status.HTTP_503_SERVICE_UNAVAILABLE,
            ApiErrorCode.BROKER_UNAVAILABLE,
            "the broker did not confirm the event; retry with the same event_id",
        )
    return _internal_error()


async def _handle_not_ready(request: Request, exc: Exception) -> JSONResponse:
    return error_response(
        status.HTTP_503_SERVICE_UNAVAILABLE,
        ApiErrorCode.NOT_READY,
        "not ready to accept events",
    )


async def _handle_http_exception(request: Request, exc: Exception) -> JSONResponse:
    error = cast(HTTPException, exc)
    fallback = (
        ApiErrorCode.INTERNAL_ERROR if error.status_code >= 500 else ApiErrorCode.INVALID_REQUEST
    )
    code = _HTTP_EXCEPTION_CODES.get(error.status_code, fallback)
    # The standard phrase, not `exc.detail`, which other code could fill with input. The headers
    # are kept: a 405 carries `Allow`.
    return error_response(
        error.status_code, code, HTTPStatus(error.status_code).phrase, headers=error.headers
    )


async def _handle_client_disconnect(request: Request, exc: Exception) -> JSONResponse:
    # The client hung up while sending the body: nothing was parsed or published. Handled here so
    # it is not re-raised and logged by the server as an application error. The response is
    # never delivered; 400 only keeps the envelope.
    logger.info("client disconnected while sending the request body")
    return error_response(
        status.HTTP_400_BAD_REQUEST,
        ApiErrorCode.INVALID_REQUEST,
        HTTPStatus.BAD_REQUEST.phrase,
    )


async def _handle_unexpected(request: Request, exc: Exception) -> JSONResponse:
    # Starlette's ServerErrorMiddleware calls this and then re-raises, so the server logs the
    # traceback; this only keeps the response in the envelope.
    return _internal_error()


def _internal_error() -> JSONResponse:
    return error_response(
        status.HTTP_500_INTERNAL_SERVER_ERROR, ApiErrorCode.INTERNAL_ERROR, "internal error"
    )


def register_error_handlers(app: FastAPI) -> None:
    app.add_exception_handler(EventRejectedError, _handle_event_rejected)
    app.add_exception_handler(QueryRejectedError, _handle_query_rejected)
    app.add_exception_handler(DBAPIError, _handle_database_error)
    app.add_exception_handler(PoolTimeoutError, _handle_database_error)
    app.add_exception_handler(BodyTooLargeError, _handle_body_too_large)
    app.add_exception_handler(EventNotConfirmedError, _handle_not_confirmed)
    app.add_exception_handler(NotReadyError, _handle_not_ready)
    app.add_exception_handler(HTTPException, _handle_http_exception)
    app.add_exception_handler(ClientDisconnect, _handle_client_disconnect)
    app.add_exception_handler(Exception, _handle_unexpected)
