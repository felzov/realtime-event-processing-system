"""Event envelope v1 and the ``order.placed`` v1 payload (ADR 0001, decisions 2, 3, 6, 7).

``parse_event`` is the single entry point: raw bytes in, a typed ``OrderPlacedEvent`` or an
``EventRejectedError`` out. Parsing happens in two stages because the payload schema depends on
``(event_type, schema_version)``:

1. the body size, then the envelope (strict, unknown fields rejected, payload kept as raw JSON),
   then duplicate keys anywhere in the body;
2. the payload, validated by the schema registered for that type and version.

Violations are raised outside any ``except`` block, so an ``EventRejectedError`` has no
``__cause__`` or ``__context__``: Pydantic's ``ValidationError`` text contains the submitted values
and must not be reachable through the exception chain (for example by a logger that walks it).
"""

import json
import re
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Annotated, Final
from uuid import UUID

from pydantic import (
    AfterValidator,
    AwareDatetime,
    BaseModel,
    BeforeValidator,
    ConfigDict,
    Field,
    JsonValue,
    Strict,
    StringConstraints,
    ValidationError,
)
from pydantic_core import ErrorDetails, PydanticCustomError

from event_pipeline.domain.errors import ROOT_PATH, EventRejectedError, ReasonCode, Violation

MAX_BODY_BYTES: Final = 16 * 1024
MAX_AMOUNT_MINOR: Final = 10_000_000_000
ORDER_PLACED: Final = "order.placed"

_STRICT = ConfigDict(strict=True, extra="forbid", frozen=True)

SourceName = Annotated[
    str, StringConstraints(min_length=1, max_length=64, pattern=r"^[a-z0-9._-]+$")
]
StoreId = Annotated[str, StringConstraints(min_length=1, max_length=64, pattern=r"^[a-z0-9_-]+$")]
OrderId = Annotated[
    str, StringConstraints(min_length=1, max_length=64, pattern=r"^[^\x00-\x1f\x7f]+$")
]
"""Any text without control characters (PostgreSQL ``text`` cannot store NUL)."""
CurrencyCode = Annotated[str, StringConstraints(pattern=r"^[A-Z]{3}$")]
"""Format check only (3 uppercase letters), not membership in the ISO 4217 list."""


# RFC 3339 section 5.6 date-time. The offset is optional here so that a naive timestamp reaches
# AwareDatetime and gets its own code (naive_datetime) instead of a generic format error. Checked
# with fullmatch: a `$` anchor would also accept a trailing newline.
_RFC3339_DATE_TIME = re.compile(
    r"[0-9]{4}-[0-9]{2}-[0-9]{2}[Tt][0-9]{2}:[0-9]{2}:[0-9]{2}(?:\.[0-9]+)?(?:[Zz]|[+-][0-9]{2}:[0-9]{2})?"
)


def _require_rfc3339(value: object) -> str:
    """Let only RFC 3339 strings reach the datetime parser.

    The field is lax (``Strict(False)``) because a before-validator hands Pydantic a Python ``str``,
    which strict mode refuses for a datetime. Lax mode would also accept numbers and numeric
    strings as Unix timestamps, so this check is what keeps the field strict.
    """
    if not isinstance(value, str):
        raise PydanticCustomError("rfc3339_type", "must be a string")
    if _RFC3339_DATE_TIME.fullmatch(value) is None:
        raise PydanticCustomError("rfc3339_format", "not an RFC 3339 date-time")
    if value.endswith("-00:00"):
        # RFC 3339 section 4.3: -00:00 means "offset unknown", not UTC.
        raise PydanticCustomError("unknown_offset", "the UTC offset is unknown")
    return value


def _to_utc(value: datetime) -> datetime:
    try:
        return value.astimezone(UTC)
    except OverflowError:
        # e.g. 0001-01-01T00:00:00+01:00 is before datetime.min once converted to UTC.
        raise PydanticCustomError(
            "datetime_out_of_range", "outside the supported date range in UTC"
        ) from None


UtcDatetime = Annotated[
    AwareDatetime, Strict(False), BeforeValidator(_require_rfc3339), AfterValidator(_to_utc)
]
"""An RFC 3339 timestamp with a known offset, normalised to UTC."""


class OrderPlacedV1(BaseModel):
    model_config = _STRICT

    order_id: OrderId
    store_id: StoreId
    amount_minor: int = Field(ge=0, le=MAX_AMOUNT_MINOR)
    currency: CurrencyCode


class _EnvelopeV1(BaseModel):
    model_config = _STRICT

    event_id: UUID
    event_type: str
    schema_version: int
    occurred_at: UtcDatetime
    source: SourceName
    payload: dict[str, JsonValue]


# Supported (event_type, schema_version) pairs. C6: a consumer supports N and N-1; today N = 1.
_PAYLOAD_SCHEMAS: Final[dict[tuple[str, int], type[OrderPlacedV1]]] = {
    (ORDER_PLACED, 1): OrderPlacedV1,
}


@dataclass(frozen=True, slots=True)
class OrderPlacedEvent:
    """A valid ``order.placed`` event. ``occurred_at`` is always in UTC."""

    event_id: UUID
    schema_version: int
    occurred_at: datetime
    source: str
    payload: OrderPlacedV1


def parse_event(raw: bytes) -> OrderPlacedEvent:
    """Validate an encoded event against the contract.

    Raises ``EventRejectedError`` with every violation found in the failing stage.
    """
    if len(raw) > MAX_BODY_BYTES:
        raise EventRejectedError((Violation(ReasonCode.BODY_TOO_LARGE, ROOT_PATH),))

    envelope = _validate(lambda: _EnvelopeV1.model_validate_json(raw), prefix=())
    if isinstance(envelope, tuple):
        raise EventRejectedError(envelope)

    if _has_duplicate_keys(raw):
        raise EventRejectedError((Violation(ReasonCode.INVALID_JSON, ROOT_PATH),))

    schema = _payload_schema(envelope.event_type, envelope.schema_version)
    payload = _validate(lambda: schema.model_validate(envelope.payload), prefix=("payload",))
    if isinstance(payload, tuple):
        raise EventRejectedError(payload)

    return OrderPlacedEvent(
        event_id=envelope.event_id,
        schema_version=envelope.schema_version,
        occurred_at=envelope.occurred_at,
        source=envelope.source,
        payload=payload,
    )


def _validate[M: BaseModel](
    validate: Callable[[], M], prefix: tuple[str, ...]
) -> M | tuple[Violation, ...]:
    """Run a Pydantic validation; return the model, or the violations instead of raising."""
    try:
        return validate()
    except ValidationError as exc:
        return pydantic_violations(exc.errors(), prefix)


class _DuplicateKeyError(Exception):
    pass


def _reject_duplicate_keys(pairs: list[tuple[str, object]]) -> dict[str, object]:
    keys = [key for key, _ in pairs]
    if len(set(keys)) != len(keys):
        raise _DuplicateKeyError
    return dict(pairs)


def _has_duplicate_keys(raw: bytes) -> bool:
    """True if any JSON object in ``raw`` repeats a key.

    Pydantic keeps the last value of a repeated key, so ``{"amount_minor": -5, "amount_minor": 7}``
    would pass as 7. Called only after Pydantic has accepted the body, so ``raw`` is known to be
    valid UTF-8 JSON within Pydantic's nesting limit, well below ``json``'s recursion limit.
    """
    try:
        json.loads(raw, object_pairs_hook=_reject_duplicate_keys)
    except _DuplicateKeyError:
        return True
    return False


def _payload_schema(event_type: str, schema_version: int) -> type[OrderPlacedV1]:
    schema = _PAYLOAD_SCHEMAS.get((event_type, schema_version))
    if schema is not None:
        return schema
    if any(known_type == event_type for known_type, _ in _PAYLOAD_SCHEMAS):
        raise EventRejectedError(
            (Violation(ReasonCode.UNSUPPORTED_SCHEMA_VERSION, "schema_version"),)
        )
    raise EventRejectedError((Violation(ReasonCode.UNKNOWN_EVENT_TYPE, "event_type"),))


# Pydantic error type -> reason code. Types not listed fall back to INVALID_FORMAT: still a
# rejection, never an acceptance. The unit tests assert the exact code for each rejection case they
# cover, so a renamed type in those cases fails a test; an error type no test triggers would get
# the fallback code without any signal.
_REASON_BY_PYDANTIC_TYPE: Final[dict[str, ReasonCode]] = {
    "missing": ReasonCode.MISSING_FIELD,
    "extra_forbidden": ReasonCode.UNKNOWN_FIELD,
    "json_invalid": ReasonCode.INVALID_JSON,
    "timezone_aware": ReasonCode.NAIVE_DATETIME,
    "greater_than_equal": ReasonCode.OUT_OF_RANGE,
    "less_than_equal": ReasonCode.OUT_OF_RANGE,
    "int_type": ReasonCode.INVALID_TYPE,
    "string_type": ReasonCode.INVALID_TYPE,
    "uuid_type": ReasonCode.INVALID_TYPE,
    "datetime_type": ReasonCode.INVALID_TYPE,
    "dict_type": ReasonCode.INVALID_TYPE,
    "model_type": ReasonCode.INVALID_TYPE,
    "string_too_short": ReasonCode.INVALID_FORMAT,
    "string_too_long": ReasonCode.INVALID_FORMAT,
    "string_pattern_mismatch": ReasonCode.INVALID_FORMAT,
    "uuid_parsing": ReasonCode.INVALID_FORMAT,
    "datetime_parsing": ReasonCode.INVALID_FORMAT,
    # Raised by our own validators above.
    "rfc3339_type": ReasonCode.INVALID_TYPE,
    "rfc3339_format": ReasonCode.INVALID_FORMAT,
    "unknown_offset": ReasonCode.NAIVE_DATETIME,
    "datetime_out_of_range": ReasonCode.OUT_OF_RANGE,
}


def pydantic_violations(
    errors: Sequence[ErrorDetails], prefix: tuple[str, ...]
) -> tuple[Violation, ...]:
    """Pydantic's errors as violations: a reason code and a dotted path each, never the input.

    Also used for the query API's parameters (domain/queries.py), so both share one mapping.
    """
    violations = []
    for error in errors:
        code = _REASON_BY_PYDANTIC_TYPE.get(error["type"], ReasonCode.INVALID_FORMAT)
        loc = prefix + tuple(str(part) for part in error["loc"])
        if code is ReasonCode.UNKNOWN_FIELD:
            # The last path element is the unknown key itself, which is client input.
            loc = loc[:-1]
        violations.append(Violation(code, ".".join(loc) or ROOT_PATH))
    return tuple(violations)
