"""Validation of the aggregate query's parameters (`GET /stores/{store_id}/aggregates`).

The parameters follow the event contract's own rules: `store_id` and `currency` have the formats of
`order.placed`, and `from`/`to` are RFC 3339 timestamps with a known offset, like `occurred_at`. A
rejection lists every invalid parameter with a reason code and its place in the request
(`path.store_id`, `query.from`, ...), never its value.

The range is half-open, `[from, to)`, and must be non-empty and at most the configured maximum: a
row is returned when its `window_start` lies in it. `from` and `to` need not be minute-aligned.

A query parameter given more than once is rejected (`invalid_format` at its path), since which value
counted would be the framework's choice; so is any parameter not in `QUERY_PARAMETERS`
(`unknown_field` at `query`, without its name), since a typo such as `curency` would otherwise drop
the filter and return every currency.
"""

from collections.abc import Set
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Final

from pydantic import BaseModel, ConfigDict, ValidationError

from event_pipeline.domain.errors import QueryRejectedError, ReasonCode, Violation
from event_pipeline.domain.events import CurrencyCode, StoreId, UtcDatetime, pydantic_violations

QUERY_PARAMETERS: Final = frozenset({"from", "to", "currency"})
"""The query string parameters the aggregate query accepts."""


class _Parameters(BaseModel):
    # Strict like the event contract; the timestamps opt out themselves (UtcDatetime). Unknown
    # query parameters never reach this model: the caller reports them (`unknown_parameters`).
    model_config = ConfigDict(strict=True, frozen=True)

    store_id: StoreId
    start: UtcDatetime
    end: UtcDatetime
    currency: CurrencyCode | None = None


# Model field -> where the parameter is in the request, for violation paths. `from` is a Python
# keyword, so the fields are named start and end.
_REQUEST_PATH = {
    "store_id": "path.store_id",
    "start": "query.from",
    "end": "query.to",
    "currency": "query.currency",
}
_QUERY_PATH = "query"
# Violations are reported in the order the parameters appear in the request line.
_PATH_ORDER = (*_REQUEST_PATH.values(), _QUERY_PATH)


@dataclass(frozen=True, slots=True)
class AggregateQuery:
    """A valid query: `start < end`, both in UTC; `currency` None means every currency."""

    store_id: str
    start: datetime
    end: datetime
    currency: str | None


def parse_aggregate_query(
    *,
    store_id: str,
    start: str | None,
    end: str | None,
    currency: str | None,
    max_range: timedelta,
    repeated: Set[str] = frozenset(),
    unknown_parameters: bool = False,
) -> AggregateQuery:
    """Validate the raw parameters; `None` means the parameter is absent.

    `repeated` names the parameters of `QUERY_PARAMETERS` given more than once (`start`/`end`
    hold one of their values); `unknown_parameters` is whether any other parameter was given.
    Raises `QueryRejectedError` with every violation found.
    """
    supplied = {"store_id": store_id, "start": start, "end": end, "currency": currency}
    violations: dict[str, Violation] = {}
    parameters: _Parameters | None = None
    try:
        parameters = _Parameters.model_validate(
            {name: value for name, value in supplied.items() if value is not None}
        )
    except ValidationError as exc:
        for violation in _in_request(pydantic_violations(exc.errors(), ())):
            violations.setdefault(violation.path, violation)
    for name in sorted(repeated & QUERY_PARAMETERS):
        path = f"query.{name}"
        # Invalid and repeated: reported once.
        violations.setdefault(path, Violation(ReasonCode.INVALID_FORMAT, path))
    if unknown_parameters:
        violations[_QUERY_PATH] = Violation(ReasonCode.UNKNOWN_FIELD, _QUERY_PATH)
    if violations or parameters is None:
        raise QueryRejectedError(tuple(violations[p] for p in _PATH_ORDER if p in violations))

    if parameters.end <= parameters.start or parameters.end - parameters.start > max_range:
        raise QueryRejectedError((Violation(ReasonCode.OUT_OF_RANGE, _REQUEST_PATH["end"]),))
    return AggregateQuery(
        store_id=parameters.store_id,
        start=parameters.start,
        end=parameters.end,
        currency=parameters.currency,
    )


def _in_request(violations: tuple[Violation, ...]) -> tuple[Violation, ...]:
    # A path starts with the model field name; the fields are scalars, so it has no further parts
    # today, and any that Pydantic added would not name client input.
    return tuple(Violation(v.code, _REQUEST_PATH[v.path.split(".")[0]]) for v in violations)
