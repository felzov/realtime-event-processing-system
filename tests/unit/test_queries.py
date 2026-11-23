"""Validation of the aggregate query's parameters (domain/queries.py)."""

from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

from event_pipeline.domain.errors import QueryRejectedError, ReasonCode, Violation
from event_pipeline.domain.queries import AggregateQuery, parse_aggregate_query

DAY = timedelta(days=1)
# Appears only in rejected input: it must never reach a violation or the error's text.
MARKER = "zz-MARKER"


def parse(**overrides: Any) -> AggregateQuery:
    parameters: dict[str, Any] = {
        "store_id": "store-7",
        "start": "2026-11-22T12:00:00Z",
        "end": "2026-11-22T13:00:00Z",
        "currency": None,
        "max_range": DAY,
    }
    return parse_aggregate_query(**{**parameters, **overrides})


def rejected(**overrides: Any) -> tuple[Violation, ...]:
    with pytest.raises(QueryRejectedError) as exc_info:
        parse(**overrides)
    return exc_info.value.violations


def test_a_valid_query_is_normalised_to_utc() -> None:
    query = parse(start="2026-11-22T14:00:00+02:00", end="2026-11-22T13:30:00.5Z", currency="EUR")

    assert query == AggregateQuery(
        store_id="store-7",
        start=datetime(2026, 11, 22, 12, 0, tzinfo=UTC),
        end=datetime(2026, 11, 22, 13, 30, 0, 500_000, tzinfo=UTC),
        currency="EUR",
    )


def test_currency_is_optional() -> None:
    assert parse().currency is None


def test_bounds_need_not_be_minute_aligned() -> None:
    query = parse(start="2026-11-22T12:00:30Z", end="2026-11-22T12:00:31Z")

    assert query.end - query.start == timedelta(seconds=1)


def test_a_range_of_exactly_the_maximum_is_accepted() -> None:
    query = parse(start="2026-11-22T00:00:00Z", end="2026-11-23T00:00:00Z")

    assert query.end - query.start == DAY


@pytest.mark.parametrize(
    ("start", "end"),
    [
        ("2026-11-22T00:00:00Z", "2026-11-23T00:00:00.000001Z"),
        ("2026-11-22T12:00:00Z", "2026-11-22T12:00:00Z"),
        ("2026-11-22T12:00:00Z", "2026-11-22T11:59:59Z"),
        # The same instant written with two offsets is an empty range.
        ("2026-11-22T14:00:00+02:00", "2026-11-22T12:00:00Z"),
    ],
    ids=["over_the_maximum", "empty", "reversed", "same_instant_other_offset"],
)
def test_a_range_that_is_empty_or_too_long_is_out_of_range(start: str, end: str) -> None:
    assert rejected(start=start, end=end) == (Violation(ReasonCode.OUT_OF_RANGE, "query.to"),)


def test_the_maximum_range_is_a_parameter() -> None:
    assert rejected(max_range=timedelta(minutes=59)) == (
        Violation(ReasonCode.OUT_OF_RANGE, "query.to"),
    )


@pytest.mark.parametrize(
    ("overrides", "violation"),
    [
        ({"store_id": "Store-7"}, Violation(ReasonCode.INVALID_FORMAT, "path.store_id")),
        ({"store_id": ""}, Violation(ReasonCode.INVALID_FORMAT, "path.store_id")),
        ({"store_id": "s" * 65}, Violation(ReasonCode.INVALID_FORMAT, "path.store_id")),
        ({"store_id": MARKER.upper()}, Violation(ReasonCode.INVALID_FORMAT, "path.store_id")),
        ({"start": None}, Violation(ReasonCode.MISSING_FIELD, "query.from")),
        ({"end": None}, Violation(ReasonCode.MISSING_FIELD, "query.to")),
        ({"start": ""}, Violation(ReasonCode.INVALID_FORMAT, "query.from")),
        ({"start": "2026-11-22"}, Violation(ReasonCode.INVALID_FORMAT, "query.from")),
        ({"start": "1795390127"}, Violation(ReasonCode.INVALID_FORMAT, "query.from")),
        ({"start": MARKER}, Violation(ReasonCode.INVALID_FORMAT, "query.from")),
        ({"start": "2026-11-22T12:00:00"}, Violation(ReasonCode.NAIVE_DATETIME, "query.from")),
        ({"end": "2026-11-22T13:00:00-00:00"}, Violation(ReasonCode.NAIVE_DATETIME, "query.to")),
        ({"end": "2026-13-22T13:00:00Z"}, Violation(ReasonCode.INVALID_FORMAT, "query.to")),
        (
            {"start": "0001-01-01T00:00:00+01:00"},
            Violation(ReasonCode.OUT_OF_RANGE, "query.from"),
        ),
        ({"currency": "eur"}, Violation(ReasonCode.INVALID_FORMAT, "query.currency")),
        ({"currency": ""}, Violation(ReasonCode.INVALID_FORMAT, "query.currency")),
        ({"currency": MARKER}, Violation(ReasonCode.INVALID_FORMAT, "query.currency")),
    ],
    ids=[
        "store_uppercase",
        "store_empty",
        "store_too_long",
        "store_marker",
        "from_missing",
        "to_missing",
        "from_empty",
        "from_date_only",
        "from_unix_seconds",
        "from_marker",
        "from_naive",
        "to_unknown_offset",
        "to_month_13",
        "from_before_utc_min",
        "currency_lowercase",
        "currency_empty",
        "currency_marker",
    ],
)
def test_each_invalid_parameter_is_reported_by_its_place_and_code(
    overrides: dict[str, Any], violation: Violation
) -> None:
    with pytest.raises(QueryRejectedError) as exc_info:
        parse(**overrides)

    assert exc_info.value.violations == (violation,)
    assert MARKER not in str(exc_info.value)
    assert MARKER.upper() not in str(exc_info.value)


def test_every_invalid_parameter_is_reported_at_once() -> None:
    violations = rejected(store_id="S", start=None, end="noon", currency="eur")

    assert violations == (
        Violation(ReasonCode.INVALID_FORMAT, "path.store_id"),
        Violation(ReasonCode.MISSING_FIELD, "query.from"),
        Violation(ReasonCode.INVALID_FORMAT, "query.to"),
        Violation(ReasonCode.INVALID_FORMAT, "query.currency"),
    )


def test_the_range_is_checked_only_once_both_bounds_are_valid() -> None:
    # An invalid `to` is reported as such, not also as an out-of-range pair.
    assert rejected(start="2026-11-22T12:00:00Z", end="2026-11-22T12:00:00") == (
        Violation(ReasonCode.NAIVE_DATETIME, "query.to"),
    )


def test_repeated_and_unknown_parameters_are_reported_with_the_others_in_request_order() -> None:
    violations = rejected(
        store_id="S",
        currency="eur",
        repeated=frozenset({"currency", "to"}),
        unknown_parameters=True,
    )

    assert violations == (
        Violation(ReasonCode.INVALID_FORMAT, "path.store_id"),
        Violation(ReasonCode.INVALID_FORMAT, "query.to"),
        # Invalid and repeated: reported once.
        Violation(ReasonCode.INVALID_FORMAT, "query.currency"),
        Violation(ReasonCode.UNKNOWN_FIELD, "query"),
    )


def test_a_repeated_parameter_is_rejected_even_when_each_value_is_valid() -> None:
    assert rejected(repeated=frozenset({"from"})) == (
        Violation(ReasonCode.INVALID_FORMAT, "query.from"),
    )


def test_an_unknown_parameter_alone_is_rejected() -> None:
    assert rejected(unknown_parameters=True) == (Violation(ReasonCode.UNKNOWN_FIELD, "query"),)
