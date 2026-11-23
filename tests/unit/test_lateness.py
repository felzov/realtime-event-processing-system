"""The late-event rule (domain/lateness.py, ADR 0005)."""

from datetime import UTC, datetime, timedelta, timezone, tzinfo

import pytest

from event_pipeline.domain.lateness import LatenessPolicy, Timing, classify

POLICY = LatenessPolicy(allowed_lateness=timedelta(minutes=5), max_future_skew=timedelta(minutes=1))
OCCURRED = datetime(2026, 11, 23, 12, 0, 30, tzinfo=UTC)
MICROSECOND = timedelta(microseconds=1)


@pytest.mark.parametrize(
    ("received_at", "expected"),
    [
        (OCCURRED, Timing.ON_TIME),
        (OCCURRED + timedelta(minutes=5), Timing.ON_TIME),
        (OCCURRED + timedelta(minutes=5) + MICROSECOND, Timing.LATE),
        (OCCURRED + timedelta(days=3), Timing.LATE),
        (OCCURRED - timedelta(minutes=1), Timing.ON_TIME),
        (OCCURRED - timedelta(minutes=1) - MICROSECOND, Timing.FUTURE),
        (OCCURRED - timedelta(days=3), Timing.FUTURE),
    ],
    ids=[
        "same_instant",
        "exactly_the_allowed_lateness",
        "one_microsecond_past_it",
        "days_late",
        "exactly_the_allowed_skew_ahead",
        "one_microsecond_past_the_skew",
        "days_ahead",
    ],
)
def test_events_are_classified_by_the_delay_to_ingest(
    received_at: datetime, expected: Timing
) -> None:
    assert classify(OCCURRED, received_at, POLICY) is expected


def test_instants_are_compared_whatever_their_offsets() -> None:
    # 14:00:30+02:00 is OCCURRED; received five minutes and a microsecond later, written in -05:00.
    occurred = OCCURRED.astimezone(timezone(timedelta(hours=2)))
    received = (OCCURRED + timedelta(minutes=5, microseconds=1)).astimezone(
        timezone(timedelta(hours=-5))
    )

    assert classify(occurred, received, POLICY) is Timing.LATE


def test_the_window_plays_no_part() -> None:
    # 12:00:59 received at 12:05:59 is on time although its window ended at 12:01:00.
    occurred = datetime(2026, 11, 23, 12, 0, 59, tzinfo=UTC)

    assert classify(occurred, occurred + timedelta(minutes=5), POLICY) is Timing.ON_TIME


def test_a_zero_skew_makes_any_future_instant_future() -> None:
    policy = LatenessPolicy(allowed_lateness=timedelta(minutes=5), max_future_skew=timedelta(0))

    assert classify(OCCURRED, OCCURRED, policy) is Timing.ON_TIME
    assert classify(OCCURRED, OCCURRED - MICROSECOND, policy) is Timing.FUTURE


@pytest.mark.parametrize("naive", ["occurred_at", "received_at"])
def test_a_naive_datetime_is_refused(naive: str) -> None:
    occurred = OCCURRED.replace(tzinfo=None) if naive == "occurred_at" else OCCURRED
    received = OCCURRED.replace(tzinfo=None) if naive == "received_at" else OCCURRED

    with pytest.raises(ValueError, match="timezone-aware"):
        classify(occurred, received, POLICY)


class _NoOffset(tzinfo):
    """A tzinfo that is set but gives no offset: the datetime is still naive."""

    def utcoffset(self, dt: datetime | None) -> timedelta | None:
        return None

    def dst(self, dt: datetime | None) -> timedelta | None:
        return None

    def tzname(self, dt: datetime | None) -> str | None:
        return None


def test_a_datetime_whose_tzinfo_has_no_offset_is_refused() -> None:
    occurred = OCCURRED.replace(tzinfo=_NoOffset())

    with pytest.raises(ValueError, match="timezone-aware"):
        classify(occurred, OCCURRED, POLICY)


@pytest.mark.parametrize(
    ("lateness", "skew"),
    [
        (timedelta(0), timedelta(minutes=1)),
        (-MICROSECOND, timedelta(0)),
        (MICROSECOND, -MICROSECOND),
    ],
    ids=["zero_lateness", "negative_lateness", "negative_skew"],
)
def test_a_policy_that_would_drop_every_event_is_refused(
    lateness: timedelta, skew: timedelta
) -> None:
    with pytest.raises(ValueError):
        LatenessPolicy(allowed_lateness=lateness, max_future_skew=skew)
