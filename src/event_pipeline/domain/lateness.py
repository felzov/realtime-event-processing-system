"""Late and future events: which events may change an aggregate (ADR 0005).

An event's lateness is how long it took to reach the ingest API: `received_at - occurred_at`.
Both instants travel with the message (`occurred_at` in the body, `received_at` in a header set by
the server), so the same message is classified the same way on every delivery, after a restart, a
rebalance or a replay. Nothing here depends on the consumer's clock or on what it has seen before.

The bounds are inclusive: an event exactly `allowed_lateness` late, or exactly `max_future_skew`
ahead of `received_at`, is on time.
"""

from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum


class Timing(Enum):
    ON_TIME = "on_time"
    # Reached ingest more than `allowed_lateness` after it occurred.
    LATE = "late"
    # Claims to have occurred more than `max_future_skew` after it reached ingest: the producer's
    # clock is wrong.
    FUTURE = "future"


@dataclass(frozen=True, slots=True)
class LatenessPolicy:
    allowed_lateness: timedelta
    max_future_skew: timedelta

    def __post_init__(self) -> None:
        if self.allowed_lateness <= timedelta(0):
            # Zero would make nearly every event late: received_at is after occurred_at.
            raise ValueError("allowed_lateness must be positive")
        if self.max_future_skew < timedelta(0):
            raise ValueError("max_future_skew must not be negative")


def classify(occurred_at: datetime, received_at: datetime, policy: LatenessPolicy) -> Timing:
    """Whether an event that occurred at `occurred_at` and reached ingest at `received_at` counts.

    Raises `ValueError` for a naive datetime: comparing it with an aware one would either fail or,
    after a guessed offset, misclassify the event.
    """
    if occurred_at.utcoffset() is None or received_at.utcoffset() is None:
        raise ValueError("occurred_at and received_at must be timezone-aware")
    delay = received_at - occurred_at
    if delay > policy.allowed_lateness:
        return Timing.LATE
    if -delay > policy.max_future_skew:
        return Timing.FUTURE
    return Timing.ON_TIME
