"""What the dead-letter table holds and why a message is in it (ADR 0006).

A dead letter is a message the consumer did not count, kept as it was read from the topic (key and
value bytes, and the `received_at` header when there is exactly one with a bytes value) with the
reason it was not counted. Its position on the topic identifies it: a redelivery of the same
message is the same dead letter.
"""

from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from typing import Final
from uuid import UUID

from event_pipeline.domain.errors import ReasonCode


class DeadLetterReason(StrEnum):
    """The values of `dead_letters.reason_code`; migration 0003 lists them in a CHECK.

    Part of the public contract like `ReasonCode`: never renamed or reused. Where a reason already
    has a code, the value is that code.
    """

    # parse_event rejected the value; the violation codes go in the detail.
    INVALID_EVENT = "invalid_event"
    INVALID_RECEIVED_AT = ReasonCode.INVALID_RECEIVED_AT.value
    LATE_EVENT = ReasonCode.LATE_EVENT.value
    FUTURE_EVENT = ReasonCode.FUTURE_EVENT.value
    CONFLICTING_DUPLICATE = ReasonCode.CONFLICTING_DUPLICATE.value
    # The database refused the event itself (SQLSTATE class 22 or 23), so a retry would fail too.
    DATABASE_REJECTED = "database_rejected"


@dataclass(frozen=True, slots=True)
class DeadLetter:
    """One message that was not counted, as it was read, and why.

    `detail` is text this project writes (violation codes, a delay, a database error's classes,
    SQLSTATE and constraint name), never a value from the message. `event_id` is set when the
    value parsed as an event.
    """

    topic: str
    partition: int
    offset: int
    key: bytes | None
    value: bytes | None
    received_at_header: bytes | None
    reason: DeadLetterReason
    detail: str | None
    event_id: UUID | None


# A replay refuses these (ADR 0006, decision 7). The first copy of a conflicting duplicate is
# already counted, so publishing the second again would only conflict again.
NOT_REPLAYABLE: Final = frozenset({DeadLetterReason.CONFLICTING_DUPLICATE})


@dataclass(frozen=True, slots=True)
class StoredDeadLetter:
    """A dead letter as the table holds it: the message, and when it was dead-lettered."""

    letter: DeadLetter
    dead_lettered_at: datetime


@dataclass(frozen=True, slots=True)
class ReplaySelection:
    """Which dead letters a replay publishes; every condition given must hold (ADR 0006, d7).

    `reasons` empty means every reason a replay accepts. `since` and `until` bound
    `dead_lettered_at` as `[since, until)`, so a run can leave out the dead letters an earlier
    replay added. Raises `ValueError` for a reason a replay refuses, a naive time, or an empty
    range.
    """

    reasons: frozenset[DeadLetterReason] = frozenset()
    event_id: UUID | None = None
    since: datetime | None = None
    until: datetime | None = None

    def __post_init__(self) -> None:
        refused = sorted(self.reasons & NOT_REPLAYABLE)
        if refused:
            raise ValueError(
                f"{', '.join(refused)} cannot be replayed: the first copy of the event is "
                "already counted, so the replayed copy would only conflict again"
            )
        for name, instant in (("since", self.since), ("until", self.until)):
            if instant is not None and instant.utcoffset() is None:
                raise ValueError(f"{name} must include a UTC offset, for example Z")
        if self.since is not None and self.until is not None and self.since >= self.until:
            raise ValueError("since must be before until")


@dataclass(frozen=True, slots=True)
class ReplayCandidates:
    """What a selection found.

    `letters` are the ones to publish, oldest first. Of the dead letters with the same event id
    (or none), key and value, only the newest is among them: a replay that failed again added a
    dead letter at its new offset, and publishing the old one too would double the copies on
    every run.
    `superseded` counts the older ones left out, and `refused` the dead letters that matched but
    have a reason a replay refuses.
    """

    letters: tuple[StoredDeadLetter, ...]
    superseded: int
    refused: int
