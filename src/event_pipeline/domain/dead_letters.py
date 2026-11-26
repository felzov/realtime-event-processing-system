"""What the dead-letter table holds and why a message is in it (ADR 0006).

A dead letter is a message the consumer did not count, kept as it was read from the topic (key and
value bytes, and the `received_at` header when there is exactly one with a bytes value) with the
reason it was not counted. Its position on the topic identifies it: a redelivery of the same
message is the same dead letter.
"""

from dataclasses import dataclass
from enum import StrEnum
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
