"""Rejection reason codes for invalid events (ADR 0001, decision 7) and invalid queries.

The codes are part of the public contract: clients and the dead-letter path see them, so a code is
never renamed or reused for a different meaning. Messages built here contain only codes and field
paths, never a submitted value. The query API reuses the same codes for its parameters.
"""

from dataclasses import dataclass
from enum import StrEnum


class ReasonCode(StrEnum):
    MISSING_FIELD = "missing_field"
    UNKNOWN_FIELD = "unknown_field"
    INVALID_TYPE = "invalid_type"
    OUT_OF_RANGE = "out_of_range"
    INVALID_FORMAT = "invalid_format"
    NAIVE_DATETIME = "naive_datetime"
    UNKNOWN_EVENT_TYPE = "unknown_event_type"
    UNSUPPORTED_SCHEMA_VERSION = "unsupported_schema_version"
    BODY_TOO_LARGE = "body_too_large"
    INVALID_JSON = "invalid_json"


ROOT_PATH = "<body>"
"""Path used when a violation concerns the body as a whole, not one field."""


@dataclass(frozen=True, slots=True)
class Violation:
    """One reason an event was rejected: a stable code and the dotted path of the field."""

    code: ReasonCode
    path: str


class EventRejectedError(Exception):
    """The event does not satisfy the contract. Carries every violation found, in order."""

    def __init__(self, violations: tuple[Violation, ...]) -> None:
        if not violations:
            raise ValueError("EventRejectedError needs at least one violation")
        self.violations = violations
        super().__init__("; ".join(f"{v.path}: {v.code}" for v in violations))


class QueryRejectedError(Exception):
    """The parameters of an aggregate query are invalid. Carries every violation found, in order."""

    def __init__(self, violations: tuple[Violation, ...]) -> None:
        if not violations:
            raise ValueError("QueryRejectedError needs at least one violation")
        self.violations = violations
        super().__init__("; ".join(f"{v.path}: {v.code}" for v in violations))
