"""The dead-letter reason codes (ADR 0006) and the list migration 0003 constrains them to."""

import importlib.util
from types import ModuleType

from event_pipeline.domain.dead_letters import DeadLetterReason
from event_pipeline.domain.errors import ReasonCode
from tests.migration_helpers import ALEMBIC_INI

MIGRATION_0003 = ALEMBIC_INI.parent / "migrations" / "versions" / "0003_create_dead_letters.py"


def load_migration_0003() -> ModuleType:
    spec = importlib.util.spec_from_file_location("migration_0003", MIGRATION_0003)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_reason_codes_are_pinned() -> None:
    # Stored in the database and listed in a CHECK: a rename would orphan stored rows.
    assert {reason.value for reason in DeadLetterReason} == {
        "invalid_event",
        "invalid_received_at",
        "late_event",
        "future_event",
        "conflicting_duplicate",
        "database_rejected",
    }


def test_a_reason_with_an_existing_reason_code_uses_that_code() -> None:
    for reason in (
        DeadLetterReason.INVALID_RECEIVED_AT,
        DeadLetterReason.LATE_EVENT,
        DeadLetterReason.FUTURE_EVENT,
        DeadLetterReason.CONFLICTING_DUPLICATE,
    ):
        assert reason.value == ReasonCode(reason.value).value


def test_migration_0003_allows_exactly_the_reason_codes() -> None:
    # The migration is frozen and cannot import the enum; this keeps the two lists equal. A new
    # reason needs a new migration that widens the CHECK.
    assert set(load_migration_0003().REASON_CODES) == {reason.value for reason in DeadLetterReason}
