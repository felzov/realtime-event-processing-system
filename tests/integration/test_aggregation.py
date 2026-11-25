"""`apply_order_placed` against the real test database (phase 3 criteria 2 and 3, phase 5
criterion 5).

Every test uses its own store_id and fresh event_ids, so tests never see each other's rows and
nothing needs deleting.
"""

import dataclasses
import threading
import time
import uuid
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any

import pytest
from psycopg import errors
from sqlalchemy import Connection, Engine, Row, select, text
from sqlalchemy.exc import DBAPIError, IntegrityError

from event_pipeline.config import Settings
from event_pipeline.data.aggregates import add_order_to_aggregate, record_processed_event
from event_pipeline.data.engine import make_engine
from event_pipeline.data.tables import order_aggregates, processed_events
from event_pipeline.domain.events import OrderPlacedEvent, OrderPlacedV1, parse_event
from event_pipeline.domain.fingerprint import fingerprint
from event_pipeline.services import aggregation
from event_pipeline.services.aggregation import ApplyOutcome, apply_order_placed
from tests.samples import encode, valid_event

BIGINT_MAX = 2**63 - 1
# How long a test waits for the other transaction to block on the primary key. Well under the
# engine's 5 s statement timeout, which would otherwise cancel the waiting insert.
LOCK_WAIT_DEADLINE_S = 3.0


@pytest.fixture
def store_id() -> str:
    return f"store-{uuid.uuid4().hex[:12]}"


@pytest.fixture
def make_event(store_id: str) -> Callable[..., OrderPlacedEvent]:
    def make(
        occurred_at: str = "2026-11-22T12:30:15Z", event_id: uuid.UUID | None = None, **payload: Any
    ) -> OrderPlacedEvent:
        body = valid_event()
        body["event_id"] = str(event_id or uuid.uuid4())
        body["occurred_at"] = occurred_at
        body["payload"].update({"store_id": store_id, **payload})
        return parse_event(encode(body))

    return make


def aggregates(engine: Engine, store_id: str) -> list[Row[Any]]:
    query = (
        select(order_aggregates)
        .where(order_aggregates.c.store_id == store_id)
        .order_by(order_aggregates.c.window_start, order_aggregates.c.currency)
    )
    with engine.connect() as connection:
        return list(connection.execute(query))


def counts(engine: Engine, store_id: str) -> list[tuple[datetime, str, int, int]]:
    return [
        (row.window_start, row.currency, row.order_count, row.revenue_minor)
        for row in aggregates(engine, store_id)
    ]


def stored_fingerprint(engine: Engine, event_id: uuid.UUID) -> bytes | None:
    query = select(processed_events.c.body_sha256).where(processed_events.c.event_id == event_id)
    with engine.connect() as connection:
        value: bytes | None = connection.execute(query).scalar_one()
        return value


def is_recorded(engine: Engine, event_id: uuid.UUID) -> bool:
    query = select(processed_events.c.event_id).where(processed_events.c.event_id == event_id)
    with engine.connect() as connection:
        return connection.execute(query).first() is not None


WINDOW = datetime(2026, 11, 22, 12, 30, tzinfo=UTC)
NEXT_WINDOW = datetime(2026, 11, 22, 12, 31, tzinfo=UTC)


# --- Normal path ---------------------------------------------------------------------------------


def test_first_event_creates_its_window_row(
    migrated_engine: Engine, make_event: Callable[..., OrderPlacedEvent], store_id: str
) -> None:
    event = make_event(amount_minor=2599)

    assert apply_order_placed(migrated_engine, event) is ApplyOutcome.APPLIED

    assert counts(migrated_engine, store_id) == [(WINDOW, "EUR", 1, 2599)]
    assert stored_fingerprint(migrated_engine, event.event_id) == fingerprint(event)


def test_second_event_in_the_same_window_adds_to_the_row(
    migrated_engine: Engine, make_event: Callable[..., OrderPlacedEvent], store_id: str
) -> None:
    apply_order_placed(migrated_engine, make_event(amount_minor=2599))
    before = aggregates(migrated_engine, store_id)[0]

    outcome = apply_order_placed(
        migrated_engine, make_event(occurred_at="2026-11-22T12:30:59.999Z", amount_minor=401)
    )

    assert outcome is ApplyOutcome.APPLIED
    assert counts(migrated_engine, store_id) == [(WINDOW, "EUR", 2, 3000)]
    assert aggregates(migrated_engine, store_id)[0].updated_at > before.updated_at


def test_other_minute_and_other_currency_get_their_own_rows(
    migrated_engine: Engine, make_event: Callable[..., OrderPlacedEvent], store_id: str
) -> None:
    apply_order_placed(migrated_engine, make_event(amount_minor=100))
    apply_order_placed(migrated_engine, make_event(amount_minor=200, currency="USD"))
    apply_order_placed(
        migrated_engine, make_event(occurred_at="2026-11-22T12:31:00Z", amount_minor=300)
    )
    # 15:30:20+03:00 is 12:30:20 UTC: the window is chosen in UTC, whatever the client's offset.
    apply_order_placed(
        migrated_engine, make_event(occurred_at="2026-11-22T15:30:20+03:00", amount_minor=400)
    )

    assert counts(migrated_engine, store_id) == [
        (WINDOW, "EUR", 2, 500),
        (WINDOW, "USD", 1, 200),
        (NEXT_WINDOW, "EUR", 1, 300),
    ]


def test_an_order_of_zero_is_counted(
    migrated_engine: Engine, make_event: Callable[..., OrderPlacedEvent], store_id: str
) -> None:
    assert apply_order_placed(migrated_engine, make_event(amount_minor=0)) is ApplyOutcome.APPLIED

    assert counts(migrated_engine, store_id) == [(WINDOW, "EUR", 1, 0)]


# --- Duplicates ----------------------------------------------------------------------------------


def test_same_event_again_is_a_duplicate_and_changes_nothing(
    migrated_engine: Engine, make_event: Callable[..., OrderPlacedEvent], store_id: str
) -> None:
    event = make_event(amount_minor=2599)
    apply_order_placed(migrated_engine, event)
    before = aggregates(migrated_engine, store_id)

    assert apply_order_placed(migrated_engine, event) is ApplyOutcome.DUPLICATE
    assert apply_order_placed(migrated_engine, event) is ApplyOutcome.DUPLICATE

    # updated_at included: a duplicate does not touch the row at all.
    assert aggregates(migrated_engine, store_id) == before


def test_same_event_id_with_another_body_is_a_conflicting_duplicate_and_changes_nothing(
    migrated_engine: Engine, make_event: Callable[..., OrderPlacedEvent], store_id: str
) -> None:
    # ADR 0004, decision 13: detected, not counted; the first copy stays.
    event_id = uuid.uuid4()
    first = make_event(event_id=event_id, amount_minor=100)
    apply_order_placed(migrated_engine, first)
    assert counts(migrated_engine, store_id) == [(WINDOW, "EUR", 1, 100)]
    before = aggregates(migrated_engine, store_id)

    outcome = apply_order_placed(
        migrated_engine,
        make_event(event_id=event_id, occurred_at="2026-11-22T12:31:00Z", amount_minor=999),
    )

    assert outcome is ApplyOutcome.CONFLICTING_DUPLICATE
    assert aggregates(migrated_engine, store_id) == before
    assert stored_fingerprint(migrated_engine, event_id) == fingerprint(first)


def test_same_event_written_with_another_utc_offset_is_a_plain_duplicate(
    migrated_engine: Engine, make_event: Callable[..., OrderPlacedEvent], store_id: str
) -> None:
    # The same instant, the same values: only the spelling of occurred_at differs.
    event_id = uuid.uuid4()
    apply_order_placed(
        migrated_engine, make_event(event_id=event_id, occurred_at="2026-11-22T12:30:15Z")
    )

    outcome = apply_order_placed(
        migrated_engine,
        make_event(event_id=event_id, occurred_at="2026-11-22T15:30:15.000+03:00"),
    )

    assert outcome is ApplyOutcome.DUPLICATE
    assert counts(migrated_engine, store_id) == [(WINDOW, "EUR", 1, 2599)]


def test_an_event_recorded_before_fingerprints_is_a_plain_duplicate_whatever_its_body(
    migrated_engine: Engine, make_event: Callable[..., OrderPlacedEvent], store_id: str
) -> None:
    # A row from before revision 0002 has no fingerprint: nothing to compare with.
    event_id = uuid.uuid4()
    with migrated_engine.begin() as connection:
        connection.execute(processed_events.insert().values(event_id=event_id))

    outcome = apply_order_placed(migrated_engine, make_event(event_id=event_id, amount_minor=999))

    assert outcome is ApplyOutcome.DUPLICATE
    assert counts(migrated_engine, store_id) == []
    assert stored_fingerprint(migrated_engine, event_id) is None  # not backfilled


def apply_in_thread(
    engine: Engine, event: OrderPlacedEvent
) -> tuple[threading.Thread, list[ApplyOutcome | BaseException]]:
    result: list[ApplyOutcome | BaseException] = []

    def run() -> None:
        try:
            result.append(apply_order_placed(engine, event))
        except BaseException as exc:  # handed to the test thread by outcome_of, which re-raises it
            result.append(exc)

    thread = threading.Thread(target=run, daemon=True)
    thread.start()
    return thread, result


def outcome_of(
    thread: threading.Thread, result: list[ApplyOutcome | BaseException]
) -> ApplyOutcome:
    thread.join(timeout=10)
    assert not thread.is_alive(), "the second apply did not finish"
    assert len(result) == 1, f"the second apply finished with {result!r}"
    [outcome] = result
    if isinstance(outcome, BaseException):
        raise outcome
    return outcome


def wait_until_blocked_by(
    engine: Engine,
    first_pid: int,
    thread: threading.Thread,
    result: list[ApplyOutcome | BaseException],
) -> None:
    """Return once some backend waits for a lock held by `first_pid`: the second apply's insert.

    Only `first`'s locks count, so another session waiting on some other lock cannot end the wait.
    """
    query = text(
        "SELECT count(*) FROM pg_stat_activity WHERE :first_pid = ANY(pg_blocking_pids(pid))"
    )
    deadline = time.monotonic() + LOCK_WAIT_DEADLINE_S
    with engine.connect() as connection:
        while connection.execute(query, {"first_pid": first_pid}).scalar_one() == 0:
            # pg_stat_activity is a snapshot taken once per transaction (stats_fetch_consistency
            # = cache): without ending the transaction, a new backend would never appear.
            connection.rollback()
            if not thread.is_alive():
                # Finished without waiting: show its exception, or what it returned.
                pytest.fail(
                    f"the second apply did not wait; it returned {outcome_of(thread, result)}"
                )
            if time.monotonic() > deadline:
                pytest.fail("the second apply never waited for the first transaction")
            time.sleep(0.02)


def apply_while_first_is_open(
    engine: Engine,
    first_statements: Callable[[Connection], None],
    event: OrderPlacedEvent,
    *,
    first_commits: bool,
) -> ApplyOutcome:
    """Run `first_statements` in an open transaction, apply `event` in a thread until it blocks on
    that transaction, then commit or roll back the first and return the thread's outcome."""
    with engine.connect() as first:
        first.begin()
        first_statements(first)
        first_pid = first.execute(text("SELECT pg_backend_pid()")).scalar_one()
        thread, result = apply_in_thread(engine, event)
        try:
            wait_until_blocked_by(engine, first_pid, thread, result)
        except BaseException:
            # Release the lock so the thread ends inside this test, not during the next one.
            first.rollback()
            thread.join(timeout=10)
            raise
        if first_commits:
            first.commit()
        else:
            first.rollback()
    return outcome_of(thread, result)


def first_delivery_of(event: OrderPlacedEvent) -> Callable[[Connection], None]:
    """The two statements of a delivery of `event`, for a transaction caught mid-apply."""

    def statements(connection: Connection) -> None:
        assert record_processed_event(connection, event.event_id, fingerprint(event))
        add_order_to_aggregate(
            connection,
            store_id=event.payload.store_id,
            currency=event.payload.currency,
            window_start=WINDOW,
            amount_minor=event.payload.amount_minor,
        )

    return statements


def test_concurrent_delivery_waits_and_is_a_duplicate_once_the_first_commits(
    migrated_engine: Engine, make_event: Callable[..., OrderPlacedEvent], store_id: str
) -> None:
    # The second insert of the same event_id waits on the primary key, at READ COMMITTED.
    event = make_event(amount_minor=2599)

    outcome = apply_while_first_is_open(
        migrated_engine, first_delivery_of(event), event, first_commits=True
    )

    assert outcome is ApplyOutcome.DUPLICATE
    assert counts(migrated_engine, store_id) == [(WINDOW, "EUR", 1, 2599)]


def test_concurrent_copy_with_another_body_is_a_conflict_once_the_first_commits(
    migrated_engine: Engine, make_event: Callable[..., OrderPlacedEvent], store_id: str
) -> None:
    # The waiting insert conflicts, and the fingerprint it then reads is the committed one.
    event_id = uuid.uuid4()
    first = make_event(event_id=event_id, amount_minor=2599)
    second = make_event(event_id=event_id, amount_minor=1)

    outcome = apply_while_first_is_open(
        migrated_engine, first_delivery_of(first), second, first_commits=True
    )

    assert outcome is ApplyOutcome.CONFLICTING_DUPLICATE
    assert counts(migrated_engine, store_id) == [(WINDOW, "EUR", 1, 2599)]
    assert stored_fingerprint(migrated_engine, event_id) == fingerprint(first)


def test_concurrent_copy_with_another_body_is_applied_if_the_first_rolls_back(
    migrated_engine: Engine, make_event: Callable[..., OrderPlacedEvent], store_id: str
) -> None:
    # Nothing of the first copy was committed, so the second is the first applied copy.
    event_id = uuid.uuid4()
    first = make_event(event_id=event_id, amount_minor=2599)
    second = make_event(event_id=event_id, amount_minor=1)

    outcome = apply_while_first_is_open(
        migrated_engine, first_delivery_of(first), second, first_commits=False
    )

    assert outcome is ApplyOutcome.APPLIED
    assert counts(migrated_engine, store_id) == [(WINDOW, "EUR", 1, 1)]
    assert stored_fingerprint(migrated_engine, event_id) == fingerprint(second)


def test_concurrent_delivery_applies_the_event_if_the_first_rolls_back(
    migrated_engine: Engine, make_event: Callable[..., OrderPlacedEvent], store_id: str
) -> None:
    event = make_event(amount_minor=2599)

    outcome = apply_while_first_is_open(
        migrated_engine, first_delivery_of(event), event, first_commits=False
    )

    assert outcome is ApplyOutcome.APPLIED
    assert counts(migrated_engine, store_id) == [(WINDOW, "EUR", 1, 2599)]


def test_concurrent_events_for_the_same_window_are_both_counted(
    migrated_engine: Engine, make_event: Callable[..., OrderPlacedEvent], store_id: str
) -> None:
    # Two different events, one row: the upsert waits for the first insert of the row, then adds
    # to it. A read-then-write would lose one of the two updates.
    first, second = make_event(amount_minor=2599), make_event(amount_minor=401)

    outcome = apply_while_first_is_open(
        migrated_engine, first_delivery_of(first), second, first_commits=True
    )

    assert outcome is ApplyOutcome.APPLIED
    assert counts(migrated_engine, store_id) == [(WINDOW, "EUR", 2, 3000)]


def test_delivery_blocked_longer_than_the_statement_timeout_raises_and_records_nothing(
    migrated_engine: Engine,
    db_settings_for: Callable[..., Settings],
    make_event: Callable[..., OrderPlacedEvent],
    store_id: str,
) -> None:
    # A second delivery waits for the first transaction at most DB_STATEMENT_TIMEOUT_MS; then it
    # fails like any database error, and the consumer stops without committing the offset.
    event = make_event(amount_minor=2599)
    impatient = make_engine(db_settings_for(db_statement_timeout_ms=300))
    try:
        with migrated_engine.connect() as first:
            first.begin()
            first_delivery_of(event)(first)

            with pytest.raises(DBAPIError) as exc_info:
                apply_order_placed(impatient, event)

            first.rollback()
    finally:
        impatient.dispose()

    assert isinstance(exc_info.value.orig, errors.QueryCanceled)
    assert not is_recorded(migrated_engine, event.event_id)
    assert aggregates(migrated_engine, store_id) == []


# --- Failure after the processed_events insert ---------------------------------------------------


def test_failure_after_recording_the_event_id_rolls_back_both_tables(
    migrated_engine: Engine,
    make_event: Callable[..., OrderPlacedEvent],
    store_id: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    apply_order_placed(migrated_engine, make_event(amount_minor=100))
    before = aggregates(migrated_engine, store_id)
    event = make_event(amount_minor=2599)

    def fail(connection: Connection, **kwargs: object) -> None:
        # The fault comes after the insert: the row is there, inside the transaction.
        recorded = select(processed_events.c.event_id).where(
            processed_events.c.event_id == event.event_id
        )
        assert connection.execute(recorded).first() is not None
        raise RuntimeError("injected fault between the two statements")

    monkeypatch.setattr(aggregation, "add_order_to_aggregate", fail)

    with pytest.raises(RuntimeError, match="injected fault"):
        apply_order_placed(migrated_engine, event)

    assert not is_recorded(migrated_engine, event.event_id)
    assert aggregates(migrated_engine, store_id) == before

    # The redelivery after a restart is applied, not mistaken for a duplicate.
    monkeypatch.undo()
    assert apply_order_placed(migrated_engine, event) is ApplyOutcome.APPLIED
    assert counts(migrated_engine, store_id) == [(WINDOW, "EUR", 2, 2699)]


def test_revenue_overflow_raises_and_records_nothing(
    migrated_engine: Engine, make_event: Callable[..., OrderPlacedEvent], store_id: str
) -> None:
    # A real database error in the second statement: bigint overflow, the known gap of ADR 0004
    # (consequences) that stops the consumer until phase 6.
    with migrated_engine.begin() as connection:
        connection.execute(
            order_aggregates.insert().values(
                store_id=store_id,
                window_start=WINDOW,
                currency="EUR",
                order_count=1,
                revenue_minor=BIGINT_MAX,
            )
        )
    before = aggregates(migrated_engine, store_id)
    event = make_event(amount_minor=1)

    with pytest.raises(DBAPIError) as exc_info:
        apply_order_placed(migrated_engine, event)

    assert isinstance(exc_info.value.orig, errors.NumericValueOutOfRange)
    # The engine hides statement parameters, so the error text does not carry the event's values
    # (this error has no PostgreSQL DETAIL that would repeat them).
    assert store_id not in str(exc_info.value)
    assert not is_recorded(migrated_engine, event.event_id)
    assert aggregates(migrated_engine, store_id) == before


def test_row_the_database_refuses_raises_and_records_nothing(
    migrated_engine: Engine, make_event: Callable[..., OrderPlacedEvent], store_id: str
) -> None:
    # parse_event cannot produce this event; a bug that bypasses it meets the CHECK constraints.
    event = make_event(amount_minor=100)
    payload = OrderPlacedV1.model_construct(**{**event.payload.model_dump(), "currency": "eur"})
    invalid = dataclasses.replace(event, payload=payload)

    with pytest.raises(IntegrityError) as exc_info:
        apply_order_placed(migrated_engine, invalid)

    assert isinstance(exc_info.value.orig, errors.CheckViolation)
    assert not is_recorded(migrated_engine, event.event_id)
    assert aggregates(migrated_engine, store_id) == []
