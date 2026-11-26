"""Replay without a broker or a database: the selection rules, publishing, and the command line.

The producer here is a fake that records what was produced and answers each message with the
delivery report a test gives it, when flushed, as librdkafka serves reports from flush().
"""

import uuid
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta, timezone
from typing import Any

import psycopg
import pytest
from confluent_kafka import KafkaError, KafkaException, Message
from sqlalchemy.exc import OperationalError
from sqlalchemy.exc import TimeoutError as PoolTimeoutError

from event_pipeline.broker.publisher import (
    CAUSE_BACKSTOP,
    CAUSE_CALLBACK_ERROR,
    CAUSE_QUEUE_FULL,
    RECEIVED_AT_HEADER,
    Delivered,
    Delivery,
    NotDelivered,
    PublishFailure,
    classify,
)
from event_pipeline.config import Settings
from event_pipeline.domain.dead_letters import (
    DeadLetter,
    DeadLetterReason,
    ReplayCandidates,
    ReplaySelection,
    StoredDeadLetter,
)
from event_pipeline.replay import __main__ as replay_main
from event_pipeline.services.replay import CAUSE_PRODUCER_FATAL, publish_dead_letters

TOPIC = "orders.placed.v1"
STORE_ID = "store-PAYLOAD-KEY"
PAYLOAD_MARKER = "ord-PAYLOAD-MARKER"
STORED_AT = datetime(2026, 11, 25, 10, 0, tzinfo=UTC)
HEADER = b"2026-11-25T09:50:00.000000Z"


def stored(
    offset: int = 7,
    *,
    reason: DeadLetterReason = DeadLetterReason.LATE_EVENT,
    received_at_header: bytes | None = HEADER,
    event_id: uuid.UUID | None = None,
) -> StoredDeadLetter:
    return StoredDeadLetter(
        letter=DeadLetter(
            topic=TOPIC,
            partition=2,
            offset=offset,
            key=STORE_ID.encode(),
            value=b'{"order_id": "' + PAYLOAD_MARKER.encode() + b'"}',
            received_at_header=received_at_header,
            reason=reason,
            detail="received 600.000 s after occurred_at",
            event_id=uuid.uuid4() if event_id is None else event_id,
        ),
        dead_lettered_at=STORED_AT,
    )


# --- The selection --------------------------------------------------------------------------------


def test_an_empty_selection_takes_every_replayable_reason() -> None:
    assert ReplaySelection().reasons == frozenset()


def test_conflicting_duplicates_are_refused() -> None:
    with pytest.raises(ValueError, match="conflicting_duplicate cannot be replayed"):
        ReplaySelection(
            reasons=frozenset({DeadLetterReason.LATE_EVENT, DeadLetterReason.CONFLICTING_DUPLICATE})
        )


@pytest.mark.parametrize("name", ["since", "until"])
def test_a_time_without_an_offset_is_refused(name: str) -> None:
    naive = datetime(2026, 11, 25, 10, 0)
    with pytest.raises(ValueError, match=f"{name} must include a UTC offset"):
        ReplaySelection(since=naive) if name == "since" else ReplaySelection(until=naive)


@pytest.mark.parametrize("until", [STORED_AT, STORED_AT - timedelta(microseconds=1)])
def test_an_empty_time_range_is_refused(until: datetime) -> None:
    with pytest.raises(ValueError, match="since must be before until"):
        ReplaySelection(since=STORED_AT, until=until)


def test_a_range_in_any_offset_is_accepted() -> None:
    plus_two = timezone(timedelta(hours=2))
    selection = ReplaySelection(
        since=STORED_AT.astimezone(plus_two), until=STORED_AT + timedelta(microseconds=1)
    )
    assert selection.since == STORED_AT


# --- Publishing -----------------------------------------------------------------------------------


type Report = tuple[KafkaError | None, Message]
type Callback = Callable[[KafkaError | None, Message], None]


@dataclass
class FakeProducer:
    """Answers the n-th produced message with `reports[n]` on flush; None means no report."""

    reports: list[Report | None] = field(default_factory=list)
    # Raised by produce() for the n-th call, instead of queueing the message.
    produce_errors: dict[int, Exception] = field(default_factory=dict)
    produced: list[dict[str, Any]] = field(default_factory=list)
    flushes: list[float] = field(default_factory=list)
    calls: int = 0
    polls: int = 0
    _pending: list[tuple[int, Callback]] = field(default_factory=list)

    def produce(
        self,
        topic: str,
        *,
        value: bytes | None,
        key: bytes | None,
        headers: list[tuple[str, str | bytes | None]],
        on_delivery: Callback,
    ) -> None:
        call = self.calls
        self.calls += 1
        if call in self.produce_errors:
            raise self.produce_errors[call]
        self._pending.append((len(self.produced), on_delivery))
        self.produced.append({"topic": topic, "value": value, "key": key, "headers": headers})

    def poll(self, timeout: float) -> int:
        self.polls += 1
        return 0

    def flush(self, timeout: float) -> int:
        self.flushes.append(timeout)
        waiting = 0
        for index, callback in self._pending:
            report = self.reports[index] if index < len(self.reports) else None
            if report is None:
                waiting += 1
            else:
                callback(*report)
        self._pending = []
        return waiting


def delivered_at(offset: int) -> Report:
    return None, Message(topic=TOPIC, partition=4, offset=offset)


def test_each_dead_letter_is_published_as_it_was_stored() -> None:
    with_header, without_header = stored(1), stored(2, received_at_header=None)
    producer = FakeProducer(reports=[delivered_at(40), delivered_at(41)])

    outcomes = publish_dead_letters(producer, TOPIC, [with_header, without_header], timeout_s=9.0)

    assert outcomes == [Delivered(partition=4, offset=40), Delivered(partition=4, offset=41)]
    # Reports are served after each message, not only at the end.
    assert producer.polls == 2
    assert producer.produced == [
        {
            "topic": TOPIC,
            "value": with_header.letter.value,
            "key": with_header.letter.key,
            "headers": [(RECEIVED_AT_HEADER, HEADER)],
        },
        {
            "topic": TOPIC,
            "value": without_header.letter.value,
            "key": without_header.letter.key,
            # Not an empty header: none, as the message had none it could keep.
            "headers": [],
        },
    ]
    assert producer.flushes == [9.0]


def test_an_empty_key_and_value_are_published_as_none() -> None:
    letter = stored()
    empty = StoredDeadLetter(
        letter=DeadLetter(**{**_fields(letter.letter), "key": None, "value": None}),
        dead_lettered_at=STORED_AT,
    )
    producer = FakeProducer(reports=[delivered_at(1)])

    publish_dead_letters(producer, TOPIC, [empty], timeout_s=1.0)

    assert producer.produced[0]["key"] is None
    assert producer.produced[0]["value"] is None


def _fields(letter: DeadLetter) -> dict[str, Any]:
    return {name: getattr(letter, name) for name in DeadLetter.__slots__}


def test_a_failed_report_or_a_refused_produce_is_reported_and_the_rest_still_published() -> None:
    timed_out = KafkaError(KafkaError._MSG_TIMED_OUT)
    too_large = KafkaError(KafkaError.MSG_SIZE_TOO_LARGE)
    producer = FakeProducer(
        # The second produce() raises, so the reports belong to the first and the third message.
        reports=[(timed_out, Message(topic=TOPIC)), delivered_at(9)],
        produce_errors={1: KafkaException(too_large)},
    )

    outcomes = publish_dead_letters(producer, TOPIC, [stored(1), stored(2), stored(3)], 1.0)

    assert outcomes == [classify(timed_out), classify(too_large), Delivered(4, 9)]
    assert outcomes[0] == NotDelivered(PublishFailure.BROKER_UNAVAILABLE, "_MSG_TIMED_OUT")
    assert outcomes[1] == NotDelivered(PublishFailure.REJECTED, "MSG_SIZE_TOO_LARGE")


def test_a_full_local_queue_is_flushed_and_the_message_produced_once_more() -> None:
    producer = FakeProducer(reports=[delivered_at(5)], produce_errors={0: BufferError()})

    outcomes = publish_dead_letters(producer, TOPIC, [stored()], timeout_s=3.0)

    assert outcomes == [Delivered(4, 5)]
    assert producer.calls == 2
    assert producer.flushes == [3.0, 3.0]


def test_a_refused_produce_after_a_full_queue_is_classified() -> None:
    too_large = KafkaError(KafkaError.MSG_SIZE_TOO_LARGE)
    producer = FakeProducer(produce_errors={0: BufferError(), 1: KafkaException(too_large)})

    outcomes = publish_dead_letters(producer, TOPIC, [stored()], timeout_s=3.0)

    assert outcomes == [NotDelivered(PublishFailure.REJECTED, "MSG_SIZE_TOO_LARGE")]


def test_a_report_that_cannot_be_read_marks_only_its_letter_unconfirmed(
    caplog: pytest.LogCaptureFixture,
) -> None:
    # Not a Message: reading it raises inside the callback, which flush() would re-raise.
    producer = FakeProducer(reports=[delivered_at(1), (None, object()), delivered_at(3)])  # type: ignore[list-item]

    outcomes = publish_dead_letters(producer, TOPIC, [stored(1), stored(2), stored(3)], 1.0)

    assert outcomes == [
        Delivered(4, 1),
        NotDelivered(PublishFailure.PUBLISHER_FAILED, CAUSE_CALLBACK_ERROR),
        Delivered(4, 3),
    ]
    assert "delivery report could not be read" in caplog.text


def test_a_fatal_producer_error_stops_producing_the_rest() -> None:
    fatal = KafkaError(KafkaError._FATAL, fatal=True)
    producer = FakeProducer(reports=[delivered_at(1)], produce_errors={1: KafkaException(fatal)})

    outcomes = publish_dead_letters(producer, TOPIC, [stored(1), stored(2), stored(3)], 1.0)

    assert outcomes == [
        Delivered(4, 1),
        NotDelivered(PublishFailure.PUBLISHER_FAILED, CAUSE_PRODUCER_FATAL),
        NotDelivered(PublishFailure.PUBLISHER_FAILED, CAUSE_PRODUCER_FATAL),
    ]
    assert producer.calls == 2


def test_a_fatal_error_in_a_report_stops_producing_the_rest() -> None:
    fatal = KafkaError(KafkaError._FATAL, fatal=True)
    producer = FakeProducer(reports=[(fatal, Message(topic=TOPIC)), delivered_at(2)])

    # The fake serves reports on flush only; a full queue makes it flush after the first letter.
    producer.produce_errors = {1: BufferError()}
    outcomes = publish_dead_letters(producer, TOPIC, [stored(1), stored(2), stored(3)], 1.0)

    assert outcomes == [
        NotDelivered(PublishFailure.PUBLISHER_FAILED, CAUSE_PRODUCER_FATAL),
        NotDelivered(PublishFailure.PUBLISHER_FAILED, CAUSE_PRODUCER_FATAL),
        NotDelivered(PublishFailure.PUBLISHER_FAILED, CAUSE_PRODUCER_FATAL),
    ]
    assert producer.calls == 2


def test_a_queue_still_full_after_the_flush_is_reported_as_broker_unavailable() -> None:
    producer = FakeProducer(
        reports=[delivered_at(5)], produce_errors={0: BufferError(), 1: BufferError()}
    )

    outcomes = publish_dead_letters(producer, TOPIC, [stored(1), stored(2)], timeout_s=3.0)

    assert outcomes == [
        NotDelivered(PublishFailure.BROKER_UNAVAILABLE, CAUSE_QUEUE_FULL),
        Delivered(4, 5),
    ]


def test_a_message_without_a_report_after_the_flush_is_not_called_published() -> None:
    producer = FakeProducer(reports=[delivered_at(5), None])

    outcomes = publish_dead_letters(producer, TOPIC, [stored(1), stored(2)], timeout_s=3.0)

    assert outcomes == [
        Delivered(4, 5),
        NotDelivered(PublishFailure.PUBLISHER_FAILED, CAUSE_BACKSTOP),
    ]


def test_nothing_to_publish_publishes_nothing() -> None:
    producer = FakeProducer()

    assert publish_dead_letters(producer, TOPIC, [], timeout_s=3.0) == []
    assert producer.produced == []


# --- The command line -----------------------------------------------------------------------------


def parse(*argv: str) -> tuple[ReplaySelection, bool]:
    return replay_main.parse_selection(replay_main.build_parser(), list(argv))


def test_the_arguments_become_a_selection() -> None:
    event_id = uuid.uuid4()

    selection, execute = parse(
        "--reason",
        "late_event",
        "--reason",
        "invalid_event",
        "--event-id",
        str(event_id),
        "--since",
        "2026-11-25T10:00:00Z",
        "--until",
        "2026-11-25T12:00:00+01:00",
        "--execute",
    )

    assert selection == ReplaySelection(
        reasons=frozenset({DeadLetterReason.LATE_EVENT, DeadLetterReason.INVALID_EVENT}),
        event_id=event_id,
        since=STORED_AT,
        until=STORED_AT + timedelta(hours=1),
    )
    assert execute is True


def test_without_arguments_it_is_a_dry_run_of_everything_replayable() -> None:
    assert parse() == (ReplaySelection(), False)


@pytest.mark.parametrize(
    ("argv", "message"),
    [
        (["--reason", "conflicting_duplicate"], "conflicting_duplicate cannot be replayed"),
        (["--reason", "no_such_reason"], "invalid choice"),
        (["--event-id", "not-a-uuid"], "--event-id"),
        (["--since", "2026-11-25T10:00:00"], "needs a UTC offset"),
        (["--until", "yesterday"], "not an RFC 3339 time"),
        (
            ["--since", "2026-11-25T10:00:00Z", "--until", "2026-11-25T10:00:00Z"],
            "since must be before until",
        ),
    ],
    ids=["refused-reason", "unknown-reason", "bad-uuid", "naive", "not-a-time", "empty-range"],
)
def test_invalid_arguments_exit_with_status_2(
    argv: list[str], message: str, capsys: pytest.CaptureFixture[str]
) -> None:
    with pytest.raises(SystemExit) as exit_:
        parse(*argv)

    assert exit_.value.code == replay_main.EXIT_USAGE
    assert message in capsys.readouterr().err


@dataclass
class Wired:
    """`main` with the database read and the publish replaced; records what reached them."""

    candidates: ReplayCandidates | Exception
    outcomes: list[Delivery] = field(default_factory=list)
    selections: list[ReplaySelection] = field(default_factory=list)
    published: list[list[StoredDeadLetter]] = field(default_factory=list)
    producers: int = 0


@pytest.fixture
def wire(monkeypatch: pytest.MonkeyPatch) -> Callable[..., Wired]:
    def make(
        candidates: ReplayCandidates | Exception, outcomes: list[Delivery] | None = None
    ) -> Wired:
        wired = Wired(candidates, outcomes or [])

        def find(engine: Any, topic: str, selection: ReplaySelection) -> ReplayCandidates:
            assert topic == TOPIC
            wired.selections.append(selection)
            if isinstance(wired.candidates, Exception):
                raise wired.candidates
            return wired.candidates

        def publish(
            producer: Any, topic: str, letters: list[StoredDeadLetter], timeout_s: float
        ) -> list[Delivery]:
            assert topic == TOPIC
            wired.published.append(list(letters))
            return wired.outcomes

        def producer(config: dict[str, Any]) -> object:
            assert config["acks"] == "all"
            assert config["partitioner"] == "murmur2_random"
            wired.producers += 1
            return object()

        monkeypatch.setattr(replay_main, "find_dead_letters_to_replay", find)
        monkeypatch.setattr(replay_main, "publish_dead_letters", publish)
        monkeypatch.setattr(replay_main, "Producer", producer)
        monkeypatch.setattr(replay_main, "make_engine", lambda settings: _Engine())
        # The developer's own setting variables must not change what Settings() reads.
        for name in Settings.model_fields:
            monkeypatch.delenv(name.upper(), raising=False)
        monkeypatch.setenv("DATABASE_URL", "postgresql+psycopg://user:pw@127.0.0.1:1/x")
        monkeypatch.setenv("EVENTS_TOPIC", TOPIC)
        monkeypatch.chdir("/")
        return wired

    return make


class _Engine:
    def dispose(self) -> None:
        pass


def test_a_dry_run_lists_without_publishing_or_a_producer(
    wire: Callable[..., Wired], capsys: pytest.CaptureFixture[str]
) -> None:
    letters = (stored(1), stored(2, reason=DeadLetterReason.INVALID_RECEIVED_AT))
    wired = wire(ReplayCandidates(letters=letters, superseded=3, refused=1))

    status = replay_main.main(["--since", "2026-11-25T00:00:00Z"])

    out = capsys.readouterr().out
    assert status == 0
    assert wired.selections == [ReplaySelection(since=datetime(2026, 11, 25, tzinfo=UTC))]
    assert wired.published == []
    assert wired.producers == 0
    assert out.splitlines() == [
        f"2 dead letter(s) from {TOPIC} would be published:",
        f"  partition 2 offset 1 late_event, event {letters[0].letter.event_id}, "
        "dead-lettered 2026-11-25T10:00:00Z",
        f"  partition 2 offset 2 invalid_received_at, event {letters[1].letter.event_id}, "
        "dead-lettered 2026-11-25T10:00:00Z",
        "left out: 3 older dead letter(s) of the same messages (only the newest of each is "
        "published)",
        "refused: 1 dead letter(s) with reason conflicting_duplicate",
        "dry run: nothing published; add --execute to publish them",
    ]


def test_execute_publishes_and_reports_each_position(
    wire: Callable[..., Wired], capsys: pytest.CaptureFixture[str]
) -> None:
    letters = (stored(1), stored(2))
    wired = wire(
        ReplayCandidates(letters=letters, superseded=0, refused=0),
        [Delivered(4, 40), Delivered(4, 41)],
    )

    status = replay_main.main(["--execute"])

    out = capsys.readouterr().out
    assert status == 0
    assert wired.published == [list(letters)]
    assert wired.producers == 1
    assert out.splitlines()[-1] == f"published 2 of 2 dead letter(s) to {TOPIC}"
    assert out.splitlines()[0].endswith(": published at partition 4 offset 40")
    assert "left out" not in out
    assert "refused" not in out


def test_a_message_not_confirmed_exits_with_status_1(
    wire: Callable[..., Wired], capsys: pytest.CaptureFixture[str]
) -> None:
    letters = (stored(1), stored(2))
    wire(
        ReplayCandidates(letters=letters, superseded=0, refused=0),
        [Delivered(4, 40), NotDelivered(PublishFailure.BROKER_UNAVAILABLE, "_MSG_TIMED_OUT")],
    )

    status = replay_main.main(["--execute"])

    captured = capsys.readouterr()
    assert status == replay_main.EXIT_NOT_PUBLISHED
    assert captured.out.splitlines()[1].endswith(
        ": NOT published: broker_unavailable (_MSG_TIMED_OUT)"
    )
    assert captured.out.splitlines()[-1] == f"published 1 of 2 dead letter(s) to {TOPIC}"
    assert "running the same replay again is safe" in captured.err


def test_nothing_selected_publishes_nothing_and_succeeds(
    wire: Callable[..., Wired], capsys: pytest.CaptureFixture[str]
) -> None:
    wired = wire(ReplayCandidates(letters=(), superseded=0, refused=2))

    status = replay_main.main(["--execute"])

    out = capsys.readouterr().out
    assert status == 0
    assert wired.published == [[]]
    assert "refused: 2 dead letter(s) with reason conflicting_duplicate" in out
    assert out.splitlines()[-1] == f"published 0 of 0 dead letter(s) to {TOPIC}"


def test_a_database_error_exits_with_status_1_without_its_message(
    wire: Callable[..., Wired], capsys: pytest.CaptureFixture[str]
) -> None:
    error = OperationalError(
        "SELECT ...", {"key": STORE_ID}, psycopg.OperationalError(PAYLOAD_MARKER)
    )
    wired = wire(error)

    status = replay_main.main(["--execute"])

    captured = capsys.readouterr()
    assert status == replay_main.EXIT_NOT_PUBLISHED
    assert captured.err == "reading dead letters failed: OperationalError OperationalError\n"
    assert wired.published == []
    assert PAYLOAD_MARKER not in captured.out + captured.err


def test_a_pool_timeout_exits_with_status_1_in_one_line(
    wire: Callable[..., Wired], capsys: pytest.CaptureFixture[str]
) -> None:
    wire(PoolTimeoutError("QueuePool limit reached"))

    status = replay_main.main([])

    assert status == replay_main.EXIT_NOT_PUBLISHED
    assert capsys.readouterr().err == "reading dead letters failed: TimeoutError\n"


def test_times_are_shown_in_utc_whatever_the_session_offset(
    wire: Callable[..., Wired], capsys: pytest.CaptureFixture[str]
) -> None:
    letter = stored(1)
    shifted = StoredDeadLetter(
        letter=letter.letter, dead_lettered_at=STORED_AT.astimezone(timezone(timedelta(hours=2)))
    )
    wire(ReplayCandidates(letters=(shifted,), superseded=0, refused=0))

    replay_main.main([])

    assert capsys.readouterr().out.splitlines()[1].endswith("dead-lettered 2026-11-25T10:00:00Z")


def test_the_output_never_shows_a_key_or_a_value(
    wire: Callable[..., Wired],
    capsys: pytest.CaptureFixture[str],
    caplog: pytest.LogCaptureFixture,
) -> None:
    letters = (stored(1), _without_event_id(stored(2, reason=DeadLetterReason.INVALID_EVENT)))
    wire(
        ReplayCandidates(letters=letters, superseded=1, refused=1),
        [Delivered(4, 40), NotDelivered(PublishFailure.REJECTED, "MSG_SIZE_TOO_LARGE")],
    )

    replay_main.main([])
    replay_main.main(["--execute"])

    captured = capsys.readouterr()
    shown = captured.out + captured.err
    assert "no event id" in shown
    # pytest owns the root logger, so log records are checked separately.
    shown += caplog.text
    assert STORE_ID not in shown
    assert PAYLOAD_MARKER not in shown


def _without_event_id(letter: StoredDeadLetter) -> StoredDeadLetter:
    return StoredDeadLetter(
        letter=DeadLetter(**{**_fields(letter.letter), "event_id": None}),
        dead_lettered_at=letter.dead_lettered_at,
    )
