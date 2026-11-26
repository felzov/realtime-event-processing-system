"""`python -m event_pipeline.replay` (`make replay`): publish dead letters again (ADR 0006, d7).

Without `--execute` it only lists what it would publish. With it, it publishes each selected dead
letter's key, value and original `received_at` header to EVENTS_TOPIC and waits for every delivery
report. The consumer then handles each copy like any other message: an event that is still late
is dead-lettered again, one already counted is a duplicate. Repeating a replay is safe.

It reads only dead letters read from EVENTS_TOPIC, refuses `conflicting_duplicate`, and of the
dead letters of one message (same event id, key and value) publishes only the newest. It prints
positions, reasons, event ids and counts, never a key or a value.

Exit status: 0 when everything selected was published (or listed, in a dry run, or nothing was
selected); 1 when a message was not confirmed by the broker or the database failed; 2 for invalid
arguments.
"""

import argparse
import logging
import sys
from collections.abc import Sequence
from datetime import UTC, datetime
from uuid import UUID

from confluent_kafka import Producer
from sqlalchemy.exc import DBAPIError
from sqlalchemy.exc import TimeoutError as PoolTimeoutError

from event_pipeline.broker.config import producer_config
from event_pipeline.broker.publisher import Delivered, Delivery, log_client_error
from event_pipeline.config import Settings
from event_pipeline.data.engine import make_engine
from event_pipeline.data.errors import describe_database_error
from event_pipeline.domain.dead_letters import (
    NOT_REPLAYABLE,
    DeadLetterReason,
    ReplayCandidates,
    ReplaySelection,
    StoredDeadLetter,
)
from event_pipeline.services.replay import find_dead_letters_to_replay, publish_dead_letters

EXIT_NOT_PUBLISHED = 1
EXIT_USAGE = 2


def parse_instant(text: str) -> datetime:
    """An RFC 3339 time with a UTC offset, for example `2026-11-25T10:00:00Z`."""
    try:
        instant = datetime.fromisoformat(text)
    except ValueError:
        raise argparse.ArgumentTypeError(f"not an RFC 3339 time: {text!r}") from None
    if instant.utcoffset() is None:
        raise argparse.ArgumentTypeError(f"needs a UTC offset, for example Z: {text!r}")
    return instant


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m event_pipeline.replay",
        description="List dead letters, or publish them to the events topic again with --execute.",
    )
    parser.add_argument(
        "--reason",
        action="append",
        default=[],
        choices=[reason.value for reason in DeadLetterReason],
        help="only dead letters with this reason (repeatable); default: every replayable reason",
    )
    parser.add_argument("--event-id", type=UUID, help="only dead letters of this event")
    parser.add_argument(
        "--since", type=parse_instant, help="only dead letters stored at or after this time"
    )
    parser.add_argument("--until", type=parse_instant, help="only dead letters stored before it")
    parser.add_argument(
        "--execute", action="store_true", help="publish them; without it, only list them"
    )
    return parser


def parse_selection(
    parser: argparse.ArgumentParser, argv: Sequence[str] | None
) -> tuple[ReplaySelection, bool]:
    """The selection and whether to publish; exits with status 2 on invalid arguments."""
    args = parser.parse_args(argv)
    try:
        selection = ReplaySelection(
            reasons=frozenset(DeadLetterReason(reason) for reason in args.reason),
            event_id=args.event_id,
            since=args.since,
            until=args.until,
        )
    except ValueError as exc:
        parser.error(str(exc))
    return selection, args.execute


def describe(stored: StoredDeadLetter) -> str:
    """One dead letter, without its key or value."""
    letter = stored.letter
    event = "no event id" if letter.event_id is None else f"event {letter.event_id}"
    stored_at = stored.dead_lettered_at.astimezone(UTC).isoformat().replace("+00:00", "Z")
    return (
        f"partition {letter.partition} offset {letter.offset} {letter.reason.value}, {event}, "
        f"dead-lettered {stored_at}"
    )


def describe_outcome(outcome: Delivery) -> str:
    if isinstance(outcome, Delivered):
        return f"published at partition {outcome.partition} offset {outcome.offset}"
    return f"NOT published: {outcome.failure.value} ({outcome.cause})"


def report_left_out(candidates: ReplayCandidates) -> None:
    if candidates.superseded:
        print(
            f"left out: {candidates.superseded} older dead letter(s) of the same messages "
            "(only the newest of each is published)"
        )
    if candidates.refused:
        refused = ", ".join(sorted(NOT_REPLAYABLE))
        print(f"refused: {candidates.refused} dead letter(s) with reason {refused}")


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    selection, execute = parse_selection(parser, argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    settings = Settings()
    topic = settings.events_topic

    engine = make_engine(settings)
    try:
        candidates = find_dead_letters_to_replay(engine, topic, selection)
    except (DBAPIError, PoolTimeoutError) as exc:
        # A database error's message can quote stored values; the class and SQLSTATE say enough.
        # The pool's timeout is not a DBAPIError and has no SQLSTATE.
        cause = describe_database_error(exc) if isinstance(exc, DBAPIError) else type(exc).__name__
        print(f"reading dead letters failed: {cause}", file=sys.stderr)
        return EXIT_NOT_PUBLISHED
    finally:
        engine.dispose()

    letters = candidates.letters
    if not execute:
        print(f"{len(letters)} dead letter(s) from {topic} would be published:")
        for stored in letters:
            print(f"  {describe(stored)}")
        report_left_out(candidates)
        print("dry run: nothing published; add --execute to publish them")
        return 0

    producer = Producer({**producer_config(settings), "error_cb": log_client_error})
    outcomes = publish_dead_letters(producer, topic, letters, settings.publish_backstop_s)
    for stored, outcome in zip(letters, outcomes, strict=True):
        print(f"  {describe(stored)}: {describe_outcome(outcome)}")
    report_left_out(candidates)
    published = sum(isinstance(outcome, Delivered) for outcome in outcomes)
    print(f"published {published} of {len(letters)} dead letter(s) to {topic}")
    if published < len(letters):
        print(
            "some were not confirmed; running the same replay again is safe, the consumer drops "
            "the copies it has already counted",
            file=sys.stderr,
        )
        return EXIT_NOT_PUBLISHED
    return 0


if __name__ == "__main__":
    sys.exit(main())
