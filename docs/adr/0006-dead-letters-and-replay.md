# ADR 0006 — Dead letters and replay

- **Status:** Accepted
- **Date:** 2026-11-25

## Context

The consumer does not count every message it reads. Until this ADR:

- a message that is not a valid event is logged at WARNING and its offset committed (ADR 0004,
  decision 7); a late or future event, or one without a valid `received_at` header, the same way
  (ADR 0005, decision 4); a conflicting duplicate is logged and committed (ADR 0004, decision 13).
  The message then survives only in the topic, until retention removes it, and in one log line;
- an event the database refuses (a `bigint` overflow of `revenue_minor`) stops the consumer, and
  every restart reads it again and stops again: its partition is blocked (ADR 0004, decision 6);
- any other database error, including a short outage, also stops the consumer. Nothing retries.

The project's invariant is that a poison event never blocks a partition forever: it goes to a
dead-letter path with a reason code and the offset moves on. That needs three things: a place to
keep what was not counted, a rule telling an event the database will never accept from a database
that is briefly unavailable, and a way to process a dead letter again once its cause is fixed.

A spike ran first, with throwaway scripts that were not committed: PostgreSQL 19.0, psycopg 3.3.9,
SQLAlchemy 2.1.9, against the project's Compose services. Each error was produced for real and
described with `describe_database_error`; the topic's settings were read with `describe_configs`.

| Case | SQLAlchemy class | psycopg class | SQLSTATE |
|---|---|---|---|
| `bigint` overflow | `DataError` | `NumericValueOutOfRange` | `22003` |
| Text not valid UTF-8 | `DataError` | `CharacterNotInRepertoire` | `22021` |
| CHECK violation | `IntegrityError` | `CheckViolation` | `23514` |
| NOT NULL violation | `IntegrityError` | `NotNullViolation` | `23502` |
| Statement timeout | `OperationalError` | `QueryCanceled` | `57014` |
| Deadlock (raised) | `OperationalError` | `DeadlockDetected` | `40P01` |
| Serialization failure (raised) | `OperationalError` | `SerializationFailure` | `40001` |
| Server shutting down (raised) | `OperationalError` | `AdminShutdown` | `57P01` |
| Missing column | `ProgrammingError` | `UndefinedColumn` | `42703` |
| Missing table | `ProgrammingError` | `UndefinedTable` | `42P01` |
| Connection refused | `OperationalError` | `OperationalError` | none |
| Wrong password | `OperationalError` | `OperationalError` | none |
| Missing database | `OperationalError` | `OperationalError` | none |

"Raised" rows were produced with `RAISE ... USING ERRCODE`, not by a real deadlock or concurrent
update: they show the classes the driver maps those codes to. Errors without a connection carry no
SQLSTATE at all. The topic `orders.placed.v1` has `cleanup.policy=delete`, `retention.ms=604800000`
(7 days) and `max.message.bytes=1048588`, all broker defaults: the project sets none of them.

## Decision

1. **Dead letters are rows in a PostgreSQL table, `dead_letters`** (migration 0003), not messages
   on a dead-letter topic. Each row holds the message as it was read (key and value as raw bytes,
   the raw `received_at` header), where it was read (topic, partition, offset), a reason code, a
   detail, the `event_id` when the value parsed, and when it was written.
2. **A dead letter is identified by its position** (topic, partition, offset; the primary key). It
   is written in its own transaction with `ON CONFLICT DO NOTHING`, and the message's offset is
   committed only after that transaction commits, exactly as for an applied event (ADR 0004,
   decisions 1 and 3). A crash in between redelivers the message; its second write is a conflict
   and the first row stays. A redelivery is therefore never a second dead letter.
3. **Every message that is not counted is dead-lettered**, with one of these reason codes:
   `invalid_event` (the violation codes go in the detail), `invalid_received_at`, `late_event`,
   `future_event` (the delay goes in the detail), `conflicting_duplicate`, and
   `database_rejected` (decision 4). The WARNING log line stays. The detail is text this project
   writes: codes, a delay, a database error's classes and SQLSTATE, never a value from the
   message. Only the `received_at` header is kept: it is the only header the ingest API sets.
4. **A database error is permanent for an event only if its SQLSTATE class is `22` (data
   exception) or `23` (integrity constraint violation).** Those classes describe the values of a
   statement, so running it again fails the same way: the event is dead-lettered as
   `database_rejected`, with the error's classes, SQLSTATE and constraint name as detail, and its
   offset moves on. The apply transaction was rolled back, so neither aggregate table changed.
5. **Every other database error is transient**: an error without a SQLSTATE (no connection),
   classes `08`, `40`, `53`, `57`, and also `42` (a missing column or table), as well as the
   connection pool's `TimeoutError`. It is retried in place, up to `DB_APPLY_ATTEMPTS` attempts
   (default 3) with a growing pause between them (0.5 s, then 1 s, at most 2 s), waiting on the
   stop flag so a stop request ends the wait. If the last attempt fails, the consumer stops
   without committing the offset, as before. Class `42` is transient on purpose: a schema that
   does not match the code (a migration not run) must stop the consumer, not dead-letter every
   event. The settings validator counts the attempts and pauses in the worst-case time per event
   that `CONSUMER_MAX_POLL_INTERVAL_MS` must cover. Writing a dead letter follows the same rule: a
   transient error is retried, and if it persists the consumer stops before the offset commit.
6. **Any other exception is a bug and stops the consumer**, without a retry or a dead letter. That
   includes a class `22` or `23` error from writing a dead letter itself: the dead-letter table
   accepts any message the broker can deliver, so such an error is in our code. Dead-lettering on
   a bug would move a whole partition into the table during one bad deploy.
7. **Replay goes through the topic.** `python -m event_pipeline.replay` (`make replay`) selects
   dead letters by reason, `event_id` and time range, and publishes each one's key, value and
   original `received_at` header back to the events topic with the same producer settings as the
   API (`acks=all`, idempotent, `murmur2_random`), waiting for every delivery report. It only
   lists what it would publish unless given `--execute`. It refuses `conflicting_duplicate`: the
   first copy is already counted, so a replay would only conflict again. It never writes to the
   aggregate tables, and it prints positions, reasons and counts, never a value.
8. **Nothing is deleted automatically.** `processed_events` and `dead_letters` are kept
   indefinitely (this settles ADR 0004, decision 8). The topic keeps messages for 7 days.

Decisions 2 and 3 are built in phase 6, slice 2; decisions 4 to 6 in slice 3; decision 7 in
slice 4. Until then the consumer behaves as ADR 0004 and ADR 0005 describe.

## Consequences

- **A poison event no longer blocks its partition.** It costs one dead-letter row, and the next
  message on the partition is processed.
- **Replay keeps every guarantee of the normal path.** A replayed message is a new message on the
  topic: the consumer deduplicates it by `event_id`, applies the lateness rule, and orders it within
  its store's partition. Replaying twice is harmless: the second copy is a duplicate. A replayed
  message that still fails becomes a new dead letter at its new offset.
- **The original `received_at` means a replay re-evaluates, it does not backfill.** A late event
  replayed under the same `ALLOWED_LATENESS_S` is late again. Raising the limit and replaying counts
  it (ADR 0005, decision 7); a replay never removes anything already counted.
- **A wrong classification has a visible cost either way.** Too broad would empty a partition into
  `dead_letters` during an environment problem; too narrow keeps blocking a partition. The
  allow-list is narrow, and a class `22` or `23` error that is really a bug in our SQL also becomes
  a dead letter; it is visible there with its SQLSTATE and constraint and can be replayed after the
  fix.
- **Raw input is stored.** `message_value` holds whatever was on the topic, up to the broker's
  `max.message.bytes` (about 1 MiB). The contract has no personal data, but an invalid message can
  contain anything. It is never logged or printed; reading it takes a database query.
- **Dead letters and `processed_events` grow without bound.** Deleting old dead letters is a manual
  `DELETE`. Deduplication holds for as long as `processed_events` keeps its rows, which is for good.
- **A recreated topic restarts its offsets.** A new message at a position already in
  `dead_letters` would conflict with the old row and not be recorded. Recreating the topic is not a
  supported operation (`TopicMismatchError` already refuses a changed one).
- **Headers other than `received_at` are dropped**, on dead-lettering and therefore on replay.
- **Rebuilding all aggregates from the topic is possible only within its 7-day retention**, and it
  is a documented manual procedure with destructive steps, not a script.
- **Retrying in place lengthens the worst case per event**, so the minimum
  `CONSUMER_MAX_POLL_INTERVAL_MS` grows with `DB_APPLY_ATTEMPTS`.

## Alternatives considered

- **A dead-letter topic** (`orders.placed.v1.dlq`). The usual Kafka pattern: it keeps the message
  byte for byte with all headers and needs no schema. Rejected: the consumer would need a producer
  and a delivery wait of its own, a redelivery would publish a second copy, the topic's retention
  would have to be set to keep dead letters longer than 7 days, and inspecting it takes Kafka
  tooling instead of a SQL query. The table shares the apply transaction's failure domain: when the
  database is down, the consumer stops anyway.
- **Only logging, as before.** Rejected: the message is lost once the topic's retention passes, and
  a blocked partition stays blocked.
- **Retry topics** (a delayed second topic for transient failures). Rejected: they reorder events
  within a store and add a topic and a consumer; a short retry in place covers a short outage, and
  a long one is better handled by stopping.
- **Every database error is permanent**, or **every one is transient**. The first dead-letters a
  partition's worth of events during one outage or one missed migration; the second is the old
  behaviour, which blocks a partition on a single bad event.
- **No retry, stop on every transient error** and rely on a restart. Simpler, but without a
  supervisor a single dropped connection stops processing until someone restarts the consumer.
- **A fresh `received_at` on replay.** That would let a replay count any late event whatever the
  policy, a backfill under another name. A backfill, if ever needed, should be its own decision.
- **Replay straight into the database**, calling the apply service. Rejected: a second write path
  that skips the topic, the lateness rule and per-store ordering.
