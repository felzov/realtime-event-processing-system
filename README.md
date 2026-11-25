# realtime-event-processing-system

A small real-time event pipeline: an ingest API writes versioned `order.placed` events to a broker
(Apache Kafka, [ADR 0002](docs/adr/0002-broker-and-client-stack.md)), a consumer group validates
and deduplicates them, and per-store, per-minute order counts and revenue are kept in PostgreSQL
behind a query API.

The goal is explicit, tested failure behaviour (duplicates, consumer restarts, offsets, late and
poison events), not feature breadth.

## Status

Phases 1 (event contract), 2 (ingest), 3 (consumer and durable aggregate), 4 (late events) and 5
(duplicates, restarts, rebalances) are built:

- `POST /events` validates an event and answers `202` only after Kafka acknowledged it with
  `acks=all` ([ADR 0003](docs/adr/0003-ingest-acknowledgement.md)).
- The consumer applies each event to its per-minute aggregate in PostgreSQL, deduplicating by
  `event_id` in the same transaction, and commits the offset only after that transaction
  ([ADR 0004](docs/adr/0004-offsets-and-database.md)).
- An event counts only if it reached the API within `ALLOWED_LATENESS_S` (5 min) of its
  `occurred_at`; the consumer skips later ones, and the API rejects events from the future
  ([ADR 0005](docs/adr/0005-late-events.md)).
- A restarted consumer resumes at its committed offset, and consumers can join or leave the group
  while events arrive without stopping each other. An `event_id` reused for different content is
  logged as `conflicting_duplicate` and not counted (ADR 0004, phase 5 update).
- `GET /stores/{store_id}/aggregates` reads the aggregates from PostgreSQL.

The dead-letter path and failure tests come in later phases; the
[component table](docs/architecture.md#components) has the build order. This is a learning project,
not a production service.

- [Architecture](docs/architecture.md): target design, data flow, crash points, build order
- [Architecture decision records](docs/adr/README.md)

## Development

Needs [uv](https://docs.astral.sh/uv/) (it installs Python 3.12 from `.python-version`).

```sh
make install                   # uv sync
uv run pre-commit install      # optional: lint, typecheck and secret scan on each commit
make check                     # lint + typecheck + unit tests, as CI runs them
make format                    # apply ruff formatting and fixes
```

Integration tests need Docker:

```sh
make up                        # Kafka 4.3.2 on 127.0.0.1:9092 and PostgreSQL 19 on 127.0.0.1:5432
make topics                    # declare orders.placed.v1 (6 partitions); safe to rerun
make db-migrate                # migrate event_pipeline to the latest revision; safe to rerun
make test-integration          # against those services: own test.* topics, event_pipeline_test DB
make down                      # stop; the data volumes are kept
```

PostgreSQL has two databases: `event_pipeline` and `event_pipeline_test`, which integration tests
use exclusively (they refuse any database whose name does not end in `_test`). The test database
is created only when the data volume is new (`docker/postgres-init/`). The password in
`compose.yaml` is a fixed local-development value; settings and their defaults are listed in
`.env.example`.

The schema comes from hand-written Alembic migrations in `migrations/`, each with a working
downgrade. `make db-migrate` applies them to the database in `DATABASE_URL`; the integration tests
migrate the test database themselves, down to an empty schema and back up. A new migration starts
from `make db-revision ID=0004 NAME="what it does"`, and `EXPECTED_REVISION` in
`src/event_pipeline/data/tables.py` must name the new head (a unit test checks it).

After pulling a change that adds a migration, run `make db-migrate` before starting the API or the
consumer. Until then `/ready` answers `migration_mismatch`, and the consumer stops on the first
event it applies (late, future and invalid messages are still skipped before that); nothing is
lost, and that event is redelivered when the consumer is restarted after the migration. When
deploying, migrate first, then replace the code: `/ready` checks for exactly the revision the code
expects, so API processes still running the old code answer `migration_mismatch` until they are
replaced, while their ingest and query paths, and an old consumer, keep working.

Integration tests create their own `test.<id>` topics and do not delete them; they stay in the
Kafka data volume until it is removed.

The broker refuses to create topics on first use, so `make topics` must run before anything
writes to `orders.placed.v1`.

## Running the pipeline

```sh
make up && make topics && make db-migrate
make run                       # the API: uvicorn on 127.0.0.1:8000; Ctrl+C stops it
make consumer                  # in a second terminal: the aggregating consumer; Ctrl+C stops it
curl -s 127.0.0.1:8000/ready   # {"status":"ready"} once Kafka, the topic and the database answer
```

Send an event that happened now, then read its window back (the consumer applies it within about
a second). The commands use GNU `date`; on macOS write `date -u -v-1H` and `date -u -v+1H`.

```sh
NOW=$(date -u +%Y-%m-%dT%H:%M:%SZ)
EVENT_ID=$(python3 -c 'import uuid; print(uuid.uuid4())')
curl -s -X POST 127.0.0.1:8000/events -H 'content-type: application/json' -d '{
  "event_id": "'"$EVENT_ID"'", "event_type": "order.placed", "schema_version": 1,
  "occurred_at": "'"$NOW"'", "source": "web-shop.checkout",
  "payload": {"order_id": "ord-1001", "store_id": "store-7", "amount_minor": 2599, "currency": "EUR"}}'
```

Wait a second for the consumer, then:

```sh
FROM=$(date -u -d '1 hour ago' +%Y-%m-%dT%H:%M:%SZ)
TO=$(date -u -d '1 hour' +%Y-%m-%dT%H:%M:%SZ)
curl -s "127.0.0.1:8000/stores/store-7/aggregates?from=$FROM&to=$TO"
# [{"window_start":"<this minute>","currency":"EUR","order_count":1,"revenue_minor":2599}]
```

Sending the same event again (same `EVENT_ID`) changes nothing: the consumer recognises its
`event_id`. Sending another event with the same `EVENT_ID` (a different amount, say) changes
nothing either, and the consumer logs `conflicting_duplicate`. An `occurred_at` more than
`ALLOWED_LATENESS_S` in the past is accepted but not counted, and the consumer logs `late_event`;
one more than `MAX_FUTURE_SKEW_S` (60 s) ahead is rejected with `422`. Timestamps in a query
string are RFC 3339; write a `+` offset as `%2B` (a bare `+` means a space in a URL) or use `Z`.

| Endpoint | Answer |
|----------|--------|
| `POST /events` | `202 {"event_id"}` after the broker acknowledged, late events included; `422` invalid event, or `out_of_range` at `occurred_at` for an event more than `MAX_FUTURE_SKEW_S` ahead of the server's clock; `413` body over 16 KiB, `503` not confirmed (retry with the same `event_id`), `500` otherwise |
| `GET /stores/{store_id}/aggregates?from=&to=[&currency=]` | `200` with the store's rows whose `window_start` is in `[from, to)`, sorted by window and currency, `[]` for an unknown store; `422 invalid_request` for a missing, invalid, repeated or unknown parameter or a range over `QUERY_MAX_RANGE_S` (24 h); `503 database_unavailable` if PostgreSQL does not answer in time |
| `GET /health` | `200` whenever the process serves HTTP; checks nothing else |
| `GET /ready` | `200` if the producer's poll thread is alive, the broker answers, the topic exists, and the database answers at the migration revision the code expects; `503 not_ready` otherwise. Each check is bounded while the other side responds: the broker by `READY_TIMEOUT_S` (2 s), the database by its own connect and statement timeouts derived from it, so a probe takes up to about 6 s with the defaults |

Settings come from environment variables or a local `.env` file (`src/event_pipeline/config.py`,
`.env.example`). The API starts even when Kafka or PostgreSQL is down; `/ready` reports it. On
shutdown it waits up to `PUBLISHER_CLOSE_TIMEOUT_S` (5 s) for delivery reports still outstanding.

The consumer process (`uv run python -m event_pipeline.consumer`, what `make consumer` runs) exits
`0` after SIGINT or SIGTERM with every applied event's offset committed, `1` after a failure (logged
once, without the error's message; the failed event is redelivered on restart), and `2` if stopping
takes longer than `CONSUMER_CLOSE_TIMEOUT_S` (10 s). A supervisor should run that command, not
`make`: `make` reports its own status when it receives a signal.

### Limitations

- No authentication or rate limiting, and the broker and database connections are plaintext:
  local use only.
- One API process holds one producer; running several is untested. Two consumers in one group
  have been tested (one joining and one leaving while events arrive); larger groups have not.
- `/ready` does not detect a fatal producer error (ADR 0003, limitations), and an outage logs one
  warning per probe. A database outage takes ingest out of rotation too: one process serves both.
- The database's timeouts are enforced by PostgreSQL: a server that stops answering after
  accepting a connection (or a network that drops packets) holds a `/ready` probe or a query until
  the operating system gives up on the socket, minutes rather than seconds. There is no client-side
  socket timeout yet.
- The app's own `INFO` log lines are not shown under `make run` (no logging configuration yet);
  warnings and errors are. The consumer logs at `INFO`.
- Database errors are logged by class and SQLSTATE only, never by message (it can repeat stored
  values). A refused connection, a wrong password and a missing database therefore all log the
  same line, `OperationalError OperationalError`, and answer `503`: the log does not tell a
  misconfiguration from an outage.
- Late events, events from the future and messages without a valid `received_at` header are
  skipped with a WARNING (reason code, `event_id`, partition, offset): until the dead-letter path
  (phase 6) they exist only in the topic and that log line. `ALLOWED_LATENESS_S` (300 s) and
  `MAX_FUTURE_SKEW_S` (60 s) are assumptions, not measured delays.
- A replay classifies old messages under the limits in force at the time. Into the same database it
  can only add: raising the limits and replaying counts events skipped before, but lowering them
  never removes events already counted, because the aggregates are incremental and nothing
  recomputes a window.
- Nothing reports whether a window is final. By ADR 0005 a window ending at `W_end` no longer
  changes once ingest time passes `W_end + ALLOWED_LATENESS_S` and the consumer has caught up.
- The consumer trusts the `received_at` header: anyone who can write to the topic can set it.
- An event the database rejects permanently (for example, revenue past the 64-bit limit) stops the
  consumer each time it is redelivered, which blocks its partition until the dead-letter path
  (phase 6). An invalid message on the topic is logged and skipped: it is lost apart from the log
  line.
- An offset commit refused because the group's membership changed (a rebalance or an eviction:
  `ILLEGAL_GENERATION`, `UNKNOWN_MEMBER_ID`, `REBALANCE_IN_PROGRESS`) is logged as a WARNING and the
  consumer goes on; the event may be redelivered and is then counted as a duplicate (ADR 0004,
  decision 10). Exceeding `CONSUMER_MAX_POLL_INTERVAL_MS` still stops the consumer (status `1`).
  Such a refusal is not retried: the partition's offset stays behind until its next message is
  committed, or until a restart redelivers from it. A group that keeps rebalancing can keep offsets
  behind for long (the aggregates stay correct, restarts redeliver more), and shows it only as
  repeated WARNINGs.
- Every consumer of a group must use the same assignment strategy (`cooperative-sticky` since
  phase 5): stop consumers started from an older version before starting this one, or it stops
  with `INCONSISTENT_GROUP_PROTOCOL`.
- Stopping the consumer during an event slower than `CONSUMER_CLOSE_TIMEOUT_S` ends with status `2`
  and that event rolled back; it is redelivered on restart.
- `processed_events` grows by one row per event and is never pruned yet.
- An `event_id` reused for different content is only logged (`conflicting_duplicate`, with the
  `event_id`, partition and offset): the first applied copy stays counted and nobody tells the
  sender. "Different" compares the parsed event (`received_at` excluded, `occurred_at` as a UTC
  instant to the microsecond), so a future `schema_version` 2 copy of a version 1 event would be
  reported as a conflict, and so would every earlier event after a change to how the fingerprint is
  computed. Events recorded before migration `0002` (or by code from before it) have no
  fingerprint; a reused `event_id` of one of those is a plain duplicate.
- A query answers at most one row per minute window and currency, 1440 windows per currency for
  the 24 h maximum; the number of currencies is not capped. `revenue_minor` is a JSON integer:
  JavaScript clients lose precision above 2^53.
