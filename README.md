# realtime-event-processing-system

A small real-time event pipeline: an ingest API writes versioned `order.placed` events to a broker
(Apache Kafka, [ADR 0002](docs/adr/0002-broker-and-client-stack.md)), a consumer group validates
and deduplicates them, and per-store, per-minute order counts and revenue are kept in PostgreSQL
behind a query API.

The goal is explicit, tested failure behaviour (duplicates, consumer restarts, offsets, late and
poison events), not feature breadth.

## Status

Phases 1 (event contract), 2 (ingest) and 3 (consumer and durable aggregate) are built:

- `POST /events` validates an event and answers `202` only after Kafka acknowledged it with
  `acks=all` ([ADR 0003](docs/adr/0003-ingest-acknowledgement.md)).
- The consumer applies each event to its per-minute aggregate in PostgreSQL, deduplicating by
  `event_id` in the same transaction, and commits the offset only after that transaction
  ([ADR 0004](docs/adr/0004-offsets-and-database.md)).
- `GET /stores/{store_id}/aggregates` reads the aggregates from PostgreSQL.

Late events, rebalances, the dead-letter path and failure tests come in later phases; the
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
from `make db-revision ID=0002 NAME="what it does"`, and `EXPECTED_REVISION` in
`src/event_pipeline/data/tables.py` must name the new head (a unit test checks it).

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

Send an event, then read its window back (the consumer applies it within about a second):

```sh
curl -s -X POST 127.0.0.1:8000/events -H 'content-type: application/json' -d '{
  "event_id": "6f1c0d2e-8a8b-4f7e-9a59-3c1f4f0b9a10", "event_type": "order.placed",
  "schema_version": 1, "occurred_at": "2026-11-23T10:15:30Z", "source": "web-shop.checkout",
  "payload": {"order_id": "ord-1001", "store_id": "store-7", "amount_minor": 2599, "currency": "EUR"}}'

curl -s '127.0.0.1:8000/stores/store-7/aggregates?from=2026-11-23T10:00:00Z&to=2026-11-23T11:00:00Z'
# [{"window_start":"2026-11-23T10:15:00Z","currency":"EUR","order_count":1,"revenue_minor":2599}]
```

Sending the same event again changes nothing: the consumer recognises its `event_id`. Timestamps in
a query string are RFC 3339; write a `+` offset as `%2B` (a bare `+` means a space in a URL) or use
`Z`.

| Endpoint | Answer |
|----------|--------|
| `POST /events` | `202 {"event_id"}` after the broker acknowledged; `422` invalid event, `413` body over 16 KiB, `503` not confirmed (retry with the same `event_id`), `500` otherwise |
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
- One API process holds one producer; running several is untested. One consumer process has been
  tested; several in one group are untested until phase 5.
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
- Every valid event is applied by its event time, however late; the late-event policy is phase 4.
- An event the database rejects permanently (for example, revenue past the 64-bit limit) stops the
  consumer each time it is redelivered, which blocks its partition until the dead-letter path
  (phase 6). An invalid message on the topic is logged and skipped: it is lost apart from the log
  line.
- A commit that lands during a consumer group rebalance stops the consumer (status `1`; nothing is
  lost or counted twice); committing on partition revoke is phase 5.
- Stopping the consumer during an event slower than `CONSUMER_CLOSE_TIMEOUT_S` ends with status `2`
  and that event rolled back; it is redelivered on restart.
- `processed_events` grows by one row per event and is never pruned yet.
- A query answers at most one row per minute window and currency, 1440 windows per currency for
  the 24 h maximum; the number of currencies is not capped. `revenue_minor` is a JSON integer:
  JavaScript clients lose precision above 2^53.
