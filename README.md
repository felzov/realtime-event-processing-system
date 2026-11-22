# realtime-event-processing-system

A small real-time event pipeline: an ingest API writes versioned `order.placed` events to a broker
(Apache Kafka, [ADR 0002](docs/adr/0002-broker-and-client-stack.md)), a consumer group validates
and deduplicates them, and per-store, per-minute order counts and revenue are kept in PostgreSQL
behind a query API.

The goal is explicit, tested failure behaviour (duplicates, consumer restarts, offsets, late and
poison events), not feature breadth.

## Status

Phases 1 (event contract) and 2 (ingest) are built: `POST /events` validates an event and answers
`202` only after Kafka acknowledged it with `acks=all`
([ADR 0003](docs/adr/0003-ingest-acknowledgement.md)). The consumer, PostgreSQL aggregates and
the query API come in phase 3; the [component table](docs/architecture.md#components) has the
build order. This is a learning project, not a production service.

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

## Running the ingest API

```sh
make up && make topics
make run                       # uvicorn on 127.0.0.1:8000; Ctrl+C stops it
curl -s 127.0.0.1:8000/ready   # {"status":"ready"} once Kafka answers and the topic exists
```

| Endpoint | Answer |
|----------|--------|
| `POST /events` | `202 {"event_id"}` after the broker acknowledged; `422` invalid event, `413` body over 16 KiB, `503` not confirmed (retry with the same `event_id`), `500` otherwise |
| `GET /health` | `200` whenever the process serves HTTP; checks nothing else |
| `GET /ready` | `200` if the producer's poll thread is alive, the broker answers and the topic exists; `503 not_ready` otherwise, within `READY_TIMEOUT_S` (2 s) |

Settings come from environment variables or a local `.env` file (`src/event_pipeline/config.py`).
The API starts even when Kafka is down; `/ready` reports it. On shutdown it waits up to
`PUBLISHER_CLOSE_TIMEOUT_S` (5 s) for delivery reports still outstanding.

### Limitations

- No authentication or rate limiting, and the broker connection is plaintext: local use only.
- One API process holds one producer; running several is untested.
- `/ready` does not detect a fatal producer error (ADR 0003, limitations), and an outage logs one
  warning per probe.
- The app's own `INFO` log lines are not shown under `make run` (no logging configuration yet);
  warnings and errors are.
