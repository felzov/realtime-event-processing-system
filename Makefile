.RECIPEPREFIX = >

.PHONY: install lint format typecheck test test-integration check check-all up down topics run \
	db-migrate db-revision consumer

# The broker from `make up`. Override for another broker; a direct `pytest -m integration` run
# without it fails instead of skipping.
TEST_KAFKA_BOOTSTRAP_SERVERS ?= 127.0.0.1:9092
export TEST_KAFKA_BOOTSTRAP_SERVERS
# The test database from `make up` (created by docker/postgres-init). Test code refuses a database
# whose name does not end in `_test`. The password is compose.yaml's local development value.
TEST_DATABASE_URL ?= postgresql+psycopg://event_pipeline:event_pipeline_local@127.0.0.1:5432/event_pipeline_test
export TEST_DATABASE_URL

install:
> uv sync

lint:
> uv run --locked ruff check .
> uv run --locked ruff format --check .

format:
> uv run --locked ruff format .
> uv run --locked ruff check --fix .

typecheck:
> uv run --locked mypy

# Unit tests; no database or broker needed.
test:
> uv run --locked pytest -m "not integration"

# Needs `make up`. Tests create their own uniquely named topics and use only the `_test` database.
test-integration:
> uv run --locked pytest -m integration

# What CI runs on each Python version (CI also scans git history for secrets).
check: lint typecheck test

check-all: check test-integration

# Starts Kafka and PostgreSQL and waits until both are healthy.
up:
> docker compose up -d --wait

# Stops the containers and keeps the data volume. Deleting it (`down -v`) is a manual decision.
down:
> docker compose down

# Creates orders.placed.v1, or checks that the existing topic matches its declaration.
topics:
> uv run --locked python -m event_pipeline.broker.topics

# The ingest API on 127.0.0.1:8000. It starts without a broker; /ready answers 200 once Kafka is up
# (`make up`) and the topic exists (`make topics`).
run:
> uv run --locked uvicorn event_pipeline.main:create_app_from_settings --factory --host 127.0.0.1 --port 8000

# The aggregating consumer: reads orders.placed.v1 and applies each event to PostgreSQL. Needs
# `make up`, `make topics` and `make db-migrate`. Ctrl+C (or SIGTERM) stops it after the current
# event.
consumer:
> uv run --locked python -m event_pipeline.consumer

# Migrates the database in DATABASE_URL (default: the local development database) to the latest
# revision.
db-migrate:
> uv run --locked alembic upgrade head

# A new, empty migration to write by hand, e.g. `make db-revision ID=0004 NAME="add an index"`.
# Then update EXPECTED_REVISION in src/event_pipeline/data/tables.py.
db-revision:
> @test -n "$(ID)" && test -n "$(NAME)" || { echo 'usage: make db-revision ID=0004 NAME="what it does"' >&2; exit 2; }
> uv run --locked alembic revision --rev-id "$(ID)" -m "$(NAME)"
