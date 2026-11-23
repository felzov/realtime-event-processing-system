# ADR 0004 — Offsets and database consistency

- **Status:** Accepted
- **Date:** 2026-11-21

## Context

The consumer reads `orders.placed.v1` and adds each event to a per-store, per-minute aggregate in
PostgreSQL (ADR 0001, decision 1). Two systems hold state: Kafka holds the consumer group's
committed offset ("where to start reading after a restart"), PostgreSQL holds the aggregates. They
cannot be committed atomically together, and the pipeline promises that a duplicate `event_id`
never changes an aggregate twice (architecture invariants).

A message can reach the consumer more than once for reasons this ADR cannot prevent:

- **A client retry.** `POST /events` answers `503` when it cannot confirm a write, and the client
  retries with the same `event_id` (ADR 0003, decision 8). If the first write did reach the broker,
  the topic now holds the event twice, **at two different offsets**.
- **A producer retry.** The idempotent producer (ADR 0002) removes duplicates caused by its own
  retries within one producer session, but not across an API restart.
- **A consumer crash or rebalance** between applying an event and committing its offset: Kafka
  redelivers from the last committed offset (crash point B in `docs/architecture.md`).

ADR 0002 fixed the client baseline (`enable.auto.commit=false`, commit "offset + 1", synchronous
consumer and synchronous SQLAlchemy) and left the offset policy to this ADR.

## Decision

1. **The offset is committed to Kafka only after the database transaction holding the event's
   effect has committed.** A crash before the offset commit means redelivery, never loss.
2. **Deduplication by `event_id` in the same transaction as the aggregate change.** Table
   `processed_events (event_id uuid PRIMARY KEY, processed_at timestamptz)`. The apply step is:
   ```sql
   BEGIN;
   INSERT INTO processed_events (event_id) VALUES (:event_id)
     ON CONFLICT (event_id) DO NOTHING RETURNING event_id;
   -- a row returned: first time, so upsert the aggregate; no row: duplicate, change nothing
   INSERT INTO order_aggregates (...) VALUES (..., 1, :amount_minor)
     ON CONFLICT (store_id, currency, window_start) DO UPDATE SET
       order_count = order_aggregates.order_count + 1,
       revenue_minor = order_aggregates.revenue_minor + EXCLUDED.revenue_minor;
   COMMIT;
   ```
   The primary key decides which delivery applies the event, not application code: if two
   transactions insert the same `event_id` at once, the second waits for the first and then gets the
   conflict. This holds at PostgreSQL's default isolation level, `READ COMMITTED`; phase 3 tests it
   with two concurrent transactions.
3. **One synchronous offset commit per message**, after its transaction (`commit(message,
   asynchronous=False)`, which stores offset + 1). A failed commit stops the consumer without
   further processing (decision 6).
4. **`auto.offset.reset=earliest`.** A group with no committed offset starts at the beginning of
   each partition, so events produced before the consumer first started are counted. A new group
   name therefore re-reads the whole topic; the duplicates table makes that a no-op for events
   already applied.
5. **The partition assignment strategy stays librdkafka's default** in phase 3, and offsets are
   committed directly with `commit(message)`, not through `enable.auto.offset.store=false` and
   `store_offsets`. With one commit per message, a rebalance can at most cause redelivery, which
   decision 2 absorbs. The two provisional settings of ADR 0002, decision 5 (`store_offsets` and
   `cooperative-sticky`), and committing in the partition-revoke callback stay open for phase 5,
   which records its choice as a dated update here.
6. **Failures stop the consumer.** An error from the database, from the offset commit, or an
   unexpected consumer error ends the loop without committing the offset; the process exits with a
   non-zero status, and a restart redelivers. Retrying in place, and telling a transient database
   error from an event that will never apply, is phase 6 (ADR 0006).
7. **A message that is not a valid event** (`parse_event` rejects it) is logged at `WARNING` with
   its reason codes, partition and offset, never its content, and its offset is committed. This is
   an interim rule until the dead-letter path (ADR 0006): it keeps one bad message from blocking its
   partition, but the message survives only as that log line.
8. **`processed_events` is kept indefinitely** in phase 3. Its size grows with the number of
   events; retention, and what it means for a replay older than the retention, is decided with the
   replay procedure (ADR 0006).

## Consequences

- The guarantee is **at-least-once delivery with an idempotent effect**: every acknowledged event
  is applied exactly once to its aggregate, as long as the consumer eventually runs and the database
  keeps `processed_events`. It is not "exactly-once processing": an event can be read, and its
  transaction attempted, several times.
- Client retries, producer retries after an API restart, consumer crashes, rebalances and a replay
  under a new group name are all handled by the same mechanism.
- Each event costs one database transaction and one synchronous round trip to the broker. Throughput
  of one consumer is bounded by those round trips; no figure is claimed (phase 8 measures).
- An event that can never be applied (for example a `bigint` overflow of `revenue_minor`) makes the
  consumer exit, restart and fail again on the same message, so its partition stops. This is the
  known gap until phase 6.
- Every consumer, current or future, must use the same apply transaction. A second write path to
  `order_aggregates` that skips `processed_events` would break decision 2.
- Same `event_id` with a different body: the second one is treated as a duplicate and ignored.
  Whether that should be detected is phase 5, as a dated update here.

## Alternatives considered

- **Offsets stored in PostgreSQL**, in the same transaction as the aggregate change, with the
  consumer seeking to them when it is assigned a partition. Atomic, and it removes redelivery after
  a crash. Rejected as the only mechanism because it does not catch a client retry: the copy has a
  new offset and the same `event_id`, so the aggregate would count it twice. Deduplication by
  `event_id` is needed anyway, and once it exists, storing offsets in PostgreSQL adds an assignment
  callback that seeks partitions but no further correctness.
- **Commit the offset first, then apply.** At-most-once: a crash between the two loses the event.
  Rejected by the invariants.
- **Auto-commit** (`enable.auto.commit=true`). Commits on a timer, independently of the database,
  so it can commit an offset whose transaction failed. Rejected by the invariants.
- **Kafka transactions** (exactly-once semantics). They make writes to Kafka topics and offset
  commits atomic together; they do not cover a PostgreSQL transaction.
- **Batching**: one transaction and one offset commit per batch of messages. Fewer round trips, at
  the cost of a larger redelivery after a crash and more code. Not done without a measurement that
  shows the per-message cost matters (phase 8).
- **Deduplicating with a hash of the body** instead of `event_id`. Two distinct orders with equal
  bodies would merge; `event_id` is the contract's identity (ADR 0001).
