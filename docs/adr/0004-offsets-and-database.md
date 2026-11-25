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

**Update 2026-11-24 (phase 5, slice 1):** decision 5 and the open question on a reused `event_id`
are settled; decisions 3 and 6 gain one exception (decision 10). A spike ran first, with throwaway
scripts that were not committed: `confluent-kafka` 2.16.2 (librdkafka 2.16.0) against the
project's Compose broker (Kafka 4.3.2), classic group protocol, `cooperative-sticky`, a
6-partition topic, two consumers in one group, each committing synchronously per message as
decision 3 does.

| Case | What a synchronous `commit(message=...)` did |
|------|----------------------------------------------|
| A second consumer joins while the first holds a message and has not polled (committed after 0, 1 and 3 s) | The held commit succeeded each time (after 0 and 1 s it landed before the rebalance was visible to the client). In the 3 s run, the next per-message commit after polling resumed **raised** `KafkaException(ILLEGAL_GENERATION)` |
| Committing every 100 ms through a whole rebalance, without consuming | Succeeded 91 times; **raised** `KafkaException(ILLEGAL_GENERATION)` once, between two generations |
| Committing for a partition that was already revoked and given to the other consumer (every 100 ms for 12 s, counted by owner) | **Succeeded**: 30 times while the first consumer owned it, 59 times while the other one did, once between the two steps when no member owned it; 1 `ILLEGAL_GENERATION`. The classic protocol does not check which member owns a partition on commit |
| After exceeding `max.poll.interval.ms` (consumer evicted), alone or with a second member | **Raised** `KafkaException(UNKNOWN_MEMBER_ID)`, not fatal, not retriable. The next `poll` returned an error message `_MAX_POLL_EXCEEDED` (not fatal), `on_lost` was called, and the poll after that rejoined the group |

`REBALANCE_IN_PROGRESS` did not occur in the spike; it did later, in real runs of the two-consumer
test of slice 3 (`tests/integration/test_rebalance.py`), next to `ILLEGAL_GENERATION`. With
callbacks that only log, `confluent-kafka` applied
each assignment itself, after the callback returned. Each rebalance took two steps: the moving
partitions were revoked first, then assigned to the other consumer in a second rebalance a few
seconds later.

9. **The assignment strategy is `cooperative-sticky`** (classic group protocol), set in
   `consumer_config` next to the other fixed settings, not as a setting. A rebalance only stops the
   partitions that move; the others keep being consumed. All consumers of one group must use it,
   so the local consumer must be stopped before the switch: in the spike, a member with the new
   strategy joining a group whose member used librdkafka's default got no partitions, and its
   `poll` returned `INCONSISTENT_GROUP_PROTOCOL`, which stops the loop (decision 6). Offsets stay
   committed with `commit(message)` per message: no `store_offsets`.
10. **A commit that fails because group membership changed is not fatal.** If the synchronous commit
    raises, or returns for the partition, `ILLEGAL_GENERATION`, `UNKNOWN_MEMBER_ID` or
    `REBALANCE_IN_PROGRESS`, the loop logs a WARNING with the partition, offset and `event_id` and
    carries on. The first two were seen in the spike; `REBALANCE_IN_PROGRESS` was not, but the Kafka
    protocol defines it for the same situation, and listing it costs at most a redelivery. The
    event's effect is already committed in PostgreSQL. If the partition stays with this consumer,
    its next successful commit on the partition covers the event; if the partition moves, or the
    process stops first, the event may be redelivered from the last committed offset, and decision 2
    makes that a duplicate. Every other commit error still stops the consumer (decision 6).
11. **The rebalance callbacks only log**: assignments and revocations at INFO, lost partitions
    (`on_lost`, after an eviction) at WARNING. Nothing is committed on revoke. Partitions are
    revoked only inside `poll`, and the loop never polls between an event's database commit and
    its offset commit, so at a revoke every event this consumer applied already has its offset
    committed, except one whose commit failed under decision 10, which is redelivered anyway.
12. **`_MAX_POLL_EXCEEDED` still stops the consumer** (decision 6 unchanged). It means the time
    between two polls exceeded `max.poll.interval.ms`, which `config.py` requires to be at least
    twice the worst-case database time of one event (the offset commit is outside that bound): the
    bound was broken, which is unexpected. Decision 10 only lets the failed commit before it
    through; the error message on the next `poll` then stops the loop, and a restart rejoins the
    group.
13. **The same `event_id` with a different body is detected, not counted.** `processed_events`
    stores a SHA-256 of the parsed event as canonical JSON (instants normalised to UTC;
    `received_at` is a header and not part of it). On a conflict the stored hash is compared: a
    different hash is logged at WARNING as `conflicting_duplicate` with the `event_id`, partition
    and offset, never values; the first applied copy stays counted, and the offset is committed as
    for any duplicate. "First applied": a copy skipped as late, future or invalid is not recorded,
    and across partitions "first" is the first to commit. Rows stored before the column existed
    have no hash and are treated as plain duplicates. The hash covers the parsed `schema_version` 1
    fields, so a future version 2 copy of the same event would hash differently and be reported as
    a conflict. Phase 5, slice 4 builds it, with a migration.

Why a stale or failed commit cannot lose or double-count an event: a consumer commits an offset
only after it has handled that message itself (applied it, or skipped it under decision 7 or
ADR 0005), and it reads a partition in order, so every earlier message of that partition was
handled too. So whatever position a late commit, or one that lands after the partition moved,
leaves behind, every message before that position has been handled. If the position is behind
what the new owner has reached, messages are redelivered, which decision 2 absorbs; no message
is ever skipped. Ownership of a partition is therefore not what keeps the aggregate correct; the
`event_id` primary key is.

Consequence of decision 10: a refused commit is not retried. The partition's committed offset
stays behind until the next message on it is committed, or until a restart or a new owner
redelivers from it. A group that keeps rebalancing can keep offsets behind for a long time; the
aggregates stay correct, a restart redelivers more, and it shows only as repeated WARNINGs. An alert
after a number of refusals in a row was considered and left out until there is a need for it.

Alternatives for this update: the default eager strategy (every rebalance stops every partition;
correct, but it pauses all partitions for every join and leave); the new consumer group protocol
(KIP-848, `group.protocol=consumer`), not evaluated in either spike; `store_offsets` with a
commit in `on_revoke`, useful only with batched commits, which this project does not do; and
keeping every commit failure fatal, which would crash a consumer each time another one joins or
leaves.

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
  From phase 5 it is also detected and logged (decision 13).

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
