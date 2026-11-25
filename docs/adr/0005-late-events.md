# ADR 0005 — Late and future events

- **Status:** Accepted
- **Date:** 2026-11-23

## Context

Each `order.placed` event is counted in the 1-minute window that contains its `occurred_at`, the
time the order happened (ADR 0001, decision 1). Events do not arrive in that order. A store's
checkout may queue events while offline and send them later, a client may retry for minutes, and
producers' clocks drift. Without a rule, an event that occurred yesterday changes yesterday's
window whenever it shows up, so no window is ever final, and an event with a wrong clock can
create windows in the future.

Two instants travel with every message on `orders.placed.v1`:

- `occurred_at`, in the body, chosen by the producer (the client);
- `received_at`, in a Kafka header, set by the ingest API when it accepts the event; clients
  cannot send it (ADR 0001, decision 4; ADR 0003, decision 4).

Any rule must keep the guarantees of ADR 0004. A message is processed at least once: it is
redelivered after a crash, after a rebalance, and on a replay. If the same message were counted on
one delivery and skipped on another, the aggregate would depend on when the consumer happened to
crash.

Nothing measured says how late real events arrive. The limits below are configuration, and their
defaults are assumptions.

## Decision

1. **Lateness is the delay to ingest.** An event is **late** if
   `received_at - occurred_at > ALLOWED_LATENESS_S` (default 300 s). It is **from the future** if
   `occurred_at - received_at > MAX_FUTURE_SKEW_S` (default 60 s). Otherwise it is **on time**.
   Both bounds are inclusive: an event exactly 300 s late is on time. The rule is
   `domain/lateness.py` `classify`.
2. **Only on-time events change an aggregate.** The window is still assigned by `occurred_at`
   alone. `received_at` decides *whether* an event counts, never *which* window it counts in.
3. **The check comes before deduplication.** A late or future event never opens a database
   transaction and is not recorded in `processed_events`. Its offset is committed like any other
   message's, so it never blocks its partition.
4. **Until the dead-letter path (phase 6, ADR 0006),** a late or future event is logged at WARNING
   with its reason code (`late_event`, `future_event`), `event_id`, partition, offset and the delay
   in seconds, never its payload, and skipped. It stays in the topic until retention removes it.
5. **A message without a valid `received_at` header is skipped** with the reason code
   `invalid_received_at`, the same way. Only the exact form the ingest API writes is accepted
   (`2026-11-19T15:36:33.191000Z`); there is no fallback to the Kafka message timestamp.
6. **The ingest API rejects an event from the future** with `422`, reason code `out_of_range` at
   `occurred_at`, and writes nothing: the client's clock is wrong, and a retry would not help. A
   late event is still accepted with `202`. The topic keeps it, so the input log stays complete and
   a later replay can still reach it.
7. **The policy in force when a message is processed decides.** Changing `ALLOWED_LATENESS_S` and
   then replaying the topic classifies the old messages under the new value.

**Update 2026-11-25 (phase 6, slice 1):** decision 4 is replaced by ADR 0006 (built in phase 6,
slice 2; until then decision 4 holds): a late or future event, and a message without a valid
`received_at` header, is written to the `dead_letters` table with its reason code and delay before
its offset is committed, and the WARNING line stays. Decision 7 applies to a replay of dead letters
too: the replay publishes the original `received_at`, so the event is classified again under the
limits in force when it is read.

## Consequences

- **Deterministic.** Both instants are part of the message, so every delivery of one message is
  classified the same way, whatever the consumer's clock, lag or restart history. There is no state
  to store, restore on restart or hand over on a rebalance.
- **Window finality.** An on-time event of a window ending at `W_end` has
  `received_at <= occurred_at + ALLOWED_LATENESS_S < W_end + ALLOWED_LATENESS_S`. So no event
  that reaches ingest at or after `W_end + ALLOWED_LATENESS_S` changes that window. The window is
  final once that time has passed and the consumer has processed everything received before it.
  "Everything received before it" is approximate: `received_at` is set before the event is
  produced, so concurrent requests can land in a partition slightly out of `received_at` order.
  Nothing in the API reports a window as final yet.
- **A replay can only add.** Under decision 7 a replay into the same database counts the events
  that now pass and skips the rest. Raising the limits and replaying therefore counts events that
  were skipped before, but lowering them never removes events already counted: the aggregates are
  incremental, and nothing recomputes a window. Applying a stricter policy to the past would need
  the windows rebuilt from the topic.
- **Our own lag does not count.** A consumer that is stopped for an hour and then catches up still
  counts every event that reached ingest in time. Only the producer-to-ingest delay counts.
- **A late retry is reported as late, not as a duplicate.** A retry gets a new `received_at`. If
  the first copy was counted and a retry arrives after the allowed lateness, the retry is skipped
  as late, which is also correct: it would have been a duplicate.
- **`received_at` is trusted.** Anyone who can write to the topic can set the header, the same
  trust as the message itself (the broker connection is local and unauthenticated).
- **Late events are only in the topic and the logs** until phase 6. They can be neither counted
  nor listed through the API. From phase 6 they are kept in `dead_letters` (ADR 0006).
- **Tests and examples must use the current time.** An event sent through the API with a fixed
  `occurred_at` becomes late as soon as that time is more than 5 minutes ago.

## Alternatives considered

- **A watermark per partition** (late if `occurred_at` is more than the allowed lateness behind the
  largest `occurred_at` seen on the partition). This is the usual stream-processing rule, and it
  measures out-of-order arrival rather than delay. But the watermark is state. Kept in memory, it is
  lost on a crash: a restarted consumer rebuilds it from the redelivered messages, and an event
  skipped as late before the crash can be counted after it, so the result depends on when the
  crash happened. Storing it in PostgreSQL in the apply transaction fixes that at the cost of a
  table and a write per message. And one store with a clock ahead moves the watermark for every
  store on its partition. Rejected for now.
- **The consumer's own clock** (late if `now - occurred_at` exceeds the allowed lateness). Simple,
  but consumer lag counts as lateness: after an outage the whole backlog would be dropped, and a
  replay would drop everything. Rejected.
- **The Kafka message timestamp** instead of a header of our own. With the default `CreateTime`
  it is the producer client's clock, which for this topic is the ingest API's, so it would work
  today. But its meaning is not part of any contract here: a producer that passes its own
  `timestamp=` changes it silently, and `LogAppendTime` would need a topic configuration change.
  `received_at` is already in the contract (ADR 0003, decision 4) and means one thing: when the
  API accepted the event. Rejected.
- **Rejecting late events at ingest** with `422`. The client would learn at once, but the event
  would be lost: retrying cannot make it on time. Accepting it keeps it in the input log for the
  dead-letter path and a possible backfill. Rejected; events from the future are rejected at
  ingest instead (decision 6), because those are a client error.
- **Counting late events anyway** (no policy). No window would ever be final. Rejected by the
  project's invariants.
