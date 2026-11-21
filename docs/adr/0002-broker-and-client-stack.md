# ADR 0002 — Broker and client stack

- **Status:** Accepted
- **Date:** 2026-11-19

## Context

The pipeline needs a broker that keeps a durable, partitioned log, a Python client for the ingest
API and for the consumer, and a way for the consumer to talk to PostgreSQL. Several invariants
depend directly on how the client behaves, not only on its documentation:

- The ingest API may answer `202` only after the broker acknowledged the write with `acks=all`, and
  must answer with an error, not hang, when the broker is unavailable (ADR 0003 builds on this).
- The consumer commits an offset only after the database transaction, so auto-commit must be off
  and a manual commit must store exactly "next offset to read" (ADR 0004).
- On a rebalance, the consumer must be able to commit what it has applied before it loses a
  partition (phase 5).
- Events are partitioned by `store_id` (ADR 0001, decision 5). Every tool that writes to the topic,
  including a replay script or Kafka's own command-line producer, must send a key to the same
  partition.

Kafka was the leaning from the start. The open questions were which Python
client, and whether the code is synchronous or asynchronous. The client was chosen from a spike
against the real broker, not from documentation. Synchronous versus asynchronous is a reasoned
choice (decision 6), not a measured one: the spike only showed that both styles have a bounded
failure on the produce path.

### The spike

Run on 2026-11-18 and 2026-11-19 against one container of `apache/kafka:4.3.2` (single node, KRaft,
the image's default configuration), with throwaway scripts that were not committed. Clients:
`confluent-kafka` 2.16.1 (bundled librdkafka 2.16.0), `aiokafka` 0.15.0, and the image's own
`kafka-console-producer` (the Java client) for the partitioner check. Python 3.12.15; the
`confluent-kafka` script was also run on Python 3.14.4 with the same results. A broker outage was
simulated with `docker pause`, which leaves the TCP connection open but unanswered, the harder case
for a client to detect. Each timing below comes from one or two single runs, not from a
measurement; they varied by up to a second between runs.

| Check | `confluent-kafka` 2.16.1 | `aiokafka` 0.15.0 |
|-------|--------------------------|-------------------|
| Produce with `acks=all` and idempotence; result known only from the broker's acknowledgement | Pass: 10 of 10 delivery reports, each with partition and offset | Pass: 10 of 10 `send_and_wait` results |
| Same key always on the same partition | Pass | Pass |
| Key → partition matches `kafka-console-producer` (12 keys, 6 partitions) | **No** with the default partitioner; **yes** with `partitioner=murmur2_random` | Yes |
| Broker paused during a produce | Pass: delivery report with `_MSG_TIMED_OUT` after about 5–6 s (`delivery.timeout.ms=5000`) | **Fail:** `send_and_wait` had not failed after 20 s with `request_timeout_ms=5000`, when the test's own `asyncio.wait_for` cancelled it |
| Auto-commit off; manual commit stores offset + 1 | Pass: message at offset 2, committed 3 | Pass: committed 3 |
| Restart after a partial commit redelivers the uncommitted messages | Pass: `m3`, `m4` redelivered | Pass |
| Second consumer joins (3 partitions, 30 messages processed by the first): revoke callback runs and can commit | Pass with `cooperative-sticky`, `enable.auto.offset.store=false` and `store_offsets` after each message: only partition 0 revoked from the first consumer; the commit in the callback stored 11, 7 and 12 for partitions 0, 1, 2 | Pass, with the eager protocol: the first consumer lost all three partitions, committed in the callback, and got 0 and 2 back. Each consumer's first revoke callback is empty and runs before any assignment |
| Asyncio producer | `confluent_kafka.aio.AIOProducer` exists; outage gives `_MSG_TIMED_OUT` after about 6–7 s | Native asyncio |

## Decision

1. **Broker: Apache Kafka 4.3.2 in KRaft mode** (no ZooKeeper), image `apache/kafka:4.3.2`, pinned
   to that exact tag in Docker Compose and CI. 4.3.2 was chosen over 4.4.0 because it is the second
   patch release of a line that has been out since May 2026, while 4.4.0 had no patch release yet.
   Locally and in CI there is one broker.
2. **Client: `confluent-kafka`** for producing, consuming and topic administration. It is added as a
   runtime dependency in phase 2, pinned to the version current at that time, which the spike will
   be rerun against if it differs from 2.16.1.
3. **Every producer sets `partitioner=murmur2_random`.** librdkafka's default partitioner
   (`consistent_random`, CRC32-based) puts keys on different partitions from the Java client, so a
   replay with `kafka-console-producer` or any Java-compatible tool would break per-store order
   (ADR 0001, decision 5). With `murmur2_random` all 12 keys landed on the same partitions as with
   `kafka-console-producer`.
4. **Producer baseline:** `acks=all`, `enable.idempotence=true`, and an explicit
   `delivery.timeout.ms`. Its value, how the API waits for the delivery report, and what it answers
   on a timeout are ADR 0003.
5. **Consumer baseline:** `enable.auto.commit=false`. Offsets are committed explicitly, as the
   processed message's offset + 1. **Provisional**, to be confirmed or replaced by ADR 0004 and
   phase 5: `enable.auto.offset.store=false` with `store_offsets` after each processed message, and
   `partition.assignment.strategy=cooperative-sticky`. They are offset and rebalance policy; this
   ADR records only that the spike ran them and they behaved as described above.
6. **The consumer process is synchronous:** one `poll` → validate → database transaction → commit
   loop, without asyncio. **SQLAlchemy is used synchronously** (2.0 style, a synchronous driver),
   in the consumer and in the query API. The driver is chosen when the database dependency is added
   in phase 3. FastAPI runs synchronous route functions in its thread pool, so the query API needs
   no async database layer.
7. How the ingest route waits for the delivery report (a synchronous `Producer` in a thread-pool
   route, or `AIOProducer` in an async route) is decided in ADR 0003, with its own measurement. Both
   showed a bounded failure in the spike.

**Update 2026-11-19 (phase 2, slice 1):** `confluent-kafka` was added at 2.16.2, the current
version, which bundles the same librdkafka 2.16.0. As decision 2 requires, the `confluent-kafka`
spike script was rerun with it against the project's Compose broker (same image, configuration
spelled out in `compose.yaml`): every `confluent-kafka` check in the spike table passed again,
and a paused broker gave `_MSG_TIMED_OUT` after 5.6 s. Setting up Compose also showed that the
`apache/kafka` image ignores its built-in defaults as soon as any `KAFKA_*` variable is set, so
`compose.yaml` spells out the whole single-node configuration and turns off
`auto.create.topics.enable`.

**Update 2026-11-21 (phase 3, slice 1):** decision 6's driver is **psycopg 3** (`psycopg[binary]`
3.3.9, used synchronously), with **SQLAlchemy 2.1.9**, the current release, which keeps the 2.0-style
API that decision 6 means. psycopg 3 is the maintained PostgreSQL driver (psycopg2 receives
maintenance fixes only) and has wheels for every Python version CI runs, 3.12 and 3.14. Every
database URL names the driver (`postgresql+psycopg://`, checked when settings load), because
SQLAlchemy's default for a bare `postgresql://` changed between versions. PostgreSQL itself is
19.0 (`postgres:19.0`, pinned by digest in `compose.yaml`), the current major release; nothing
depends on a feature new in 19.

## Consequences

- The consumer loop reads top to bottom with no event loop, so the order "database commit, then
  offset commit" is visible in the code and easy to test and to explain. librdkafka already does the
  network I/O in its own background threads, so asyncio would add little to the consumer.
- A synchronous consumer handles one event at a time per process. Throughput comes from more
  consumers in the group (up to one per partition), not from concurrency inside one process. No
  throughput figure is claimed; phase 8 measures.
- With one broker, `acks=all` means one replica. An acknowledged write should survive a clean
  restart of the broker process (not tested), but not a host crash: Kafka does not flush each write
  to disk before acknowledging it and relies on replication instead. The project relies on the
  topic as a durable log only in that limited sense; a real deployment needs a replication factor
  of at least 3 with `min.insync.replicas=2`. Recorded as a limitation.
- In `confluent-kafka`, `commit()` without arguments commits the stored offsets of **all** the
  consumer's partitions, not only the revoked ones, and raises `KafkaError._NO_OFFSET` when nothing
  new was stored (seen in the spike when the consumers closed). Phase 5 treats `_NO_OFFSET` as
  "nothing to commit", not as a failure.
- `confluent-kafka` wraps a C library. Errors come as `KafkaError` codes (for example
  `_MSG_TIMED_OUT`), which the code maps to its own categories, and its logs are librdkafka's.
- Every producer configuration must carry `partitioner=murmur2_random`; forgetting it in one place
  would silently send keys to different partitions. Phase 2 builds the producer configuration in
  one function and tests that it contains this setting.

### What the spike did not check

- A broker restart or a lost broker (only an unresponsive one); several brokers and replication.
- A consumer crashing between the database commit and the offset commit: phase 5 and 7 tests.
- Kafka transactions and exactly-once processing, which this design does not use (ADR 0004).
- Latency or throughput of any kind.
- `AIOProducer`'s batching options (`batch_size`, `buffer_timeout`) and their effect on ingest
  latency; part of ADR 0003.

## Alternatives considered

- **`aiokafka`.** Native asyncio, which fits FastAPI. Rejected on the spike results: a produce to a
  paused broker had not failed after 20 s despite `request_timeout_ms=5000`, so an ingest request
  would wait until an outer timeout cancelled it instead of getting a client error. Its rebalance
  worked, including a commit in the revoke callback, but it revoked every partition, not only the
  one that moved.
- **`kafka-python`.** A pure-Python client. Not tested in the spike; left out to keep the
  comparison to two candidates, one wrapping librdkafka and one native asyncio.
- **Redpanda.** Kafka-API compatible, a single binary, fast to start. Rejected for this project: the
  goal is to explain Kafka's own semantics (KRaft, consumer groups, offsets), and using the
  reference implementation avoids having to separate Kafka behaviour from Redpanda behaviour when
  explaining a test result.
- **Asynchronous consumer and async SQLAlchemy.** Possible with `AIOConsumer` and `asyncpg`, but the
  consumer handles one event at a time in a strict order, so concurrency gives nothing, while
  cancellation and two event loops' worth of error paths make the commit order harder to reason
  about. The distributed-task-platform project used async I/O; this one deliberately does not.
- **librdkafka's default partitioner.** Works while only this code produces, but a Java tool used for
  replay would put the same store on a different partition. Rejected (decision 3).
- **Kafka 4.4.0.** Newer, but without a patch release yet. Revisit when the project next updates
  its pinned versions.
- **The new consumer group protocol (KIP-848, `group.protocol=consumer`).** Not evaluated in the
  spike; only the classic protocol with `cooperative-sticky` was run. Revisit in phase 5, together
  with the rebalance tests.
