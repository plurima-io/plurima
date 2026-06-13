# kafka-plurima — User Guide

> A production-grade abstraction over `KafkaShareConsumer` (KIP-932) and vanilla `KafkaConsumer`. Per-key ordering with intra-partition parallelism, exponential retry, dead-letter routing, and Spring Boot integration.

**Status:** v0.1.0 — first release with both engines.

---

## Contents

1. [Prerequisites](#prerequisites)
2. [Quick start](#quick-start)
3. [Builder configuration](#builder-configuration)
4. [Listener variants](#listener-variants)
5. [Deserializers](#deserializers)
6. [Ordering modes](#ordering-modes)
7. [Retry policies](#retry-policies)
8. [Dead-letter topic](#dead-letter-topic)
9. [Slow handlers and broker lock expiry](#slow-handlers-and-broker-lock-expiry)
10. [Adaptive drain barrier](#adaptive-drain-barrier-share-opt-in)
11. [Metrics](#metrics)
12. [Spring Boot integration](#spring-boot-integration)
13. [Shutdown semantics](#shutdown-semantics)
14. [Known constraints & gotchas](#known-constraints--gotchas)
15. [Troubleshooting](#troubleshooting)

---

## Prerequisites

| Requirement | Version |
|---|---|
| Apache Kafka broker | **4.2.0** or later (KIP-932 share groups GA in 4.2) |
| Java | **21+** (virtual threads + sealed types) |
| Gradle / Maven | any modern version |
| Spring Boot (optional) | 3.4+ for the starter |

The Kafka broker must have **share groups enabled**. Per the [Kafka 4.2 upgrade guide](https://kafka.apache.org/42/documentation.html#upgrade_4_2_0), this requires TWO things in `server.properties`:

1. `share.version=1` — turns on the share-group feature (defaults to `0` in 4.2; will default to `1` in a future release).
2. `group.coordinator.rebalance.protocols` must include `share`. The default for 4.2 is `classic,consumer,streams`; add `share` to that list:

   ```properties
   group.coordinator.rebalance.protocols=classic,consumer,share
   share.version=1
   ```

You'll also typically want these single-broker dev settings so the share-coordinator topic can come up:

```properties
share.coordinator.state.topic.replication.factor=1
share.coordinator.state.topic.min.isr=1
```

For the Apache Kafka Docker image, the equivalent environment variables used by the
release and CI workflows are:

```yaml
KAFKA_GROUP_COORDINATOR_REBALANCE_PROTOCOLS: classic,consumer,share
KAFKA_SHARE_GROUP_ENABLE: "true"
KAFKA_SHARE_COORDINATOR_STATE_TOPIC_REPLICATION_FACTOR: 1
KAFKA_SHARE_COORDINATOR_STATE_TOPIC_MIN_ISR: 1
KAFKA_SHARE_AUTO_OFFSET_RESET: earliest
```

For a full working example, see `.github/workflows/ci.yml` — the Kafka service block
that powers the live integration suite. In GitHub service-container health checks,
use `/opt/kafka/bin/kafka-topics.sh`; the Apache Kafka 4.2 Docker image does not
put Kafka scripts on `PATH`.

For the `earliest` start position, configure it broker-side (the client can't set this, per KIP-932). Either default it server-wide:

```properties
share.auto.offset.reset=earliest
```

…or per-group via the admin CLI:

```bash
kafka-share-groups.sh --bootstrap-server localhost:9092 \
  --alter --group my-share-group \
  --config share.auto.offset.reset=earliest
```

---

## Quick start

### Gradle dependency

```kotlin
dependencies {
    implementation("io.plurima:kafka-plurima-core:0.1.0")
    // Optional:
    implementation("io.plurima:kafka-plurima-metrics:0.1.0")
    implementation("io.plurima:kafka-plurima-spring-boot-starter:0.1.0")
}
```

### Minimal consumer

```java
import io.plurima.kafka.PlurimaConsumer;
import io.plurima.kafka.OrderingMode;

import java.time.Duration;
import java.util.Properties;

Properties kafkaProps = new Properties();
kafkaProps.put("bootstrap.servers", "localhost:9092");
kafkaProps.put("group.id", "my-share-group");

try (PlurimaConsumer<byte[], byte[]> consumer = PlurimaConsumer.<byte[], byte[]>builder()
        .kafkaProperties(kafkaProps)
        .topic("orders")
        .concurrency(50)
        .pollTimeout(Duration.ofSeconds(1))
        .listener((record, ctx) -> {
            System.out.println("got: " + new String(record.value()));
        })
        .build()) {

    consumer.start();

    // Block until you shut the app down
    Thread.currentThread().join();
}
```

The consumer runs until you `close()` it (try-with-resources does this automatically). On normal listener return the record is auto-ACCEPTed; on exception, the default policy is REJECT (configure retry/DLT below).

---

## Builder configuration

```java
PlurimaConsumer.<K, V>builder()
    .kafkaProperties(props)              // REQUIRED — bootstrap.servers, group.id, etc.
    .topic("orders")                      // REQUIRED — single topic per consumer in v0.1
    .listener((r, ctx) -> { ... })       // REQUIRED — OR .manualAckListener(...)
    .ordering(OrderingMode.UNORDERED)    // default UNORDERED; KEY/PARTITION require CLASSIC_BASIC
    .concurrency(50)                      // max concurrent in-flight records, default 50
    .shardCount(200)                      // KEY mode only; default = concurrency × 4
    .pollTimeout(Duration.ofSeconds(1))  // poll() timeout, default 1s
    .lockDuration(Duration.ofSeconds(30))// broker's record-lock timeout (informational)
    .shutdownDrainTimeout(Duration.ofSeconds(30))
    .keyDeserializer(RecordDeserializer.utf8String())   // default: identity bytes
    .valueDeserializer(RecordDeserializer.utf8String()) // default: identity bytes
    .retry(RetryPolicy.exponential().maxAttempts(5).build())
    .deadLetterTopic(DltConfig.builder().producerProperties(producerProps).build())
    .metrics(new MicrometerPlurimaMetrics(meterRegistry))
    .build();
```

### Kafka property validation

For the default SHARE engine, the builder rejects these at construction time
(`IllegalArgumentException`) because Kafka share groups don't support them per KIP-932:

| Property | Why rejected |
|---|---|
| `auto.offset.reset` | Replaced by broker-side dynamic config `share.auto.offset.reset` |
| `enable.auto.commit` | Share groups have no auto-commit |
| `auto.commit.interval.ms` | Same |
| `group.instance.id` | Static membership not supported |
| `isolation.level` | Replaced by `share.isolation.level` |
| `partition.assignment.strategy` | Broker assigns; client can't influence |

The builder also force-overrides `share.acknowledgement.mode=explicit` on SHARE —
Plurima's pipeline (per-record RELEASE / REJECT control + retry / DLT mapping)
requires explicit mode.

For `ConsumerEngine.CLASSIC_BASIC`, normal classic-consumer properties such as
`auto.offset.reset`, `isolation.level`, `partition.assignment.strategy`,
`session.timeout.ms`, and `group.instance.id` are allowed. Share-only properties
such as `share.acknowledgement.mode` and `share.acquire.mode` are rejected on
CLASSIC_BASIC.

---

## Listener variants

### Auto-ack (`RecordListener`) — recommended default

```java
.listener((record, ctx) -> {
    process(record);
    // Normal return → record is ACCEPTed on the next poll.
    // Throwing → routes through the retry/DLT pipeline.
})
```

The `ConsumerContext ctx` exposes:

- `short deliveryCount()` — number of times this record has been delivered. On the **SHARE** engine this is the KIP-932 broker-tracked count (durable across rebalances). On **CLASSIC_BASIC** there is no broker-side counter; the value is Plurima's local in-process attempt counter (1 = first invocation; increments on each retry).
- `Optional<Short> deliveryCountOptional()` — same value wrapped (mirrors the underlying Kafka API shape).
- `OrderingMode orderingMode()` — UNORDERED, KEY, or PARTITION.

### Manual ack (`ManualAckListener`)

```java
import org.apache.kafka.clients.consumer.AcknowledgeType;

.manualAckListener((record, ack) -> {
    if (validate(record)) {
        ack.acknowledge(AcknowledgeType.ACCEPT);
    } else {
        ack.acknowledge(AcknowledgeType.REJECT);
    }
})
```

`AckContext` extends `ConsumerContext` with `acknowledge(AcknowledgeType type)`.
`AcknowledgeType` is Kafka's `org.apache.kafka.clients.consumer.AcknowledgeType`.
The call is **idempotent** — the first `acknowledge` call wins; subsequent calls
are silent no-ops.

If a manual-ack listener throws without acknowledging, the retry/DLT pipeline runs as usual.

`.listener(...)` and `.manualAckListener(...)` are **mutually exclusive** — calling both throws `IllegalStateException` at `build()`.

---

## Deserializers

```java
import io.plurima.kafka.deserializer.RecordDeserializer;

// Built-in:
RecordDeserializer.bytes()         // byte[] → byte[] (default)
RecordDeserializer.utf8String()    // byte[] → String

// Custom:
RecordDeserializer<MyEvent> json = (topic, bytes) -> objectMapper.readValue(bytes, MyEvent.class);
```

Usage:

```java
PlurimaConsumer.<String, MyEvent>builder()
    .keyDeserializer(RecordDeserializer.utf8String())
    .valueDeserializer(json)
    .listener((record, ctx) -> {
        String key = record.key();        // typed
        MyEvent value = record.value();   // typed
        ...
    })
    ...
```

**Deserialization errors** (poison-pill records): per KIP-932 the broker raises `RecordDeserializationException` on the next `poll()`. Plurima catches this and **REJECTs** the record (so it doesn't loop), emitting the metric `plurima.consumer.records.poison_pill{cause=deserialization}`. The record will NOT be redelivered.

---

## Engines: SHARE vs CLASSIC_BASIC

Plurima offers two underlying Kafka client primitives, each suited to different workloads. Pick with `.engine(ConsumerEngine.SHARE)` (default) or `.engine(ConsumerEngine.CLASSIC_BASIC)`.

### `ConsumerEngine.SHARE` (default)

Uses `KafkaShareConsumer` (KIP-932). Records are individually acquired by ANY consumer in the share group; the broker tracks per-record acquisition. Different consumers may concurrently hold different records from the same partition.

- **Best for**: high-concurrency, unordered processing where multiple consumers share the work; fine-grained per-record retry/DLT semantics.
- **Ordering**: SHARE supports `OrderingMode.UNORDERED` only. KEY and PARTITION are rejected at build time — the broker may hand same-key or same-partition records to different members of the group, so any in-process ordering is instance-local and does not extend across the cluster. Use `CLASSIC_BASIC` for KEY/PARTITION.
- **Manual ack** (`ManualAckListener`): supported. `AckContext.acknowledge(ACCEPT/REJECT/RELEASE)` maps to broker acknowledgement types.
- **Concurrency knob** (`.concurrency(N)`): tunable — bounds in-flight records via a semaphore.

### `ConsumerEngine.CLASSIC_BASIC`

Uses vanilla `KafkaConsumer` with consumer-group partition assignment. Each assigned partition is owned by exactly one consumer in the group, and records flow through the Plurima dispatcher that matches your `OrderingMode`:

- **PARTITION**: one worker per partition, in offset order. Cross-cluster per-partition FIFO holds because classic assignment is exclusive.
- **KEY**: intra-partition key-shard parallelism — records are sharded by `(TopicPartition, hash(key) % shardCount)`, same-shard records serialise, distinct shards run on different workers. With Kafka's default key-aware partitioner this yields cross-cluster per-key FIFO **with** intra-partition concurrency.
- **UNORDERED**: each record gets its own worker — records on the same partition run concurrently. Backpressure caps the total in-flight count via `.concurrency(N)`. Use this when you don't care about order and want maximum throughput per partition.

Engine details:

- **Best for**: workloads requiring real cross-cluster per-partition or per-key FIFO ordering with multiple consumer instances horizontally scaled. `OrderingMode.PARTITION` gives STRICT cross-cluster ordering; `OrderingMode.KEY` gives the same plus intra-partition parallelism.
- **STRICT means steady-state**: when partition ownership is stable, KEY and PARTITION deliver strict cross-cluster ordering of records AND of listener side-effects. During a **rebalance window** the picture is at-least-once with potential overlap: a worker on the old owner may finish user code (or DLT-produce) AFTER the new owner has already redelivered the same offsets. Plurima minimises this by (a) identity-checking the commit frontier so stale completions never advance offsets, (b) re-checking ownership before every retry attempt and before DLT routing so orphan workers stop calling the listener once they notice revoke, and (c) draining queued KEY-shard entries + firing onRecordDone on revoke. Residual overlap (a single in-flight record's user code already running when revoke fires) is the unavoidable cost of cooperative rebalance — design handlers idempotently, especially for KEY mode where the new owner picks up the same key promptly.
- **Concurrency** (`.concurrency(N)`): drives pause/resume backpressure. The poll loop pauses all assigned partitions when in-flight ≥ `N` and resumes at ≤ `N/2`. The cap is enforced AFTER each poll, so a single saturating batch can briefly push in-flight up to `N + max.poll.records − 1` before the next poll observes the pause condition; the next poll then returns zero records for paused partitions until workers drain. If you need a hard cap, set `max.poll.records ≤ concurrency`. Default `concurrency` is 50.
- **Continuous-poll heartbeating**: the poll thread calls `poll()` every iteration regardless of worker progress. Long handlers and long retry delays cannot fence the consumer — `max.poll.interval.ms` is satisfied by the poll-thread cadence, not by handler completion.
- **Manual ack**: NOT supported. `ManualAckListener` is rejected at build time (`AckContext.acknowledge(RELEASE)` has no equivalent in classic consumers). Use `RecordListener` with auto-ack.
- **Per-record lease**: classic consumer groups have no per-record lease. Plurima's continuous-poll loop heartbeats every iteration independently of worker progress, so `max.poll.interval.ms` is satisfied by the poll-thread cadence and does **not** bound individual handler runtime — long handlers and long retry delays cannot fence the consumer.
- **Delayed retry**: the worker sleeps for the configured delay, then retries. No partition pause needed for the retry itself — backpressure already handles new-record flow. There is no max-delay cap; the continuous-poll heartbeat decouples worker sleep from fencing.
- **Commit semantics**: each partition has a commit frontier tracking the smallest offset NOT yet completed; KEY mode's out-of-order completions are parked until the gap closes, then advance in one sweep. The frontier value is what gets committed — guaranteeing at-least-once with no offset advance past unfinished work.
- **Configs allowed**: `auto.offset.reset` (legitimate classic config), `isolation.level`, `partition.assignment.strategy`, `session.timeout.ms`, `heartbeat.interval.ms`, `group.protocol`, `group.instance.id`. Share-only configs (`share.acknowledgement.mode`, `share.acquire.mode`) are rejected at build time.

### Engine-by-feature comparison

| Feature | SHARE | CLASSIC_BASIC |
|---|---|---|
| Underlying client | `KafkaShareConsumer` (KIP-932) | `KafkaConsumer` |
| Supported ordering modes | UNORDERED only | UNORDERED, KEY, PARTITION |
| Cross-cluster ordering for KEY/PARTITION | ❌ not supported (build rejects) | ✅ STRICT in steady state; at-least-once with possible overlap during rebalance window |
| Per-record acquisition lock | ✅ | ❌ |
| Broker-side delivery counter | ✅ | ❌ (Plurima tracks in-process attempts) |
| `ManualAckListener` (per-record RELEASE) | ✅ | ❌ (build rejects) |
| Per-record lock lease | Broker-managed (`group.share.record.lock.duration.ms`) | N/A (continuous-poll loop heartbeats every iteration; `max.poll.interval.ms` satisfied by poll cadence, **not** by handler runtime) |
| `.concurrency(N)` knob | ✅ semaphore-bounded | ✅ drives pause/resume backpressure (bounded overshoot ≤ max.poll.records − 1 between polls) |
| Intra-partition parallelism | N/A (share group fans out per record) | ✅ KEY mode (shard by key hash) |
| Delayed retry mechanism | Queue RELEASE → broker redelivers | Sleep on worker (continuous-poll heartbeats) |
| Worst-case retry delay cap | Broker lock duration | None (continuous-poll heartbeats independently) |
| Switching engine across deploys | Broker state is share-group | Broker state is consumer-group |

### Asserting your scope with `OrderingGuarantee`

Optional belt-and-suspenders: set `.orderingGuarantee(OrderingGuarantee.STRICT)` to make your intent explicit. The combinations that matter are mostly already covered by the engine + ordering pair (`SHARE` + `KEY|PARTITION` is rejected outright; CLASSIC_BASIC + KEY|PARTITION is STRICT by construction), so the guarantee setter is primarily a self-documenting marker that the author wanted cross-cluster FIFO and a tripwire against the pathological pairing `UNORDERED + STRICT`.

```java
.engine(ConsumerEngine.CLASSIC_BASIC)
.ordering(OrderingMode.PARTITION)
.orderingGuarantee(OrderingGuarantee.STRICT)
```

### Switching engines

The broker-side group state for `SHARE` (share-group) and `CLASSIC_BASIC` (consumer-group) is different and does NOT carry over. Switching engines on a deployed application means:

- Offsets/positions reset to `auto.offset.reset` (CLASSIC_BASIC) or `share.auto.offset.reset` (SHARE) for the new group.
- You may want to use the same `group.id` value to keep operational tooling pointing at the same name, but the broker treats them as distinct group types.

For migration patterns from share to classic, see § "When global (cross-consumer) ordering matters" below.

---

## Ordering modes

Ordering guarantees are paired with the engine — `SHARE` supports only `UNORDERED`, while `CLASSIC_BASIC` supports all three. The KEY and PARTITION modes are CLASSIC-only because cross-cluster FIFO requires exclusive partition ownership, which only consumer-group assignment provides; share groups can hand same-key records to any member.

### `OrderingMode.UNORDERED` (default)

Records are dispatched to workers as polled, with no ordering guarantee. Maximum throughput. The only mode supported by `SHARE`.

### `OrderingMode.KEY` (CLASSIC_BASIC only)

Records with the same key (`record.key()` bytes) are processed in producer-publish order across the entire consumer group. Different keys may process concurrently (subject to partition ownership and `shardCount`).

```java
.engine(ConsumerEngine.CLASSIC_BASIC)
.ordering(OrderingMode.KEY)
.shardCount(200)  // optional; default = concurrency × 4
```

**How cross-cluster FIFO holds.** With Kafka's default key-aware partitioner, same-key records always land on the same partition. Classic consumer-group assignment owns each partition exclusively at any moment, so all same-key records flow through one consumer. Within that consumer, keys hash into `shardCount` in-memory shards; only one worker per shard runs at a time. Different shards run concurrently — KEY mode therefore gives intra-partition parallelism on top of per-key FIFO.

**Collision tradeoff.** Different keys may hash to the same shard and be serialised unnecessarily. With `shardCount = concurrency × 4` (default), collision rate is < 2 % for typical key cardinalities.

**Tuning:**
- Hot-key workloads → increase `shardCount`.
- Memory-constrained → decrease `shardCount`.

`null` keys are valid and hash deterministically (all `null`-keyed records serialise to one shard — effectively partition-serial).

### `OrderingMode.PARTITION` (CLASSIC_BASIC only)

Records on the same `(topic, partition)` are processed strictly in offset order by one worker; different partitions run concurrently up to the number of assigned partitions. Use when records carry partition-sequential meaning but no useful key — per-source feeds where source identity is encoded by partition assignment, audit/event streams where ordering is bounded by partition, change-data-capture pipelines.

```java
.engine(ConsumerEngine.CLASSIC_BASIC)
.ordering(OrderingMode.PARTITION)
// .shardCount(...) is ignored in PARTITION mode
```

**How cross-cluster FIFO holds.** Classic consumer-group assignment pins each partition to exactly one member at a time, so per-partition records flow through one consumer in offset order across the whole group.

**Compared to KEY mode.** PARTITION is strict serial within each partition (no intra-partition parallelism); KEY hashes keys into shards within each partition so distinct keys can process concurrently. Pick PARTITION when records on a partition are sequentially dependent regardless of key; pick KEY when independent per-key streams happen to be co-located on a partition and you want them to run in parallel.

**When to choose which:**
- Records have a meaningful key and you need per-key FIFO with intra-partition parallelism → **KEY**.
- Records have no key, or order matters across all records on a partition → **PARTITION**.
- Order doesn't matter → **UNORDERED** (highest throughput; the only mode SHARE supports).

### Migration note: SHARE + KEY/PARTITION pre-v0.1

Earlier Plurima releases allowed `SHARE + KEY` and `SHARE + PARTITION`, but they only ordered records that arrived at one consumer — share groups can deliver same-key records to any member, so the "ordering" was never load-bearing across the cluster. v0.1 rejects the combination at build time so users don't get a false expectation. Migrate by either switching to `CLASSIC_BASIC` (genuine cross-cluster ordering) or to `UNORDERED` on SHARE (same throughput, honest semantics).

---

## Retry policies

Default behavior: any exception causes `REJECT` (no retry).

```java
import io.plurima.kafka.retry.RetryPolicy;

RetryPolicy policy = RetryPolicy.exponential()
    .maxAttempts(5)
    .initialDelay(Duration.ofMillis(100))
    .multiplier(2.0)             // 100ms → 200ms → 400ms → 800ms → 1.6s
    .jitter(0.2)                  // ±20% randomization
    .retryOn(IOException.class, TimeoutException.class)
    .build();

PlurimaConsumer.<...>builder()
    .retry(policy)
    ...
```

### How retry works

| Decision | SHARE engine | CLASSIC_BASIC engine |
|---|---|---|
| `RetryInline` (delay ≤ 1 s) | Worker sleeps for the delay, then re-invokes the listener on the same virtual thread. | Same — worker sleeps for the delay and re-invokes. |
| `RetryDelayed` (delay > 1 s) | Worker queues `RELEASE` immediately. **The configured `delay()` is NOT enforced** — Apache Kafka through 4.2 has no broker-side scheduled-redelivery primitive. The broker re-acquires the record on the next share-fetch (typically tens of milliseconds later). The delay value is informational only and reserved for a future KIP that may add broker-side scheduling. | Worker sleeps for the full delay on its virtual thread, then re-invokes the listener. The continuous-poll loop heartbeats independently of worker progress so the sleep cannot fence the consumer regardless of length, and backpressure pauses the partition while the worker sleeps. **The delay is honoured exactly.** |
| `Reject` | Classifier marked the exception as non-retriable → immediate `REJECT` ack. | Same — `markComplete` (commit past) + `recordsProcessed{result=reject}` metric. |
| `Exhausted` | `attempt >= maxAttempts` → routed to DLT if configured, else `REJECT`. | Same — routed to DLT if configured (frontier advances on success), else `markComplete` past with ERROR. On DLT publish failure, the frontier stalls; see § DLT failure handling. |

### Custom classifier

```java
RetryPolicy.exponential()
    .maxAttempts(3)
    .initialDelay(Duration.ofMillis(50))
    .classifier(t -> t.getMessage() != null && t.getMessage().startsWith("retry-"))
    .build();
```

---

## Dead-letter topic

```java
import io.plurima.kafka.dlt.DltConfig;

Properties dltProducerProps = new Properties();
dltProducerProps.put("bootstrap.servers", "localhost:9092");

DltConfig dlt = DltConfig.builder()
    .producerProperties(dltProducerProps)
    .namingStrategy(t -> t + ".DLT")   // default
    .includeStackTrace(true)            // adds plurima-dlt-stack-trace header (4 KB max)
    .build();

PlurimaConsumer.<...>builder()
    .retry(retryPolicy)
    .deadLetterTopic(dlt)
    ...
```

### What gets sent to the DLT

The **original record's bytes** (key + value, exact) are produced to `{topic}.DLT` (or your custom naming). Plurima never re-serializes. The following headers are added as the public DLT metadata contract:

| Header | Value |
|---|---|
| `plurima-dlt-original-topic` | source topic |
| `plurima-dlt-original-partition` | source partition |
| `plurima-dlt-original-offset` | source offset |
| `plurima-dlt-failure-class` | exception FQCN |
| `plurima-dlt-failure-message` | exception message (or "null") |
| `plurima-dlt-attempt-count` | retry attempt count |
| `plurima-dlt-routed-at` | ISO-8601 timestamp |
| `plurima-dlt-stack-trace` | (optional) truncated to 4096 bytes |

Original record headers pass through unchanged.

### DLT failure handling

If the producer.send() to the DLT fails (configured DLT but the publish fails — broker reject, timeout, network error), Plurima never silently loses the record. The recovery path differs by engine:

- **SHARE engine**: the original record is `RELEASE`d to the broker, which re-delivers it on the next poll to any consumer in the share group. Progress continues on the share group as a whole.
- **CLASSIC_BASIC engine**: the partition's commit frontier does **not** advance past the failing offset. Later records on the same partition still process and their completions park in the frontier's `completedAhead` set, but the commit stays pinned at the failing offset. On the next restart (or rebalance handing the partition to another consumer in the same group), the broker re-delivers from the last committed offset and the DLT route is re-attempted.

  **Operational consequence**: if DLT publishing is broken permanently (DLT topic deleted, broker quota exceeded), the partition's commit halts and a restart is required once DLT is healthy. **Alert on `plurima.consumer.dlt.failures{topic=...}` and treat any non-zero value as an active incident.** A healthy DLT is a hard prerequisite for healthy classic consumer progress.

Either way, the record is **never silently lost** — at-least-once delivery still holds.

---

## Slow handlers and broker lock expiry

The SHARE engine relies on Kafka's `group.share.record.lock.duration.ms` — if a
handler runs longer than that, the broker redelivers the record. Plurima does NOT
expose a "renew" hook: any client-side RENEW would have to land between polls, but
the explicit-acknowledgement contract forbids that. Earlier versions shipped an
`AutoRenewPolicy` that turned out to be a no-op for this reason and was removed in
v0.1.

### Local force-RELEASE timeout (`.lockDuration(...)`)

Plurima has its own local deadline for in-flight records, set via the builder's
`.lockDuration(Duration)` (default 30 s). After a record sits in Plurima this long
without completing, the poll thread RELEASEs it explicitly — the broker hands the
record straight back to the share group instead of waiting for its own acquisition-
lock expiry.

**Set this BELOW the broker's `group.share.record.lock.duration.ms`** — typically
≈ 80 % of it. At startup, on the first successful poll, Plurima compares the value
against the broker's reported lock duration:

- **local < broker** → INFO log:
  `PollLoop lockDuration check OK: local=PT24S < broker=PT30S (force-RELEASE will fire 6000ms before broker would expire the lease).`
- **local ≥ broker** → ERROR log:
  `Plurima's local lockDuration=PT30S is NOT smaller than the broker's group.share.record.lock.duration.ms=PT30S. The broker will redeliver stuck records before Plurima's force-RELEASE can fire — no early-recovery benefit. Lower .lockDuration(...) on the builder to ≈ 0.8 × broker lock for faster handler-stuck recovery.`

The ERROR is loud but non-fatal: the consumer still functions correctly, it just
doesn't get the early-recovery benefit. Tune `.lockDuration(...)` and restart.

### When handlers genuinely run longer than the broker lock

Plurima cannot fix this client-side; either configuration or handler design must
change:

1. **Raise `group.share.record.lock.duration.ms`** on the broker so it comfortably
   exceeds your worst-case handler runtime, and align `.lockDuration(...)` to ≈ 80 %
   of the new broker value.
2. **Block on the work inside the handler** — `future.get(timeout)` if you've
   structured work as a future. Plurima virtual-thread workers make blocking cheap,
   and the listener's return is the ack signal.
3. **Add idempotency at the handler** — at-least-once delivery may produce duplicates;
   make the handler idempotent (e.g. upsert by record key + offset).
4. **Switch to `ConsumerEngine.CLASSIC_BASIC`** — classic consumer groups have no
   per-record lease. Plurima's continuous-poll loop heartbeats every iteration
   independently of workers, so `max.poll.interval.ms` is satisfied by the poll-thread
   cadence and individual handlers can run arbitrarily long without fencing the
   consumer.

---

## Adaptive drain barrier (SHARE, opt-in)

The SHARE engine must acknowledge every record from a poll before the next poll
(KIP-932 explicit mode). A single slow handler therefore holds the batch until the
drain barrier times out — by default a flat `lockDuration` (30 s). Enable the
adaptive barrier to force-RELEASE stragglers sooner, based on observed handler
latency:

```java
.adaptiveDrainBarrier()  // p99 × 3, clamped to [max(1s, pollTimeout), lockDuration]
// or tune:
.adaptiveDrainBarrier(AdaptiveBarrierConfig.builder().percentile(0.99).multiplier(3.0).build())
```

Only affects the straggler path (no change when handlers finish promptly). A
RELEASE'd straggler is redelivered — so this trades a shorter stall for duplicate
reprocessing of slow records (at-least-once already permits duplicates). SHARE only;
rejected at build time on `CLASSIC_BASIC`. Watch `plurima.consumer.barrier.timeout_ms`
to see the effective barrier and tune `multiplier`.

---

## Metrics

Plurima emits 15 metrics (11 counters, 2 gauges, 2 timers) via a stable `PlurimaMetrics` interface. Defaults to no-op; opt in by wiring an implementation.

### Micrometer (recommended)

```java
import io.micrometer.core.instrument.MeterRegistry;
import io.plurima.kafka.metrics.MicrometerPlurimaMetrics;

MeterRegistry registry = ...;  // from your application

PlurimaConsumer.<...>builder()
    .metrics(new MicrometerPlurimaMetrics(registry))
    ...
```

### Emitted metrics

Plurima emits 15 metrics: 11 counters, 2 gauges, and 2 timers.

| Name | Type | Tags | Fired when |
|---|---|---|---|
| `plurima.consumer.records.polled` | Counter | `topic` | Each batch returned from poll() |
| `plurima.consumer.records.processed` | Counter | `topic`, `result` | Listener completes (`accept`/`release`/`reject`) |
| `plurima.consumer.records.failed` | Counter | `topic`, `exception_class` | Listener throws |
| `plurima.consumer.records.poison_pill` | Counter | `topic`, `cause` | RecordDeserialization or CorruptRecord |
| `plurima.consumer.records.in_flight` | Gauge | `topic`, `group_id`, `client_id` | Current count of records being processed. Emitted by both SHARE and CLASSIC_BASIC. |
| `plurima.consumer.barrier.timeout_ms` | Gauge | `topic`, `group_id`, `client_id` | Current adaptive/share drain barrier timeout in milliseconds. |
| `plurima.consumer.retry.attempts` | Counter | `topic`, `attempt` | Each retry attempt |
| `plurima.consumer.dlt.routed` | Counter | `topic`, `dlt_topic` | DLT send success |
| `plurima.consumer.dlt.failures` | Counter | `topic`, `cause` | DLT send failure |
| `plurima.consumer.ack.queued` | Counter | `type` | Ack of given type queued (ACCEPT/RELEASE/REJECT) |
| `plurima.consumer.ack.committed` | Counter | `topic`, `type` | Ack applied to consumer's in-memory ack set |
| `plurima.consumer.ack.commit_failed` | Counter | `topic`, `exception_class` | `commitAsync` fails per-partition |
| `plurima.consumer.backpressure.events` | Counter | `topic`, `event` | CLASSIC_BASIC pause/resume events (`event=paused` / `event=resumed`). Not emitted by SHARE. |
| `plurima.consumer.process.duration` | Timer | `topic` | Listener invocation latency |
| `plurima.consumer.poll.duration` | Timer | — | Duration of a single poll() call |

Metric names are **stable for the 0.1.x line**. Additional metrics may be added in
future minor releases without removing or renaming the existing public metrics.

### Custom implementation

Implement `PlurimaMetrics` directly to integrate with other systems:

```java
PlurimaMetrics myMetrics = new PlurimaMetrics() {
    @Override
    public void recordsProcessed(String topic, String result) {
        myStatsdClient.increment("plurima." + topic + "." + result);
    }
    // ... other methods default to no-op
};
```

---

## Spring Boot integration

The starter auto-configures `PlurimaConsumer` instances for any `@PlurimaListener`-annotated method on a Spring bean.

### Add the starter

```kotlin
implementation("io.plurima:kafka-plurima-spring-boot-starter:0.1.0")
```

### Configure `application.yml`

```yaml
plurima:
  bootstrap-servers: localhost:9092
  client-id: my-app
  properties:
    # Anything else you want passed to KafkaShareConsumer
    # Note: forbidden keys (auto.offset.reset, etc) still rejected
```

### Annotate a method

```java
import io.plurima.kafka.ConsumerEngine;
import io.plurima.kafka.spring.PlurimaListener;
import io.plurima.kafka.OrderingMode;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.stereotype.Component;

@Component
public class OrderHandler {

    @PlurimaListener(
        topics = "orders",
        groupId = "order-processor",
        engine = ConsumerEngine.CLASSIC_BASIC,  // required for ordering = KEY
        ordering = OrderingMode.KEY,
        concurrency = 25
    )
    public void onOrder(ConsumerRecord<byte[], byte[]> record) {
        // Plurima starts a consumer per annotated method when the context refreshes,
        // and stops it on context close.
    }
}
```

### Wiring retry, DLT, and metrics

Three optional integrations are resolved from the Spring context:

- **Retry**: declare a `RetryPolicy` bean and reference it by name from the annotation.
- **DLT**: declare a `DltConfig` bean and reference it by name from the annotation.
- **Metrics**: declare any `PlurimaMetrics` bean (e.g. `MicrometerPlurimaMetrics`); the starter discovers it automatically and wires it into every listener — no annotation field needed.

```java
@Configuration
class OrdersConfig {
    @Bean("orderRetry")
    RetryPolicy orderRetry() {
        return RetryPolicy.exponential()
            .maxAttempts(3)
            .initialDelay(Duration.ofMillis(100))
            .retryOn(IOException.class)
            .build();
    }

    @Bean("orderDlt")
    DltConfig orderDlt(@Value("${kafka.bootstrap}") String bootstrap) {
        Properties producerProps = new Properties();
        producerProps.put("bootstrap.servers", bootstrap);
        return DltConfig.builder().producerProperties(producerProps).build();
    }

    @Bean
    PlurimaMetrics metrics(MeterRegistry registry) {
        return new MicrometerPlurimaMetrics(registry);
    }
}

@Component
class OrderHandler {
    @PlurimaListener(
        topics = "orders",
        groupId = "order-processor",
        retryPolicyBeanName = "orderRetry",   // optional — empty default = no retry
        dltConfigBeanName = "orderDlt"        // optional — empty default = no DLT
    )
    public void onOrder(ConsumerRecord<byte[], byte[]> record) { … }
}
```

### Constraints

- Single topic per `@PlurimaListener` (`topics = "X"`, not an array).
- Listener method must accept exactly one `ConsumerRecord<byte[], byte[]>`.
- The annotation exposes the auto-ack `RecordListener` path + retry/DLT/metrics wiring. For **manual ack** (`ManualAckListener`) or **typed deserializers** (e.g. `RecordDeserializer<MyEvent>`), build a `PlurimaConsumer` programmatically and register it as a Spring bean — the annotation can't carry generic type parameters through reflection.

For programmatic registration:

```java
@Bean
PlurimaConsumer<String, MyEvent> orderConsumer(MeterRegistry meterRegistry) {
    return PlurimaConsumer.<String, MyEvent>builder()
        .kafkaProperties(props)
        .topic("orders")
        .keyDeserializer(RecordDeserializer.utf8String())
        .valueDeserializer((t, b) -> objectMapper.readValue(b, MyEvent.class))
        .metrics(new MicrometerPlurimaMetrics(meterRegistry))
        .manualAckListener((r, ack) -> { ... })
        .build();
}

@PostConstruct
void startConsumer(@Autowired PlurimaConsumer<String, MyEvent> orderConsumer) {
    orderConsumer.start();
}
```

---

## Shutdown semantics

`PlurimaConsumer.close()` (or try-with-resources exit) does:

1. Stop accepting new poll batches (sets `running = false`, sends `wakeup()`).
2. Wait up to `shutdownDrainTimeout` for in-flight workers to finish. **Both engines honor this budget end-to-end** — SHARE waits via `awaitDrain` in `PollLoop`, CLASSIC_BASIC waits via the dispatch-batch loop in `ClassicPollLoop`. Handlers shorter than the configured budget complete naturally and their offsets commit; handlers longer than the budget get abandoned with a WARN log.
3. **SHARE engine**: force-RELEASE any records still in-flight when the timeout hits (broker will re-deliver them to a consumer in the same share group). **CLASSIC_BASIC engine**: abandon in-flight workers; their pending offsets do NOT commit; next owner of the partitions redelivers from the last committed offset (at-least-once duplicate, per-partition ordering preserved).
4. Drain the final ack queue (SHARE) or commit completed-handler offsets (CLASSIC_BASIC).
5. Close the underlying Kafka client. (Delayed retry queues an immediate `RELEASE` and lets the broker handle redelivery — there is no in-process retry scheduler to shut down.)
6. Close the worker virtual-thread executor (5s extra grace, then `shutdownNow()` interrupts any straggler workers).

Default `shutdownDrainTimeout` is 30 seconds. Tune to match your handler max-runtime + slack:

```java
.shutdownDrainTimeout(Duration.ofSeconds(60))
```

For Kubernetes deployments, set the pod's `terminationGracePeriodSeconds` to `shutdownDrainTimeout + 10–15s` (the extra covers worker-executor grace + KafkaConsumer close + DLT producer close) so the kubelet doesn't SIGKILL before drain finishes.

---

## Known constraints & gotchas

### Batch-parallel, not pipeline-parallel

Per KIP-932's explicit-mode contract, the consumer must acknowledge all records from a poll before the next poll. Plurima's `PollLoop` therefore **waits for all workers from one poll batch to finish before calling `poll()` again**. This is sometimes called a "drain barrier."

**Throughput is bounded by `batch_size / slowest_handler_time`**, not by pipeline depth. For typical share-group workloads (batch sizes 50–500, fast handlers), this is plenty. If a single slow handler dominates a batch, all other workers must wait for it before the next batch can start.

### SHARE `auto.offset.reset` is server-side

For the SHARE engine, the legacy `auto.offset.reset` property is **rejected** by
`KafkaShareConsumer`. To start a share group from the earliest position, configure it
broker-side:

```bash
kafka-share-groups.sh --bootstrap-server localhost:9092 \
  --alter --group <your-group-id> \
  --config share.auto.offset.reset=earliest
```

Otherwise the group starts at `latest` (the default) and only sees records produced AFTER its first heartbeat.

### Single topic per consumer (v0.1)

`PlurimaConsumerBuilder` accepts one `.topic("X")`. Multi-topic subscriptions in one consumer are deferred. Workaround: build multiple `PlurimaConsumer` instances, one per topic, in the same share group.

### Bytes-in/bytes-out by default

Without explicit deserializers, the consumer is `PlurimaConsumer<byte[], byte[]>`. Use `.keyDeserializer(...)` / `.valueDeserializer(...)` to escape this.

### Deferred metrics

The v0.1.0 metrics surface is the 15 metrics listed above: 11 counters, 2 gauges
(`plurima.consumer.records.in_flight`, `plurima.consumer.barrier.timeout_ms`), and
2 timers (`plurima.consumer.process.duration`, `plurima.consumer.poll.duration`). Histograms
(`records.delivery_count`, etc.), the broker-lag gauge (`plurima.consumer.lag`,
KIP-1226 bridge), and shard-internal gauges (`shard.queue.depth`, `shard.busy`) remain
deferred for a follow-up minor release.

---

## Troubleshooting

### SHARE: `IllegalArgumentException: kafkaProperties contains 'auto.offset.reset' …`

You set `auto.offset.reset` while using the SHARE engine. Remove it — see
[Known constraints](#known-constraints--gotchas) above for the server-side alternative.

Same for `enable.auto.commit`, `group.instance.id`, `isolation.level`, `partition.assignment.strategy`.

### `IllegalStateException: All records must be acknowledged in explicit acknowledgement mode`

You're not seeing this in normal usage — Plurima's drain barrier prevents it. If you see it, one of:
- You're using a custom `PollLoop` (not supported — `internal` API).
- A custom `RecordListener` is hanging indefinitely AND `shutdownDrainTimeout` is too short to force-RELEASE stuck records.

### Records aren't being delivered

Likely causes:
1. The share group's `share.auto.offset.reset` is `latest` (the default) and you produced records BEFORE the consumer subscribed. Either configure it `earliest` server-side or produce records AFTER `consumer.start()` returns + a brief priming wait (~6 s, one heartbeat interval).
2. Share groups are not enabled on the broker. For Apache Kafka's Docker image, set
   `KAFKA_SHARE_GROUP_ENABLE=true`; for `server.properties`, enable the share-group
   feature/configuration required by your Kafka 4.2 broker package.
3. Network / firewall between client and broker.

### `IllegalStateException` from `manualAckListener` after `acknowledge()`

The `AckContext.acknowledge()` is idempotent — second calls are no-ops. If you're seeing an ISE, it's likely from elsewhere (the broker rejecting the ack). Check the broker logs.

### Consumer hangs at close

Likely: a listener is blocked indefinitely (e.g., I/O without timeout). Set a finite `shutdownDrainTimeout`:

```java
.shutdownDrainTimeout(Duration.ofSeconds(30))
```

After the timeout, Plurima force-RELEASEs in-flight records (broker re-delivers them).

### DLT records aren't appearing

1. The DLT topic doesn't exist. Plurima doesn't auto-create it; pre-create with the right partition count.
2. The DLT producer's `bootstrap.servers` is wrong. Verify in your `DltConfig.producerProperties()`.
3. The original exception was NOT classified as retriable (default `RetryPolicy.noRetry()`). DLT only fires on `RetryDecision.Exhausted`, which requires at least one retry to be exhausted. With `RetryPolicy.noRetry()`, any exception becomes `Reject` → just REJECT, no DLT routing.

To route every failure to DLT:

```java
.retry(RetryPolicy.exponential()
    .maxAttempts(0)             // no retries
    .initialDelay(Duration.ofMillis(1))
    .retryOn(Throwable.class)   // anything goes
    .build())
```

With `maxAttempts(0)`, the first failure is classified as retriable (matches `Throwable`) but immediately `Exhausted` → routed to DLT.

---

## Further reading

- [KIP-932](https://cwiki.apache.org/confluence/display/KAFKA/KIP-932%3A+Queues+for+Kafka) — Queues for Kafka (share groups)
- [KIP-1222](https://cwiki.apache.org/confluence/display/KAFKA/KIP-1222) — Acquisition lock timeout renewal
- [KIP-1226](https://cwiki.apache.org/confluence/display/KAFKA/KIP-1226) — Share partition lag persistence

## License

Apache 2.0 — see [`LICENSE`](../LICENSE).
