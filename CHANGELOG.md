# Changelog

All notable changes to kafka-plurima are recorded here. Format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); versions follow
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] — 2026-05-24

The first release that targets both Kafka engines. Brings cross-cluster per-key FIFO
with intra-partition parallelism on the CLASSIC consumer-group engine, and trims the
SHARE engine to its honest one mode (UNORDERED) since instance-local KEY / PARTITION
ordering on share groups was never load-bearing across the cluster.

### Compatibility notes for early internal users

- **`ConsumerEngine.SHARE` rejects `OrderingMode.KEY` and `OrderingMode.PARTITION` at
  build time.** Earlier internal builds allowed the combinations but ordered only this
  consumer's slice; the labels misled users into expecting cross-cluster ordering they
  never had.
  Use `ConsumerEngine.CLASSIC_BASIC` for cross-cluster KEY / PARTITION ordering, or
  stay on `UNORDERED` for share-group throughput.
- **`PlurimaConsumerBuilder.autoRenew(AutoRenewPolicy)` removed.** The setter,
  `io.plurima.kafka.renew.AutoRenewPolicy`, the `LeaseRenewer`, and the
  `plurima.consumer.lease.renewed` metric are all gone. Auto-RENEW was an architectural
  no-op under the explicit-ack drain barrier in earlier internal builds and has been
  removed entirely.
- **`ConsumerContext.lockExpiresAt()` and `lockExpiresAtOptional()` removed.** The
  values were only ever consumed by the now-deleted `LeaseRenewer`. The CLASSIC engine
  also exposed an `Instant.MAX` sentinel that made the API asymmetric. The methods are
  gone; `ConsumerContext` now exposes `deliveryCount`, `deliveryCountOptional`, and
  `orderingMode`.
- **`PlurimaMetrics.leaseRenewed(String)` removed**, along with the
  `plurima.consumer.lease.renewed` counter.
- **`ConsumerContext.lockExpiresAt` field on `InFlightRecord` removed.** The
  `InFlightRecord` constructor is now single-arg (`new InFlightRecord<>(record)`); the
  older two-arg form is gone.
- **`PlurimaConsumerBuilder`'s `max.poll.interval.ms / 2` retry-delay cap removed
  for CLASSIC_BASIC.** The continuous-poll model heartbeats the consumer independently
  of worker progress, so arbitrarily long retry delays no longer fence. The cap
  validation has been deleted; existing CLASSIC retry policies that previously failed
  validation now build successfully.

### Added

- **CLASSIC engine: intra-partition key-shard parallelism (`OrderingMode.KEY`).**
  Records are sharded by `(TopicPartition, hash(key) % shardCount)`; same-shard records
  serialise in offset order, distinct shards run concurrently. Combined with classic
  consumer-group partition assignment and Kafka's default key-aware partitioner, this
  yields cross-cluster per-key FIFO **with** parallel processing — the pattern
  Confluent Parallel Consumer's `ProcessingOrder.KEY` delivers.
- **CLASSIC engine: continuous-poll + backpressure model.** The poll thread polls
  every iteration regardless of worker progress. Pauses all assigned partitions when
  in-flight ≥ `concurrency`; resumes at ≤ `concurrency / 2`. Long handlers and long
  retry delays cannot fence the consumer.
- **`CommitFrontier` for out-of-order completion.** Per-partition tracker of the
  smallest offset NOT yet completed; KEY mode's out-of-order worker completions are
  parked until the gap closes, then absorbed in one sweep. The frontier value is what
  gets committed — at-least-once with no commit advance past unfinished work.
- **Lock-free worker hot paths.** `CommitFrontier.complete()` is a single
  `ConcurrentSkipListSet.add()`; `ClassicKeyShardDispatcher`'s per-shard ownership
  uses `AtomicBoolean` CAS instead of a `ReentrantLock`. Workers never block each other.
- **Identity-deserializer fast path.** When both key and value deserializers are
  `RecordDeserializer.bytes()` (the default), the per-record `new ConsumerRecord<>(...)`
  allocation is skipped — the raw record is returned cast to `ConsumerRecord<K, V>`.
- **Local-vs-broker `lockDuration` validation.** On the first successful poll the
  SHARE engine compares the builder's `.lockDuration(...)` against the broker-reported
  `acquisitionLockTimeoutMs` and logs INFO (good) or ERROR (bad) exactly once.
- **`ClassicBasicKeyParallelismIntegrationTest`, `ClassicBasicBackpressureIntegrationTest`,
  `ClassicBasicCrossClusterOrderingIntegrationTest`** — new live coverage for the
  v0.1 CLASSIC engine work.

### Fixed

- **Classic rebalance-revoke commit-frontier resurrection race.** Worker completions
  arriving after a partition was revoked called `markComplete`, which used
  `computeIfAbsent` and recreated the frontier — risking spurious `commitAsync` for
  partitions the consumer no longer owned (and in KEY mode, bootstrap from an
  out-of-order offset that could skip lower offsets the new owner expected to redeliver).
  `markComplete` now drops silently when the partition has been revoked.
- **`ClassicKeyShardDispatcher.purgePartitions` shard-queue drain.** Pre-fix, an
  in-flight worker on a revoked shard would call `launchNext` after completion, polling
  and processing the still-queued (now-revoked) records. The dispatcher now drains the
  queue AND fires `onRecordDone` for each entry, keeping `inFlight` accurate.
- **`ClassicPartitionSerialDispatcher` exception safety.** If `processOne` threw, the
  per-record `finally` fired `onRecordDone` but the throw propagated out of the launched
  lambda's for-loop and subsequent records were neither processed nor signalled. Now
  the body is wrapped in try/catch/log/continue.

### Documentation

- UserGuide rewritten for the v0.1 engine matrix: § Engines, § Ordering modes,
  § Slow handlers, § Shutdown, § Metrics.
- `ConsumerEngine` and `OrderingMode` Javadoc clarified for the new build-time
  rejection rules.

## Pre-0.1.0

Internal pre-release iterations. No published artifacts. See git history for context.

[0.1.0]: https://github.com/plurima-io/kafka-plurima/releases/tag/v0.1.0
