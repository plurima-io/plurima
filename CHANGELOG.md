# Changelog

All notable changes to Plurima are recorded here. Format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); versions follow
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased] — 0.3.0

Public-API hardening, core concurrency/correctness fixes, metrics/Spring polish,
and build/CI supply-chain hardening. This release contains **breaking changes**;
each is flagged **BREAKING** below with its migration.

### Changed

- **BREAKING: `PlurimaConsumer.builder()` is no longer generic.** It returns a
  `PlurimaConsumerBuilder<byte[], byte[]>`; `.keyDeserializer(...)` /
  `.valueDeserializer(...)` each return a **re-typed** builder
  (`<K2, V>` / `<K, V2>`). Deserializers must now be configured **before** any
  handler setter (`listener` / `manualAckListener` / `onMessage` / `onMessageAck`);
  calling them after a handler throws `IllegalStateException`. Migration: delete
  `<byte[], byte[]>` type witnesses on `builder()` calls and move
  `keyDeserializer`/`valueDeserializer` above the handler in the chain.
- **BREAKING: `AckType` replaces Kafka's `AcknowledgeType` on the public API.**
  `AckContext.acknowledge(...)` and `AckMessage.acknowledge(...)` now take
  `io.plurima.kafka.ack.AckType` (`ACCEPT` / `RELEASE` / `REJECT`; Kafka's `RENEW`
  is deliberately not carried over). Migration: replace the
  `org.apache.kafka.clients.consumer.AcknowledgeType` import with
  `io.plurima.kafka.ack.AckType`.
- **BREAKING: `Message.headers()` returns `MessageHeaders`** (Kafka-decoupled view
  with `values(name)`, `lastValue(name)`, `names()`) instead of Kafka's
  `org.apache.kafka.common.header.Headers`, completing the Kafka-free `Message`
  surface. `Message.header(name)` is unchanged.
- **BREAKING: `ConsumerContext.deliveryCount()` returns `int`** (was `short`), and
  `deliveryCountOptional()` is **removed** — the value is always present (≥ 1).
  Migration: drop the `Optional` unwrapping and any down-casts.
- **BREAKING: `PlurimaConsumerBuilder.deadLetterTopic(DltConfig)` renamed to
  `deadLetter(DltConfig)`** (the argument configures more than the topic name).
- **BREAKING: `RetryDecision` moved out of the public API** (from
  `io.plurima.kafka.retry` to the `@Internal` `io.plurima.kafka.internal` package).
  It was an implementation detail of the retry pipeline; `RetryPolicy` and
  `ExceptionClassifier` remain public and unchanged.
- **BREAKING (metrics SPI): typed tag enums replace strings.**
  `PlurimaMetrics.recordsProcessed` takes a `ProcessResult`, `ackQueued` /
  `ackCommitted` take an `AckOutcome`, and `backpressureEvent` takes a
  `BackpressureEvent` (all in `io.plurima.kafka.metrics`, all rendering the
  lower-case tag value via `toString()`). Custom `PlurimaMetrics` implementations
  must update those signatures; the emitted tag values are unchanged except the
  SHARE ack-casing fix noted below.
- **BREAKING (metric rename): `plurima.consumer.barrier.timeout_ms` →
  `plurima.consumer.barrier.timeout`.** The unit is now carried as Micrometer
  `baseUnit` metadata (`milliseconds`) instead of a name suffix. Update dashboards
  and alerts that reference the old name.
- `plurima.consumer.poll.duration` is now tagged with `topic` and `group_id`
  (`PlurimaMetrics.recordPollDuration(topic, groupId, duration)`).
- Spring Boot starter: `@PlurimaListener.concurrency` is now a **`String`**
  (default `"50"`) so it supports `${...}` property placeholders. Integer literals
  become strings: `concurrency = 25` → `concurrency = "25"`. `topics` and
  `groupId` resolve `${...}` placeholders too.
- `core` no longer leaks `slf4j-api` onto consumers' compile classpaths
  (`api` → `implementation`); it now exposes `jspecify` nullability annotations
  as an `api` dependency instead. Add an explicit slf4j dependency if you were
  relying on the transitive one.

### Added

- **Consumer lifecycle observability**: `PlurimaConsumer.state()` (`NEW` /
  `RUNNING` / `CLOSED` / `FAILED`) and `isRunning()`. On an unrecoverable
  poll-thread error the consumer transitions to `FAILED`, closes itself, and
  invokes the new builder callback `.onFatalError(Consumer<Throwable>)` exactly
  once — previously a fatal crash was silent.
- **DLT-failure backpressure (CLASSIC_BASIC)**: when a DLT publish fails, the
  affected partition is paused and the publish is retried with capped exponential
  backoff, instead of continuing to fetch records that can never commit. Bounds
  memory growth during a DLT outage.
- **`PlurimaMetrics.close()`** (default no-op): called exactly once from
  `PlurimaConsumer.close()` — including the fatal-failure self-close path — so
  implementations can deregister per-consumer gauges. `MicrometerPlurimaMetrics`
  implements it and now deregisters every gauge it registered.
- **Micrometer auto-registration in the Spring Boot starter**: with
  `kafka-plurima-metrics` on the runtime classpath, a `MeterRegistry` bean
  present, and no user-defined `PlurimaMetrics` bean, a `MicrometerPlurimaMetrics`
  bean is registered automatically. The starter depends on `:metrics` only at
  compile time, so Micrometer is never forced onto starter users.
- **`MicrometerPlurimaMetrics(registry, publishPercentileHistograms)`**: opt-in
  percentile histograms on the `process.duration` / `poll.duration` timers for
  backends that compute quantiles server-side (default `false`).
- **`plurima.enabled` starter property** (default `true`): master switch that
  suppresses all Plurima auto-configuration when `false`, plus generated Spring
  configuration metadata for IDE completion of `plurima.*` properties.
- **Per-endpoint `client.id` suffix in the Spring Boot starter**: with a global
  `plurima.client-id` configured, each `@PlurimaListener` endpoint now gets
  `<client-id>-<beanName>-<methodName>` (spring-kafka style) instead of every
  listener sharing one raw `client.id` — avoiding Kafka client MBean-name
  collisions and collapsed `client_id` metric tags across listeners.
- CI/CD hardening: CodeQL static-analysis workflow, Dependabot (Gradle + Actions),
  all GitHub Actions pinned to commit SHAs, release workflow gated behind a
  GitHub `release` environment, snapshot-publish guard now hard-fails when
  `master` carries a non-SNAPSHOT version, Jacoco coverage reports, reproducible
  archives, and `FAIL_ON_PROJECT_REPOS` repository centralization. CI now runs on
  pushes to **all** branches.

### Fixed

- **SHARE ack metric tag casing (`plurima.consumer.ack.queued` /
  `plurima.consumer.ack.committed`)**: the SHARE engine emitted upper-case `type`
  tag values (`ACCEPT`/`RELEASE`/`REJECT`, from `AcknowledgeType.name()`) while
  CLASSIC_BASIC emitted lower-case. Both engines now uniformly emit lower-case
  (`accept`/`release`/`reject`). Update dashboards keyed on the upper-case values.
- **Commit-frontier stall on non-dense offsets (CLASSIC_BASIC)**: the frontier
  assumed contiguous offsets, so a gap (compacted topic, `read_committed`
  transaction markers) could stall a partition's commits forever. The frontier now
  tracks actually-delivered offsets and advances past gaps; the poll loop also
  registers every delivered offset (not just the first of each batch).
- **Per-partition FIFO violation in PARTITION mode**: a second batch dispatched
  while the previous batch's worker was still running could process the same
  partition on two workers concurrently. Dispatch now flows through a persistent
  per-partition serial queue.
- **A single failed ack no longer kills the consumer (SHARE)**: non-fatal
  `KafkaException`s during acknowledge (timeouts, disconnects) were escaping the
  ack path and terminating the poll thread; they are now contained per-record.
- **Handler-timeout watchdog race**: a watchdog firing after the handler had
  already failed could misclassify the handler's own exception as
  `HandlerTimeoutException`, and a late watchdog could leak a stray interrupt
  into a subsequent retry sleep. Settlement is now a single-winner state
  transition; late watchdogs can neither interrupt nor misclassify. A force-RELEASE
  arriving during an inline retry backoff is also detected before re-invoking the
  listener, avoiding double processing.
- **Lost final offset commit on shutdown (CLASSIC_BASIC)**: the shutdown `wakeup()`
  could land on the final `commitSync` and be misread as a commit failure, silently
  dropping the last commits; the final commit is retried once after absorbing the
  expected wakeup. The drain → final-commit → close sequence now also shares one
  bounded deadline instead of three open-ended budgets.
- **`onPartitionsLost` no longer commits (CLASSIC_BASIC)**: lost partitions
  (already reassigned, no orderly handoff) previously fell through to the revoke
  path's `commitSync`, which could race the new owner and regress committed
  offsets. Lost partitions now clean up local state without committing.
- Spring Boot configuration-metadata annotation processing was silently skipped
  due to an explicit-processor javac interaction; `plurima.*` property metadata
  is now generated correctly.

### Documentation

- README + UserGuide swept for the 0.3.0 API (every code block compiles against
  the new builder chain, `AckType`, `MessageHeaders`, `deadLetter(...)`,
  `int deliveryCount()`); new UserGuide § Consumer state & fatal errors;
  clarified that deserialization poison pills are REJECTed with the
  `records.poison_pill` metric, never DLT-routed.
- New `SECURITY.md` (private disclosure via GitHub security advisories) and
  `CONTRIBUTING.md` (build, unit/integration test, and PR conventions).
- Trademark notices reworded to plain nominative attribution (no license claim).

## [0.2.0] — 2026-06-28

SHARE-engine reliability and handler-ergonomics upgrade. All changes are additive and sit off
the verified completion/ack core.

### Added
- **`Message` handler API** (preferred for application handlers): `Message<K,V>` — a
  Kafka-decoupled view unifying payload + metadata — with `MessageListener` (auto-ack) and
  `AckMessage`/`MessageAckListener` (explicit ack: `accept()` / `release()` / `reject()`), wired
  via `PlurimaConsumerBuilder.onMessage(...)` / `onMessageAck(...)`. Plus a `Messages` factory for
  building messages in unit tests with no broker.
- **Handler timeout** (`.handlerTimeout(Duration)`, SHARE only): interrupts a handler that runs
  too long and routes it through retry/DLT as `HandlerTimeoutException` (classifiable via
  `retryOn(...)`).
- **Lock-duration auto-alignment** (SHARE): when `.lockDuration(...)` is not set, the drain
  barrier is auto-aligned to 0.8 × the broker's record-lock duration once discovered, maximizing
  the no-force-release window.
- *(belatedly recorded)* **Adaptive drain barrier** (SHARE, opt-in, `@Experimental`):
  `.adaptiveDrainBarrier()` / `.adaptiveDrainBarrier(AdaptiveBarrierConfig)` force-RELEASE
  stragglers based on observed handler latency (p99 × multiplier, clamped to
  [max(1s, pollTimeout), lockDuration]) instead of waiting the full flat `lockDuration`. This
  shipped in 0.2.0 but was missing from these release notes.

### Changed / Fixed
- **Slow records are no longer mistaken for failures**: drain-barrier force-RELEASEs are tracked
  and subtracted from the effective attempt count, so a slow-but-healthy record is not wrongly
  routed to the DLT. Slowness counts are cleared on terminal completion.
- **Builder rejects more than one handler** immediately (exactly one of
  `listener`/`manualAckListener`/`onMessage`/`onMessageAck`).
- Handler-timeout interrupt handling preserves non-timeout (e.g. shutdown) interrupts so retries
  abort promptly.

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

[Unreleased]: https://github.com/plurima-io/plurima/compare/v0.2.0...HEAD
[0.2.0]: https://github.com/plurima-io/plurima/releases/tag/v0.2.0
[0.1.0]: https://github.com/plurima-io/plurima/releases/tag/v0.1.0
