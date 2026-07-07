# Plurima vs. Vanilla Kafka: Benchmark Results

This benchmark compares idiomatic direct Kafka client implementations with
Plurima while holding the workload constant.

## Environment

- Broker: `apache/kafka:4.2.1-rc5`, single node at `localhost:9092`
- Runtime: Java 21
- Workload: the same record count and per-record handler latency for both
  implementations in each scenario
- Measurement: wall-clock time from the first handled record to the last handled
  record

> **Note:** these numbers were captured against the `4.2.1-rc5` release candidate
> broker image because it was the newest available Kafka 4.2 build with share-group
> support at the time of measurement. They will be refreshed against a GA (non-rc)
> broker once one is available; treat the figures as indicative, not final.

## Results

| Scenario | Vanilla Kafka | Plurima | Result |
|---|---:|---:|---:|
| SHARE throughput, 200 records with 100 ms handlers | 20,741 ms | 1,684 ms | **12.3x** |
| CLASSIC per-key FIFO, 100 records across 20 keys with 100 ms handlers | 10,360 ms | 1,039 ms | **10.0x** |
| CLASSIC partition FIFO, 200 records across 4 partitions with 100 ms handlers | 20,755 ms | 12,194 ms | **1.7x** |
| Slow handler with `max.poll.interval.ms=6000` | 13,035 ms | 13,154 ms | Correctness scenario |
| Retry and DLT for a permanently failing record | Partition remained blocked | DLT-routed in 6,095 ms | Correctness scenario |

All figures came from one captured run. Results vary with hardware, JVM state,
broker load, record distribution, handler behavior, and Kafka configuration.
They should not be interpreted as a universal performance guarantee.

## Reproduce

With a Kafka 4.2 broker available at `localhost:9092`:

```bash
./gradlew benchmark
```

To use another broker:

```bash
./gradlew benchmark -PappArgs="--bootstrap broker.example.com:9092"
```

## Raw Summary

```text
========================================================================================================================
  Plurima vs vanilla Kafka - benchmark summary
========================================================================================================================
  scenario                                                         vanilla       plurima   speedup
  --------------------------------------------------------------------------------------------------------------------
  CLASSIC multi-partition throughput (200 recs / 4 parts x ...)     20755 ms      12194 ms      1.7x
  SHARE throughput (200 records x 100ms handler)                    20741 ms       1684 ms     12.3x
  CLASSIC + per-key FIFO (100 records, 20 keys x 100ms)             10360 ms       1039 ms     10.0x
  Continuous-poll vs fencing (10s handler, max.poll.interva...)     13035 ms      13154 ms      1.0x
  Retry + DLT on permanently-failing record                         5000 ms       6095 ms      0.8x
========================================================================================================================
```

The numeric ratios in the last two rows are not meaningful speed comparisons.
The vanilla measurements there represent observation windows for failure modes,
not equivalent successfully completed work.

## SHARE Throughput

Setup:

- One partition
- 200 records
- 100 ms handler latency per record
- Vanilla implementation: serial `KafkaShareConsumer` poll/handle loop
- Plurima: `SHARE`, `UNORDERED`, concurrency `16`

Results:

```text
vanilla: 20,741 ms
plurima:  1,684 ms
result:   12.3x
```

The direct loop handles one record at a time. Plurima dispatches independent
records to bounded virtual-thread workers while retaining the explicit
acknowledgement and poll-drain discipline required by Kafka share groups.

## CLASSIC Per-Key FIFO

Setup:

- One partition
- 100 records
- 20 distinct keys
- 100 ms handler latency per record
- Vanilla implementation: serial processing to preserve FIFO
- Plurima: `CLASSIC_BASIC`, `KEY`, concurrency `16`

Results:

```text
vanilla: 10,360 ms
plurima:  1,039 ms
result:   10.0x
```

The benchmark verified that each key's sequence remained ordered. Plurima runs
different key shards concurrently while serializing records mapped to the same
key shard.

This comparison is against an idiomatic serial direct-client implementation.
A custom application can implement its own keyed dispatcher, but that requires
the ordering, backpressure, commit-frontier, rebalance, and shutdown logic that
Plurima provides.

## CLASSIC Partition FIFO

Setup:

- Four partitions
- 200 records
- 100 ms handler latency per record
- Vanilla implementation: serial poll/handle loop
- Plurima: `CLASSIC_BASIC`, `PARTITION`, concurrency `16`

Results:

```text
vanilla: 20,755 ms
plurima: 12,194 ms
result:   1.7x
```

Partition ordering permits at most one active handler per assigned partition.
With four partitions, effective parallelism is structurally capped near four,
regardless of the configured worker concurrency. Assignment, polling, and batch
distribution further affect the observed result.

## Slow-Handler Fencing

Setup:

- 10 second handler
- `max.poll.interval.ms=6000`

Captured result:

```text
vanilla: 13,035 ms, listener invoked once
plurima: 13,154 ms, listener invoked once
```

This is a correctness scenario, not a throughput comparison. The vanilla run
happened to complete during this capture, but processing longer than
`max.poll.interval.ms` in the poll thread can make group stability and commit
success timing-dependent. Plurima's classic engine keeps polling independently
of worker progress, allowing the consumer thread to continue participating in
the group while a slow handler runs.

## Retry and Dead-Letter Routing

The direct client scenario used a seek/redelivery loop for a permanently failing
record. During the five-second observation window, the same record was delivered
four times and continued blocking partition progress.

Plurima invoked the handler four times, representing the initial attempt plus
three configured retries, then published the record to the dead-letter topic and
allowed the partition commit frontier to advance:

```text
vanilla: partition still blocked after a 5,000 ms observation window
plurima: DLT-routed and partition unblocked in 6,095 ms
```

The result demonstrates provided retry/DLT behavior, not an inherent limitation
of the Kafka client. Applications can build equivalent behavior directly, but
must implement and operate the retry state, DLT metadata, failure handling, and
commit semantics themselves.

## Interpretation

- Plurima's largest measured gains occur when handler work is significant and
  independent records or keys can be processed concurrently.
- `PARTITION` ordering deliberately limits concurrency to the partition count.
- The slow-handler and DLT cases primarily demonstrate built-in correctness and
  recovery behavior.
- Direct Kafka clients are lower-level primitives. Comparable application-level
  concurrency and recovery behavior can be implemented directly, but the
  benchmark's vanilla side intentionally represents idiomatic minimal client
  loops rather than a second custom framework.
