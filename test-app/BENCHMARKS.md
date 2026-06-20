# Plurima vs vanilla Kafka — benchmark results

Head-to-head comparison between idiomatic stock-Kafka consumer code and Plurima v0.1
against a live Kafka 4.2.0 broker at `localhost:9092`. Each scenario produces the same
record set, runs both the vanilla loop and the Plurima consumer, and reports wall-clock
from first-record-handled to last-record-handled.

Run via `./gradlew :test-app:runBench`. Source lives in
[`test-app/src/main/java/io/plurima/testapp/bench/`](src/main/java/io/plurima/testapp/bench/).

## Summary

| Scenario                                                       | Vanilla    | Plurima  | Speedup | Why                                                                                    |
| -------------------------------------------------------------- | ---------- | -------- | ------- | -------------------------------------------------------------------------------------- |
| CLASSIC multi-partition throughput (200 records / 4 parts × 100 ms) | 21 443 ms  | 5 793 ms | **3.7×** | Plurima runs 4 partition workers in parallel; vanilla's single poll thread is serial.   |
| SHARE throughput (200 records × 100 ms handler)                | 21 534 ms  | 1 843 ms | **11.7×** | Plurima SHARE + UNORDERED uses 16 workers; vanilla `KafkaShareConsumer` is single-threaded. |
| CLASSIC + per-key FIFO (100 records, 20 keys × 100 ms)         | 10 703 ms  | 761 ms   | **14.1×** | Plurima KEY mode preserves per-key FIFO **while** running distinct keys on 16 shards concurrently. Vanilla cannot do parallel + per-key FIFO without custom code. |
| Continuous-poll vs fencing (10 s handler, mpi = 6 s)           | 13 037 ms¹ | 13 180 ms | n/a     | Vanilla raised `CommitFailedException` (kicked out of the group); Plurima committed cleanly. The wall-clock numbers are similar but the outcomes differ. |
| Retry + DLT on permanently-failing record                      | partition blocked² | 6 077 ms | n/a | Vanilla redelivered the same record 4 times in a 5 s observation window and never made progress. Plurima retried 1+3 attempts, routed to DLT, and advanced past. |

¹ Vanilla wall-clock includes the broker's fencing latency plus retry attempt — the run's
"single invocation" was actually re-fired after the broker kicked the consumer; the
`CommitFailedException` confirms the commit never landed.
² Vanilla never finished; the observation window is the 5 s during which we counted 4
redeliveries.

## Detailed results

### CLASSIC multi-partition throughput

4 partitions × 50 records each = 200 records; 100 ms handler.

- **Vanilla** (single-threaded `KafkaConsumer.poll()` loop): 21 443 ms.
- **Plurima** (`ConsumerEngine.CLASSIC_BASIC` + `OrderingMode.PARTITION` + `concurrency=16`): 5 793 ms.

Plurima's per-partition worker model gives 4-way parallelism (one worker per partition,
running in offset order). Vanilla's idiomatic poll loop walks the records sequentially
on one thread; partition count doesn't accelerate it. Theoretical perfect parallelism
would be 5 000 ms (= 50 × 100 ms); we observed 5 793 ms — 16 % overhead over the
theoretical floor.

### SHARE throughput

1 partition, 200 records, 100 ms handler.

- **Vanilla** (`KafkaShareConsumer` with implicit ack): 21 534 ms.
- **Plurima** (`ConsumerEngine.SHARE` + `OrderingMode.UNORDERED` + `concurrency=16`): 1 843 ms.

Plurima dispatches each polled record to a separate worker; vanilla's
single-threaded poll loop processes records in series. The 11.7× speedup reflects
the 16-way concurrency, with a small loss to setup and to records arriving in
batches rather than one-per-poll.

### CLASSIC + per-key FIFO with parallelism (Plurima-unique)

1 partition, 100 records across 20 distinct keys, 100 ms handler.

- **Vanilla** (single-threaded, FIFO preserved by construction): 10 703 ms.
- **Plurima** (`ConsumerEngine.CLASSIC_BASIC` + `OrderingMode.KEY` + `concurrency=16` + `shardCount=64`): 761 ms with per-key FIFO **verified** post-run.

This is Plurima's killer feature for ordered workloads. Vanilla has only two options
on a single partition: process serially (FIFO holds, no parallelism) or dispatch to a
thread pool (parallelism, FIFO breaks). Plurima's key-shard model gives both:
same-key records serialize inside their shard, distinct keys run on different shards.
The bench asserts per-key FIFO after the run completes — no breakage.

### Continuous-poll vs fencing

1 partition, 1 record, 10 s handler, `max.poll.interval.ms = 6 s`.

- **Vanilla**: raised `CommitFailedException` — the broker fenced the consumer once
  `max.poll.interval.ms` lapsed during the 10 s handler. The handler had already
  finished, but the commit could not land. Without a retry framework the application
  would either crash (uncaught exception) or block on a partition the broker now
  considers re-assigned.
- **Plurima**: handler ran once, committed once, no redelivery. The poll thread keeps
  polling on a separate cadence from the workers, so a slow handler cannot fence the
  consumer.

The wall-clock numbers look comparable but the *outcomes* differ: vanilla failed to
commit; Plurima succeeded.

### Retry + DLT on permanently-failing record

1 record that always throws.

- **Vanilla**: idiomatic "don't commit + seek back on failure" → 4 redeliveries in a
  5 s observation window. The partition is permanently stalled until an operator
  manually skips the offset.
- **Plurima**: 1 initial + 3 configured retries → exhaustion → record routed to the
  configured DLT topic → original partition's frontier advances past the bad offset.
  Total time: 6 077 ms, partition unblocked, retried-attempt count exact.

Vanilla can be made to behave like this only by hand-rolling a retry policy,
exception classifier, exponential-backoff scheduler, DLT producer, and offset
management on top of `KafkaConsumer`. Plurima ships all of it with three builder calls.

## Methodology

- Same broker (Kafka 4.2.0 at `localhost:9092`), same producer settings (`acks=all`).
- Each scenario uses fresh consumer groups + unique topics to avoid cross-run state.
- Both consumers see the same record set with the same per-record handler latency.
- Vanilla loops are the straightforward "what you'd write starting from the Kafka
  docs" implementation — single-threaded poll + commit, no retry, no DLT. They are
  not deliberately handicapped.
- Wall-clock starts when the FIRST record's handler enters and ends when the LAST
  record's handler returns. Setup time (poll, deserialize, assignment) is excluded.

## Reproducing

```bash
# Pre-flight: ensure a Kafka 4.2+ broker is running at localhost:9092
./gradlew :test-app:runBench
```

To target a different broker:

```bash
./gradlew :test-app:runBench -PappArgs="--bootstrap broker.example.com:9092"
```
