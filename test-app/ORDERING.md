# Plurima PARTITION + KEY ordering — verification results

Live verification that ordering invariants hold under stress for the two ordered modes
on `ConsumerEngine.CLASSIC_BASIC`. Each scenario produces records with monotonic
sequence numbers per ordering group, consumes them via Plurima, and asserts that the
observed sequence inside every group is monotonically non-decreasing.

Run via `./gradlew :test-app:runOrdering`. Source:
[`OrderingTestApp.java`](src/main/java/io/plurima/testapp/ordering/OrderingTestApp.java).

## Results (9/9 PASS against localhost:9092, Kafka 4.2.0)

| Scenario                                       | Mode      | Records | Groups          | Concurrency | Notes |
| ---------------------------------------------- | --------- | ------- | --------------- | ----------- | ----- |
| partition-strict-offset-order                  | PARTITION | 1 000   | 4 partitions    | 16          | Per-partition offset order verified for all 4 partitions. |
| partition-survives-rebalance-window            | PARTITION | 500     | 1 partition     | 16          | High-throughput single partition; no reordering. |
| key-strict-per-key-fifo-high-concurrency       | KEY       | 1 000   | 20 keys / 1 part | 16 / 64 shards | Per-key FIFO verified; peak concurrent = 13. |
| key-hot-key-contention                         | KEY       | 350     | 1 hot key (200) + 30 cold keys × 5 | 16 / 64 shards | Hot key serialised; cold keys parallel; all in per-key order. |
| key-fifo-survives-retry                        | KEY       | 80      | 10 keys × 8     | 16 / 64 shards | 30 retries fired; per-key FIFO intact (retry doesn't let later same-key records pass). |
| partition-fifo-survives-retry                  | PARTITION | 50      | 1 partition     | 16          | 10 retries fired; per-partition offset order intact. |
| key-null-keys-collapse-and-order               | KEY       | 25      | null key        | 16 / 16 shards | All null-keyed records collapse to one shard; peak concurrent = 1; in order. |
| key-cross-partition-same-key-fifo              | KEY       | 180     | 6 keys / 4 partitions | 16 / 64 shards | Same-key always lands on same partition (default partitioner); per-key FIFO across the topic. |
| key-fifo-under-retry-storm                     | KEY       | 5 000   | 50 keys / 1 part | 24 / 96 shards | 30%+ records retry (every 3rd / 7th / 11th seq); per-key FIFO holds end-to-end under heavy churn. |

## What each scenario proves

### `partition-strict-offset-order` (PARTITION mode)

4 partitions × 250 records each, sequenced 0..249 within each partition. The classic
poll loop dispatches each partition to one worker; that worker walks the partition's
records in offset order. The test asserts the observed sequence for every partition
is monotonic.

### `partition-survives-rebalance-window` (PARTITION mode)

Single partition, 500 records, 5 ms handler. Stress-tests the in-order serial worker
under high churn — proves there's no dispatch reordering even when batches arrive
back-to-back.

### `key-strict-per-key-fifo-high-concurrency` (KEY mode)

20 distinct keys, 50 records per key (1 000 total), 1 partition, `concurrency=16`,
`shardCount=64`, 20 ms handler. With these settings the dispatcher has 16-way
intra-partition parallelism (peak observed: 13). The test asserts that for every key,
the records arrive in the order they were produced — proving that key-shard
parallelism does NOT let later same-key records pass earlier ones.

### `key-hot-key-contention` (KEY mode)

One hot key with 200 records, 30 cold keys with 5 records each. The hot key's shard
becomes the bottleneck; cold-key shards process in parallel. Both must preserve
per-key FIFO. The bench checks that every key's sequence is monotonic AND the hot
key received all 200 records (no shards dropped).

### `key-fifo-survives-retry` (KEY mode)

10 keys × 8 records each. Every 3rd sequence number fails on its first attempt and
succeeds on retry. The retry sleeps on the worker thread that owns the shard, so
later same-key records must wait — this is the load-bearing test that **retry does
not let later same-key records pass earlier ones**. We observe 30 retries fired and
per-key FIFO intact in the final observed order.

### `partition-fifo-survives-retry` (PARTITION mode)

Single partition, 50 records, every 5th retries once. The partition-serial worker
is the same one for all records, so a retry blocks the next record naturally. We
observe 10 retries fired and offset order intact.

### `key-null-keys-collapse-and-order` (KEY mode)

25 null-keyed records on 1 partition. Null keys hash to a deterministic shard
(`Arrays.hashCode(null) % shardCount`), so they all collapse onto one shard and
process strictly serially. Peak concurrent observed: 1 (proves the collapse) and
offsets arrive in order.

### `key-cross-partition-same-key-fifo` (KEY mode)

6 keys × 30 records on a 4-partition topic. With Kafka's default key-aware
partitioner each key always lands on the same partition — and the test prints the
key → partition layout (e.g. `{key1=0, key2=3, key0=2, key5=3, key3=1, key4=1}`)
showing keys distribute across all partitions. Even though records flow through
different partitions concurrently, per-key FIFO holds because each key is owned by
exactly one partition. The test also asserts that no key was observed on more than
one partition (a producer-side partitioner-violation check).

### `key-fifo-under-retry-storm` (KEY mode)

End-to-end stress: 5 000 records across 50 distinct keys (100 records per key) on
a 1-partition topic, `concurrency=24`, `shardCount=96`,
`RetryPolicy.exponential(maxAttempts=5, initialDelay=20 ms, multiplier=1.5)`. The
listener fails records on a deterministic schedule — every 3rd `seq` fails its
first attempt, every 7th its first two, every 11th its first three — producing a
mix of single retries, double retries, and triple retries interleaved across keys.

Even under this churn the dispatcher + retry engine + commit frontier must hold
per-key FIFO: a retry on key `K` at sequence `n` blocks key `K` at sequence `n+1`
on the same shard, while distinct-key shards continue making progress. The test
asserts every key's observed sequence is monotonic, all 5 000 records eventually
land, and the commit frontier never advances past an unfinished offset.

## Reproducing

```bash
./gradlew :test-app:runOrdering
```

Override the broker:

```bash
./gradlew :test-app:runOrdering -PappArgs="--bootstrap broker.example.com:9092"
```

## Related coverage

- Unit tests for the KEY-shard dispatcher itself live in
  [`ClassicKeyShardDispatcherTest`](../../core/src/test/java/io/plurima/kafka/internal/ClassicKeyShardDispatcherTest.java) (8 cases, including
  same-shard serialisation, distinct-key parallelism, per-key FIFO preservation).
- Cross-cluster ordering with two consumers in the same group is verified by
  [`ClassicBasicCrossClusterOrderingIntegrationTest`](../../core/src/test/java/io/plurima/kafka/integration/ClassicBasicCrossClusterOrderingIntegrationTest.java).
- The commit frontier (G2) — out-of-order completion never advances commits past
  unfinished offsets — is unit-tested in
  [`CommitFrontierTest`](../../core/src/test/java/io/plurima/kafka/internal/CommitFrontierTest.java) and verified live in `PlurimaTestApp`'s
  `classic-commit-frontier-no-gap-advance` scenario.
