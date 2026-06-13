package io.plurima.kafka.internal;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link ClassicKeyShardDispatcher}. Verifies the load-bearing invariants:
 * (1) records with the same key are serialised, (2) records with different keys run
 * concurrently, (3) per-key offset order is preserved.
 */
class ClassicKeyShardDispatcherTest {

    private WorkerLauncher launcher;

    @BeforeEach
    void setUp() {
        launcher = new WorkerLauncher();
    }

    @AfterEach
    void tearDown() {
        launcher.close();
    }

    private static ConsumerRecord<byte[], byte[]> rec(TopicPartition tp, long offset, String key) {
        return new ConsumerRecord<>(
            tp.topic(), tp.partition(), offset,
            key.getBytes(),
            ("v" + offset).getBytes());
    }

    @Test
    void sameKeyRecordsAreSerialised() throws Exception {
        TopicPartition tp = new TopicPartition("t", 0);
        // Use shardCount=1 to force everything onto the same shard regardless of key —
        // this proves the SHARD-level serialisation contract (which subsumes the
        // same-key serialisation contract: same key → same shard → serialised).
        int totalRecords = 20;
        AtomicInteger concurrentlyRunning = new AtomicInteger();
        AtomicInteger maxConcurrent = new AtomicInteger();
        CountDownLatch done = new CountDownLatch(totalRecords);

        ClassicRecordProcessor processOne = (fr, t, r) -> {
            int now = concurrentlyRunning.incrementAndGet();
            maxConcurrent.updateAndGet(prev -> Math.max(prev, now));
            try { Thread.sleep(20); }
            catch (InterruptedException ie) { Thread.currentThread().interrupt(); }
            concurrentlyRunning.decrementAndGet();
        };

        ClassicKeyShardDispatcher d = new ClassicKeyShardDispatcher(
            1, launcher, processOne, () -> true);

        List<ConsumerRecord<byte[], byte[]>> records = new ArrayList<>();
        for (int i = 0; i < totalRecords; i++) {
            records.add(rec(tp, i, "k" + i));  // varied keys, but shardCount=1 collapses all
        }
        d.dispatch(tp, new CommitFrontier(), records, done::countDown);
        assertThat(done.await(5, TimeUnit.SECONDS)).isTrue();

        assertThat(maxConcurrent.get())
            .as("shard with capacity 1 must run records strictly serially")
            .isEqualTo(1);
    }

    @Test
    void differentKeysRunConcurrently() throws Exception {
        TopicPartition tp = new TopicPartition("t", 0);
        // With shardCount large enough that distinct keys land on distinct shards, we
        // should see parallelism. Use 16 distinct keys + shardCount=64 — collision rate
        // is negligible for this small a key set.
        int distinctKeys = 16;
        int recordsPerKey = 2;
        int totalRecords = distinctKeys * recordsPerKey;

        AtomicInteger concurrentlyRunning = new AtomicInteger();
        AtomicInteger maxConcurrent = new AtomicInteger();
        CountDownLatch done = new CountDownLatch(totalRecords);

        ClassicRecordProcessor processOne = (fr, t, r) -> {
            int now = concurrentlyRunning.incrementAndGet();
            maxConcurrent.updateAndGet(prev -> Math.max(prev, now));
            // Hold long enough that all distinct-key workers can be in flight together.
            try { Thread.sleep(100); }
            catch (InterruptedException ie) { Thread.currentThread().interrupt(); }
            concurrentlyRunning.decrementAndGet();
        };

        ClassicKeyShardDispatcher d = new ClassicKeyShardDispatcher(
            64, launcher, processOne, () -> true);

        List<ConsumerRecord<byte[], byte[]>> records = new ArrayList<>();
        // Interleave keys so the dispatcher sees a realistic mix per dispatch call.
        for (int round = 0; round < recordsPerKey; round++) {
            for (int k = 0; k < distinctKeys; k++) {
                records.add(rec(tp, (long) round * distinctKeys + k, "key-" + k));
            }
        }
        d.dispatch(tp, new CommitFrontier(), records, done::countDown);
        assertThat(done.await(10, TimeUnit.SECONDS)).isTrue();

        // We expect parallelism on the order of distinctKeys; require at least 4 to be
        // safe across CI noise. The point is "much > 1", not the exact peak.
        assertThat(maxConcurrent.get())
            .as("distinct keys must run in parallel across shards; observed peak concurrency")
            .isGreaterThanOrEqualTo(4);
    }

    @Test
    void perKeyOffsetOrderIsPreserved() throws Exception {
        TopicPartition tp = new TopicPartition("t", 0);
        // Even with intra-partition parallelism, records with the same key must complete
        // in the order they were dispatched (which is offset-ascending). Track per-key
        // observed order; assert each key's list is monotonically increasing.
        int distinctKeys = 8;
        int recordsPerKey = 25;
        int totalRecords = distinctKeys * recordsPerKey;

        Map<String, List<Long>> observedByKey = new ConcurrentHashMap<>();
        CountDownLatch done = new CountDownLatch(totalRecords);

        ClassicRecordProcessor processOne = (fr, t, r) -> {
            String k = new String(r.key());
            observedByKey.computeIfAbsent(k, x -> new CopyOnWriteArrayList<>()).add(r.offset());
            // Vary work time slightly to encourage reordering across shards.
            try { Thread.sleep((r.offset() % 3) * 5L); }
            catch (InterruptedException ie) { Thread.currentThread().interrupt(); }
        };

        ClassicKeyShardDispatcher d = new ClassicKeyShardDispatcher(
            32, launcher, processOne, () -> true);

        List<ConsumerRecord<byte[], byte[]>> records = new ArrayList<>();
        // Build round-robin so the dispatcher sees keys interleaved (typical broker output).
        for (int round = 0; round < recordsPerKey; round++) {
            for (int k = 0; k < distinctKeys; k++) {
                long offset = (long) round * distinctKeys + k;
                records.add(rec(tp, offset, "k" + k));
            }
        }
        d.dispatch(tp, new CommitFrontier(), records, done::countDown);
        assertThat(done.await(15, TimeUnit.SECONDS)).isTrue();

        assertThat(observedByKey).hasSize(distinctKeys);
        observedByKey.forEach((key, offsets) -> {
            // The list must be monotonically increasing — strict per-key FIFO inside the
            // shard, regardless of cross-shard interleaving.
            List<Long> sorted = new ArrayList<>(offsets);
            sorted.sort(Long::compareTo);
            assertThat(offsets)
                .as("per-key FIFO for key=%s", key)
                .isEqualTo(sorted);
        });
    }

    @Test
    void onRecordDoneFiresOncePerRecord() throws Exception {
        TopicPartition tp = new TopicPartition("t", 0);
        int totalRecords = 50;
        AtomicInteger doneCount = new AtomicInteger();
        CountDownLatch done = new CountDownLatch(totalRecords);

        ClassicRecordProcessor processOne = (fr, t, r) -> {
            // no-op handler
        };

        ClassicKeyShardDispatcher d = new ClassicKeyShardDispatcher(
            16, launcher, processOne, () -> true);

        List<ConsumerRecord<byte[], byte[]>> records = new ArrayList<>();
        for (int i = 0; i < totalRecords; i++) {
            records.add(rec(tp, i, "k" + (i % 10)));
        }
        d.dispatch(tp, new CommitFrontier(), records, () -> {
            doneCount.incrementAndGet();
            done.countDown();
        });
        assertThat(done.await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(doneCount.get()).isEqualTo(totalRecords);
    }

    @Test
    void handlerExceptionStillCountsDownLatch() throws Exception {
        // If processOne throws (e.g. a misbehaving listener with no try/catch at the
        // dispatcher boundary), onRecordDone must still fire. Otherwise the poll thread
        // deadlocks waiting on a latch that will never reach zero.
        TopicPartition tp = new TopicPartition("t", 0);
        int totalRecords = 10;
        CountDownLatch done = new CountDownLatch(totalRecords);

        ClassicRecordProcessor processOne = (fr, t, r) ->
            { throw new RuntimeException("boom"); };

        ClassicKeyShardDispatcher d = new ClassicKeyShardDispatcher(
            4, launcher, processOne, () -> true);

        List<ConsumerRecord<byte[], byte[]>> records = new ArrayList<>();
        for (int i = 0; i < totalRecords; i++) {
            records.add(rec(tp, i, "k" + (i % 3)));
        }
        d.dispatch(tp, new CommitFrontier(), records, done::countDown);

        assertThat(done.await(5, TimeUnit.SECONDS))
            .as("onRecordDone must fire even when processOne throws")
            .isTrue();
    }

    @Test
    void purgePartitionsDropsQueuedRecordsForRevokedPartition() {
        TopicPartition tp0 = new TopicPartition("t", 0);
        TopicPartition tp1 = new TopicPartition("t", 1);
        // Block all records in processOne so they accumulate in shards.
        CountDownLatch blocker = new CountDownLatch(1);
        ClassicRecordProcessor processOne = (fr, t, r) -> {
            try { blocker.await(); }
            catch (InterruptedException ie) { Thread.currentThread().interrupt(); }
        };

        ClassicKeyShardDispatcher d = new ClassicKeyShardDispatcher(
            8, launcher, processOne, () -> true);

        List<ConsumerRecord<byte[], byte[]>> recs0 = new ArrayList<>();
        for (int i = 0; i < 10; i++) recs0.add(rec(tp0, i, "k" + i));
        List<ConsumerRecord<byte[], byte[]>> recs1 = new ArrayList<>();
        for (int i = 0; i < 10; i++) recs1.add(rec(tp1, i, "k" + i));

        d.dispatch(tp0, new CommitFrontier(), recs0, () -> {});
        d.dispatch(tp1, new CommitFrontier(), recs1, () -> {});
        // tp0 and tp1 each occupy some number of shards.
        int beforePurge = d.activeShardCount();
        assertThat(beforePurge).isGreaterThan(0);

        d.purgePartitions(List.of(tp0));
        int afterPurge = d.activeShardCount();
        assertThat(afterPurge)
            .as("purge of tp0 should remove tp0 shards, leaving tp1 shards behind")
            .isLessThan(beforePurge)
            .isGreaterThan(0);

        // Release blocked workers so the launcher can shut down cleanly.
        blocker.countDown();
    }

    @Test
    void nullKeysCollapseToOneShard() {
        TopicPartition tp = new TopicPartition("t", 0);
        ClassicKeyShardDispatcher d = new ClassicKeyShardDispatcher(
            16, launcher, (fr, t, r) -> {}, () -> true);

        // All null-keyed records hash to the same shard index; verify deterministically.
        int idx1 = d.shardIndexFor(null);
        int idx2 = d.shardIndexFor(null);
        assertThat(idx1).isEqualTo(idx2);

        // Distinct non-null keys SHOULD generally map to different shards (16 keys, 16
        // shards: collisions possible but rare). At least verify the index is in range.
        for (int i = 0; i < 16; i++) {
            int idx = d.shardIndexFor(("k" + i).getBytes());
            assertThat(idx).isBetween(0, 15);
        }
    }

    @Test
    void fastHandlerRaceDoesNotStallShard() throws Exception {
        // The lock-free busy-flag pattern has a known race: a producer adds an entry,
        // tries CAS busy false→true, but a worker may have JUST set busy=false between
        // its poll-returns-null and our CAS. The double-check (worker peeks queue after
        // setting busy=false and reclaims via CAS if non-empty) is the load-bearing
        // recovery. This test exercises the race by interleaving rapid dispatch with
        // handlers that finish in microseconds — so producer-vs-worker CAS contention
        // is at its worst — and verifies every record still runs (no shard stall).
        TopicPartition tp = new TopicPartition("t", 0);
        int totalRecords = 50_000;
        AtomicInteger seen = new AtomicInteger();
        CountDownLatch done = new CountDownLatch(totalRecords);

        ClassicRecordProcessor processOne = (fr, t, r) -> {
            seen.incrementAndGet();
            // Empty body — let the handler finish as fast as possible to maximize
            // the busy=false / queue.add race window.
        };

        ClassicKeyShardDispatcher d = new ClassicKeyShardDispatcher(
            16, launcher, processOne, () -> true);

        // Dispatch from a separate thread so this thread can call dispatch() in
        // bursts of small batches concurrently with worker completions.
        Thread dispatcher = new Thread(() -> {
            int sent = 0;
            int batchSize = 50;
            while (sent < totalRecords) {
                int n = Math.min(batchSize, totalRecords - sent);
                List<ConsumerRecord<byte[], byte[]>> batch = new ArrayList<>(n);
                for (int i = 0; i < n; i++) {
                    // Mix of keys to spread across shards; one record per partition.
                    batch.add(rec(tp, sent + i, "k" + ((sent + i) % 64)));
                }
                d.dispatch(tp, new CommitFrontier(), batch, done::countDown);
                sent += n;
            }
        }, "rapid-dispatcher");
        dispatcher.start();

        assertThat(done.await(30, TimeUnit.SECONDS))
            .as("every record must run — busy-flag race must not stall any shard")
            .isTrue();
        dispatcher.join(5_000);
        assertThat(seen.get())
            .as("seen-count must equal dispatch count")
            .isEqualTo(totalRecords);
    }

    @Test
    void shardIndexDistributesAcrossShards() {
        TopicPartition tp = new TopicPartition("t", 0);
        ClassicKeyShardDispatcher d = new ClassicKeyShardDispatcher(
            32, launcher, (fr, t, r) -> {}, () -> true);

        // With many keys, we should see most shards used.
        Map<Integer, Integer> hist = new HashMap<>();
        for (int i = 0; i < 1000; i++) {
            int idx = d.shardIndexFor(("key-" + i).getBytes());
            hist.merge(idx, 1, Integer::sum);
        }
        // At least 24 of 32 shards should see traffic (75% coverage with 1000 samples).
        assertThat(hist.size())
            .as("key-shard hash should distribute roughly evenly across the shard ring")
            .isGreaterThanOrEqualTo(24);
    }
}
