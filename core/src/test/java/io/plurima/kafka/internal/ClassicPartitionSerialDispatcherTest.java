package io.plurima.kafka.internal;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link ClassicPartitionSerialDispatcher}: one worker per partition,
 * records processed strictly in offset order, {@code onRecordDone} fires per record,
 * exceptions don't break the latch, and a flipped {@code running} flag stops dispatch.
 */
class ClassicPartitionSerialDispatcherTest {

    private WorkerLauncher launcher;

    @BeforeEach
    void setUp() {
        launcher = new WorkerLauncher();
    }

    @AfterEach
    void tearDown() {
        launcher.close();
    }

    private static ConsumerRecord<byte[], byte[]> rec(TopicPartition tp, long offset) {
        return new ConsumerRecord<>(
            tp.topic(), tp.partition(), offset,
            ("k" + offset).getBytes(),
            ("v" + offset).getBytes());
    }

    @Test
    void recordsOnSinglePartitionProcessSerially() throws Exception {
        TopicPartition tp = new TopicPartition("t", 0);
        int total = 100;
        AtomicInteger concurrentlyRunning = new AtomicInteger();
        AtomicInteger maxConcurrent = new AtomicInteger();
        CountDownLatch done = new CountDownLatch(total);

        ClassicRecordProcessor processOne = (fr, t, r) -> {
            int now = concurrentlyRunning.incrementAndGet();
            maxConcurrent.updateAndGet(prev -> Math.max(prev, now));
            try { Thread.sleep(2); }
            catch (InterruptedException ie) { Thread.currentThread().interrupt(); }
            concurrentlyRunning.decrementAndGet();
        };

        ClassicPartitionSerialDispatcher d = new ClassicPartitionSerialDispatcher(launcher, processOne, () -> true, (__tp, __fr) -> true);

        List<ConsumerRecord<byte[], byte[]>> records = new ArrayList<>();
        for (int i = 0; i < total; i++) records.add(rec(tp, i));
        d.dispatch(tp, new CommitFrontier(), records, done::countDown);

        assertThat(done.await(10, TimeUnit.SECONDS)).isTrue();
        assertThat(maxConcurrent.get())
            .as("partition-serial dispatch must run one record at a time on a single partition")
            .isEqualTo(1);
    }

    @Test
    void recordsArriveInOffsetOrder() throws Exception {
        TopicPartition tp = new TopicPartition("t", 0);
        int total = 50;
        List<Long> observed = new CopyOnWriteArrayList<>();
        CountDownLatch done = new CountDownLatch(total);

        ClassicRecordProcessor processOne = (fr, t, r) -> {
            observed.add(r.offset());
            // Vary work time slightly to make any reordering visible.
            try { Thread.sleep((r.offset() % 3) * 2L); }
            catch (InterruptedException ie) { Thread.currentThread().interrupt(); }
        };

        ClassicPartitionSerialDispatcher d = new ClassicPartitionSerialDispatcher(launcher, processOne, () -> true, (__tp, __fr) -> true);

        List<ConsumerRecord<byte[], byte[]>> records = new ArrayList<>();
        for (int i = 0; i < total; i++) records.add(rec(tp, i));
        d.dispatch(tp, new CommitFrontier(), records, done::countDown);

        assertThat(done.await(15, TimeUnit.SECONDS)).isTrue();
        List<Long> expected = new ArrayList<>();
        for (long i = 0; i < total; i++) expected.add(i);
        assertThat(observed).as("records must arrive in offset order").isEqualTo(expected);
    }

    @Test
    void onRecordDoneFiresExactlyOncePerRecord() throws Exception {
        TopicPartition tp = new TopicPartition("t", 0);
        int total = 30;
        AtomicInteger doneCount = new AtomicInteger();
        CountDownLatch done = new CountDownLatch(total);

        ClassicRecordProcessor processOne = (fr, t, r) -> {};

        ClassicPartitionSerialDispatcher d = new ClassicPartitionSerialDispatcher(launcher, processOne, () -> true, (__tp, __fr) -> true);

        List<ConsumerRecord<byte[], byte[]>> records = new ArrayList<>();
        for (int i = 0; i < total; i++) records.add(rec(tp, i));
        d.dispatch(tp, new CommitFrontier(), records, () -> {
            doneCount.incrementAndGet();
            done.countDown();
        });

        assertThat(done.await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(doneCount.get()).isEqualTo(total);
    }

    @Test
    void handlerExceptionStillCountsDownLatch() throws Exception {
        // If processOne throws, the dispatcher must still call onRecordDone so the
        // poll-thread latch isn't deadlocked.
        TopicPartition tp = new TopicPartition("t", 0);
        int total = 10;
        CountDownLatch done = new CountDownLatch(total);

        ClassicRecordProcessor processOne = (fr, t, r) ->
            { throw new RuntimeException("boom"); };

        ClassicPartitionSerialDispatcher d = new ClassicPartitionSerialDispatcher(launcher, processOne, () -> true, (__tp, __fr) -> true);

        List<ConsumerRecord<byte[], byte[]>> records = new ArrayList<>();
        for (int i = 0; i < total; i++) records.add(rec(tp, i));
        d.dispatch(tp, new CommitFrontier(), records, done::countDown);

        assertThat(done.await(5, TimeUnit.SECONDS))
            .as("onRecordDone must fire even when processOne throws")
            .isTrue();
    }

    @Test
    void runningFlippedFalseAbortsRemainingRecords() throws Exception {
        // Shutdown mid-batch: the dispatcher should observe running=false and stop
        // calling processOne for remaining records, but MUST still call onRecordDone
        // for each so the latch doesn't deadlock.
        TopicPartition tp = new TopicPartition("t", 0);
        int total = 20;
        AtomicBoolean running = new AtomicBoolean(true);
        AtomicInteger processed = new AtomicInteger();
        CountDownLatch done = new CountDownLatch(total);

        ClassicRecordProcessor processOne = (fr, t, r) -> {
            int p = processed.incrementAndGet();
            // Flip running false after 5 records.
            if (p == 5) running.set(false);
        };

        ClassicPartitionSerialDispatcher d = new ClassicPartitionSerialDispatcher(launcher, processOne, running::get, (__tp, __fr) -> true);

        List<ConsumerRecord<byte[], byte[]>> records = new ArrayList<>();
        for (int i = 0; i < total; i++) records.add(rec(tp, i));
        d.dispatch(tp, new CommitFrontier(), records, done::countDown);

        assertThat(done.await(5, TimeUnit.SECONDS))
            .as("onRecordDone must fire for ALL records even after running flipped false")
            .isTrue();
        assertThat(processed.get())
            .as("processOne must NOT be called for records dispatched after running flipped")
            .isEqualTo(5);
    }

    @Test
    void differentPartitionsRunOnSeparateWorkers() throws Exception {
        // Each partition gets its own worker, so two partitions can run concurrently.
        // We hold both workers in their handler until both have entered; if dispatch
        // somehow shared a worker, the second handler would never enter and the test
        // would time out.
        TopicPartition tp0 = new TopicPartition("t", 0);
        TopicPartition tp1 = new TopicPartition("t", 1);
        CountDownLatch bothEntered = new CountDownLatch(2);
        CountDownLatch holdHandlers = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(2);

        ClassicRecordProcessor processOne = (fr, t, r) -> {
            bothEntered.countDown();
            try { holdHandlers.await(5, TimeUnit.SECONDS); }
            catch (InterruptedException ie) { Thread.currentThread().interrupt(); }
        };

        ClassicPartitionSerialDispatcher d = new ClassicPartitionSerialDispatcher(launcher, processOne, () -> true, (__tp, __fr) -> true);

        d.dispatch(tp0, new CommitFrontier(), List.of(rec(tp0, 0)), done::countDown);
        d.dispatch(tp1, new CommitFrontier(), List.of(rec(tp1, 0)), done::countDown);

        assertThat(bothEntered.await(3, TimeUnit.SECONDS))
            .as("both partition workers must enter their handler concurrently")
            .isTrue();
        holdHandlers.countDown();
        assertThat(done.await(3, TimeUnit.SECONDS)).isTrue();
    }

    @Test
    void overlappingBatchesForSamePartitionDoNotInterleave() throws Exception {
        // The load-bearing PARTITION-mode invariant: two poll batches for the SAME
        // partition must NOT run concurrently. Batch 1 (offsets 0..2) blocks inside the
        // handler on `batch1Blocking`; while it's held we dispatch batch 2 (offsets 3..5).
        // With the buggy per-batch-worker design, batch 2 launches its own worker and its
        // records run immediately → batch2Started flips true. With the chain design, batch
        // 2's entries queue behind batch 1 on the same partition chain and stay parked
        // until batch 1's worker drains them — so batch2Started stays false, and the final
        // observed order is strict offset order across both batches.
        TopicPartition tp = new TopicPartition("t", 0);
        List<Long> observed = new CopyOnWriteArrayList<>();
        AtomicBoolean batch2Started = new AtomicBoolean(false);
        CountDownLatch batch1Blocking = new CountDownLatch(1);
        CountDownLatch batch1FirstEntered = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(6);

        ClassicRecordProcessor processOne = (fr, t, r) -> {
            if (r.offset() >= 3) {
                batch2Started.set(true);
            }
            observed.add(r.offset());
            if (r.offset() == 0L) {
                // Signal that batch 1's worker is running, then block so batch 2 is
                // dispatched while batch 1 still holds the partition.
                batch1FirstEntered.countDown();
                try { batch1Blocking.await(5, TimeUnit.SECONDS); }
                catch (InterruptedException ie) { Thread.currentThread().interrupt(); }
            }
        };

        ClassicPartitionSerialDispatcher d = new ClassicPartitionSerialDispatcher(
            launcher, processOne, () -> true, (__tp, __fr) -> true);

        List<ConsumerRecord<byte[], byte[]>> batch1 = new ArrayList<>();
        for (int i = 0; i < 3; i++) batch1.add(rec(tp, i));
        List<ConsumerRecord<byte[], byte[]>> batch2 = new ArrayList<>();
        for (int i = 3; i < 6; i++) batch2.add(rec(tp, i));

        d.dispatch(tp, new CommitFrontier(), batch1, done::countDown);
        // Wait until batch 1's worker is actually running (and blocked) before dispatching
        // batch 2, so the test deterministically exercises the overlap.
        assertThat(batch1FirstEntered.await(3, TimeUnit.SECONDS)).isTrue();
        d.dispatch(tp, new CommitFrontier(), batch2, done::countDown);

        // Poll the flag for ~200ms: batch 2 must NOT begin while batch 1 is blocked.
        for (int i = 0; i < 20; i++) {
            assertThat(batch2Started.get())
                .as("batch 2 must not start while batch 1 still owns the partition chain")
                .isFalse();
            Thread.sleep(10);
        }

        // Release batch 1; everything must now drain in strict offset order.
        batch1Blocking.countDown();
        assertThat(done.await(5, TimeUnit.SECONDS)).isTrue();

        List<Long> sorted = new ArrayList<>(observed);
        sorted.sort(Long::compareTo);
        assertThat(observed)
            .as("records across overlapping batches must be processed in offset order")
            .isEqualTo(sorted)
            .containsExactly(0L, 1L, 2L, 3L, 4L, 5L);
    }

    /**
     * F3 — mirrors {@code ClassicKeyShardDispatcherTest.launchRejectionDrainsDeepQueueIteratively}
     * for the partition-serial dispatcher. A worker holds the chain busy while 200k entries
     * queue behind it, then the launcher is shut down so EVERY subsequent {@code launch()}
     * throws {@code RejectedExecutionException}. When the blocked worker finally finishes and
     * calls {@code launchNext}, a recursive rejection-recovery implementation would recurse
     * once per queued entry on a single thread — an unbounded stack-depth hazard. The fix
     * makes {@code launchNext} iterative (same shape as ClassicKeyShardDispatcher), so depth
     * is O(1) regardless of backlog size and {@code onRecordDone} still fires exactly once
     * per record.
     */
    @Test
    void launchRejectionDrainsDeepQueueIteratively() throws Exception {
        TopicPartition tp = new TopicPartition("t", 0);
        WorkerLauncher rejectingLauncher = new WorkerLauncher();

        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        int deepQueueSize = 200_000;
        AtomicInteger doneCount = new AtomicInteger();
        CountDownLatch done = new CountDownLatch(deepQueueSize + 1);

        ClassicRecordProcessor processOne = (fr, t, r) -> {
            if (r.offset() == 0) {
                started.countDown();
                try { release.await(10, TimeUnit.SECONDS); }
                catch (InterruptedException ie) { Thread.currentThread().interrupt(); }
            }
            // Records after offset 0 are never actually reached here — once the
            // launcher is shut down below, every launch() attempt for them is
            // rejected before processOne would run.
        };

        ClassicPartitionSerialDispatcher d = new ClassicPartitionSerialDispatcher(
            rejectingLauncher, processOne, () -> true, (__tp, __fr) -> true);

        // Record 0 claims the chain and blocks in processOne, holding busy=true so the
        // backlog below just accumulates without triggering launchNext yet.
        d.dispatch(tp, new CommitFrontier(),
            List.of(rec(tp, 0)),
            () -> { doneCount.incrementAndGet(); done.countDown(); });
        assertThat(started.await(5, TimeUnit.SECONDS)).isTrue();

        List<ConsumerRecord<byte[], byte[]>> backlog = new ArrayList<>(deepQueueSize);
        for (int i = 1; i <= deepQueueSize; i++) {
            backlog.add(rec(tp, i));
        }
        d.dispatch(tp, new CommitFrontier(), backlog,
            () -> { doneCount.incrementAndGet(); done.countDown(); });

        // Shut the launcher down NOW so every subsequent launch() throws
        // RejectedExecutionException, then release the blocked worker. Its finally
        // block calls launchNext, which must drain the entire 200k backlog through
        // the rejection-recovery path in one go.
        rejectingLauncher.close(java.time.Duration.ZERO);
        release.countDown();

        assertThat(done.await(30, TimeUnit.SECONDS))
            .as("every record's onRecordDone must fire, and draining a deep "
                + "launch-rejection backlog must not stack-overflow the worker thread")
            .isTrue();
        assertThat(doneCount.get()).isEqualTo(deepQueueSize + 1);
    }

    @Test
    void stillOwnedFalseSkipsProcessingButStillFiresOnRecordDone() throws Exception {
        // The revoke-skip branch: !stillOwned.test(tp, frontier). Every other test above
        // passes (tp, fr) -> true. Here we flip an AtomicBoolean-backed predicate to false
        // mid-stream (simulating a revoke landing between records) and assert the
        // processor is NOT invoked for the entries dispatched after the flip, while
        // onRecordDone still fires exactly once for EVERY entry regardless.
        TopicPartition tp = new TopicPartition("t", 0);
        int total = 10;
        AtomicBoolean owned = new AtomicBoolean(true);
        List<Long> processedOffsets = new CopyOnWriteArrayList<>();
        CountDownLatch done = new CountDownLatch(total);

        ClassicRecordProcessor processOne = (fr, t, r) -> {
            processedOffsets.add(r.offset());
            // Flip ownership false after the 4th processed record (offsets 0..3 processed).
            if (r.offset() == 3L) owned.set(false);
        };

        ClassicPartitionSerialDispatcher d = new ClassicPartitionSerialDispatcher(
            launcher, processOne, () -> true, (__tp, __fr) -> owned.get());

        List<ConsumerRecord<byte[], byte[]>> records = new ArrayList<>();
        for (int i = 0; i < total; i++) records.add(rec(tp, i));
        d.dispatch(tp, new CommitFrontier(), records, done::countDown);

        assertThat(done.await(5, TimeUnit.SECONDS))
            .as("onRecordDone must fire for every entry, including those skipped by the "
                + "stillOwned==false branch")
            .isTrue();
        assertThat(processedOffsets)
            .as("the processor must NOT be invoked for any entry dispatched/drained after "
                + "stillOwned flips false")
            .containsExactly(0L, 1L, 2L, 3L);
    }

    @Test
    void differentPartitionsRunConcurrently() throws Exception {
        // Serialising same-partition batches must NOT serialise DIFFERENT partitions:
        // per-partition chains run independently. Dispatch a blocking batch to each of
        // two partitions; both handlers must enter concurrently (proving distinct chains
        // launch distinct workers).
        TopicPartition tp0 = new TopicPartition("t", 0);
        TopicPartition tp1 = new TopicPartition("t", 1);
        CountDownLatch bothEntered = new CountDownLatch(2);
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(2);

        ClassicRecordProcessor processOne = (fr, t, r) -> {
            bothEntered.countDown();
            try { release.await(5, TimeUnit.SECONDS); }
            catch (InterruptedException ie) { Thread.currentThread().interrupt(); }
        };

        ClassicPartitionSerialDispatcher d = new ClassicPartitionSerialDispatcher(
            launcher, processOne, () -> true, (__tp, __fr) -> true);

        d.dispatch(tp0, new CommitFrontier(), List.of(rec(tp0, 0)), done::countDown);
        d.dispatch(tp1, new CommitFrontier(), List.of(rec(tp1, 0)), done::countDown);

        assertThat(bothEntered.await(3, TimeUnit.SECONDS))
            .as("distinct partitions must run their chains concurrently")
            .isTrue();
        release.countDown();
        assertThat(done.await(3, TimeUnit.SECONDS)).isTrue();
    }
}
