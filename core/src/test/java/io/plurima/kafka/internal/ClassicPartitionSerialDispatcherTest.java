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
}
