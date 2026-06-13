package io.plurima.kafka.internal;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiPredicate;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link ClassicUnorderedDispatcher}: every record runs on its own worker,
 * records on the same partition run concurrently, frontier-identity guard skips
 * revoked records, and {@code onRecordDone} fires exactly once per record across
 * the success / skip paths.
 */
class ClassicUnorderedDispatcherTest {

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
        return new ConsumerRecord<>(tp.topic(), tp.partition(), offset,
            ("k" + offset).getBytes(), ("v" + offset).getBytes());
    }

    @Test
    void recordsOnSamePartitionRunConcurrently() throws Exception {
        TopicPartition tp = new TopicPartition("t", 0);
        int total = 64;
        AtomicInteger concurrentlyRunning = new AtomicInteger();
        AtomicInteger maxConcurrent = new AtomicInteger();
        CountDownLatch done = new CountDownLatch(total);

        ClassicRecordProcessor processOne = (fr, t, r) -> {
            int now = concurrentlyRunning.incrementAndGet();
            maxConcurrent.updateAndGet(prev -> Math.max(prev, now));
            try { Thread.sleep(50); }
            catch (InterruptedException ie) { Thread.currentThread().interrupt(); }
            concurrentlyRunning.decrementAndGet();
        };

        ClassicUnorderedDispatcher d = new ClassicUnorderedDispatcher(
            launcher, processOne, () -> true, (__1, __2) -> true);

        List<ConsumerRecord<byte[], byte[]>> records = new ArrayList<>();
        for (int i = 0; i < total; i++) records.add(rec(tp, i));
        d.dispatch(tp, new CommitFrontier(), records, done::countDown);

        assertThat(done.await(15, TimeUnit.SECONDS)).isTrue();
        // Headline UNORDERED guarantee: distinct records on the same partition DO
        // run concurrently. Concrete threshold ≥ 8 (much > 1) is conservative for
        // CI noise.
        assertThat(maxConcurrent.get())
            .as("UNORDERED must launch records concurrently; observed peak")
            .isGreaterThanOrEqualTo(8);
    }

    @Test
    void onRecordDoneFiresExactlyOncePerRecord() throws Exception {
        TopicPartition tp = new TopicPartition("t", 0);
        int total = 30;
        AtomicInteger doneCount = new AtomicInteger();
        CountDownLatch done = new CountDownLatch(total);

        ClassicRecordProcessor processOne = (fr, t, r) -> {};

        ClassicUnorderedDispatcher d = new ClassicUnorderedDispatcher(
            launcher, processOne, () -> true, (__1, __2) -> true);

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
    void stillOwnedFalseSkipsListenerButFiresOnRecordDone() throws Exception {
        // stillOwned returns false right out of the gate (simulating "partition
        // revoked before workers run"). Listener must NOT be called for any record,
        // but onRecordDone MUST fire for every record so inFlight stays balanced.
        TopicPartition tp = new TopicPartition("t", 0);
        int total = 20;
        AtomicInteger listenerCalls = new AtomicInteger();
        CountDownLatch done = new CountDownLatch(total);

        ClassicRecordProcessor processOne = (fr, t, r) -> listenerCalls.incrementAndGet();
        BiPredicate<TopicPartition, CommitFrontier> alwaysFalse = (__1, __2) -> false;

        ClassicUnorderedDispatcher d = new ClassicUnorderedDispatcher(
            launcher, processOne, () -> true, alwaysFalse);

        List<ConsumerRecord<byte[], byte[]>> records = new ArrayList<>();
        for (int i = 0; i < total; i++) records.add(rec(tp, i));
        d.dispatch(tp, new CommitFrontier(), records, done::countDown);

        assertThat(done.await(5, TimeUnit.SECONDS))
            .as("onRecordDone fires for every record even when ownership check fails")
            .isTrue();
        assertThat(listenerCalls.get())
            .as("listener must NOT run when stillOwned returns false")
            .isZero();
    }

    @Test
    void runningFlippedFalseSkipsListenerButFiresOnRecordDone() throws Exception {
        TopicPartition tp = new TopicPartition("t", 0);
        int total = 15;
        AtomicInteger listenerCalls = new AtomicInteger();
        CountDownLatch done = new CountDownLatch(total);

        ClassicRecordProcessor processOne = (fr, t, r) -> listenerCalls.incrementAndGet();

        ClassicUnorderedDispatcher d = new ClassicUnorderedDispatcher(
            launcher, processOne, () -> false /* running=false */, (__1, __2) -> true);

        List<ConsumerRecord<byte[], byte[]>> records = new ArrayList<>();
        for (int i = 0; i < total; i++) records.add(rec(tp, i));
        d.dispatch(tp, new CommitFrontier(), records, done::countDown);

        assertThat(done.await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(listenerCalls.get()).isZero();
    }

    @Test
    void handlerExceptionStillFiresOnRecordDone() throws Exception {
        // Same exception-safety contract as the other dispatchers: even when processOne
        // throws, onRecordDone must fire so inFlight stays balanced.
        TopicPartition tp = new TopicPartition("t", 0);
        int total = 10;
        CountDownLatch done = new CountDownLatch(total);

        ClassicRecordProcessor processOne = (fr, t, r) ->
            { throw new RuntimeException("boom"); };

        ClassicUnorderedDispatcher d = new ClassicUnorderedDispatcher(
            launcher, processOne, () -> true, (__1, __2) -> true);

        List<ConsumerRecord<byte[], byte[]>> records = new ArrayList<>();
        for (int i = 0; i < total; i++) records.add(rec(tp, i));
        d.dispatch(tp, new CommitFrontier(), records, done::countDown);

        assertThat(done.await(5, TimeUnit.SECONDS))
            .as("onRecordDone fires even when processOne throws")
            .isTrue();
    }
}
