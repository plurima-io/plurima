package io.plurima.kafka.internal;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies the launcher-rejection recovery paths in all three classic dispatchers.
 *
 * <p>If {@code launcher.launch(...)} throws (typically {@code RejectedExecutionException}
 * during shutdown), the dispatcher must still fire {@code onRecordDone} for every
 * affected record so the caller's {@code inFlight} counter stays balanced. A bug here
 * would manifest as backpressure permanently stalled at the cap (counter > 0 with no
 * workers running to ever decrement).
 *
 * <p>We force the failure by {@code close()}-ing the launcher BEFORE dispatch. Any
 * subsequent {@code executor.execute(...)} throws {@code RejectedExecutionException}.
 */
class DispatcherLauncherFailureTest {

    private static ConsumerRecord<byte[], byte[]> rec(TopicPartition tp, long offset) {
        return new ConsumerRecord<>(tp.topic(), tp.partition(), offset,
            ("k" + offset).getBytes(), ("v" + offset).getBytes());
    }

    private static List<ConsumerRecord<byte[], byte[]>> batch(TopicPartition tp, int count) {
        List<ConsumerRecord<byte[], byte[]>> records = new ArrayList<>();
        for (int i = 0; i < count; i++) records.add(rec(tp, i));
        return records;
    }

    @Test
    void partitionSerialDispatcherFiresOnRecordDoneForEntireBatchWhenLaunchRejected() throws Exception {
        WorkerLauncher launcher = new WorkerLauncher();
        launcher.close();  // subsequent launch() throws RejectedExecutionException

        TopicPartition tp = new TopicPartition("t", 0);
        int total = 10;
        CountDownLatch done = new CountDownLatch(total);

        ClassicPartitionSerialDispatcher d = new ClassicPartitionSerialDispatcher(
            launcher,
            (fr, t, r) -> { throw new AssertionError("listener must not be called when launch failed"); },
            () -> true,
            (__1, __2) -> true);

        // dispatch is a single launcher.launch for the whole batch; failure must fire
        // onRecordDone for EVERY record so inFlight stays balanced.
        d.dispatch(tp, new CommitFrontier(), batch(tp, total), done::countDown);

        assertThat(done.await(2, TimeUnit.SECONDS))
            .as("PartitionSerial: onRecordDone must fire for all %d records when launch rejected", total)
            .isTrue();
    }

    @Test
    void unorderedDispatcherFiresOnRecordDoneForEntireBatchWhenLaunchRejected() throws Exception {
        WorkerLauncher launcher = new WorkerLauncher();
        launcher.close();

        TopicPartition tp = new TopicPartition("t", 0);
        int total = 10;
        CountDownLatch done = new CountDownLatch(total);

        ClassicUnorderedDispatcher d = new ClassicUnorderedDispatcher(
            launcher,
            (fr, t, r) -> { throw new AssertionError("listener must not be called when launch failed"); },
            () -> true,
            (__1, __2) -> true);

        // dispatch is per-record launcher.launch; EACH failure must fire onRecordDone.
        d.dispatch(tp, new CommitFrontier(), batch(tp, total), done::countDown);

        assertThat(done.await(2, TimeUnit.SECONDS))
            .as("Unordered: onRecordDone must fire for all %d records when each launch rejected", total)
            .isTrue();
    }

    @Test
    void keyShardDispatcherFiresOnRecordDoneForEntireBatchWhenLaunchRejected() throws Exception {
        // KeyShard's failure path is more subtle: dispatch adds entries to shard queues
        // BEFORE the busy-CAS triggers launchNext. When launchNext's launcher.launch
        // throws, the catch in launchNext fires onRecordDone for the AFFECTED entry and
        // recurses to drain the queue (each next launchNext also fails → recovery
        // fires onRecordDone again → recurse). Eventually the queue empties.
        //
        // The contract: every entry's onRecordDone fires exactly once regardless of
        // which records actually ran on workers.
        WorkerLauncher launcher = new WorkerLauncher();
        launcher.close();

        TopicPartition tp = new TopicPartition("t", 0);
        int total = 20;
        AtomicInteger doneCount = new AtomicInteger();
        CountDownLatch done = new CountDownLatch(total);

        ClassicKeyShardDispatcher d = new ClassicKeyShardDispatcher(
            8,  // shardCount — multiple shards so we exercise the multi-shard launch-fail path
            launcher,
            (fr, t, r) -> { throw new AssertionError("listener must not be called when launch failed"); },
            () -> true);

        d.dispatch(tp, new CommitFrontier(), batch(tp, total), () -> {
            doneCount.incrementAndGet();
            done.countDown();
        });

        assertThat(done.await(2, TimeUnit.SECONDS))
            .as("KeyShard: onRecordDone must fire for all %d records when each launch rejected", total)
            .isTrue();
        assertThat(doneCount.get())
            .as("KeyShard: exactly once per record")
            .isEqualTo(total);
    }

    @Test
    void keyShardDispatcherSameShardLaunchFailureDrainsAllQueuedEntries() throws Exception {
        // Special case: 50 records on the SAME key (same shard). The first dispatch
        // queues all 50 entries into one shard's queue, then CAS busy → launchNext on
        // the first entry. launchNext throws → recovery fires onRecordDone for entry 1,
        // recurses to entry 2, throws again → onRecordDone for entry 2, recurses to
        // entry 3, ... until the queue is empty.
        //
        // This exercises the recursion depth path. 50 records means 50 recursive
        // launchNext calls. If recovery weren't recursing properly, only the first
        // entry would fire and the rest would be stuck.
        WorkerLauncher launcher = new WorkerLauncher();
        launcher.close();

        TopicPartition tp = new TopicPartition("t", 0);
        int total = 50;
        CountDownLatch done = new CountDownLatch(total);

        ClassicKeyShardDispatcher d = new ClassicKeyShardDispatcher(
            16, launcher,
            (fr, t, r) -> { throw new AssertionError("listener must not be called"); },
            () -> true);

        // All 50 records have the same key → same shard.
        List<ConsumerRecord<byte[], byte[]>> records = new ArrayList<>();
        for (int i = 0; i < total; i++) {
            records.add(new ConsumerRecord<>("t", 0, i, "same-key".getBytes(), ("v" + i).getBytes()));
        }
        d.dispatch(tp, new CommitFrontier(), records, done::countDown);

        assertThat(done.await(2, TimeUnit.SECONDS))
            .as("KeyShard: recursive launch-failure recovery must drain a 50-entry same-key shard")
            .isTrue();
    }
}
