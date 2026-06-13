package io.plurima.kafka.internal;

import io.plurima.kafka.OrderingMode;
import io.plurima.kafka.RecordListener;
import io.plurima.kafka.deserializer.RecordDeserializer;
import io.plurima.kafka.metrics.PlurimaMetrics;
import io.plurima.kafka.retry.RetryPolicy;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.OptionalLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

/**
 * Generation-guard tests for the rebalance-revoke race in {@link ClassicPollLoop}.
 *
 * <p>The original bug: after revoke, in-flight worker completions called
 * {@code markComplete}, which resurrected the frontier via {@code computeIfAbsent}.
 * The first fix dropped completions when the frontier was absent — but it missed the
 * harder case where the same TopicPartition is revoked AND reassigned before the
 * orphan worker finishes. The old worker holds a reference to generation-1's frontier
 * and {@code markComplete} would have applied its completion to generation-2's
 * frontier (since the map now holds a different instance for the same TP), polluting
 * the new generation's offset tracking. In KEY mode where completions can be
 * out-of-order, this could falsely advance the commit past records gen-2 hasn't yet
 * processed.
 *
 * <p>The current fix: workers carry the frontier reference captured at dispatch;
 * {@code markComplete} identity-checks the reference against the current map entry
 * and drops on mismatch. PARTITION-mode workers also re-check per record before
 * invoking the listener so they stop iterating revoked batches.
 */
class ClassicPollLoopRebalanceRaceTest {

    private WorkerLauncher launcher;
    private KafkaConsumer<byte[], byte[]> consumer;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        launcher = new WorkerLauncher();
        consumer = mock(KafkaConsumer.class);
    }

    @AfterEach
    void tearDown() {
        launcher.close();
    }

    private ClassicPollLoop<byte[], byte[]> newLoop(OrderingMode ordering) {
        RecordListener<byte[], byte[]> listener = (r, ctx) -> {};
        return new ClassicPollLoop<>(
            consumer, "t", listener,
            RecordDeserializer.bytes(), RecordDeserializer.bytes(),
            ordering,
            new RetryEngine(RetryPolicy.noRetry()),
            /* dltRouter */ null,
            Duration.ofMillis(50),
            /* shutdownDrainTimeoutMs */ 1_000L,
            /* concurrency */ 8,
            /* shardCount */ 16,
            PlurimaMetrics.noOp(), launcher,
            /* onLoopExit */ () -> {});
    }

    @Test
    void markCompleteWithStaleFrontierReferenceIsDroppedWhenNoCurrentFrontier() {
        // Simplest case: orphan worker holds a frontier reference that's no longer in the
        // map (revoke removed it). markComplete must drop without resurrecting.
        TopicPartition tp = new TopicPartition("t", 0);
        ClassicPollLoop<byte[], byte[]> loop = newLoop(OrderingMode.PARTITION);

        // Install a frontier (gen 1), then simulate revoke removing it.
        CommitFrontier gen1 = loop.installFrontier(tp, 0L);
        assertThat(loop.hasFrontier(tp)).isTrue();

        loop.simulateRevoke(List.of(tp));
        assertThat(loop.hasFrontier(tp))
            .as("revoke must remove the frontier from the map")
            .isFalse();

        // Orphan worker completes against the stale gen-1 reference.
        loop.markComplete(gen1, tp, 5L);

        // Frontier must NOT be resurrected.
        assertThat(loop.hasFrontier(tp))
            .as("markComplete with stale frontier ref must NOT recreate the frontier")
            .isFalse();
    }

    @Test
    void gen1CompletionCannotPolluteGen2FrontierAfterRevokeReassign() {
        // The PRODUCTION race the previous "absent → drop" fix missed:
        //   1. Generation 1 frontier installed via dispatch on tp.
        //   2. Worker mid-flight on gen-1, captured gen-1 reference.
        //   3. Revoke fires — gen-1 frontier removed from map.
        //   4. Same tp reassigned back. Generation 2 frontier installed via fresh dispatch
        //      starting at offset 100 (the broker's last committed value).
        //   5. Orphan gen-1 worker finishes record at offset 50 (an offset from gen-1's
        //      now-discarded batch) and calls markComplete with the gen-1 reference.
        //   6. Without the identity check this would have applied complete(50) to gen-2's
        //      frontier, which expected offsets starting at 100. CommitFrontier's
        //      defensive bootstrap would set nextExpected to 50; subsequent drain would
        //      commit 51 for a partition we never processed offsets 51..99 on, skipping
        //      records gen-2 expected to deliver.
        TopicPartition tp = new TopicPartition("t", 0);
        ClassicPollLoop<byte[], byte[]> loop = newLoop(OrderingMode.PARTITION);

        CommitFrontier gen1 = loop.installFrontier(tp, 0L);
        loop.simulateRevoke(List.of(tp));

        // Generation 2: same TP, fresh frontier observing offset 100 (where the new
        // owner picks up from the last committed offset).
        CommitFrontier gen2 = loop.installFrontier(tp, 100L);
        assertThat(gen2)
            .as("revoke followed by re-install must produce a DIFFERENT CommitFrontier instance")
            .isNotSameAs(gen1);
        assertThat(loop.frontier(tp))
            .as("map now holds gen-2 reference")
            .isSameAs(gen2);

        // Step 5: orphan gen-1 worker tries to complete offset 50 against gen-1's frontier.
        loop.markComplete(gen1, tp, 50L);

        // Step 6 invariant: gen-2's frontier must be UNAFFECTED. Specifically:
        //   - drainCommittable must return 100 (gen-2's next-to-consume), NOT 51
        //   - gen-2's completedAhead must be empty (gen-1's offset never reached the set)
        OptionalLong commit = gen2.drainCommittable();
        assertThat(commit)
            .as("gen-2 frontier must still be at its observed start (100), "
                + "not contaminated by gen-1's offset 50")
            .hasValue(100L);
        assertThat(gen2.nextExpected())
            .as("gen-2 nextExpected unchanged by gen-1 completion")
            .isEqualTo(100L);
        assertThat(gen2.gapSize())
            .as("gen-2 completedAhead must be empty — gen-1's offset 50 never reached gen-2's set")
            .isZero();
    }

    @Test
    void multipleStaleGen1CompletionsAllDropped() {
        // PARTITION mode worker iterates a list of records. After revoke + reassign,
        // the orphan keeps walking the OLD batch and could fire many markComplete calls.
        // Each must be dropped against gen-2's frontier.
        TopicPartition tp = new TopicPartition("t", 0);
        ClassicPollLoop<byte[], byte[]> loop = newLoop(OrderingMode.PARTITION);

        CommitFrontier gen1 = loop.installFrontier(tp, 0L);
        loop.simulateRevoke(List.of(tp));
        CommitFrontier gen2 = loop.installFrontier(tp, 100L);

        // Multiple stale completions — should all be dropped silently, no throw, no
        // pollution of gen-2.
        for (long off = 10; off < 30; off++) {
            loop.markComplete(gen1, tp, off);
        }

        // gen-2 is untouched.
        assertThat(gen2.drainCommittable()).hasValue(100L);
        assertThat(gen2.nextExpected()).isEqualTo(100L);
        assertThat(gen2.gapSize()).isZero();
    }

    @Test
    void gen2CompletionsApplyNormallyAfterStaleGen1Completions() {
        // Mixing real gen-2 completions with stale gen-1 completions: gen-2 must still
        // advance correctly, gen-1 completions must still drop.
        TopicPartition tp = new TopicPartition("t", 0);
        ClassicPollLoop<byte[], byte[]> loop = newLoop(OrderingMode.PARTITION);

        CommitFrontier gen1 = loop.installFrontier(tp, 0L);
        loop.simulateRevoke(List.of(tp));
        CommitFrontier gen2 = loop.installFrontier(tp, 100L);

        // Interleave a real gen-2 completion (offset 100) with stale gen-1 ones.
        loop.markComplete(gen1, tp, 50L);   // stale → drop
        loop.markComplete(gen2, tp, 100L);  // real → advance
        loop.markComplete(gen1, tp, 51L);   // stale → drop
        loop.markComplete(gen2, tp, 101L);  // real → advance

        // gen-2 must reflect ONLY its own completions.
        assertThat(gen2.drainCommittable()).hasValue(102L);
        assertThat(gen2.gapSize()).isZero();
    }

    @Test
    void keyModeRevokeDrainsShardQueueAndAdjustsInflight() throws Exception {
        // KEY mode: a shard has multiple queued entries when revoke fires. purgePartitions
        // must drain the queue AND fire onRecordDone for each so inFlight stays accurate.
        TopicPartition tp = new TopicPartition("t", 0);

        java.util.concurrent.atomic.AtomicInteger inFlight =
            new java.util.concurrent.atomic.AtomicInteger();
        java.util.concurrent.CountDownLatch blocker = new java.util.concurrent.CountDownLatch(1);

        ClassicKeyShardDispatcher dispatcher = new ClassicKeyShardDispatcher(
            16, launcher,
            (fr, t, r) -> {
                try { blocker.await(); }
                catch (InterruptedException ie) { Thread.currentThread().interrupt(); }
            },
            () -> true);

        java.util.List<org.apache.kafka.clients.consumer.ConsumerRecord<byte[], byte[]>> records
            = new java.util.ArrayList<>();
        for (int i = 0; i < 10; i++) {
            records.add(new org.apache.kafka.clients.consumer.ConsumerRecord<>(
                "t", 0, i, "samekey".getBytes(), ("v" + i).getBytes()));
        }
        inFlight.addAndGet(records.size());
        dispatcher.dispatch(tp, new CommitFrontier(), records, inFlight::decrementAndGet);

        Thread.sleep(100);
        assertThat(inFlight.get())
            .as("before purge: all 10 records are accounted for in inFlight")
            .isEqualTo(10);

        dispatcher.purgePartitions(java.util.List.of(tp));

        assertThat(inFlight.get())
            .as("after purge: queued 9 entries fired onRecordDone; only the running worker remains")
            .isEqualTo(1);

        blocker.countDown();

        long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(5);
        while (inFlight.get() != 0 && System.nanoTime() < deadline) {
            Thread.sleep(20);
        }
        assertThat(inFlight.get())
            .as("after worker finishes: inFlight returns to zero")
            .isZero();
    }
}
