package io.plurima.kafka.internal;

import io.plurima.kafka.OrderingMode;
import io.plurima.kafka.RecordListener;
import io.plurima.kafka.deserializer.RecordDeserializer;
import io.plurima.kafka.dlt.DltConfig;
import io.plurima.kafka.metrics.PlurimaMetrics;
import io.plurima.kafka.retry.RetryPolicy;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * A4: DLT-publish failure must PAUSE the partition (bounding {@code completedAhead} and
 * keeping commits from stalling silently) and the failing worker must retry the publish
 * with capped backoff, resuming + advancing the frontier once the DLT recovers. On
 * revoke while paused the pause bookkeeping must be dropped and the orphan worker must
 * stop without advancing the frontier.
 */
class ClassicDltFailureBackpressureTest {

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

    /** Producer whose {@code send} synchronously fails the first {@code failuresBeforeSuccess}
     *  callbacks, then succeeds — models a DLT broker outage that heals. Counts every attempt. */
    private static MockProducer<byte[], byte[]> failingThenHealingProducer(
        int failuresBeforeSuccess, AtomicInteger attempts) {
        return new MockProducer<>(true, null, new ByteArraySerializer(), new ByteArraySerializer()) {
            @Override
            public Future<RecordMetadata> send(ProducerRecord<byte[], byte[]> record, Callback callback) {
                int n = attempts.incrementAndGet();
                if (n <= failuresBeforeSuccess) {
                    callback.onCompletion(null, new RuntimeException("DLT broker down (attempt " + n + ")"));
                } else {
                    callback.onCompletion(null, null);
                }
                return CompletableFuture.completedFuture(null);
            }
        };
    }

    private static DltRouter dltRouter(MockProducer<byte[], byte[]> producer, PlurimaMetrics metrics) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "ignored");
        return new DltRouter(producer, DltConfig.builder().producerProperties(props).build(), metrics);
    }

    private ClassicPollLoop<byte[], byte[]> newLoop(DltRouter dltRouter, PlurimaMetrics metrics) {
        return newLoop(dltRouter, metrics, OrderingMode.PARTITION, /* shutdownDrainTimeoutMs */ 1_000L);
    }

    private ClassicPollLoop<byte[], byte[]> newLoop(
        DltRouter dltRouter, PlurimaMetrics metrics, OrderingMode ordering, long shutdownDrainTimeoutMs) {
        RecordListener<byte[], byte[]> alwaysFails = (r, ctx) -> {
            throw new RuntimeException("listener always fails → exhausts → DLT");
        };
        // maxAttempts(0) + a retriable classifier → the first failure is immediately
        // EXHAUSTED (not Reject), which is the path that routes to the DLT.
        RetryPolicy exhaustImmediately = RetryPolicy.exponential()
            .maxAttempts(0)
            .retryOn(RuntimeException.class)
            .build();
        return new ClassicPollLoop<>(
            consumer, "t", "g1", alwaysFails,
            RecordDeserializer.bytes(), RecordDeserializer.bytes(),
            ordering,
            new RetryEngine(exhaustImmediately),
            dltRouter,
            Duration.ofMillis(10),
            shutdownDrainTimeoutMs,
            /* concurrency */ 8,
            /* shardCount */ 16,
            metrics, launcher,
            /* onLoopExit */ () -> {});
    }

    /**
     * Like {@link #newLoop}, but with a caller-supplied listener instead of the
     * hardcoded always-fails one — needed by
     * {@link #backpressureResumeNeverLiftsALiveDltPauseAndInverse} to make only ONE
     * partition (tp0) fail into the DLT while the others just hold inFlight up.
     */
    private ClassicPollLoop<byte[], byte[]> newLoopWithListener(
        DltRouter dltRouter, PlurimaMetrics metrics, RecordListener<byte[], byte[]> listener) {
        RetryPolicy exhaustImmediately = RetryPolicy.exponential()
            .maxAttempts(0)
            .retryOn(RuntimeException.class)
            .build();
        return new ClassicPollLoop<>(
            consumer, "t", "g1", listener,
            RecordDeserializer.bytes(), RecordDeserializer.bytes(),
            OrderingMode.PARTITION,
            new RetryEngine(exhaustImmediately),
            dltRouter,
            Duration.ofMillis(10),
            /* shutdownDrainTimeoutMs */ 1_000L,
            /* concurrency */ 8,
            /* shardCount */ 16,
            metrics, launcher,
            /* onLoopExit */ () -> {});
    }

    /**
     * G1 — the backpressure-resume path ({@code toResume.removeAll(pausedByDltFailure)} in
     * {@code applyBackpressure}) must never lift a DLT pause, and the flip side
     * ({@code !pausedByBackpressure.contains(tp)} in {@code applyDltFailurePause}) must
     * never let a DLT recovery resume a partition backpressure still holds down.
     *
     * <p>Setup: tp0's listener always throws (exhausts immediately → DLT); tp1..tp7's
     * listener blocks on a latch. Dispatching one batch with one record on each of the 8
     * partitions pushes {@code inFlight} to exactly {@code concurrency} (8), triggering
     * backpressure to pause ALL 8 assigned partitions — tp0 included — in the SAME
     * {@code consumer.pause} call that also independently becomes DLT-paused once its
     * worker's publish fails.
     *
     * <ol>
     *   <li>While tp0 is DLT-paused, {@code resume(tp0)} must never fire (backpressure
     *       can't be the one to lift a live DLT hold — the direct target of G1).</li>
     *   <li>Heal the DLT (tp0's hold clears) but keep tp1..tp7 blocked so backpressure
     *       stays active: {@code resume(tp0)} must STILL not fire (the inverse
     *       independence — a DLT recovery alone can't override a live backpressure
     *       hold).</li>
     *   <li>Release tp1..tp7 so inFlight drains and backpressure genuinely resumes:
     *       NOW {@code resume(tp0)} must fire, since neither hold is live anymore.</li>
     * </ol>
     */
    @Test
    @SuppressWarnings("unchecked")
    void backpressureResumeNeverLiftsALiveDltPauseAndInverse() throws Exception {
        TopicPartition tp0 = new TopicPartition("t", 0);
        List<TopicPartition> others = new ArrayList<>();
        for (int i = 1; i <= 7; i++) others.add(new TopicPartition("t", i));
        Set<TopicPartition> assignment = new HashSet<>(others);
        assignment.add(tp0);

        AtomicBoolean healDlt = new AtomicBoolean(false);
        MockProducer<byte[], byte[]> producer =
            new MockProducer<>(true, null, new ByteArraySerializer(), new ByteArraySerializer()) {
                @Override
                public Future<RecordMetadata> send(ProducerRecord<byte[], byte[]> record, Callback callback) {
                    callback.onCompletion(null, healDlt.get() ? null : new RuntimeException("DLT down"));
                    return CompletableFuture.completedFuture(null);
                }
            };
        DltRouter router = dltRouter(producer, PlurimaMetrics.noOp());

        CountDownLatch releaseOthers = new CountDownLatch(1);
        RecordListener<byte[], byte[]> listener = (r, ctx) -> {
            if (r.partition() == 0) {
                throw new RuntimeException("tp0 always fails on first attempt -> DLT");
            }
            // tp1..tp7: hold inFlight up without ever touching the DLT path.
            releaseOthers.await(10, TimeUnit.SECONDS);
        };

        ClassicPollLoop<byte[], byte[]> loop = newLoopWithListener(router, PlurimaMetrics.noOp(), listener);
        loop.setDltRetryBackoffForTest(10L, 20L);

        when(consumer.assignment()).thenReturn(assignment);
        Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> byTp = new HashMap<>();
        for (TopicPartition tp : assignment) {
            byTp.put(tp, List.of(new ConsumerRecord<>(tp.topic(), tp.partition(), 0L, "k".getBytes(), "v".getBytes())));
        }
        ConsumerRecords<byte[], byte[]> batch = new ConsumerRecords<>(byTp, Map.of());
        ConsumerRecords<byte[], byte[]> empty = new ConsumerRecords<>(Map.of(), Map.of());
        AtomicBoolean firstPoll = new AtomicBoolean(true);
        when(consumer.poll(any(Duration.class))).thenAnswer(
            inv -> firstPoll.compareAndSet(true, false) ? batch : empty);

        Thread pollThread = new Thread(loop, "g1-poll");
        pollThread.start();
        try {
            // inFlight hits exactly concurrency (8) as soon as the batch dispatches, so
            // backpressure pauses ALL 8 assigned partitions in one bulk call.
            verify(consumer, timeout(3_000)).pause(argThat(c -> c.containsAll(assignment)));

            // tp0 independently lands in DLT-failure pause once its worker's first publish fails.
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
            while (!loop.isPausedByDltFailure(tp0) && System.nanoTime() < deadline) {
                Thread.sleep(5);
            }
            assertThat(loop.isPausedByDltFailure(tp0)).as("tp0 must be DLT-paused").isTrue();

            // 1) THE BUG THIS GUARDS: backpressure's resume path must never include tp0
            // while its DLT hold is live.
            verify(consumer, never())
                .resume(argThat(c -> c.contains(tp0)));

            // 2) Heal the DLT — tp0's hold clears — but keep the other 7 workers blocked so
            // backpressure stays active (inFlight drops from 8 to 7, still > concurrency/2==4).
            healDlt.set(true);
            deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
            while (loop.isPausedByDltFailure(tp0) && System.nanoTime() < deadline) {
                Thread.sleep(5);
            }
            assertThat(loop.isPausedByDltFailure(tp0))
                .as("DLT publish recovered — the worker's hold must clear")
                .isFalse();

            // Give applyDltFailurePause several iterations a chance to (wrongly) resume tp0.
            // (INVARIANT: a DLT recovery alone must not resume a partition backpressure
            // still holds.)
            Thread.sleep(150);
            verify(consumer, never())
                .resume(argThat(c -> c.contains(tp0)));

            // 3) Release the other workers: inFlight drains to 0, backpressure genuinely
            // resumes, and — since the DLT hold is already clear — tp0 is included this time.
            releaseOthers.countDown();
            verify(consumer, timeout(3_000))
                .resume(argThat(c -> c.contains(tp0)));
        } finally {
            loop.close();
            pollThread.join(5_000);
        }
        assertThat(pollThread.isAlive()).as("poll thread exited cleanly").isFalse();
    }

    private static ConsumerRecords<byte[], byte[]> batchOf(TopicPartition tp, long offset) {
        ConsumerRecord<byte[], byte[]> rec = new ConsumerRecord<>(
            tp.topic(), tp.partition(), offset, "k".getBytes(), "v".getBytes());
        return new ConsumerRecords<>(Map.of(tp, List.of(rec)), Map.of());
    }

    @Test
    @SuppressWarnings("unchecked")
    void dltFailurePausesPartitionThenResumesAndAdvancesFrontierOnRecovery() throws Exception {
        TopicPartition tp = new TopicPartition("t", 0);
        AtomicInteger sendAttempts = new AtomicInteger();
        MockProducer<byte[], byte[]> producer = failingThenHealingProducer(2, sendAttempts);
        DltRouter router = dltRouter(producer, PlurimaMetrics.noOp());
        ClassicPollLoop<byte[], byte[]> loop = newLoop(router, PlurimaMetrics.noOp());
        loop.setDltRetryBackoffForTest(20L, 100L);

        ConsumerRecords<byte[], byte[]> batch = batchOf(tp, 0L);
        ConsumerRecords<byte[], byte[]> empty = new ConsumerRecords<>(Map.of(), Map.of());
        when(consumer.poll(any(Duration.class))).thenReturn(batch, empty);
        when(consumer.assignment()).thenReturn(java.util.Set.of(tp));

        Thread pollThread = new Thread(loop, "test-poll");
        pollThread.start();
        try {
            // While the DLT is failing the poll loop MUST pause the partition.
            verify(consumer, timeout(3_000).atLeastOnce())
                .pause(argThat(c -> c.contains(tp)));

            // Once the DLT heals the worker advances the frontier and the loop resumes.
            verify(consumer, timeout(3_000).atLeastOnce())
                .resume(argThat(c -> c.contains(tp)));

            // Record NOT lost: the frontier advanced past offset 0, so a commitAsync
            // carrying offset 1 (next-to-consume) is issued.
            verify(consumer, timeout(3_000).atLeastOnce()).commitAsync(
                argThat((Map<TopicPartition, OffsetAndMetadata> m) ->
                    m.containsKey(tp) && m.get(tp).offset() == 1L),
                any(OffsetCommitCallback.class));

            assertThat(sendAttempts.get())
                .as("DLT publish retried until success: 2 failures + 1 success")
                .isGreaterThanOrEqualTo(3);
        } finally {
            loop.close();
            pollThread.join(5_000);
        }
        assertThat(pollThread.isAlive()).as("poll thread exited cleanly").isFalse();
    }

    /**
     * F1 — lost-pause race with concurrent retryers. With UNORDERED (or KEY) ordering, TWO
     * workers on the SAME partition can be inside {@code pauseAndRetryDlt}'s retry loop at
     * once. The pause bookkeeping must be per-worker refcounted: the FIRST worker's publish
     * success must NOT lift the partition pause while the second worker is still retrying
     * — otherwise the poll thread resumes the partition mid-outage and {@code completedAhead}
     * grows unboundedly for the rest of the outage (the exact condition the DLT pause
     * exists to prevent).
     */
    @Test
    void partitionStaysPausedUntilLastConcurrentDltRetryerFinishes() throws Exception {
        TopicPartition tp = new TopicPartition("t", 0);
        AtomicInteger aAttempts = new AtomicInteger();
        AtomicInteger bAttempts = new AtomicInteger();
        java.util.concurrent.atomic.AtomicBoolean aSucceeded = new java.util.concurrent.atomic.AtomicBoolean();
        java.util.concurrent.atomic.AtomicBoolean healB = new java.util.concurrent.atomic.AtomicBoolean();
        // DltRouter copies the ORIGINAL record's value into the DLT record, so the value
        // identifies WHICH worker is publishing. Worker a's publish succeeds only once
        // worker b has provably entered its own retry loop (bAttempts >= 2 means b failed
        // its first attempt, acquired its pause hold, slept, and retried) — making the
        // "first success while the second still retries" interleaving deterministic.
        MockProducer<byte[], byte[]> producer =
            new MockProducer<>(true, null, new ByteArraySerializer(), new ByteArraySerializer()) {
                @Override
                public Future<RecordMetadata> send(ProducerRecord<byte[], byte[]> record, Callback callback) {
                    if ("a".equals(new String(record.value()))) {
                        boolean succeed = aAttempts.incrementAndGet() >= 2 && bAttempts.get() >= 2;
                        if (succeed) aSucceeded.set(true);
                        callback.onCompletion(null, succeed ? null : new RuntimeException("DLT down (a)"));
                    } else {
                        bAttempts.incrementAndGet();
                        boolean succeed = healB.get();
                        callback.onCompletion(null, succeed ? null : new RuntimeException("DLT down (b)"));
                    }
                    return CompletableFuture.completedFuture(null);
                }
            };
        DltRouter router = dltRouter(producer, PlurimaMetrics.noOp());
        ClassicPollLoop<byte[], byte[]> loop =
            newLoop(router, PlurimaMetrics.noOp(), OrderingMode.UNORDERED, 1_000L);
        loop.setDltRetryBackoffForTest(10L, 20L);

        // One batch, two records → UNORDERED dispatches two concurrent workers on tp.
        ConsumerRecord<byte[], byte[]> ra = new ConsumerRecord<>(
            tp.topic(), tp.partition(), 0L, "ka".getBytes(), "a".getBytes());
        ConsumerRecord<byte[], byte[]> rb = new ConsumerRecord<>(
            tp.topic(), tp.partition(), 1L, "kb".getBytes(), "b".getBytes());
        loop.dispatchBatchAsync(new ConsumerRecords<>(Map.of(tp, List.of(ra, rb)), Map.of()));

        // Wait for worker a to succeed its publish and exit its retry loop.
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (!aSucceeded.get() && System.nanoTime() < deadline) {
            Thread.sleep(5);
        }
        assertThat(aSucceeded.get()).as("worker a's DLT publish must eventually succeed").isTrue();

        // THE INVARIANT: b is still failing, so a's success must not have lifted the
        // pause. Sample across several of b's backoff cycles to catch a lost pause.
        int bAttemptsBefore = bAttempts.get();
        long sampleUntil = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(300);
        while (System.nanoTime() < sampleUntil) {
            assertThat(loop.isPausedByDltFailure(tp))
                .as("partition must stay DLT-paused while ANY worker is still retrying "
                    + "(first success must not lift the pause held by the second retryer)")
                .isTrue();
            Thread.sleep(10);
        }
        assertThat(bAttempts.get())
            .as("worker b kept retrying during the sample window (the race was real)")
            .isGreaterThan(bAttemptsBefore);

        // Once the LAST retryer succeeds, the pause lifts and in-flight drains to zero.
        healB.set(true);
        deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while ((loop.isPausedByDltFailure(tp) || loop.inFlightCount() != 0)
            && System.nanoTime() < deadline) {
            Thread.sleep(5);
        }
        assertThat(loop.isPausedByDltFailure(tp))
            .as("pause lifts once the last retryer finishes")
            .isFalse();
        assertThat(loop.inFlightCount()).isZero();
    }

    /**
     * F1-follow-up — a STALE-generation acquirer must not displace a LIVE newer-generation
     * hold. Scenario: a worker was descheduled between {@code handleExhaustion}'s ownership
     * check and {@code pauseAndRetryDlt}'s {@code acquireDltPause} call (a genuine but
     * vanishingly narrow production race — revoke+reassign lands in that single-instruction
     * gap). By the time it resumes and calls {@code acquireDltPause} with its now-stale
     * frontier, a brand-new generation's worker has ALREADY failed and acquired its own
     * hold for the SAME partition. The stale caller's acquire must leave that live hold
     * untouched (not replace it keyed to the stale generation), and its later
     * generation-guarded release must correctly no-op — the partition must stay paused
     * until the LIVE worker's own release lifts it.
     *
     * <p>Drives {@link ClassicPollLoop#acquireDltPause}/{@code releaseDltPause} directly
     * via the {@code ForTest} seams (see their javadoc): the real call sites have no
     * injectable delay, so this is the only deterministic way to hit the exact
     * generation-mismatch branch without relying on incidental thread scheduling.
     */
    @Test
    void staleGenerationAcquireDoesNotDisplaceLiveNewerGenerationHold() {
        TopicPartition tp = new TopicPartition("t", 0);
        DltRouter router = dltRouter(failingThenHealingProducer(0, new AtomicInteger()), PlurimaMetrics.noOp());
        ClassicPollLoop<byte[], byte[]> loop = newLoop(router, PlurimaMetrics.noOp());

        // Live generation: install it as the partition's current frontier, then have its
        // worker acquire the DLT-pause hold — this is the pause that MUST survive.
        CommitFrontier liveGeneration = loop.installFrontier(tp, 0L);
        loop.acquireDltPauseForTest(tp, liveGeneration);
        assertThat(loop.isPausedByDltFailure(tp)).isTrue();

        // Stale generation: NOT installed as the live frontier for tp (frontiers.get(tp)
        // is liveGeneration) — models the straggler worker's frontier reference captured
        // before a revoke+reassign it never observed in time.
        CommitFrontier staleGeneration = new CommitFrontier();
        assertThat(loop.frontier(tp)).as("live frontier is NOT the stale one").isSameAs(liveGeneration);

        // THE BUG: the stale caller's acquire must NOT clobber the live hold.
        loop.acquireDltPauseForTest(tp, staleGeneration);
        assertThat(loop.isPausedByDltFailure(tp))
            .as("partition must still be paused — the live generation's hold must survive "
                + "a stale caller's acquire")
            .isTrue();

        // The stale worker's own (generation-guarded) release must no-op — it never owned
        // the live hold, so it must not be able to lift it.
        loop.releaseDltPauseForTest(tp, staleGeneration);
        assertThat(loop.isPausedByDltFailure(tp))
            .as("stale release must no-op: partition stays paused for the live generation")
            .isTrue();

        // Only the LIVE generation's own release may lift the pause.
        loop.releaseDltPauseForTest(tp, liveGeneration);
        assertThat(loop.isPausedByDltFailure(tp))
            .as("live generation's own release lifts the pause it actually owns")
            .isFalse();
    }

    /**
     * F5 — fatal poll-loop exit must flip {@code running} to false BEFORE the shutdown
     * drain. Only {@code shutdown()} used to flip it; on the fatal path (generic
     * {@code catch (Throwable)}) the drain previously ran with {@code running} still true,
     * so a worker parked in {@code pauseAndRetryDlt}'s {@code while (running)} loop kept
     * retrying (pinning inFlight) for the WHOLE drain budget instead of exiting promptly.
     */
    @Test
    @SuppressWarnings("unchecked")
    void fatalPollErrorDuringDltRetryDrainsPromptlyNotAfterFullTimeout() throws Exception {
        TopicPartition tp = new TopicPartition("t", 0);
        AtomicInteger sendAttempts = new AtomicInteger();
        // DLT never heals — the worker stays in the retry loop until told to stop.
        MockProducer<byte[], byte[]> producer =
            failingThenHealingProducer(Integer.MAX_VALUE, sendAttempts);
        DltRouter router = dltRouter(producer, PlurimaMetrics.noOp());
        // Drain budget must exceed the reserved commit+close tail so the drain phase has a
        // real multi-second slice; the assertion below proves we do NOT sit in it.
        long shutdownDrainTimeoutMs = 10_000L;
        ClassicPollLoop<byte[], byte[]> loop =
            newLoop(router, PlurimaMetrics.noOp(), OrderingMode.PARTITION, shutdownDrainTimeoutMs);
        loop.setDltRetryBackoffForTest(20L, 40L);

        ConsumerRecords<byte[], byte[]> batch = batchOf(tp, 0L);
        ConsumerRecords<byte[], byte[]> empty = new ConsumerRecords<>(Map.of(), Map.of());
        java.util.concurrent.atomic.AtomicBoolean first = new java.util.concurrent.atomic.AtomicBoolean(true);
        java.util.concurrent.atomic.AtomicLong fatalAtNanos = new java.util.concurrent.atomic.AtomicLong();
        when(consumer.assignment()).thenReturn(java.util.Set.of(tp));
        when(consumer.poll(any(Duration.class))).thenAnswer(inv -> {
            if (first.compareAndSet(true, false)) return batch;
            if (loop.isPausedByDltFailure(tp)) {
                // The worker is now provably inside its DLT retry loop — blow up the
                // poll thread with an unrecoverable error (the fatal path).
                fatalAtNanos.set(System.nanoTime());
                throw new RuntimeException("simulated fatal poll error");
            }
            return empty;
        });

        Thread pollThread = new Thread(loop, "fatal-drain-loop");
        pollThread.start();
        pollThread.join(shutdownDrainTimeoutMs + 5_000);
        assertThat(pollThread.isAlive())
            .as("poll thread must exit within the overall budget")
            .isFalse();
        assertThat(fatalAtNanos.get()).as("the fatal poll error must have fired").isPositive();

        long drainMs = (System.nanoTime() - fatalAtNanos.get()) / 1_000_000L;
        assertThat(drainMs)
            .as("fatal exit must flip running=false BEFORE the drain so the DLT-retry "
                + "worker exits at its next check (~backoff interval), not after the "
                + "full drain budget")
            .isLessThan(2_000L);
        assertThat(loop.inFlightCount())
            .as("the retrying worker observed shutdown and unwound")
            .isZero();
    }

    /**
     * G7 — {@code publishToDlt}'s caller-side give-up: when the DLT future doesn't
     * complete within its budget (a hung/slow-forever producer — no synchronous failure,
     * no callback ever invoked), the worker must treat the attempt as a failure: emit
     * {@code dltFailed} exactly via the caller-side CAS path (never {@code dltRouted},
     * since nothing ever actually completes) and keep the partition DLT-paused so the
     * retry loop continues. Uses the {@code setDltPublishBudgetForTest} seam added
     * alongside this test to shrink the 30s production budget to milliseconds — the real
     * call site has no other injectable delay.
     */
    @Test
    void publishToDltCallerSideTimeoutEmitsDltFailedAndKeepsRetrying() throws Exception {
        TopicPartition tp = new TopicPartition("t", 0);
        // Never invokes the callback and never throws synchronously — the future backing
        // route.future() never completes, so publishToDlt's get(budget, MILLISECONDS)
        // always times out.
        MockProducer<byte[], byte[]> hangingProducer =
            new MockProducer<>(true, null, new ByteArraySerializer(), new ByteArraySerializer()) {
                @Override
                public Future<RecordMetadata> send(ProducerRecord<byte[], byte[]> record, Callback callback) {
                    return new CompletableFuture<>(); // never completed; callback never invoked
                }
            };
        PlurimaMetrics metrics = mock(PlurimaMetrics.class);
        DltRouter router = dltRouter(hangingProducer, metrics);
        ClassicPollLoop<byte[], byte[]> loop = newLoop(router, metrics);
        loop.setDltRetryBackoffForTest(10L, 20L);
        loop.setDltPublishBudgetForTest(50L);

        loop.dispatchBatchAsync(batchOf(tp, 0L));

        // The worker's first publishToDlt attempt times out at the (shrunk) budget and the
        // caller-side give-up emits dltFailed with the timeout exception's simple name.
        verify(metrics, timeout(3_000).atLeastOnce()).dltFailed(eq("t"), eq("TimeoutException"));
        verify(metrics, never()).dltRouted(any(String.class), any(String.class));

        // The attempt is treated as a failure: the partition stays DLT-paused and the
        // worker keeps retrying (a caller-side timeout must never be silently treated as
        // success).
        assertThat(loop.isPausedByDltFailure(tp))
            .as("caller-side timeout must be treated as a DLT-publish failure — partition "
                + "stays paused and the worker keeps retrying")
            .isTrue();

        loop.close();
    }

    @Test
    void revokeWhileDltPausedClearsPauseAndStopsWorkerWithoutAdvancing() throws Exception {
        TopicPartition tp = new TopicPartition("t", 0);
        AtomicInteger sendAttempts = new AtomicInteger();
        // Never heals — the worker stays in the retry loop until revoke stops it.
        MockProducer<byte[], byte[]> producer = failingThenHealingProducer(Integer.MAX_VALUE, sendAttempts);
        DltRouter router = dltRouter(producer, PlurimaMetrics.noOp());
        ClassicPollLoop<byte[], byte[]> loop = newLoop(router, PlurimaMetrics.noOp());
        loop.setDltRetryBackoffForTest(20L, 40L);

        // Dispatch a real batch → worker runs processOne → exhausts → handleExhaustion →
        // DLT fails → partition marked DLT-paused, worker retrying.
        loop.dispatchBatchAsync(batchOf(tp, 0L));

        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
        while (!loop.isPausedByDltFailure(tp) && System.nanoTime() < deadline) {
            Thread.sleep(10);
        }
        assertThat(loop.isPausedByDltFailure(tp))
            .as("DLT publish failure must pause the partition")
            .isTrue();
        assertThat(loop.hasFrontier(tp)).isTrue();

        // Revoke while paused: the real rebalance path must drop the DLT-pause entry
        // AND remove the frontier so the orphan worker stops.
        loop.rebalanceListener().onPartitionsRevoked(List.of(tp));

        assertThat(loop.isPausedByDltFailure(tp))
            .as("revoke must clear the DLT-pause bookkeeping for the revoked partition")
            .isFalse();
        assertThat(loop.hasFrontier(tp))
            .as("revoke removes the frontier")
            .isFalse();

        // Worker notices ownership loss and stops — inFlight returns to zero and it does
        // NOT re-add the pause.
        deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
        while (loop.inFlightCount() != 0 && System.nanoTime() < deadline) {
            Thread.sleep(10);
        }
        assertThat(loop.inFlightCount())
            .as("orphan worker stops retrying after revoke")
            .isZero();
        assertThat(loop.isPausedByDltFailure(tp))
            .as("worker must not re-add the DLT pause after revoke")
            .isFalse();
    }
}
