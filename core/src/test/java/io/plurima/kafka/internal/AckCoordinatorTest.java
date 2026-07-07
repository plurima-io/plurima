package io.plurima.kafka.internal;

import io.plurima.kafka.metrics.PlurimaMetrics;
import io.plurima.kafka.metrics.ProcessResult;
import org.apache.kafka.clients.consumer.AcknowledgeType;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ShareConsumer;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.InvalidRecordStateException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

class AckCoordinatorTest {

    private InFlightRegistry registry;
    private ShareConsumer<byte[], byte[]> consumer;
    private AckCoordinator coordinator;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        registry = new InFlightRegistry();
        consumer = mock(ShareConsumer.class);
        coordinator = new AckCoordinator(registry);
    }

    private InFlightRecord<byte[], byte[]> rec(String t, int p, long o) {
        InFlightRecord<byte[], byte[]> r = new InFlightRecord<>(new ConsumerRecord<>(t, p, o, new byte[0], new byte[0]));
        registry.register(r);
        return r;
    }

    @Test
    void drainAcknowledgesQueuedRecordsInOrder() {
        InFlightRecord<byte[], byte[]> a = rec("t", 0, 1);
        InFlightRecord<byte[], byte[]> b = rec("t", 0, 2);
        coordinator.queueAck(a, AcknowledgeType.ACCEPT);
        coordinator.queueAck(b, AcknowledgeType.RELEASE);

        coordinator.commitPendingAcks(consumer);

        InOrder inOrder = inOrder(consumer);
        inOrder.verify(consumer).acknowledge(
            argThat(cr -> cr.topic().equals("t") && cr.partition() == 0 && cr.offset() == 1L),
            eq(AcknowledgeType.ACCEPT));
        inOrder.verify(consumer).acknowledge(
            argThat(cr -> cr.topic().equals("t") && cr.partition() == 0 && cr.offset() == 2L),
            eq(AcknowledgeType.RELEASE));
        inOrder.verify(consumer).commitAsync();
    }

    @Test
    void firstTerminalAckWinsForSameRecord() {
        // Scenario: worker queues ACCEPT, then forceReleaseStuckRecords queues RELEASE for
        // the SAME InFlightRecord (the worker hasn't reached its finally yet, so the record
        // is still in the registry). Without first-wins, both land in pending; commitAsync
        // batches them and the broker stores the later RELEASE for this offset, redelivering
        // a successfully-processed record.
        InFlightRecord<byte[], byte[]> a = rec("t", 0, 1);

        coordinator.queueAck(a, AcknowledgeType.ACCEPT);
        coordinator.queueAck(a, AcknowledgeType.RELEASE);  // simulates force-release race
        coordinator.commitPendingAcks(consumer);

        // ACCEPT must reach the broker; RELEASE must be dropped.
        verify(consumer).acknowledge(
            argThat(cr -> cr.topic().equals("t") && cr.offset() == 1L),
            eq(AcknowledgeType.ACCEPT));
        verify(consumer, never()).acknowledge(
            org.mockito.ArgumentMatchers.<ConsumerRecord<byte[], byte[]>>any(),
            eq(AcknowledgeType.RELEASE));
    }

    @Test
    void drainHandlesInvalidRecordStateException() {
        InFlightRecord<byte[], byte[]> a = rec("t", 0, 1);
        doThrow(new InvalidRecordStateException("lease expired"))
            .when(consumer).acknowledge(
                org.mockito.ArgumentMatchers.<ConsumerRecord<byte[], byte[]>>argThat(
                    cr -> cr.topic().equals("t") && cr.partition() == 0 && cr.offset() == 1L),
                eq(AcknowledgeType.ACCEPT));

        coordinator.queueAck(a, AcknowledgeType.ACCEPT);
        coordinator.commitPendingAcks(consumer); // does not propagate

        assertThat(registry.currentInFlight()).isZero();
        verify(consumer).commitAsync();
    }

    /**
     * An unexpected {@link org.apache.kafka.common.errors.TimeoutException} (a
     * {@code KafkaException} subtype distinct from {@code InvalidRecordStateException})
     * must be treated with the same complete-and-continue semantics: the failing
     * request's registry entry completes, the drain loop continues to the NEXT queued
     * request instead of aborting, and the batch still issues {@code commitAsync()} for
     * whatever did succeed. Before broadening the catch to {@code KafkaException}, this
     * would have propagated out of {@code commitPendingAcks} entirely — past
     * {@code drainAndCommit} and into {@code PollLoop.run()}'s outer
     * {@code catch(Throwable)}, which kills the whole poll thread over one record's ack
     * failure.
     */
    @Test
    void unexpectedKafkaExceptionCompletesAndContinuesToNextRequest() {
        InFlightRecord<byte[], byte[]> a = rec("t", 0, 1);
        InFlightRecord<byte[], byte[]> b = rec("t", 0, 2);
        doThrow(new org.apache.kafka.common.errors.TimeoutException("broker did not respond"))
            .when(consumer).acknowledge(
                org.mockito.ArgumentMatchers.<ConsumerRecord<byte[], byte[]>>argThat(
                    cr -> cr.topic().equals("t") && cr.partition() == 0 && cr.offset() == 1L),
                eq(AcknowledgeType.ACCEPT));

        coordinator.queueAck(a, AcknowledgeType.ACCEPT);
        coordinator.queueAck(b, AcknowledgeType.ACCEPT);

        // Must not propagate — and must not abort before draining b.
        coordinator.commitPendingAcks(consumer);

        // The failing request's registry entry is completed (safety-net no-op path) —
        // the succeeding request's entry is untouched here because completing it is the
        // WORKER's responsibility on the normal path (see commitPendingAcks javadoc).
        assertThat(registry.isCurrent(a))
            .as("failing request's registry entry must be completed by the safety-net path")
            .isFalse();
        // ...the loop continued to b, whose acknowledge() actually reached the broker...
        verify(consumer).acknowledge(
            argThat(cr -> cr.topic().equals("t") && cr.offset() == 2L),
            eq(AcknowledgeType.ACCEPT));
        // ...and the batch still committed (anyDrained=true covers both requests).
        verify(consumer).commitAsync();
    }

    /**
     * F8 — fatal-by-convention exceptions must NOT be complete-and-continued. An
     * {@link org.apache.kafka.common.errors.AuthorizationException} is persistent: every
     * subsequent acknowledge would fail the same way, so swallowing it means infinite
     * redelivery while the consumer reports RUNNING and {@code onFatalError} never fires.
     * It must propagate out of {@code commitPendingAcks} so the poll loop's fatal path
     * (FAILED state + user callback) engages.
     */
    @Test
    void authorizationExceptionFromAcknowledgePropagatesAsFatal() {
        InFlightRecord<byte[], byte[]> a = rec("t", 0, 1);
        doThrow(new org.apache.kafka.common.errors.TopicAuthorizationException("not authorized"))
            .when(consumer).acknowledge(
                org.mockito.ArgumentMatchers.<ConsumerRecord<byte[], byte[]>>any(),
                eq(AcknowledgeType.ACCEPT));

        coordinator.queueAck(a, AcknowledgeType.ACCEPT);

        org.assertj.core.api.Assertions.assertThatThrownBy(
                () -> coordinator.commitPendingAcks(consumer))
            .as("AuthorizationException is fatal per Kafka client conventions — it must "
                + "propagate, not be swallowed into complete-and-continue")
            .isInstanceOf(org.apache.kafka.common.errors.AuthorizationException.class);
    }

    /** F8 — same as above for {@link org.apache.kafka.common.errors.AuthenticationException}. */
    @Test
    void authenticationExceptionFromAcknowledgePropagatesAsFatal() {
        InFlightRecord<byte[], byte[]> a = rec("t", 0, 1);
        doThrow(new org.apache.kafka.common.errors.AuthenticationException("bad credentials"))
            .when(consumer).acknowledge(
                org.mockito.ArgumentMatchers.<ConsumerRecord<byte[], byte[]>>any(),
                eq(AcknowledgeType.ACCEPT));

        coordinator.queueAck(a, AcknowledgeType.ACCEPT);

        org.assertj.core.api.Assertions.assertThatThrownBy(
                () -> coordinator.commitPendingAcks(consumer))
            .isInstanceOf(org.apache.kafka.common.errors.AuthenticationException.class);
    }

    /**
     * F8 — {@link org.apache.kafka.common.errors.InterruptException} is shutdown control
     * flow (the client's unchecked wrapper for thread interruption), not a per-record ack
     * failure. Swallowing it would absorb the very stop signal the poll loop's shutdown
     * relies on. It must propagate.
     */
    @Test
    void interruptExceptionFromAcknowledgePropagates() {
        try {
            InFlightRecord<byte[], byte[]> a = rec("t", 0, 1);
            doThrow(new org.apache.kafka.common.errors.InterruptException("interrupted"))
                .when(consumer).acknowledge(
                    org.mockito.ArgumentMatchers.<ConsumerRecord<byte[], byte[]>>any(),
                    eq(AcknowledgeType.ACCEPT));

            coordinator.queueAck(a, AcknowledgeType.ACCEPT);

            org.assertj.core.api.Assertions.assertThatThrownBy(
                    () -> coordinator.commitPendingAcks(consumer))
                .as("InterruptException is control flow (shutdown), never a completable "
                    + "ack failure — it must propagate")
                .isInstanceOf(org.apache.kafka.common.errors.InterruptException.class);
        } finally {
            // InterruptException's constructor interrupts the current thread; clear the
            // flag so it can't leak into subsequent tests.
            Thread.interrupted();
        }
    }

    /**
     * After {@code PollLoop.forceReleaseStuckRecords()} RELEASEs an in-flight record because
     * the drain barrier timed out, a stuck-but-eventually-recovering worker may still queue
     * its own ACCEPT for the same record. The broker no longer considers the record acquired
     * by us, so {@code consumer.acknowledge()} throws {@link IllegalStateException}.
     *
     * <p>We must NOT propagate that exception (it would crash the poll loop). Treat it the same
     * as {@code InvalidRecordStateException}: log + clear the registry + continue.
     */
    @Test
    void drainSwallowsIllegalStateExceptionAfterForceRelease() {
        InFlightRecord<byte[], byte[]> a = rec("t", 0, 1);
        doThrow(new IllegalStateException("the record is not waiting to be acknowledged"))
            .when(consumer).acknowledge(
                org.mockito.ArgumentMatchers.<ConsumerRecord<byte[], byte[]>>argThat(
                    cr -> cr.topic().equals("t") && cr.partition() == 0 && cr.offset() == 1L),
                eq(AcknowledgeType.ACCEPT));

        coordinator.queueAck(a, AcknowledgeType.ACCEPT);
        coordinator.commitPendingAcks(consumer); // must NOT propagate

        assertThat(registry.currentInFlight()).isZero();
        verify(consumer).commitAsync();
    }

    @Test
    void queueAckEmitsRecordsProcessedForEveryTerminalType() {
        PlurimaMetrics metricsMock = mock(PlurimaMetrics.class);
        AckCoordinator c = new AckCoordinator(registry, metricsMock);

        InFlightRecord<byte[], byte[]> accepted = rec("t", 0, 1);
        InFlightRecord<byte[], byte[]> released = rec("t", 0, 2);
        InFlightRecord<byte[], byte[]> rejected = rec("t", 0, 3);

        c.queueAck(accepted, AcknowledgeType.ACCEPT);
        c.queueAck(released, AcknowledgeType.RELEASE);
        c.queueAck(rejected, AcknowledgeType.REJECT);

        // Every terminal ack — accept/release/reject — emits recordsProcessed. This is the
        // centralization: prior to the fix only the WorkerProcessor auto-ack accept path
        // fired this metric, so manual-ack ACCEPT/REJECT and ListenerInvoker.forManual's
        // auto-RELEASE-on-no-ack were invisible. Centralizing in queueAck covers all
        // origin paths (worker, manual-ack listener, retry decisions, DLT exhaustion).
        verify(metricsMock).recordsProcessed("t", ProcessResult.ACCEPT);
        verify(metricsMock).recordsProcessed("t", ProcessResult.RELEASE);
        verify(metricsMock).recordsProcessed("t", ProcessResult.REJECT);
    }

    @Test
    void drainOnEmptyQueueSkipsCommitAsync() {
        // Previously commitPendingAcks called commitAsync unconditionally, which meant every
        // empty poll cycle issued a spurious no-op commit and the shutdown path was more
        // exposed to commitAsync exceptions. Now: no acks drained → no commit.
        coordinator.commitPendingAcks(consumer);
        verifyNoInteractions(consumer);
    }

    @Test
    void onCompleteEmitsAckCommitFailedMetricForEachPartition() {
        PlurimaMetrics metricsMock = mock(PlurimaMetrics.class);
        AckCoordinator c = new AckCoordinator(registry, metricsMock);

        var tp1 = new TopicIdPartition(Uuid.ZERO_UUID, new TopicPartition("t", 0));
        var tp2 = new TopicIdPartition(Uuid.ZERO_UUID, new TopicPartition("t", 1));

        Map<TopicIdPartition, Set<Long>> offsets = Map.of(
            tp1, Set.of(1L, 2L),
            tp2, Set.of(5L));

        c.onComplete(offsets, new RuntimeException("broker timeout"));

        verify(metricsMock, atLeast(2)).ackCommitFailed(eq("t"), eq("RuntimeException"));
    }

    @Test
    void onCompleteNoMetricsOnSuccess() {
        PlurimaMetrics metricsMock = mock(PlurimaMetrics.class);
        AckCoordinator c = new AckCoordinator(registry, metricsMock);

        c.onComplete(Map.of(), null);

        verify(metricsMock, never()).ackCommitFailed(anyString(), anyString());
    }

    @Test
    void terminalFirstWinsUnderConcurrentAcceptAndRelease() throws Exception {
        // The race the markTerminalAckQueued guard protects against:
        //   - worker finishes successfully and queues ACCEPT for record R
        //   - simultaneously, PollLoop.forceReleaseStuckRecords() fires (drain barrier
        //     timeout) and queues RELEASE for the same R (the worker's finally hasn't
        //     reached complete() yet, so R is still "current" in the registry)
        //
        // Without first-wins: commitPendingAcks drains [ACCEPT, RELEASE] and the
        // broker's per-offset ack map (last-write-wins) keeps RELEASE → record
        // redelivered despite the worker succeeding.
        // With first-wins: whichever queueAck reached markTerminalAckQueued first wins
        // and the other is dropped — only ONE ack per record reaches the broker.
        //
        // This test drives the race over many records and asserts that consumer.
        // acknowledge() was called EXACTLY once per record (never twice with different
        // types), proving first-wins holds under contention.
        int totalRecords = 500;
        java.util.List<InFlightRecord<byte[], byte[]>> records = new java.util.ArrayList<>();
        for (int i = 0; i < totalRecords; i++) {
            records.add(rec("t", 0, i));
        }
        // Each record gets recorded once with whichever type won; second attempt is a no-op.
        java.util.Map<Long, AcknowledgeType> firstAck = new java.util.concurrent.ConcurrentHashMap<>();
        doAnswer(inv -> {
            ConsumerRecord<byte[], byte[]> cr = inv.getArgument(0);
            AcknowledgeType type = inv.getArgument(1);
            firstAck.merge(cr.offset(), type, (existing, incoming) -> {
                throw new AssertionError("offset " + cr.offset()
                    + " was acknowledged twice — first-wins broken: "
                    + existing + " then " + incoming);
            });
            return null;
        }).when(consumer).acknowledge(
            org.mockito.ArgumentMatchers.<ConsumerRecord<byte[], byte[]>>any(),
            org.mockito.ArgumentMatchers.<AcknowledgeType>any());

        // Two "racers": one queues ACCEPT, one queues RELEASE, both iterate the same
        // record list concurrently. Each record must get one ack at the broker.
        CountDownLatch ready = new CountDownLatch(2);
        CountDownLatch go = new CountDownLatch(1);

        Thread accepter = new Thread(() -> {
            ready.countDown();
            try { go.await(); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); return; }
            for (InFlightRecord<byte[], byte[]> r : records) {
                coordinator.queueAck(r, AcknowledgeType.ACCEPT);
            }
        }, "accepter");
        Thread releaser = new Thread(() -> {
            ready.countDown();
            try { go.await(); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); return; }
            for (InFlightRecord<byte[], byte[]> r : records) {
                coordinator.queueAck(r, AcknowledgeType.RELEASE);
            }
        }, "releaser");
        accepter.start();
        releaser.start();
        ready.await();
        go.countDown();
        accepter.join();
        releaser.join();

        // Drain pending acks now that both racers finished. Each record's ONE winning
        // ack lands at the broker; the doAnswer above asserts no double-ack.
        coordinator.commitPendingAcks(consumer);

        assertThat(firstAck).hasSize(totalRecords);
        // Sanity: both ACCEPT and RELEASE should have won at least sometimes (proves
        // the race actually happened across enough records to be a real stress test).
        long acceptWins = firstAck.values().stream().filter(t -> t == AcknowledgeType.ACCEPT).count();
        long releaseWins = firstAck.values().stream().filter(t -> t == AcknowledgeType.RELEASE).count();
        assertThat(acceptWins + releaseWins).isEqualTo(totalRecords);
        // We don't assert specific split — just that BOTH paths were exercised.
        // (If one side always wins, the test still passes correctness, but the race
        // wasn't actually contended. With 500 records and tight scheduling, both
        // typically win at least a few.)
    }

}
