package io.plurima.kafka.internal;

import org.apache.kafka.clients.consumer.AcknowledgeType;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ShareConsumer;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * Verifies the write-side of Pillar 2: {@link AckCoordinator#queueReleaseForSlowness} both
 * records a slowness release in the {@link SlownessReleaseTracker} AND queues a normal RELEASE
 * (so the contract/redelivery behavior is unchanged) — while a failure-driven
 * {@link AckCoordinator#queueAck}(RELEASE) does NOT touch the tracker.
 */
class AckCoordinatorSlownessTest {

    private InFlightRegistry registry;
    private SlownessReleaseTracker tracker;
    private ShareConsumer<byte[], byte[]> consumer;
    private AckCoordinator coordinator;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        registry = new InFlightRegistry();
        tracker = new SlownessReleaseTracker(1024);
        consumer = mock(ShareConsumer.class);
        coordinator = new AckCoordinator(registry, io.plurima.kafka.metrics.PlurimaMetrics.noOp(), tracker);
    }

    private InFlightRecord<byte[], byte[]> rec(long offset) {
        InFlightRecord<byte[], byte[]> r =
            new InFlightRecord<>(new ConsumerRecord<>("t", 0, offset, new byte[0], new byte[0]));
        registry.register(r);
        return r;
    }

    @Test
    void slownessReleaseRecordsAndQueuesRelease() {
        InFlightRecord<byte[], byte[]> r = rec(1);

        coordinator.queueReleaseForSlowness(r);

        assertThat(tracker.releaseCount(r.coord())).isEqualTo(1);

        coordinator.commitPendingAcks(consumer);
        verify(consumer).acknowledge(
            argThat(cr -> cr.offset() == 1L), eq(AcknowledgeType.RELEASE));
    }

    @Test
    void failureDrivenReleaseDoesNotRecordSlowness() {
        InFlightRecord<byte[], byte[]> r = rec(2);

        // A normal RELEASE (WorkerProcessor RetryDelayed path) must NOT be counted as slowness.
        coordinator.queueAck(r, AcknowledgeType.RELEASE);

        assertThat(tracker.releaseCount(r.coord())).isZero();
    }

    // The slowness count is cleared only when the terminal ack's COMMIT succeeds (onComplete with
    // no exception), not at queue time — so a failed commit + redelivery keeps the subtraction.
    // These seed the tracker directly (= a prior delivery's force-release) and drive the lifecycle.

    private static Map<TopicIdPartition, Set<Long>> committed(long offset) {
        return Map.of(new TopicIdPartition(Uuid.ZERO_UUID, new TopicPartition("t", 0)), Set.of(offset));
    }

    @Test
    void terminalAcceptClearsSlownessCountOnlyAfterCommitSuccess() {
        InFlightRecord<byte[], byte[]> r = rec(3);
        tracker.recordRelease(r.coord());                       // a prior delivery was force-released
        coordinator.queueAck(r, AcknowledgeType.ACCEPT);

        // NOT cleared at queue time — the commit hasn't been confirmed yet.
        assertThat(tracker.releaseCount(r.coord())).isEqualTo(1);

        coordinator.onComplete(committed(3L), null);            // commit succeeds → now clear
        assertThat(tracker.releaseCount(r.coord())).isZero();
    }

    @Test
    void failedCommitKeepsSlownessCountForRedelivery() {
        InFlightRecord<byte[], byte[]> r = rec(4);
        tracker.recordRelease(r.coord());
        coordinator.queueAck(r, AcknowledgeType.ACCEPT);

        coordinator.onComplete(committed(4L), new RuntimeException("commit failed"));

        // Commit failed → record will be redelivered; the slowness subtraction MUST survive so
        // the redelivery isn't prematurely exhausted/DLT'd.
        assertThat(tracker.releaseCount(r.coord())).isEqualTo(1);
    }

    @Test
    void terminalRejectClearsOnCommitSuccess() {
        InFlightRecord<byte[], byte[]> r = rec(6);
        tracker.recordRelease(r.coord());
        coordinator.queueAck(r, AcknowledgeType.REJECT);
        coordinator.onComplete(committed(6L), null);
        assertThat(tracker.releaseCount(r.coord())).isZero();
    }

    @Test
    void committedReleaseDoesNotClearSlownessCount() {
        InFlightRecord<byte[], byte[]> r = rec(5);
        tracker.recordRelease(r.coord());
        coordinator.queueAck(r, AcknowledgeType.RELEASE);   // non-terminal
        coordinator.onComplete(committed(5L), null);        // even though its offset committed
        assertThat(tracker.releaseCount(r.coord())).isEqualTo(1);   // count kept for the redelivery
    }

    /** Typed any()-matcher for ConsumerRecord to avoid unchecked-conversion warnings (-Werror). */
    private static ConsumerRecord<byte[], byte[]> anyCr() {
        return org.mockito.ArgumentMatchers.<ConsumerRecord<byte[], byte[]>>any();
    }

    @Test
    void staleMarkFromFailedTerminalCommitIsNotClearedByLaterSuccessfulCommit() {
        // Regression for the exact previously-broken sequence: a terminal ACCEPT commit FAILS
        // (mark must be dropped, count kept), then a LATER successful callback for the SAME coord
        // (e.g. a redelivery's RELEASE) must NOT clear the still-live slowness count.
        InFlightRecord<byte[], byte[]> r = rec(7);
        tracker.recordRelease(r.coord());
        coordinator.queueAck(r, AcknowledgeType.ACCEPT);                            // mark added
        coordinator.onComplete(committed(7L), new RuntimeException("commit failed")); // fails
        assertThat(tracker.releaseCount(r.coord())).isEqualTo(1);                   // count kept

        coordinator.onComplete(committed(7L), null);   // later success for same coord
        assertThat(tracker.releaseCount(r.coord()))
            .as("stale mark from the failed terminal commit must not be consumed by a later commit")
            .isEqualTo(1);
    }

    @Test
    void acknowledgeThrowDropsPendingClearMarkSoLaterSuccessCannotClear() {
        // If consumer.acknowledge(...) throws (lease expired / record not waiting), the terminal
        // ack never lands — its slowness-clear mark must be dropped so a later success callback for
        // the same coord cannot clear the still-live count.
        InFlightRecord<byte[], byte[]> r = rec(8);
        tracker.recordRelease(r.coord());
        coordinator.queueAck(r, AcknowledgeType.ACCEPT);   // mark added
        doThrow(new IllegalStateException("the record cannot be acknowledged"))
            .when(consumer).acknowledge(anyCr(), eq(AcknowledgeType.ACCEPT));

        coordinator.commitPendingAcks(consumer);           // acknowledge throws → mark dropped
        assertThat(tracker.releaseCount(r.coord())).isEqualTo(1);   // count kept

        coordinator.onComplete(committed(8L), null);       // later success for same coord
        assertThat(tracker.releaseCount(r.coord()))
            .as("mark dropped on acknowledge() throw → later success must not clear")
            .isEqualTo(1);
    }
}
