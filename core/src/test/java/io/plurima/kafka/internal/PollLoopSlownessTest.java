package io.plurima.kafka.internal;

import io.plurima.kafka.metrics.PlurimaMetrics;
import org.apache.kafka.clients.consumer.AcknowledgeType;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies the PollLoop → tracker link of Pillar 2: when the drain barrier force-RELEASEs a
 * stuck record, it is recorded as a slowness release (via {@code queueReleaseForSlowness}), so a
 * later RetryEngine evaluation will not count that redelivery toward exhaustion.
 */
class PollLoopSlownessTest {

    private Thread loopThread;
    private PollLoop loop;

    @AfterEach
    void tearDown() throws InterruptedException {
        if (loop != null) loop.shutdown();
        if (loopThread != null) loopThread.join(5_000);
    }

    @Test
    void forceReleaseAtBarrierRecordsSlowness() {
        FakeShareConsumer consumer = new FakeShareConsumer();
        InFlightRegistry registry = new InFlightRegistry();
        SlownessReleaseTracker tracker = new SlownessReleaseTracker(1024);
        AckCoordinator coordinator = new AckCoordinator(registry, PlurimaMetrics.noOp(), tracker);
        BackpressureGate gate = new BackpressureGate(4);
        WorkDispatcher noop = r -> { /* never completes → stuck → force-released at barrier */ };

        Duration barrier = Duration.ofMillis(40);
        loop = new PollLoop(
            consumer, noop, coordinator, registry, gate,
            Duration.ofMillis(40), barrier, Duration.ofSeconds(2), barrier,
            PlurimaMetrics.noOp(), "t", "g1", null, null, null, /*lockDurationExplicitlySet*/ true);

        consumer.enqueue(new ConsumerRecord<>("t", 0, 7L, new byte[0], new byte[0]));

        loopThread = new Thread(loop, "test-poll-slowness");
        loopThread.setDaemon(true);
        loopThread.start();

        RecordCoord coord = new RecordCoord("t", 0, 7L);
        long deadline = System.nanoTime() + Duration.ofSeconds(3).toNanos();
        while (System.nanoTime() < deadline && tracker.releaseCount(coord) == 0) {
            try { Thread.sleep(10); } catch (InterruptedException e) { Thread.currentThread().interrupt(); break; }
        }

        assertThat(tracker.releaseCount(coord))
            .as("a barrier force-RELEASE must be recorded as a slowness release")
            .isGreaterThanOrEqualTo(1);
        // And the broker actually received a RELEASE for it.
        assertThat(consumer.acks().stream()
            .anyMatch(a -> a.offset() == 7L && a.type() == AcknowledgeType.RELEASE)).isTrue();
    }
}
