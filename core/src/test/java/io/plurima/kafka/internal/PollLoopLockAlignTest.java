package io.plurima.kafka.internal;

import io.plurima.kafka.metrics.PlurimaMetrics;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies Pillar 1: when {@code .lockDuration(...)} was NOT set explicitly, the drain barrier
 * auto-aligns to 0.8 × the broker's record-lock duration once discovered; when it WAS set
 * explicitly, the configured value is preserved.
 */
class PollLoopLockAlignTest {

    private Thread loopThread;
    private PollLoop loop;

    @AfterEach
    void tearDown() throws InterruptedException {
        if (loop != null) loop.shutdown();
        if (loopThread != null) loopThread.join(5_000);
    }

    private PollLoop start(boolean explicit, int brokerLockMs) {
        FakeShareConsumer consumer = new FakeShareConsumer();
        consumer.setAcquisitionLockTimeoutMs(brokerLockMs);
        InFlightRegistry registry = new InFlightRegistry();
        AckCoordinator coordinator = new AckCoordinator(registry, PlurimaMetrics.noOp());
        BackpressureGate gate = new BackpressureGate(4);
        WorkDispatcher noop = r -> { };
        Duration configured = Duration.ofSeconds(30);
        loop = new PollLoop(
            consumer, noop, coordinator, registry, gate,
            Duration.ofMillis(50), configured, Duration.ofSeconds(2), configured,
            PlurimaMetrics.noOp(), "t", null, null, null, explicit);
        loopThread = new Thread(loop, "test-lock-align");
        loopThread.setDaemon(true);
        loopThread.start();
        return loop;
    }

    private void awaitBarrier(PollLoop l, long expectedMillis) {
        long deadline = System.nanoTime() + Duration.ofSeconds(3).toNanos();
        while (System.nanoTime() < deadline) {
            if (l.currentBarrierMillis() == expectedMillis) return;
            try { Thread.sleep(10); } catch (InterruptedException e) { Thread.currentThread().interrupt(); return; }
        }
        assertThat(l.currentBarrierMillis()).isEqualTo(expectedMillis);
    }

    @Test
    void autoAlignsToEightyPercentOfBrokerLockWhenNotExplicit() {
        PollLoop l = start(/*explicit*/ false, /*brokerLockMs*/ 60_000);
        awaitBarrier(l, 48_000);   // 0.8 × 60_000
    }

    @Test
    void preservesExplicitLockDuration() {
        PollLoop l = start(/*explicit*/ true, /*brokerLockMs*/ 60_000);
        // Give the loop time to run several polls; the barrier must stay at the configured 30s.
        try { Thread.sleep(300); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        assertThat(l.currentBarrierMillis()).isEqualTo(30_000);
    }
}
