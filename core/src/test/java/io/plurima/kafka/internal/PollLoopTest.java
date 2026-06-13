package io.plurima.kafka.internal;

import io.plurima.kafka.AdaptiveBarrierConfig;
import io.plurima.kafka.OrderingMode;
import io.plurima.kafka.RecordListener;
import io.plurima.kafka.deserializer.RecordDeserializer;
import org.apache.kafka.clients.consumer.AcknowledgeType;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.CorruptRecordException;
import org.apache.kafka.common.errors.RecordDeserializationException;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import io.plurima.kafka.metrics.PlurimaMetrics;

import static org.assertj.core.api.Assertions.assertThat;

class PollLoopTest {

    @Test
    void duplicateCoordIsReleasedDirectlySoKip932ContractHolds() throws Exception {
        // Simulate the rare race where the broker redelivers a record whose coord is
        // STILL in-flight in our registry (lock-expiry / force-RELEASE edges). The
        // duplicate must be:
        //   1. NOT dispatched (older worker retains ownership of the coord).
        //   2. Direct-RELEASE acked to the broker so KIP-932 explicit-mode's
        //      "every polled record must be acked before the next poll" holds.
        // Without the direct ack, the next poll would throw IllegalStateException.
        FakeShareConsumer consumer = new FakeShareConsumer();
        consumer.subscribe(List.of("t"));

        InFlightRegistry registry = new InFlightRegistry();
        AckCoordinator coordinator = new AckCoordinator(registry);
        WorkerLauncher launcher = new WorkerLauncher();
        BackpressureGate gate = new BackpressureGate(10);

        // Pre-register a record at offset 1 — simulates an older worker still in flight
        // for the same coord.
        ConsumerRecord<byte[], byte[]> original = new ConsumerRecord<>("t", 0, 1L, new byte[0], new byte[]{1});
        InFlightRecord<byte[], byte[]> originalInFlight = new InFlightRecord<>(original);
        assertThat(registry.register(originalInFlight))
            .as("original registration succeeds")
            .isTrue();

        // Listener should NOT see the duplicate record — the older worker owns coord (0,1).
        AtomicInteger listenerCalls = new AtomicInteger();
        RecordListener<byte[], byte[]> listener = (r, ctx) -> listenerCalls.incrementAndGet();
        ListenerInvoker invoker = ListenerInvoker.forImplicit(
            listener, RecordDeserializer.bytes(), RecordDeserializer.bytes());
        WorkerProcessor processor = new WorkerProcessor(
            invoker, new RetryEngine(io.plurima.kafka.retry.RetryPolicy.noRetry()),
            coordinator);
        UnorderedDispatcher dispatcher = new UnorderedDispatcher(
            processor, registry, coordinator, launcher, gate, OrderingMode.UNORDERED);

        // Enqueue ONE duplicate delivery of coord (0,1) — same topic+partition+offset.
        consumer.enqueue(new ConsumerRecord<>("t", 0, 1L, new byte[0], new byte[]{2}));

        PollLoop loop = new PollLoop(
            consumer, dispatcher, coordinator, registry, gate,
            Duration.ofMillis(50), Duration.ofSeconds(30), Duration.ofSeconds(5));

        Thread t = new Thread(loop, "duplicate-coord-loop");
        t.start();
        try {
            // Wait until the FakeShareConsumer recorded the RELEASE for the duplicate.
            long start = System.nanoTime();
            while (consumer.acks().isEmpty()
                && (System.nanoTime() - start) < TimeUnit.SECONDS.toNanos(3)) {
                Thread.sleep(20);
            }

            assertThat(consumer.acks())
                .as("duplicate must be direct-RELEASEd so the next poll's batch contract holds")
                .anySatisfy(call -> {
                    assertThat(call.topic()).isEqualTo("t");
                    assertThat(call.partition()).isEqualTo(0);
                    assertThat(call.offset()).isEqualTo(1L);
                    assertThat(call.type()).isEqualTo(AcknowledgeType.RELEASE);
                });
            assertThat(listenerCalls.get())
                .as("listener must NOT be invoked for the duplicate — older worker still owns the coord")
                .isZero();
            assertThat(registry.isCurrent(originalInFlight))
                .as("original instance must remain the registered one")
                .isTrue();
        } finally {
            loop.shutdown();
            t.join(5_000);
            launcher.close();
        }
    }

    @Test
    void pollsRecordsDispatchesAndAcks() throws Exception {
        FakeShareConsumer consumer = new FakeShareConsumer();
        consumer.subscribe(List.of("t"));
        consumer.enqueue(new ConsumerRecord<>("t", 0, 1L, new byte[0], new byte[]{1}));
        consumer.enqueue(new ConsumerRecord<>("t", 0, 2L, new byte[0], new byte[]{2}));

        AtomicInteger processed = new AtomicInteger();
        CountDownLatch latch = new CountDownLatch(2);
        RecordListener<byte[], byte[]> listener = (r, ctx) -> {
            processed.incrementAndGet();
            latch.countDown();
        };

        InFlightRegistry registry = new InFlightRegistry();
        AckCoordinator coordinator = new AckCoordinator(registry);
        WorkerLauncher launcher = new WorkerLauncher();
        BackpressureGate gate = new BackpressureGate(10);
        ListenerInvoker invoker = ListenerInvoker.forImplicit(listener, RecordDeserializer.bytes(), RecordDeserializer.bytes());
        WorkerProcessor processor = new WorkerProcessor(
            invoker, new RetryEngine(io.plurima.kafka.retry.RetryPolicy.noRetry()),
            coordinator);
        UnorderedDispatcher dispatcher = new UnorderedDispatcher(processor, registry, coordinator, launcher, gate, OrderingMode.UNORDERED);

        PollLoop loop = new PollLoop(
            consumer, dispatcher, coordinator, registry, gate,
            Duration.ofMillis(50), Duration.ofSeconds(30), Duration.ofSeconds(5));

        Thread t = new Thread(loop, "test-poll-loop");
        t.start();
        try {
            assertThat(latch.await(2, TimeUnit.SECONDS)).isTrue();

            // Give the poll loop a tick to drain acks
            long start = System.nanoTime();
            while (consumer.acks().size() < 2
                && (System.nanoTime() - start) < TimeUnit.SECONDS.toNanos(2)) {
                Thread.sleep(20);
            }

            assertThat(consumer.acks())
                .extracting(FakeShareConsumer.AckCall::type)
                .containsOnly(AcknowledgeType.ACCEPT);
            assertThat(consumer.commitAsyncCalls()).isGreaterThanOrEqualTo(1);
            assertThat(processed.get()).isEqualTo(2);
        } finally {
            loop.shutdown();
            t.join(5_000);
            launcher.close();
        }
    }

    @Test
    void consumerCloseUsesBoundedTimeoutAlignedToShutdownDrain() throws Exception {
        // PollLoop must pass an explicit Duration to consumer.close(...) rather
        // than fall through to the consumer's default (30s). If the broker is
        // unresponsive during close, the non-daemon poll thread would otherwise
        // outlive ShareConsumerRuntime's join budget (shutdownDrainTimeout + 5s)
        // and PlurimaConsumer.close() would return while close was still in
        // progress in the background.
        FakeShareConsumer consumer = new FakeShareConsumer();
        consumer.subscribe(List.of("t"));

        InFlightRegistry registry = new InFlightRegistry();
        AckCoordinator coordinator = new AckCoordinator(registry);
        WorkerLauncher launcher = new WorkerLauncher();
        BackpressureGate gate = new BackpressureGate(10);
        ListenerInvoker invoker = ListenerInvoker.forImplicit(
            (r, ctx) -> {}, RecordDeserializer.bytes(), RecordDeserializer.bytes());
        WorkerProcessor processor = new WorkerProcessor(
            invoker, new RetryEngine(io.plurima.kafka.retry.RetryPolicy.noRetry()),
            coordinator);
        UnorderedDispatcher dispatcher = new UnorderedDispatcher(
            processor, registry, coordinator, launcher, gate, OrderingMode.UNORDERED);

        Duration shutdownDrainTimeout = Duration.ofSeconds(7);
        PollLoop loop = new PollLoop(
            consumer, dispatcher, coordinator, registry, gate,
            Duration.ofMillis(50), Duration.ofSeconds(30), shutdownDrainTimeout);

        Thread t = new Thread(loop, "close-timeout-loop");
        t.start();
        Thread.sleep(100);
        loop.shutdown();
        t.join(5_000);

        assertThat(consumer.lastCloseTimeout())
            .as("PollLoop must pass an explicit Duration to consumer.close (NOT the no-arg form)")
            .isNotNull();
        // The close timeout is the REMAINING budget after awaitDrain — bounded
        // above by shutdownDrainTimeout (used the empty registry case here so the
        // drain returns immediately) and below by the 2s floor. The end-to-end
        // budget (drain + close) must NEVER exceed shutdownDrainTimeout, otherwise
        // ShareConsumerRuntime's join could timeout while the consumer is still
        // closing.
        Duration closeTimeout = consumer.lastCloseTimeout();
        assertThat(closeTimeout)
            .as("close timeout must not exceed configured shutdownDrainTimeout")
            .isLessThanOrEqualTo(shutdownDrainTimeout);
        assertThat(closeTimeout)
            .as("close timeout must remain above the 2s broker-contact floor")
            .isGreaterThanOrEqualTo(Duration.ofSeconds(2));
        launcher.close();
    }

    @Test
    void consumerCloseFallsToFloorWhenDrainConsumesEntireBudget() throws Exception {
        // Drain budget exhaustion: a registered in-flight record forces the
        // shutdown awaitDrain to wait the full shutdownDrainTimeout. consumer.close
        // must then fall to the floor (~2s) so the total drainAndClose stays within
        // the runtime's join window. Previously close was given another full
        // shutdownDrainTimeout, exceeding the join.
        //
        // Strategy: pre-register a record directly with the registry (bypassing the
        // poll loop). The loop starts, polls empty, sees wakeup, exits to finally.
        // shutdown's awaitDrain (this loop iteration's drainAndClose call) sees the
        // pre-registered record and waits the full shutdownDrainTimeout.
        FakeShareConsumer consumer = new FakeShareConsumer();
        consumer.subscribe(List.of("t"));

        InFlightRegistry registry = new InFlightRegistry();
        AckCoordinator coordinator = new AckCoordinator(registry);
        WorkerLauncher launcher = new WorkerLauncher();
        BackpressureGate gate = new BackpressureGate(10);
        ListenerInvoker invoker = ListenerInvoker.forImplicit(
            (r, ctx) -> {}, RecordDeserializer.bytes(), RecordDeserializer.bytes());
        WorkerProcessor processor = new WorkerProcessor(
            invoker, new RetryEngine(io.plurima.kafka.retry.RetryPolicy.noRetry()),
            coordinator);
        UnorderedDispatcher dispatcher = new UnorderedDispatcher(
            processor, registry, coordinator, launcher, gate, OrderingMode.UNORDERED);

        // Pre-register an in-flight record — no worker will ever complete it.
        InFlightRecord<byte[], byte[]> stuck = new InFlightRecord<>(
            new ConsumerRecord<>("t", 0, 1L, new byte[0], new byte[]{1}));
        registry.register(stuck);

        // Use a small barrierTimeout too so the per-iteration drain barrier
        // doesn't blow past our test budget while waiting for the (never-going-
        // to-complete) record. The barrier timeout is what awaitDrain uses
        // inside drainAndCommit on each iteration — we don't care about that
        // path; we care about the SHUTDOWN drainAndClose budget.
        Duration shutdownDrainTimeout = Duration.ofMillis(600);
        PollLoop loop = new PollLoop(
            consumer, dispatcher, coordinator, registry, gate,
            Duration.ofMillis(50), Duration.ofSeconds(30), shutdownDrainTimeout,
            /* barrierTimeout */ Duration.ofMillis(50), PlurimaMetrics.noOp());

        Thread t = new Thread(loop, "drain-budget-loop");
        t.start();
        Thread.sleep(150);  // let the loop poll once
        loop.shutdown();
        t.join(shutdownDrainTimeout.toMillis() + 5_000);

        assertThat(t.isAlive())
            .as("poll thread must finish within join budget so consumer.close has actually been invoked")
            .isFalse();
        Duration closeTimeout = consumer.lastCloseTimeout();
        assertThat(closeTimeout)
            .as("when awaitDrain consumes the full budget, close must fall to the floor (not 0, not the full timeout)")
            .isNotNull();
        assertThat(closeTimeout)
            .as("floor must be ≥ 2s so the broker has a chance to ack the close")
            .isGreaterThanOrEqualTo(Duration.ofSeconds(2));
        launcher.close();
    }

    @Test
    void shutdownStopsLoopAndClosesConsumer() throws Exception {
        FakeShareConsumer consumer = new FakeShareConsumer();
        consumer.subscribe(List.of("t"));

        InFlightRegistry registry = new InFlightRegistry();
        AckCoordinator coordinator = new AckCoordinator(registry);
        WorkerLauncher launcher = new WorkerLauncher();
        BackpressureGate gate = new BackpressureGate(10);
        ListenerInvoker invoker2 = ListenerInvoker.forImplicit((r, ctx) -> {}, RecordDeserializer.bytes(), RecordDeserializer.bytes());
        WorkerProcessor processor2 = new WorkerProcessor(
            invoker2, new RetryEngine(io.plurima.kafka.retry.RetryPolicy.noRetry()),
            coordinator);
        UnorderedDispatcher dispatcher = new UnorderedDispatcher(processor2, registry, coordinator, launcher, gate, OrderingMode.UNORDERED);

        PollLoop loop = new PollLoop(
            consumer, dispatcher, coordinator, registry, gate,
            Duration.ofMillis(50), Duration.ofSeconds(30), Duration.ofSeconds(5));

        Thread t = new Thread(loop);
        t.start();
        Thread.sleep(150);
        loop.shutdown();
        t.join(5_000);

        assertThat(t.isAlive()).isFalse();
        launcher.close();
    }

    @Test
    void pollThrowsRecordDeserializationExceptionRecordsRejectedAndLoopContinues() throws Exception {
        FakeShareConsumer consumer = new FakeShareConsumer();
        consumer.subscribe(List.of("t"));

        InFlightRegistry registry = new InFlightRegistry();
        AckCoordinator coordinator = new AckCoordinator(registry);
        WorkerLauncher launcher = new WorkerLauncher();
        BackpressureGate gate = new BackpressureGate(10);

        AtomicInteger calls = new AtomicInteger();
        RecordListener<byte[], byte[]> listener = (r, ctx) -> calls.incrementAndGet();
        ListenerInvoker invoker = ListenerInvoker.forImplicit(listener, RecordDeserializer.bytes(), RecordDeserializer.bytes());
        WorkerProcessor processor = new WorkerProcessor(
            invoker, new RetryEngine(io.plurima.kafka.retry.RetryPolicy.noRetry()),
            coordinator);
        UnorderedDispatcher dispatcher = new UnorderedDispatcher(processor, registry, coordinator, launcher, gate, OrderingMode.UNORDERED);

        // Inject a poison-pill exception on the first poll
        consumer.throwOnNextPoll(new RecordDeserializationException(
            RecordDeserializationException.DeserializationExceptionOrigin.VALUE,
            new TopicPartition("t", 0), 42L, -1L,
            TimestampType.NO_TIMESTAMP_TYPE, null, null, new RecordHeaders(),
            "bad bytes", new RuntimeException("decode failed")));

        // Then enqueue a normal record for the second poll
        consumer.enqueue(new ConsumerRecord<>("t", 0, 43L, new byte[0], new byte[]{1}));

        PollLoop loop = new PollLoop(
            consumer, dispatcher, coordinator, registry, gate,
            Duration.ofMillis(50), Duration.ofSeconds(30), Duration.ofSeconds(5));

        Thread t = new Thread(loop, "poison-pill-loop");
        t.start();
        try {
            long start = System.nanoTime();
            while (calls.get() == 0 && (System.nanoTime() - start) < TimeUnit.SECONDS.toNanos(3)) {
                Thread.sleep(20);
            }
            assertThat(calls.get()).isEqualTo(1);

            // Verify the poison pill was REJECTed via the 4-arg form
            boolean foundReject = consumer.acks().stream()
                .anyMatch(a -> a.offset() == 42L
                    && a.type() == AcknowledgeType.REJECT);
            assertThat(foundReject).isTrue();
        } finally {
            loop.shutdown();
            t.join(5_000);
            launcher.close();
        }
    }

    @Test
    void forceReleaseStuckRecordsRemovesFromRegistryAllowingRedelivery() throws Exception {
        FakeShareConsumer consumer = new FakeShareConsumer();
        consumer.subscribe(List.of("t"));

        InFlightRegistry registry = new InFlightRegistry();
        AckCoordinator coordinator = new AckCoordinator(registry);
        WorkerLauncher launcher = new WorkerLauncher();
        // concurrency=1 — exactly the pathological case from the finding: when the single
        // worker is stuck, force-RELEASE must release the permit or the next poll blocks
        // forever on gate.acquire(1) and the redelivered record never arrives.
        BackpressureGate gate = new BackpressureGate(1);

        CountDownLatch handlerEntered = new CountDownLatch(1);
        CountDownLatch handlerRelease = new CountDownLatch(1);
        AtomicInteger calls = new AtomicInteger();
        RecordListener<byte[], byte[]> listener = (r, ctx) -> {
            calls.incrementAndGet();
            handlerEntered.countDown();
            try {
                handlerRelease.await(10, TimeUnit.SECONDS);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            }
        };
        ListenerInvoker invoker = ListenerInvoker.forImplicit(
            listener, RecordDeserializer.bytes(), RecordDeserializer.bytes());
        WorkerProcessor processor = new WorkerProcessor(
            invoker, new RetryEngine(io.plurima.kafka.retry.RetryPolicy.noRetry()),
            coordinator);
        UnorderedDispatcher dispatcher = new UnorderedDispatcher(processor, registry, coordinator, launcher, gate, OrderingMode.UNORDERED);

        // barrierTimeout=200ms so the drain barrier trips fast while the handler is still blocked.
        PollLoop loop = new PollLoop(
            consumer, dispatcher, coordinator, registry, gate,
            Duration.ofMillis(50), Duration.ofSeconds(30), Duration.ofSeconds(5),
            Duration.ofMillis(200), PlurimaMetrics.noOp());

        ConsumerRecord<byte[], byte[]> first = new ConsumerRecord<>("t", 0, 1L, new byte[0], new byte[]{1});
        consumer.enqueue(first);

        Thread t = new Thread(loop, "force-release-loop");
        t.start();
        try {
            assertThat(handlerEntered.await(2, TimeUnit.SECONDS)).isTrue();

            // Wait for the barrier to fire and the coord to be removed from the registry.
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
            while (registry.currentInFlight() != 0 && System.nanoTime() < deadline) {
                Thread.sleep(20);
            }
            assertThat(registry.currentInFlight())
                .as("force-RELEASE must remove abandoned coord from registry")
                .isEqualTo(0);

            boolean sawRelease = consumer.acks().stream()
                .anyMatch(a -> a.offset() == 1L && a.type() == AcknowledgeType.RELEASE);
            assertThat(sawRelease)
                .as("force-RELEASE must queue an actual RELEASE ack")
                .isTrue();

            // Simulate broker redelivery of the same coord. With concurrency=1, this only
            // gets processed if forceReleaseStuckRecords released the backpressure permit
            // held by the (still-stuck) worker — otherwise the next gate.acquire(1) blocks
            // indefinitely and the listener never fires a second time. The call-count
            // assertion below IS the proof that the permit was released; asserting
            // gate.availablePermits() directly is racy because the poll loop re-acquires
            // immediately for the next iteration.
            consumer.enqueue(new ConsumerRecord<>("t", 0, 1L, new byte[0], new byte[]{1}));

            deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
            while (calls.get() < 2 && System.nanoTime() < deadline) {
                Thread.sleep(20);
            }
            assertThat(calls.get())
                .as("redelivered record after force-RELEASE must trigger a fresh listener invocation "
                    + "— if this fails with concurrency=1, force-RELEASE failed to free the permit")
                .isGreaterThanOrEqualTo(2);
        } finally {
            handlerRelease.countDown();
            loop.shutdown();
            t.join(5_000);
            launcher.close();
        }
    }

    private static long ms(long millis) { return millis * 1_000_000L; }

    private PollLoop newAdaptiveLoop(
            FakeShareConsumer consumer, HandlerLatencyWindow window,
            AdaptiveBarrierConfig config, Duration pollTimeout, Duration lockDuration) {
        InFlightRegistry registry = new InFlightRegistry();
        AckCoordinator coordinator = new AckCoordinator(registry);
        WorkerLauncher launcher = new WorkerLauncher();
        BackpressureGate gate = new BackpressureGate(10);
        ListenerInvoker invoker = ListenerInvoker.forImplicit(
            (r, ctx) -> {}, RecordDeserializer.bytes(), RecordDeserializer.bytes());
        WorkerProcessor processor = new WorkerProcessor(
            invoker, new RetryEngine(io.plurima.kafka.retry.RetryPolicy.noRetry()),
            coordinator, null, io.plurima.kafka.metrics.PlurimaMetrics.noOp(), window);
        UnorderedDispatcher dispatcher = new UnorderedDispatcher(
            processor, registry, coordinator, launcher, gate, OrderingMode.UNORDERED);
        return new PollLoop(
            consumer, dispatcher, coordinator, registry, gate,
            pollTimeout, lockDuration, Duration.ofSeconds(5), lockDuration,
            io.plurima.kafka.metrics.PlurimaMetrics.noOp(), "t",
            /* onLoopExit */ null, window, config);
    }

    @Test
    void effectiveBarrierIsFlatLockDurationDuringWarmup() {
        HandlerLatencyWindow window = new HandlerLatencyWindow(1024);
        for (int i = 0; i < 50; i++) window.record(ms(100));   // < MIN_SAMPLES (100)
        PollLoop loop = newAdaptiveLoop(new FakeShareConsumer(), window,
            AdaptiveBarrierConfig.defaults(), Duration.ofMillis(50), Duration.ofSeconds(30));
        assertThat(loop.effectiveBarrier()).isEqualTo(Duration.ofSeconds(30));
    }

    @Test
    void effectiveBarrierAdaptsDownWhenInRange() {
        HandlerLatencyWindow window = new HandlerLatencyWindow(1024);
        for (int i = 0; i < 200; i++) window.record(ms(1000));  // p99 = 1000ms
        PollLoop loop = newAdaptiveLoop(new FakeShareConsumer(), window,
            AdaptiveBarrierConfig.defaults(), Duration.ofMillis(50), Duration.ofSeconds(30));
        assertThat(loop.effectiveBarrier()).isEqualTo(Duration.ofMillis(3000));  // 1000 * 3
    }

    @Test
    void effectiveBarrierClampsToLockDurationCeiling() {
        HandlerLatencyWindow window = new HandlerLatencyWindow(1024);
        for (int i = 0; i < 200; i++) window.record(ms(20_000)); // 20s * 3 = 60s > 30s
        PollLoop loop = newAdaptiveLoop(new FakeShareConsumer(), window,
            AdaptiveBarrierConfig.defaults(), Duration.ofMillis(50), Duration.ofSeconds(30));
        assertThat(loop.effectiveBarrier()).isEqualTo(Duration.ofSeconds(30));
    }

    @Test
    void effectiveBarrierClampsToFloor() {
        HandlerLatencyWindow window = new HandlerLatencyWindow(1024);
        for (int i = 0; i < 200; i++) window.record(ms(10));    // 10ms * 3 = 30ms < floor 1s
        PollLoop loop = newAdaptiveLoop(new FakeShareConsumer(), window,
            AdaptiveBarrierConfig.defaults(), Duration.ofMillis(50), Duration.ofSeconds(30));
        assertThat(loop.effectiveBarrier()).isEqualTo(Duration.ofSeconds(1));  // max(FLOOR=1s, pollTimeout=50ms)
    }

    @Test
    void effectiveBarrierIsFlatWhenAdaptiveDisabled() {
        PollLoop loop = newAdaptiveLoop(new FakeShareConsumer(), null, null,
            Duration.ofMillis(50), Duration.ofSeconds(30));
        assertThat(loop.effectiveBarrier()).isEqualTo(Duration.ofSeconds(30));
    }

    @Test
    void effectiveBarrierCeilingWinsWhenLockDurationBelowFloor() {
        // Pathological config: lockDuration (== barrierTimeout) is BELOW the 1s floor.
        // The ceiling must still win — the adaptive barrier can never exceed lockDuration,
        // or it would break the local<broker-lock invariant the whole feature rests on.
        HandlerLatencyWindow window = new HandlerLatencyWindow(1024);
        for (int i = 0; i < 200; i++) window.record(ms(10));  // p99×3 = 30ms < floor 1s
        PollLoop loop = newAdaptiveLoop(new FakeShareConsumer(), window,
            AdaptiveBarrierConfig.defaults(), Duration.ofMillis(50), Duration.ofMillis(500));
        // floor would push to 1s, but ceiling (500ms) caps it — ceiling wins.
        assertThat(loop.effectiveBarrier()).isEqualTo(Duration.ofMillis(500));
    }

    @Test
    void currentBarrierMillisReflectsLastEffectiveBarrier() {
        // The gauge reads currentBarrierMillis(); it must track effectiveBarrier()'s result.
        HandlerLatencyWindow window = new HandlerLatencyWindow(1024);
        for (int i = 0; i < 200; i++) window.record(ms(1000));  // p99×3 = 3000ms (in range)
        PollLoop loop = newAdaptiveLoop(new FakeShareConsumer(), window,
            AdaptiveBarrierConfig.defaults(), Duration.ofMillis(50), Duration.ofSeconds(30));
        Duration eff = loop.effectiveBarrier();
        assertThat(loop.currentBarrierMillis()).isEqualTo(eff.toMillis()).isEqualTo(3000L);
    }

    @Test
    void pollThrowsCorruptRecordExceptionLogsAndContinues() throws Exception {
        FakeShareConsumer consumer = new FakeShareConsumer();
        consumer.subscribe(List.of("t"));

        InFlightRegistry registry = new InFlightRegistry();
        AckCoordinator coordinator = new AckCoordinator(registry);
        WorkerLauncher launcher = new WorkerLauncher();
        BackpressureGate gate = new BackpressureGate(10);

        AtomicInteger calls = new AtomicInteger();
        RecordListener<byte[], byte[]> listener = (r, ctx) -> calls.incrementAndGet();
        ListenerInvoker invoker = ListenerInvoker.forImplicit(listener, RecordDeserializer.bytes(), RecordDeserializer.bytes());
        WorkerProcessor processor = new WorkerProcessor(
            invoker, new RetryEngine(io.plurima.kafka.retry.RetryPolicy.noRetry()),
            coordinator);
        UnorderedDispatcher dispatcher = new UnorderedDispatcher(processor, registry, coordinator, launcher, gate, OrderingMode.UNORDERED);

        consumer.throwOnNextPoll(new CorruptRecordException("checksum mismatch"));
        consumer.enqueue(new ConsumerRecord<>("t", 0, 100L, new byte[0], new byte[]{1}));

        PollLoop loop = new PollLoop(
            consumer, dispatcher, coordinator, registry, gate,
            Duration.ofMillis(50), Duration.ofSeconds(30), Duration.ofSeconds(5));

        Thread t = new Thread(loop, "corrupt-batch-loop");
        t.start();
        try {
            long start = System.nanoTime();
            while (calls.get() == 0 && (System.nanoTime() - start) < TimeUnit.SECONDS.toNanos(3)) {
                Thread.sleep(20);
            }
            assertThat(calls.get()).isEqualTo(1);
        } finally {
            loop.shutdown();
            t.join(5_000);
            launcher.close();
        }
    }
}
