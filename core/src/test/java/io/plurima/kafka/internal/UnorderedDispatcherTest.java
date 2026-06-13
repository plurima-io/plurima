package io.plurima.kafka.internal;

import io.plurima.kafka.ConsumerContext;
import io.plurima.kafka.OrderingMode;
import io.plurima.kafka.RecordListener;
import io.plurima.kafka.deserializer.RecordDeserializer;
import org.apache.kafka.clients.consumer.AcknowledgeType;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ShareConsumer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class UnorderedDispatcherTest {

    private InFlightRegistry registry;
    private AckCoordinator coordinator;
    private WorkerLauncher launcher;
    private BackpressureGate gate;
    private ShareConsumer<byte[], byte[]> consumer;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        registry = new InFlightRegistry();
        coordinator = new AckCoordinator(registry);
        launcher = new WorkerLauncher();
        gate = new BackpressureGate(10);
        consumer = mock(ShareConsumer.class);
    }

    private InFlightRecord<byte[], byte[]> rec(long offset) {
        InFlightRecord<byte[], byte[]> r = new InFlightRecord<>(new ConsumerRecord<>("t", 0, offset, new byte[]{1}, new byte[]{2}));
        registry.register(r);
        return r;
    }

    @Test
    void successfulListenerQueuesAcceptAndReleasesPermit() throws Exception {
        CountDownLatch ran = new CountDownLatch(1);
        AtomicReference<ConsumerContext> seenCtx = new AtomicReference<>();
        RecordListener<byte[], byte[]> listener = (r, ctx) -> {
            seenCtx.set(ctx);
            ran.countDown();
        };

        ListenerInvoker invoker = ListenerInvoker.forImplicit(listener, RecordDeserializer.bytes(), RecordDeserializer.bytes());
        WorkerProcessor processor = new WorkerProcessor(
            invoker, new RetryEngine(io.plurima.kafka.retry.RetryPolicy.noRetry()),
            coordinator);
        UnorderedDispatcher d = new UnorderedDispatcher(processor, registry, coordinator, launcher, gate, OrderingMode.UNORDERED);

        gate.acquire(1);
        d.dispatch(rec(1));

        assertThat(ran.await(1, TimeUnit.SECONDS)).isTrue();
        // worker should release the permit
        long start = System.nanoTime();
        while (gate.availablePermits() < 10 &&
            (System.nanoTime() - start) < TimeUnit.SECONDS.toNanos(1)) {
            Thread.sleep(5);
        }
        assertThat(gate.availablePermits()).isEqualTo(10);

        coordinator.commitPendingAcks(consumer);
        verify(consumer).acknowledge(
            argThat(cr -> cr.topic().equals("t") && cr.partition() == 0 && cr.offset() == 1L),
            eq(AcknowledgeType.ACCEPT));
        assertThat(seenCtx.get().orderingMode()).isEqualTo(OrderingMode.UNORDERED);
    }

    @Test
    void throwingListenerQueuesRejectAndReleasesPermit() throws Exception {
        AtomicInteger calls = new AtomicInteger();
        RecordListener<byte[], byte[]> listener = (r, ctx) -> {
            calls.incrementAndGet();
            throw new RuntimeException("boom");
        };

        ListenerInvoker invoker2 = ListenerInvoker.forImplicit(listener, RecordDeserializer.bytes(), RecordDeserializer.bytes());
        WorkerProcessor processor = new WorkerProcessor(
            invoker2, new RetryEngine(io.plurima.kafka.retry.RetryPolicy.noRetry()),
            coordinator);
        UnorderedDispatcher d = new UnorderedDispatcher(processor, registry, coordinator, launcher, gate, OrderingMode.UNORDERED);

        gate.acquire(1);
        d.dispatch(rec(2));

        long start = System.nanoTime();
        while (calls.get() == 0 && (System.nanoTime() - start) < TimeUnit.SECONDS.toNanos(1)) {
            Thread.sleep(5);
        }
        assertThat(calls.get()).isEqualTo(1);

        start = System.nanoTime();
        while (gate.availablePermits() < 10 &&
            (System.nanoTime() - start) < TimeUnit.SECONDS.toNanos(1)) {
            Thread.sleep(5);
        }

        coordinator.commitPendingAcks(consumer);
        verify(consumer).acknowledge(
            argThat(cr -> cr.topic().equals("t") && cr.partition() == 0 && cr.offset() == 2L),
            eq(AcknowledgeType.REJECT));
    }

    @Test
    void launchRejectionCompensatesRegistryAndBackpressure() throws Exception {
        // PollLoop calls registry.register + gate.acquire BEFORE dispatch. If the launcher
        // rejects the task (typically RejectedExecutionException once the worker pool has
        // been close()d), the dispatcher MUST complete the registry entry and release the
        // gate permit — otherwise the next poll() would block forever on the drain barrier
        // (registry never drains) and on the gate (permit never returns).
        ListenerInvoker invoker = ListenerInvoker.forImplicit(
            (r, ctx) -> { throw new AssertionError("listener must not be called"); },
            RecordDeserializer.bytes(), RecordDeserializer.bytes());
        WorkerProcessor processor = new WorkerProcessor(
            invoker, new RetryEngine(io.plurima.kafka.retry.RetryPolicy.noRetry()),
            coordinator);

        // Closing the launcher up front means every subsequent launch() throws
        // RejectedExecutionException — exact reproduction of the shutdown race.
        launcher.close();

        UnorderedDispatcher d = new UnorderedDispatcher(processor, registry, coordinator, launcher, gate, OrderingMode.UNORDERED);

        // Mirror PollLoop.dispatchBatch: register + permit consumed before dispatch.
        InFlightRecord<byte[], byte[]> in = rec(42);  // registers as a side effect
        gate.acquire(1);
        assertThat(gate.availablePermits()).isEqualTo(9);
        assertThat(registry.currentInFlight()).isEqualTo(1);

        d.dispatch(in);

        // Compensation must be synchronous on the caller thread — the launch() throw
        // happens immediately, so the dispatcher fires registry.complete + gate.release
        // before dispatch returns.
        assertThat(registry.currentInFlight())
            .as("registry must be cleared so the next poll's drain barrier doesn't hang")
            .isZero();
        assertThat(gate.availablePermits())
            .as("backpressure permit must return so the gate doesn't deadlock")
            .isEqualTo(10);

        // KIP-932 explicit mode: every polled record MUST be acked before the next
        // poll() — otherwise the next consumer.poll() throws IllegalStateException
        // ("The record cannot be acknowledged"). The dispatcher queues RELEASE so the
        // coordinator's next commitPendingAcks fires the ack and the broker redelivers.
        coordinator.commitPendingAcks(consumer);
        verify(consumer).acknowledge(
            argThat(cr -> cr.topic().equals("t") && cr.partition() == 0 && cr.offset() == 42L),
            eq(AcknowledgeType.RELEASE));
    }
}
