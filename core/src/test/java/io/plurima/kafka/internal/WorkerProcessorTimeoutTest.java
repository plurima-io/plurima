package io.plurima.kafka.internal;

import io.plurima.kafka.ConsumerContext;
import io.plurima.kafka.HandlerTimeoutException;
import io.plurima.kafka.OrderingMode;
import io.plurima.kafka.RecordListener;
import io.plurima.kafka.deserializer.RecordDeserializer;
import io.plurima.kafka.retry.RetryPolicy;
import org.apache.kafka.clients.consumer.AcknowledgeType;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ShareConsumer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * Verifies Pillar 3: a listener that exceeds {@code handlerTimeout} is interrupted and its
 * failure surfaces as {@link HandlerTimeoutException} into the retry/DLT pipeline.
 */
class WorkerProcessorTimeoutTest {

    private InFlightRegistry registry;
    private AckCoordinator coordinator;
    private ShareConsumer<byte[], byte[]> consumer;
    private ScheduledExecutorService scheduler;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        registry = new InFlightRegistry();
        coordinator = new AckCoordinator(registry);
        consumer = mock(ShareConsumer.class);
        scheduler = Executors.newSingleThreadScheduledExecutor();
    }

    @AfterEach
    void tearDown() {
        scheduler.shutdownNow();
    }

    private InFlightRecord<byte[], byte[]> rec(long offset) {
        InFlightRecord<byte[], byte[]> r =
            new InFlightRecord<>(new ConsumerRecord<>("t", 0, offset, new byte[0], new byte[0]));
        registry.register(r);
        return r;
    }

    private ConsumerContext ctx() {
        return new ConsumerContext() {
            @Override public short deliveryCount() { return 1; }
            @Override public Optional<Short> deliveryCountOptional() { return Optional.of((short) 1); }
            @Override public OrderingMode orderingMode() { return OrderingMode.UNORDERED; }
        };
    }

    @Test
    void slowHandlerIsInterruptedAndRejectedQuickly() {
        AtomicInteger invocations = new AtomicInteger();
        RecordListener<byte[], byte[]> listener = (r, c) -> {
            invocations.incrementAndGet();
            Thread.sleep(10_000);   // far longer than the timeout; interruptible
        };
        ListenerInvoker invoker = ListenerInvoker.forImplicit(
            listener, RecordDeserializer.bytes(), RecordDeserializer.bytes());
        // noRetry → HandlerTimeoutException is non-retriable → Reject → REJECT.
        WorkerProcessor p = new WorkerProcessor(
            invoker, new RetryEngine(RetryPolicy.noRetry()), coordinator, null,
            io.plurima.kafka.metrics.PlurimaMetrics.noOp(), null,
            Duration.ofMillis(100), scheduler);

        InFlightRecord<byte[], byte[]> r = rec(1);
        long start = System.nanoTime();
        p.process(r, ctx());
        long elapsedMs = (System.nanoTime() - start) / 1_000_000;

        // Interrupted at ~100ms, not after the 10s sleep.
        assertThat(elapsedMs).as("handler interrupted near the timeout, not after the full sleep")
            .isLessThan(3_000);
        assertThat(invocations.get()).isEqualTo(1);
        coordinator.commitPendingAcks(consumer);
        verify(consumer).acknowledge(
            argThat(cr -> cr.offset() == 1L), eq(AcknowledgeType.REJECT));
    }

    @Test
    void nonWatchdogInterruptIsPreserved() {
        // handlerTimeout is enabled (watchdog scheduled) but set long enough that it never fires.
        // A non-timeout interrupt (here simulating a shutdown interrupt) set during the handler must
        // SURVIVE process() — otherwise a later retryAfter() Thread.sleep would not abort on shutdown.
        AtomicInteger ran = new AtomicInteger();
        RecordListener<byte[], byte[]> listener = (r, c) -> {
            Thread.currentThread().interrupt();   // external interrupt, NOT from the watchdog
            ran.incrementAndGet();
            // returns normally
        };
        ListenerInvoker invoker = ListenerInvoker.forImplicit(
            listener, RecordDeserializer.bytes(), RecordDeserializer.bytes());
        WorkerProcessor p = new WorkerProcessor(
            invoker, new RetryEngine(RetryPolicy.noRetry()), coordinator, null,
            io.plurima.kafka.metrics.PlurimaMetrics.noOp(), null,
            Duration.ofSeconds(60), scheduler);   // long timeout → watchdog won't fire

        InFlightRecord<byte[], byte[]> r = rec(3);
        try {
            p.process(r, ctx());
            assertThat(ran.get()).isEqualTo(1);
            assertThat(Thread.currentThread().isInterrupted())
                .as("a non-watchdog interrupt must survive process() (not be swallowed)")
                .isTrue();
        } finally {
            Thread.interrupted();   // clear for subsequent tests
        }
    }

    @Test
    void timeoutIsClassifiableAsHandlerTimeoutExceptionForRetry() {
        AtomicInteger invocations = new AtomicInteger();
        RecordListener<byte[], byte[]> listener = (r, c) -> {
            invocations.incrementAndGet();
            Thread.sleep(10_000);
        };
        ListenerInvoker invoker = ListenerInvoker.forImplicit(
            listener, RecordDeserializer.bytes(), RecordDeserializer.bytes());
        // Retry on timeouts: a timed-out handler must be classified as HandlerTimeoutException and
        // retried (inline), so it is invoked more than once before exhausting at maxAttempts.
        WorkerProcessor p = new WorkerProcessor(
            invoker,
            new RetryEngine(RetryPolicy.exponential()
                .maxAttempts(2).initialDelay(Duration.ofMillis(10)).multiplier(1.0).jitter(0.0)
                .retryOn(HandlerTimeoutException.class).build()),
            coordinator, null, io.plurima.kafka.metrics.PlurimaMetrics.noOp(), null,
            Duration.ofMillis(80), scheduler);

        InFlightRecord<byte[], byte[]> r = rec(2);
        p.process(r, ctx());

        // Initial + retries → invoked more than once (proves the timeout was retriable-classified).
        assertThat(invocations.get()).isGreaterThan(1);
        coordinator.commitPendingAcks(consumer);
        // Exhausted (no DLT) → REJECT.
        verify(consumer).acknowledge(
            argThat(cr -> cr.offset() == 2L), eq(AcknowledgeType.REJECT));
    }
}
