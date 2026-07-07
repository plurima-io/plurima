package io.plurima.kafka.internal;

import io.plurima.kafka.ConsumerContext;
import io.plurima.kafka.HandlerTimeoutException;
import io.plurima.kafka.OrderingMode;
import io.plurima.kafka.RecordListener;
import io.plurima.kafka.deserializer.RecordDeserializer;
import io.plurima.kafka.metrics.PlurimaMetrics;
import io.plurima.kafka.retry.RetryPolicy;
import org.apache.kafka.clients.consumer.AcknowledgeType;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ShareConsumer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
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
            @Override public int deliveryCount() { return 1; }
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

    // ── A5: watchdog cancel-race hardening ────────────────────────────────────

    /**
     * A5 test (a) — classification. When the handler throws its OWN exception and the worker
     * settles the invocation (winning the RUNNING→DONE CAS) before the watchdog fires, the
     * ORIGINAL exception must be reported into the retry/DLT pipeline — NOT wrapped as
     * {@link HandlerTimeoutException}.
     *
     * <p>Deterministic via the {@link ManualScheduler} seam: the watchdog is captured but not
     * executed while the handler runs, so the worker's DONE-CAS wins unconditionally. This pins
     * the invariant "handler exception is misclassified as a timeout only when the watchdog
     * actually won the race", closing the old {@code firedTimeout.get() ? timeout : cause} defect.
     */
    @Test
    void handlerOwnExceptionNotMisclassifiedAsTimeout() {
        ManualScheduler manual = new ManualScheduler();
        CapturingMetrics metrics = new CapturingMetrics();
        RecordListener<byte[], byte[]> listener = (r, c) -> {
            throw new BusinessException("boom");
        };
        ListenerInvoker invoker = ListenerInvoker.forImplicit(
            listener, RecordDeserializer.bytes(), RecordDeserializer.bytes());
        WorkerProcessor p = new WorkerProcessor(
            invoker, new RetryEngine(RetryPolicy.noRetry()), coordinator, null,
            metrics, null, Duration.ofSeconds(60), manual);

        InFlightRecord<byte[], byte[]> r = rec(4);
        try {
            p.process(r, ctx());

            assertThat(metrics.lastFailedClass)
                .as("handler's own exception must be reported, not wrapped as a timeout, "
                    + "when the worker wins the settle race")
                .isEqualTo(BusinessException.class.getName());

            // The watchdog fires late — after the worker already won the settle CAS on the
            // handler's own exception. Its RUNNING→TIMED_OUT CAS must lose unconditionally, so
            // firing it now must be a complete no-op: classification stays the original
            // BusinessException (classification already happened synchronously inside process(),
            // before this late fire, in both old and new code) and no interrupt leaks onto the
            // worker thread — on the old implementation the watchdog interrupted unconditionally,
            // which is what the interrupt assertion below actually catches.
            manual.fireCapturedWatchdog();

            assertThat(metrics.lastFailedClass)
                .as("a late-firing watchdog that loses the settle CAS must not retroactively "
                    + "reclassify an already-settled handler exception as a timeout")
                .isEqualTo(BusinessException.class.getName());
            assertThat(Thread.interrupted())
                .as("a watchdog that loses the settle CAS must NOT interrupt the worker thread")
                .isFalse();
        } finally {
            Thread.interrupted();   // defensively clear for subsequent tests
            manual.shutdownNow();
        }
    }

    /**
     * A5 test (b) — no leaked interrupt. Reproduces the exact race the fix targets: a watchdog
     * that fires AFTER the handler already returned and the worker settled must NOT interrupt the
     * worker thread (a stray interrupt would poison a later retry sleep or DLT wait — and, per
     * Task A4, the classic DLT loop treats any interrupt as shutdown).
     *
     * <p>Deterministic via the {@link ManualScheduler} seam: the handler returns normally, the
     * worker settles (RUNNING→DONE CAS wins), then the test fires the stale watchdog. With the
     * per-invocation state CAS the watchdog's RUNNING→TIMED_OUT CAS loses and it never interrupts.
     * The pre-fix code interrupted unconditionally, leaking the interrupt onto the worker thread.
     */
    @Test
    void strayWatchdogAfterSettleDoesNotInterruptWorker() {
        ManualScheduler manual = new ManualScheduler();
        AtomicInteger ran = new AtomicInteger();
        RecordListener<byte[], byte[]> listener = (r, c) -> ran.incrementAndGet();
        ListenerInvoker invoker = ListenerInvoker.forImplicit(
            listener, RecordDeserializer.bytes(), RecordDeserializer.bytes());
        WorkerProcessor p = new WorkerProcessor(
            invoker, new RetryEngine(RetryPolicy.noRetry()), coordinator, null,
            io.plurima.kafka.metrics.PlurimaMetrics.noOp(), null,
            Duration.ofSeconds(60), manual);

        InFlightRecord<byte[], byte[]> r = rec(5);
        try {
            p.process(r, ctx());
            assertThat(ran.get()).isEqualTo(1);
            assertThat(Thread.currentThread().isInterrupted())
                .as("no interrupt should be pending after a clean invocation")
                .isFalse();

            // The watchdog fires late — after the worker already settled the invocation.
            manual.fireCapturedWatchdog();

            assertThat(Thread.interrupted())
                .as("a watchdog that fires after the invocation settled must NOT interrupt "
                    + "the worker thread (would poison a later retry sleep / DLT wait)")
                .isFalse();
        } finally {
            Thread.interrupted();   // defensively clear for subsequent tests
            manual.shutdownNow();
        }
    }

    /**
     * A5 review follow-up — success-path plumbing failures stay classified. Before the CAS
     * restructure, the single try/catch over the invocation also covered the success-path
     * plumbing (duration metric, latency window, ACCEPT queueing), so an exception thrown
     * there was routed through failure classification (retry / reject / DLT). The restructure
     * must preserve that: a throwing {@code coordinator.queueAck(ACCEPT)} goes down the
     * classified-failure path (here noRetry → Reject → REJECT queued) and is reported as the
     * ORIGINAL plumbing exception — never propagated out of {@code process()}, and never
     * wrapped as {@link HandlerTimeoutException} (the worker won the settle CAS).
     */
    @Test
    void successPathPlumbingFailureIsClassifiedNotPropagated() {
        AckCoordinator throwingCoordinator = mock(AckCoordinator.class);
        // First terminal ack attempt (ACCEPT, from the success path) blows up; the subsequent
        // REJECT from the failure path succeeds (mock default no-op).
        doThrow(new IllegalStateException("ack queue full"))
            .when(throwingCoordinator).queueAck(any(), eq(AcknowledgeType.ACCEPT));
        CapturingMetrics metrics = new CapturingMetrics();
        RecordListener<byte[], byte[]> listener = (r, c) -> { /* succeeds */ };
        ListenerInvoker invoker = ListenerInvoker.forImplicit(
            listener, RecordDeserializer.bytes(), RecordDeserializer.bytes());
        WorkerProcessor p = new WorkerProcessor(
            invoker, new RetryEngine(RetryPolicy.noRetry()), throwingCoordinator, null,
            metrics, null, Duration.ofSeconds(60), scheduler);   // watchdog never fires

        InFlightRecord<byte[], byte[]> r = rec(6);

        assertThatCode(() -> p.process(r, ctx()))
            .as("a success-path plumbing failure must be routed through failure "
                + "classification, not propagate out of the worker")
            .doesNotThrowAnyException();

        assertThat(metrics.lastFailedClass)
            .as("the plumbing exception is reported as-is (worker won the settle CAS, "
                + "so it must not be wrapped as HandlerTimeoutException)")
            .isEqualTo(IllegalStateException.class.getName());
        assertThat(metrics.durationRecordCount())
            .as("the process duration must be recorded exactly once, even when success-path "
                + "plumbing (here ACCEPT queueing) fails and falls through to the failure path")
            .isEqualTo(1);
        // noRetry → Reject → the record leaves the pipeline via a REJECT ack.
        verify(throwingCoordinator).queueAck(
            argThat(ifr -> ifr.coord().offset() == 6L), eq(AcknowledgeType.REJECT));
    }

    /**
     * F9 — a plumbing failure AFTER a successful handler must never be classified as a
     * handler timeout, even when the watchdog won the settle CAS. Scenario: the watchdog
     * fires while the handler is running (winning RUNNING→TIMED_OUT), but the handler
     * SUCCEEDS anyway ({@code thrown == null} at settle); then the success-path plumbing
     * ({@code coordinator.queueAck(ACCEPT)}) throws. Pre-fix, {@code timedOut == true}
     * wrapped that plumbing exception as {@link HandlerTimeoutException} — misclassifying
     * an infrastructure failure on a successfully-processed record as a handler timeout,
     * so user retry policies that retry timeouts would re-invoke completed business logic.
     * The failure must be reported as the plumbing exception itself; only failures that
     * originated from the handler invocation may be wrapped as timeouts.
     */
    @Test
    void plumbingFailureAfterSuccessfulHandlerIsNotWrappedAsTimeoutWhenWatchdogWins() {
        ManualScheduler manual = new ManualScheduler();
        CapturingMetrics metrics = new CapturingMetrics();
        AckCoordinator throwingCoordinator = mock(AckCoordinator.class);
        doThrow(new IllegalStateException("ack queue full"))
            .when(throwingCoordinator).queueAck(any(), eq(AcknowledgeType.ACCEPT));
        // The handler fires the captured watchdog itself, mid-invocation: the watchdog
        // wins the RUNNING→TIMED_OUT CAS (and self-interrupts this thread, which the
        // worker's settle path then drains) — yet the handler still returns SUCCESS.
        RecordListener<byte[], byte[]> listener = (r, c) -> manual.fireCapturedWatchdog();
        ListenerInvoker invoker = ListenerInvoker.forImplicit(
            listener, RecordDeserializer.bytes(), RecordDeserializer.bytes());
        WorkerProcessor p = new WorkerProcessor(
            invoker, new RetryEngine(RetryPolicy.noRetry()), throwingCoordinator, null,
            metrics, null, Duration.ofSeconds(60), manual);

        InFlightRecord<byte[], byte[]> r = rec(7);
        try {
            assertThatCode(() -> p.process(r, ctx()))
                .doesNotThrowAnyException();

            assertThat(metrics.lastFailedClass)
                .as("the plumbing exception must be classified as ITSELF — the handler "
                    + "succeeded, so wrapping it as HandlerTimeoutException would send a "
                    + "successfully-processed record through timeout retry policies")
                .isEqualTo(IllegalStateException.class.getName())
                .isNotEqualTo(HandlerTimeoutException.class.getName());
            // noRetry → Reject → the record leaves the pipeline via a REJECT ack.
            verify(throwingCoordinator).queueAck(
                argThat(ifr -> ifr.coord().offset() == 7L), eq(AcknowledgeType.REJECT));
        } finally {
            Thread.interrupted();   // defensively clear for subsequent tests
            manual.shutdownNow();
        }
    }

    /** A business failure distinct from {@link HandlerTimeoutException} for classification asserts. */
    private static final class BusinessException extends RuntimeException {
        private static final long serialVersionUID = 1L;
        BusinessException(String message) { super(message); }
    }

    /**
     * Captures the last {@code recordsFailed} exception class so classification can be asserted,
     * and counts {@code recordProcessDuration} calls so a double-record on the plumbing-failure
     * path (success-path record falling through to the failure-path record) can be caught.
     */
    private static final class CapturingMetrics implements PlurimaMetrics {
        volatile String lastFailedClass;
        private final AtomicInteger durationRecords = new AtomicInteger();
        @Override public void recordsFailed(String topic, String exceptionClass) {
            this.lastFailedClass = exceptionClass;
        }
        @Override public void recordProcessDuration(String topic, Duration duration) {
            durationRecords.incrementAndGet();
        }
        int durationRecordCount() {
            return durationRecords.get();
        }
    }

    /**
     * A {@link java.util.concurrent.ScheduledExecutorService} test seam that captures the watchdog
     * task instead of running it on a timer, so a test can fire it at a chosen instant relative to
     * the worker's settle. {@code schedule(...)} returns a real (never-firing) future so
     * {@code WorkerProcessor}'s best-effort {@code cancel(false)} still works.
     */
    private static final class ManualScheduler extends ScheduledThreadPoolExecutor {
        private volatile Runnable captured;

        ManualScheduler() { super(1); }

        @Override
        public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
            this.captured = command;
            // Hand back a real, far-future no-op future so cancel(false) is valid; the actual
            // watchdog body is fired explicitly by the test to control ordering.
            return super.schedule(() -> { }, 1, TimeUnit.HOURS);
        }

        void fireCapturedWatchdog() {
            Runnable r = captured;
            assertThat(r).as("a watchdog must have been scheduled").isNotNull();
            r.run();
        }
    }
}
