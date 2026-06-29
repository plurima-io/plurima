package io.plurima.kafka.internal;

import io.plurima.kafka.ConsumerContext;
import io.plurima.kafka.HandlerTimeoutException;
import io.plurima.kafka.annotation.Internal;
import io.plurima.kafka.metrics.PlurimaMetrics;
import io.plurima.kafka.retry.RetryDecision;
import org.apache.kafka.clients.consumer.AcknowledgeType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

@Internal
public final class WorkerProcessor {

    private static final Logger log = LoggerFactory.getLogger(WorkerProcessor.class);

    private final ListenerInvoker invoker;
    private final RetryEngine retryEngine;
    private final AckCoordinator coordinator;
    private final DltRouter dltRouter; // nullable
    private final PlurimaMetrics metrics;
    private final HandlerLatencyWindow latencyWindow; // nullable — adaptive barrier off when null
    private final Duration handlerTimeout;            // nullable — no timeout when null
    private final ScheduledExecutorService timeoutScheduler; // nullable — paired with handlerTimeout

    /** Constructor without DLT support. */
    public WorkerProcessor(
        ListenerInvoker invoker,
        RetryEngine retryEngine,
        AckCoordinator coordinator) {
        this(invoker, retryEngine, coordinator, null, PlurimaMetrics.noOp());
    }

    /** Constructor with DLT routing on retry exhaustion. */
    public WorkerProcessor(
        ListenerInvoker invoker,
        RetryEngine retryEngine,
        AckCoordinator coordinator,
        DltRouter dltRouter) {
        this(invoker, retryEngine, coordinator, dltRouter, PlurimaMetrics.noOp());
    }

    /** Full constructor with DLT routing and metrics. */
    public WorkerProcessor(
        ListenerInvoker invoker,
        RetryEngine retryEngine,
        AckCoordinator coordinator,
        DltRouter dltRouter,
        PlurimaMetrics metrics) {
        this(invoker, retryEngine, coordinator, dltRouter, metrics, null);
    }

    /** Constructor including the adaptive-barrier latency window (nullable); no handler timeout. */
    public WorkerProcessor(
        ListenerInvoker invoker,
        RetryEngine retryEngine,
        AckCoordinator coordinator,
        DltRouter dltRouter,
        PlurimaMetrics metrics,
        HandlerLatencyWindow latencyWindow) {
        this(invoker, retryEngine, coordinator, dltRouter, metrics, latencyWindow, null, null);
    }

    /** Full constructor including the optional handler timeout + its watchdog scheduler. */
    public WorkerProcessor(
        ListenerInvoker invoker,
        RetryEngine retryEngine,
        AckCoordinator coordinator,
        DltRouter dltRouter,
        PlurimaMetrics metrics,
        HandlerLatencyWindow latencyWindow,
        Duration handlerTimeout,
        ScheduledExecutorService timeoutScheduler) {
        this.invoker = invoker;
        this.retryEngine = retryEngine;
        this.coordinator = coordinator;
        this.dltRouter = dltRouter;
        this.metrics = metrics;
        this.latencyWindow = latencyWindow;
        this.handlerTimeout = handlerTimeout;
        this.timeoutScheduler = timeoutScheduler;
    }

    public void process(InFlightRecord<byte[], byte[]> r, ConsumerContext ctx) {
        while (true) {
            Throwable failure = invokeListener(r, ctx);
            if (failure == null) return;                          // success
            // Manual-ack listener that terminal-acked BEFORE throwing — the user's ack
            // stands. Running retry/DLT here would: (a) inline-retry duplicate an
            // already-ACCEPTed record, (b) delayed-retry queue a RELEASE the first-wins
            // guard drops (wasted work + log spam), (c) DLT-route a record the user
            // already chose to ACCEPT. Just log + exit.
            if (r.isTerminalAckQueued()) {
                log.warn("Listener threw AFTER terminal ack already queued for {} — "
                    + "skipping retry/DLT; user-chosen ack stands. Exception: {}",
                    r.coord(), failure.toString());
                return;
            }
            if (!handleProcessingFailure(r, failure)) return;     // terminal
            // else: retry decided — loop
        }
    }

    /**
     * Run the user listener once and emit accept-path metrics. Returns {@code null}
     * on success; on failure, returns the thrown cause so the caller can decide
     * retry / reject / DLT. The single try/catch over user code lives here.
     */
    private Throwable invokeListener(InFlightRecord<byte[], byte[]> r, ConsumerContext ctx) {
        long startNanos = System.nanoTime();
        AtomicBoolean firedTimeout = new AtomicBoolean(false);
        ScheduledFuture<?> watchdog = scheduleTimeout(firedTimeout);
        try {
            invoker.invoke(r, ctx, coordinator);
            long elapsed = System.nanoTime() - startNanos;
            metrics.recordProcessDuration(r.coord().topic(), Duration.ofNanos(elapsed));
            if (latencyWindow != null) latencyWindow.record(elapsed);
            if (!invoker.isManualAck()) {
                // recordsProcessed is emitted inside coordinator.queueAck — centralised
                // there so manual-ack ACCEPT/REJECT and auto-RELEASE-on-no-ack paths
                // all count consistently.
                coordinator.queueAck(r, AcknowledgeType.ACCEPT);
            }
            return null;
        } catch (Throwable t) {
            metrics.recordProcessDuration(r.coord().topic(),
                Duration.ofNanos(System.nanoTime() - startNanos));
            // If our watchdog interrupted the handler, surface it as HandlerTimeoutException so
            // the user's RetryPolicy can classify timeouts explicitly. Otherwise pass the cause through.
            Throwable cause = firedTimeout.get()
                ? new HandlerTimeoutException("handler exceeded timeout " + handlerTimeout, t)
                : t;
            metrics.recordsFailed(r.coord().topic(), cause.getClass().getName());
            return cause;
        } finally {
            if (watchdog != null) {
                watchdog.cancel(false);
                // Consume ONLY our watchdog's interrupt (so a stray timeout interrupt can't leak
                // into a later retry sleep). Crucially, do NOT clear when the watchdog did not fire:
                // a non-timeout interrupt (e.g. shutdown interrupting the worker) must survive so
                // retryAfter()'s Thread.sleep aborts instead of sleeping/retrying through shutdown.
                if (firedTimeout.get()) {
                    Thread.interrupted();
                }
            }
        }
    }

    /**
     * Schedules an interrupt of the current worker thread after {@code handlerTimeout}, recording
     * that it fired via {@code firedTimeout}. Returns {@code null} when no timeout is configured.
     */
    private ScheduledFuture<?> scheduleTimeout(AtomicBoolean firedTimeout) {
        if (handlerTimeout == null || timeoutScheduler == null) return null;
        Thread worker = Thread.currentThread();
        return timeoutScheduler.schedule(() -> {
            firedTimeout.set(true);
            worker.interrupt();
        }, handlerTimeout.toNanos(), TimeUnit.NANOSECONDS);
    }

    /**
     * Decide what happens after a listener failure. Returns {@code true} when the
     * caller should retry (inline sleep already happened inside), {@code false}
     * when the record is terminal (rejected, exhausted, or interrupted-during-sleep).
     */
    private boolean handleProcessingFailure(InFlightRecord<byte[], byte[]> r, Throwable cause) {
        RetryDecision decision = retryEngine.evaluate(r, cause);
        return switch (decision) {
            case RetryDecision.RetryInline inline -> retryAfter(inline.delay(), r);
            // Delayed retry on SHARE means "release now, broker schedules redelivery".
            // KIP-932 forbids holding a record across poll() calls, so we cannot
            // sleep on the worker for delays > inline threshold.
            case RetryDecision.RetryDelayed delayed -> {
                r.incrementAttempt();
                metrics.retryAttempt(r.coord().topic(), r.attempt());
                coordinator.queueAck(r, AcknowledgeType.RELEASE);
                yield false;
            }
            case RetryDecision.Reject reject -> {
                log.warn("Rejecting non-retriable {}: {}", r.coord(),
                    reject.cause().toString());
                coordinator.queueAck(r, AcknowledgeType.REJECT);  // emits recordsProcessed
                yield false;
            }
            case RetryDecision.Exhausted exh -> {
                handleExhaustion(r, exh.cause());
                yield false;
            }
        };
    }

    /** Sleep inline, bump attempt, emit retry metric. Returns false on interrupt (queues RELEASE). */
    private boolean retryAfter(Duration delay, InFlightRecord<byte[], byte[]> r) {
        try {
            Thread.sleep(delay.toMillis());
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            coordinator.queueAck(r, AcknowledgeType.RELEASE);
            return false;
        }
        r.incrementAttempt();
        metrics.retryAttempt(r.coord().topic(), r.attempt());
        return true;
    }

    private void handleExhaustion(InFlightRecord<byte[], byte[]> r, Throwable cause) {
        if (dltRouter == null) {
            log.error("Retry exhausted for {} (no DLT configured): {}", r.coord(),
                cause.toString());
            coordinator.queueAck(r, AcknowledgeType.REJECT);  // emits recordsProcessed
            return;
        }
        log.warn("Retry exhausted for {}, routing to DLT: {}", r.coord(), cause.toString());
        DltRouter.DltRoute route = dltRouter.route(r, cause);
        try {
            route.future().get(30, TimeUnit.SECONDS);
            coordinator.queueAck(r, AcknowledgeType.ACCEPT);  // emits recordsProcessed
        } catch (java.util.concurrent.TimeoutException | InterruptedException callerSide) {
            // Caller-side give-up. CAS metricEmitted so a late-arriving producer
            // callback does NOT also emit a metric for the same record — first
            // completion wins.
            if (callerSide instanceof InterruptedException) Thread.currentThread().interrupt();
            if (route.metricEmitted().compareAndSet(false, true)) {
                metrics.dltFailed(r.coord().topic(), callerSide.getClass().getSimpleName());
            }
            log.error("DLT routing did not complete within budget for {}; releasing "
                + "original. Alert on plurima.consumer.dlt.failures.", r.coord(), callerSide);
            coordinator.queueAck(r, AcknowledgeType.RELEASE);  // emits recordsProcessed
        } catch (Exception ex) {
            // ExecutionException wraps a producer failure — DltRouter already
            // CAS-emitted dltFailed inside the callback (or in its synchronous-send catch).
            log.error("DLT produce failed for {}; releasing original", r.coord(), ex);
            coordinator.queueAck(r, AcknowledgeType.RELEASE);  // emits recordsProcessed
        }
    }
}
