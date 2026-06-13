package io.plurima.kafka.internal;

import io.plurima.kafka.ConsumerContext;
import io.plurima.kafka.annotation.Internal;
import io.plurima.kafka.metrics.PlurimaMetrics;
import io.plurima.kafka.retry.RetryDecision;
import org.apache.kafka.clients.consumer.AcknowledgeType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

@Internal
public final class WorkerProcessor {

    private static final Logger log = LoggerFactory.getLogger(WorkerProcessor.class);

    private final ListenerInvoker invoker;
    private final RetryEngine retryEngine;
    private final AckCoordinator coordinator;
    private final DltRouter dltRouter; // nullable
    private final PlurimaMetrics metrics;
    private final HandlerLatencyWindow latencyWindow; // nullable — adaptive barrier off when null

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

    /** Full constructor including the adaptive-barrier latency window (nullable). */
    public WorkerProcessor(
        ListenerInvoker invoker,
        RetryEngine retryEngine,
        AckCoordinator coordinator,
        DltRouter dltRouter,
        PlurimaMetrics metrics,
        HandlerLatencyWindow latencyWindow) {
        this.invoker = invoker;
        this.retryEngine = retryEngine;
        this.coordinator = coordinator;
        this.dltRouter = dltRouter;
        this.metrics = metrics;
        this.latencyWindow = latencyWindow;
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
            metrics.recordsFailed(r.coord().topic(), t.getClass().getName());
            return t;
        }
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
