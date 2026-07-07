package io.plurima.kafka.internal;

import io.plurima.kafka.ConsumerContext;
import io.plurima.kafka.HandlerTimeoutException;
import io.plurima.kafka.annotation.Internal;
import io.plurima.kafka.metrics.PlurimaMetrics;
import org.apache.kafka.clients.consumer.AcknowledgeType;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

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
            // Per-attempt terminal-ack re-check — mirrors ClassicPollLoop.processOne's per-attempt
            // ownership re-check. Inline retry backoff (retryAfter's Thread.sleep) can run for up
            // to INLINE_THRESHOLD with the record still "in flight" from the drain barrier's point
            // of view; PollLoop.forceReleaseStuckRecords() can fire during that sleep and queue a
            // slowness RELEASE (AckCoordinator.queueReleaseForSlowness), which marks
            // terminalAckQueued via the first-wins guard. Checking only once, after the PRECEDING
            // invokeListener call failed (below), misses this: that check runs BEFORE the backoff
            // sleep, not after, so a force-RELEASE landing during the sleep would otherwise go
            // unnoticed and the loop would re-invoke the listener on a record Plurima has already
            // abandoned to the broker. Re-checking at the top of every iteration closes that gap.
            if (r.isTerminalAckQueued()) {
                log.warn("Skipping retry re-invocation for {} — terminal ack already queued "
                    + "(likely force-RELEASE by the drain barrier during retry backoff)", r.coord());
                return;
            }
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
     * Lifecycle of a single handler invocation, guarded by an {@link AtomicReference} CAS so the
     * watchdog and the worker can never both act on the same invocation.
     *
     * <ul>
     *   <li>{@code RUNNING} — handler in flight; both parties may still transition it.</li>
     *   <li>{@code DONE} — the worker settled first (handler returned or threw its own error).
     *       The watchdog's {@code RUNNING→TIMED_OUT} CAS now fails, so it can NEVER interrupt
     *       this thread. This is the guarantee the whole design exists to provide.</li>
     *   <li>{@code TIMED_OUT} — the watchdog settled first; it (and only it) interrupts the
     *       worker. The worker's {@code RUNNING→DONE} CAS fails, telling it a timeout occurred.</li>
     * </ul>
     */
    private enum InvocationState { RUNNING, DONE, TIMED_OUT }

    /**
     * Run the user listener once and emit accept-path metrics. Returns {@code null}
     * on success; on failure, returns the thrown cause so the caller can decide
     * retry / reject / DLT. The single try/catch over user code lives here.
     *
     * <p><b>Concurrency (A5 — watchdog cancel-race).</b> A {@link ScheduledFuture#cancel(boolean)
     * cancel(false)} cannot stop an already-running watchdog task, so cancellation alone cannot
     * decide "did the timeout fire?". Instead a per-invocation {@link AtomicReference} state gates
     * both parties:
     * <ul>
     *   <li>The watchdog interrupts the worker <em>only</em> if it wins {@code RUNNING→TIMED_OUT}.</li>
     *   <li>The worker settles with {@code RUNNING→DONE}. If it wins, the watchdog can no longer
     *       win its CAS and therefore can never interrupt this thread — so no stray interrupt can
     *       leak past this method into a later retry sleep or DLT wait. This is critical because
     *       Task A4's classic DLT retry loop treats <em>any</em> interrupt as shutdown; the
     *       watchdog must be provably unable to interrupt a worker outside this invocation window.</li>
     *   <li>If the worker loses the settle CAS the watchdog won: an interrupt is (or is about to be)
     *       delivered, so the worker waits until the watchdog has actually issued it
     *       ({@code interruptIssued}) and then clears it, and only then is the failure classified
     *       as {@link HandlerTimeoutException}.</li>
     * </ul>
     * A non-watchdog interrupt (e.g. shutdown) leaves the state {@code RUNNING→DONE} (the worker
     * wins), so it is <em>not</em> cleared and correctly survives to abort {@code retryAfter()}'s
     * sleep.
     */
    private @Nullable Throwable invokeListener(InFlightRecord<byte[], byte[]> r, ConsumerContext ctx) {
        long startNanos = System.nanoTime();
        AtomicReference<InvocationState> state = new AtomicReference<>(InvocationState.RUNNING);
        AtomicBoolean interruptIssued = new AtomicBoolean(false);
        ScheduledFuture<?> watchdog = scheduleTimeout(state, interruptIssued);

        Throwable thrown = null;
        // Tracks WHERE `thrown` originated. Only handler-invocation failures may be
        // wrapped as HandlerTimeoutException below; a success-path plumbing failure
        // (latency window / ACCEPT queueing) must be classified as itself even when
        // the watchdog won the settle CAS — the handler SUCCEEDED, so reporting a
        // "timeout" would send an already-completed record through user retry
        // policies that re-invoke business logic on timeouts.
        boolean handlerThrew = false;
        try {
            invoker.invoke(r, ctx, coordinator);
        } catch (Throwable t) {
            thrown = t;
            handlerThrew = true;
        }
        long elapsed = System.nanoTime() - startNanos;

        // Settle this invocation exactly once. Losing the CAS means the watchdog already won
        // (state == TIMED_OUT) and is committed to interrupting this thread.
        boolean timedOut = !state.compareAndSet(InvocationState.RUNNING, InvocationState.DONE);
        if (watchdog != null) {
            watchdog.cancel(false);   // best-effort: frees the scheduler slot if not yet started
        }
        if (timedOut) {
            // The watchdog won and WILL call worker.interrupt(); it may still be between its
            // winning CAS and that call, so spin until it signals the interrupt has been issued,
            // then consume it. If the handler already absorbed the interrupt (e.g. an
            // InterruptedException from a blocking call cleared the flag), Thread.interrupted()
            // is simply a no-op. Either way, no interrupt survives past this point.
            while (!interruptIssued.get()) {
                Thread.onSpinWait();
            }
            Thread.interrupted();
        }

        // Recorded exactly once regardless of outcome: success, handler failure, or success-path
        // plumbing failure (which falls through to the failure path below without looping back
        // here).
        metrics.recordProcessDuration(r.coord().topic(), Duration.ofNanos(elapsed));

        if (thrown == null) {
            try {
                if (latencyWindow != null) latencyWindow.record(elapsed);
                if (!invoker.isManualAck()) {
                    // recordsProcessed is emitted inside coordinator.queueAck — centralised
                    // there so manual-ack ACCEPT/REJECT and auto-RELEASE-on-no-ack paths
                    // all count consistently.
                    coordinator.queueAck(r, AcknowledgeType.ACCEPT);
                }
                return null;
            } catch (Throwable plumbing) {
                // Success-path plumbing (latency window / ACCEPT queueing) failed. Historically
                // the single try/catch over the whole invocation caught this and routed it
                // through failure classification (retry / reject / DLT) — preserve that rather
                // than letting it propagate uncaught out of the worker. The invocation is
                // already settled above (CAS + interrupt drain), so running this plumbing after
                // settle does not weaken the watchdog guarantee at all: the watchdog still
                // cannot interrupt this thread once DONE won. handlerThrew stays FALSE: this
                // failure is infrastructure, not handler code — even if the watchdog won the
                // settle CAS (timedOut == true), it must NOT be reported as a handler timeout.
                thrown = plumbing;
            }
        }

        // Classify as HandlerTimeoutException ONLY when the watchdog won the CAS AND the
        // failure originated from the handler invocation itself — so the user's RetryPolicy
        // can classify timeouts explicitly. Two deliberate exclusions:
        //  - A handler that threw its own exception while the WORKER won the settle race
        //    reports that ORIGINAL exception, never a timeout.
        //  - A plumbing failure after a SUCCESSFUL handler (handlerThrew == false) reports
        //    the plumbing exception itself even when timedOut is true — the record's
        //    business logic completed, so a timeout classification would be a lie that
        //    triggers re-invocation via timeout-retrying policies.
        // The documented tradeoff stands for handler-origin exceptions during a real
        // timeout: we can't distinguish "handler failed on its own" from "handler failed
        // because the watchdog interrupted it", so the timeout wrapper (with the original
        // as cause) wins.
        Throwable cause = timedOut && handlerThrew
            ? new HandlerTimeoutException("handler exceeded timeout " + handlerTimeout, thrown)
            : thrown;
        metrics.recordsFailed(r.coord().topic(), cause.getClass().getName());
        return cause;
    }

    /**
     * Schedules an interrupt of the current worker thread after {@code handlerTimeout}. The
     * interrupt is issued <em>only</em> if the watchdog wins the {@code RUNNING→TIMED_OUT} CAS;
     * {@code interruptIssued} is then set after {@link Thread#interrupt()} returns so the worker
     * can wait for the interrupt to be delivered before clearing it. Returns {@code null} when no
     * timeout is configured.
     */
    private @Nullable ScheduledFuture<?> scheduleTimeout(
        AtomicReference<InvocationState> state, AtomicBoolean interruptIssued) {
        if (handlerTimeout == null || timeoutScheduler == null) return null;
        Thread worker = Thread.currentThread();
        return timeoutScheduler.schedule(() -> {
            if (state.compareAndSet(InvocationState.RUNNING, InvocationState.TIMED_OUT)) {
                worker.interrupt();
                interruptIssued.set(true);
            }
            // Lost the CAS → the worker already settled (DONE); do NOT interrupt: a stray
            // interrupt here would poison a later retry sleep or DLT wait.
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
