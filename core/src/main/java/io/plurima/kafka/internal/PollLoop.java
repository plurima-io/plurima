package io.plurima.kafka.internal;

import io.plurima.kafka.AdaptiveBarrierConfig;
import io.plurima.kafka.annotation.Internal;
import io.plurima.kafka.metrics.PlurimaMetrics;
import org.apache.kafka.clients.consumer.AcknowledgeType;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ShareConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.CorruptRecordException;
import org.apache.kafka.common.errors.RecordDeserializationException;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Optional;

@Internal
public final class PollLoop implements Runnable, AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(PollLoop.class);

    private static final int ADAPTIVE_MIN_SAMPLES = 100;
    private static final Duration ADAPTIVE_FLOOR = Duration.ofSeconds(1);

    private final ShareConsumer<byte[], byte[]> consumer;
    private final WorkDispatcher dispatcher;
    private final AckCoordinator coordinator;
    private final InFlightRegistry registry;
    private final BackpressureGate gate;
    private final Duration pollTimeout;
    private final Duration lockDuration;
    private final Duration shutdownDrainTimeout;
    private final Duration barrierTimeout;
    private final PlurimaMetrics metrics;
    private final String boundTopic;
    /**
     * Called from the poll thread's {@code finally} block after the loop exits — on
     * normal shutdown OR on a fatal {@code Throwable}. ShareConsumerRuntime uses this to
     * trigger {@code close()} so an unexpected loop death cannot leak the WorkerLauncher
     * or DltRouter. {@code null} for legacy/test constructors that don't wire one up.
     */
    private final Runnable onLoopExit;
    private final HandlerLatencyWindow latencyWindow;       // nullable — adaptive off when null
    private final AdaptiveBarrierConfig adaptiveConfig;     // nullable — adaptive off when null
    private volatile long effectiveBarrierMillis;           // exposed via gauge

    private volatile boolean running = true;
    private volatile Optional<Duration> brokerLockDuration = Optional.empty();
    /**
     * Logged-once flag for the local-vs-broker lock-duration relationship check. Set
     * once we've successfully read {@code acquisitionLockTimeoutMs()} and compared it
     * against the user-configured local {@link #lockDuration}. Avoids spamming the WARN
     * on every poll.
     */
    private volatile boolean localLockComparedOnce = false;

    public PollLoop(
        ShareConsumer<byte[], byte[]> consumer,
        WorkDispatcher dispatcher,
        AckCoordinator coordinator,
        InFlightRegistry registry,
        BackpressureGate gate,
        Duration pollTimeout,
        Duration lockDuration,
        Duration shutdownDrainTimeout) {
        this(consumer, dispatcher, coordinator, registry, gate,
            pollTimeout, lockDuration, shutdownDrainTimeout, lockDuration, PlurimaMetrics.noOp(), "unknown", null);
    }

    public PollLoop(
        ShareConsumer<byte[], byte[]> consumer,
        WorkDispatcher dispatcher,
        AckCoordinator coordinator,
        InFlightRegistry registry,
        BackpressureGate gate,
        Duration pollTimeout,
        Duration lockDuration,
        Duration shutdownDrainTimeout,
        Duration barrierTimeout,
        PlurimaMetrics metrics) {
        this(consumer, dispatcher, coordinator, registry, gate,
            pollTimeout, lockDuration, shutdownDrainTimeout, barrierTimeout, metrics, "unknown", null);
    }

    /** Full constructor with all parameters including the bound topic for metrics. */
    public PollLoop(
        ShareConsumer<byte[], byte[]> consumer,
        WorkDispatcher dispatcher,
        AckCoordinator coordinator,
        InFlightRegistry registry,
        BackpressureGate gate,
        Duration pollTimeout,
        Duration lockDuration,
        Duration shutdownDrainTimeout,
        Duration barrierTimeout,
        PlurimaMetrics metrics,
        String boundTopic) {
        this(consumer, dispatcher, coordinator, registry, gate,
            pollTimeout, lockDuration, shutdownDrainTimeout, barrierTimeout, metrics, boundTopic, null);
    }

    /** Full constructor with all parameters including the loop-exit callback. */
    public PollLoop(
        ShareConsumer<byte[], byte[]> consumer,
        WorkDispatcher dispatcher,
        AckCoordinator coordinator,
        InFlightRegistry registry,
        BackpressureGate gate,
        Duration pollTimeout,
        Duration lockDuration,
        Duration shutdownDrainTimeout,
        Duration barrierTimeout,
        PlurimaMetrics metrics,
        String boundTopic,
        Runnable onLoopExit) {
        this(consumer, dispatcher, coordinator, registry, gate,
            pollTimeout, lockDuration, shutdownDrainTimeout, barrierTimeout, metrics, boundTopic,
            onLoopExit, null, null);
    }

    /** Full constructor with all parameters including the adaptive-barrier window + config. */
    public PollLoop(
        ShareConsumer<byte[], byte[]> consumer,
        WorkDispatcher dispatcher,
        AckCoordinator coordinator,
        InFlightRegistry registry,
        BackpressureGate gate,
        Duration pollTimeout,
        Duration lockDuration,
        Duration shutdownDrainTimeout,
        Duration barrierTimeout,
        PlurimaMetrics metrics,
        String boundTopic,
        Runnable onLoopExit,
        HandlerLatencyWindow latencyWindow,
        AdaptiveBarrierConfig adaptiveConfig) {
        this.consumer = consumer;
        this.dispatcher = dispatcher;
        this.coordinator = coordinator;
        this.registry = registry;
        this.gate = gate;
        this.pollTimeout = pollTimeout;
        this.lockDuration = lockDuration;
        this.shutdownDrainTimeout = shutdownDrainTimeout;
        this.barrierTimeout = barrierTimeout;
        this.metrics = metrics;
        this.boundTopic = boundTopic;
        this.onLoopExit = onLoopExit;
        this.latencyWindow = latencyWindow;
        this.adaptiveConfig = adaptiveConfig;
        this.effectiveBarrierMillis = barrierTimeout.toMillis();
    }

    @Override
    public void run() {
        try {
            while (running) {
                if (!acquirePermit()) break;

                ConsumerRecords<byte[], byte[]> batch = pollOrHandlePoison();
                if (batch == null) continue;          // poison-pill / corrupt batch handled

                refreshBrokerLockOnce();
                compareLockDurationOnce();
                recordPolledMetrics(batch);
                dispatchBatch(batch);
                drainAndCommit();
            }
        } catch (WakeupException e) {
            if (running) log.warn("Unexpected WakeupException in PollLoop", e);
        } catch (Throwable t) {
            // Any uncaught error reaching this point is fatal for the poll thread — the
            // loop will exit. Log loudly and let the finally block run drainAndClose +
            // the onLoopExit callback so callers can release the surrounding runtime
            // (WorkerLauncher, DltRouter) instead of leaving them dangling.
            log.error("Fatal error in PollLoop; consumer will stop", t);
        } finally {
            drainAndClose();
            if (onLoopExit != null) RuntimeCleanup.logIfRaised("onLoopExit callback", onLoopExit::run);
        }
    }

    /** Block on the backpressure gate; on interrupt restore the flag and signal the loop to stop. */
    private boolean acquirePermit() {
        try {
            gate.acquire(1);
            return true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    /**
     * Poll the broker once and time the call. Returns the batch, or {@code null} when
     * poll surfaced a poison-pill / corrupt-batch error and we've already REJECTed +
     * released the permit; the caller continues to the next loop iteration.
     */
    private ConsumerRecords<byte[], byte[]> pollOrHandlePoison() {
        long pollStartNanos = System.nanoTime();
        try {
            ConsumerRecords<byte[], byte[]> batch = consumer.poll(pollTimeout);
            metrics.recordPollDuration(Duration.ofNanos(System.nanoTime() - pollStartNanos));
            return batch;
        } catch (RecordDeserializationException e) {
            metrics.recordPollDuration(Duration.ofNanos(System.nanoTime() - pollStartNanos));
            rejectPoisonPill(e);
            gate.release(1);
            return null;
        } catch (CorruptRecordException e) {
            metrics.recordPollDuration(Duration.ofNanos(System.nanoTime() - pollStartNanos));
            log.warn("Corrupt record batch on topic {}: {}; broker auto-rejected, continuing",
                boundTopic, e.getMessage());
            metrics.recordsPoisonPill(boundTopic, "corrupt_batch");
            gate.release(1);
            return null;
        }
    }

    private void rejectPoisonPill(RecordDeserializationException e) {
        String topic = e.topicPartition().topic();
        int partition = e.topicPartition().partition();
        long offset = e.offset();
        log.warn("Poison-pill record {}-{}@{}: {}", topic, partition, offset, e.getMessage());
        metrics.recordsPoisonPill(topic, "deserialization");
        try {
            consumer.acknowledge(topic, partition, offset, AcknowledgeType.REJECT);
        } catch (Exception ackEx) {
            log.error("Failed to REJECT poison-pill record; broker will redeliver", ackEx);
        }
    }

    /**
     * Query the broker's lock duration once. Quasi-static value — polling it every
     * iteration burns CPU on identical responses. Pre-assignment polls may return
     * empty; we keep trying until the broker hands us a value.
     */
    private void refreshBrokerLockOnce() {
        if (brokerLockDuration.isPresent()) return;
        try {
            Optional<Integer> brokerMs = consumer.acquisitionLockTimeoutMs();
            brokerLockDuration = brokerMs.map(ms -> Duration.ofMillis(ms.longValue()));
        } catch (Exception ignored) {
            // leave empty; fall back to user-supplied lockDuration
        }
    }

    /**
     * Once we have the broker's lock duration, sanity-check that Plurima's local
     * force-RELEASE deadline beats it. Logged once per consumer lifetime.
     */
    private void compareLockDurationOnce() {
        if (localLockComparedOnce || brokerLockDuration.isEmpty()) return;
        Duration broker = brokerLockDuration.get();
        if (lockDuration.compareTo(broker) >= 0) {
            log.error("Plurima's local lockDuration={} is NOT smaller than the broker's "
                + "group.share.record.lock.duration.ms={}. The broker will redeliver "
                + "stuck records before Plurima's force-RELEASE can fire — no early-"
                + "recovery benefit. Lower .lockDuration(...) on the builder to ≈ "
                + "0.8 × broker lock for faster handler-stuck recovery.",
                lockDuration, broker);
        } else {
            log.info("PollLoop lockDuration check OK: local={} < broker={} "
                + "(force-RELEASE will fire {}ms before broker would expire the lease).",
                lockDuration, broker, broker.minus(lockDuration).toMillis());
        }
        localLockComparedOnce = true;
    }

    private void recordPolledMetrics(ConsumerRecords<byte[], byte[]> batch) {
        if (batch.isEmpty()) return;
        for (TopicPartition tp : batch.partitions()) {
            metrics.recordsPolled(tp.topic(), batch.records(tp).size());
        }
    }

    /**
     * Register and dispatch every record in the batch. The first record reuses the
     * permit acquired by the poll itself; subsequent records each acquire their own.
     * If the batch is empty we release the phantom permit consumed by the poll.
     *
     * <p><b>Duplicate-coord guard.</b> A broker can redeliver a record whose coord
     * is still live in our registry (lock-expiry edges, force-RELEASE races). When
     * {@link InFlightRegistry#register} returns {@code false} we skip dispatch AND
     * release the permit we already took — otherwise the dispatched worker's
     * complete() would identity-fail (the registered record is the OLDER one),
     * leaving the permit leaked. The older worker remains the rightful owner and
     * will eventually clean up its own permit when it finishes.
     */
    private void dispatchBatch(ConsumerRecords<byte[], byte[]> batch) {
        int dispatched = 0;
        for (ConsumerRecord<byte[], byte[]> raw : batch) {
            if (dispatched > 0 && !acquirePermit()) {
                running = false;
                break;
            }
            InFlightRecord<byte[], byte[]> in = new InFlightRecord<>(raw);
            if (!registry.register(in)) {
                log.warn("Duplicate coord {} arrived while still in-flight (broker "
                    + "redelivery or force-RELEASE race); skipping dispatch, releasing "
                    + "permit, and RELEASEing this delivery. Older worker retains "
                    + "ownership of the original.", in.coord());
                // KIP-932 explicit-mode contract: every record returned by poll() must
                // be acked before the next poll() or the next consumer.poll throws ISE
                // with "The record cannot be acknowledged." The AckCoordinator path
                // can't help us here — its queueAck drops non-current records (the
                // older instance is the registered one; this duplicate is not "current").
                // Acknowledge the duplicate directly with RELEASE so the broker
                // re-acquires it and a later poll redelivers it for normal handling.
                acknowledgeDirectly(raw, AcknowledgeType.RELEASE);
                gate.release(1);
                dispatched++;       // count it so the empty-batch release path doesn't double-release
                continue;
            }
            dispatcher.dispatch(in);
            dispatched++;
        }
        if (dispatched == 0) gate.release(1);
    }

    /**
     * Direct broker-side acknowledge that bypasses {@link AckCoordinator}. Used only
     * by the duplicate-coord skip path in {@link #dispatchBatch}, where the registry
     * holds an older instance and the coordinator's {@code isCurrent} check would
     * silently drop our ack — the very behaviour we need to avoid.
     */
    private void acknowledgeDirectly(ConsumerRecord<byte[], byte[]> raw, AcknowledgeType type) {
        try {
            consumer.acknowledge(raw, type);
            metrics.ackCommitted(raw.topic(), type.name());
        } catch (Exception e) {
            // Worst case: broker rejects (lease already expired, batch closed, etc.).
            // The drain barrier still trips on next poll if anything is outstanding;
            // force-RELEASE then cleans up. Log so the operator sees the path firing.
            log.warn("Direct RELEASE of duplicate-coord delivery {}-{}@{} failed: {}",
                raw.topic(), raw.partition(), raw.offset(), e.toString());
        }
    }

    /**
     * Drain barrier + commit. Per KIP-932 explicit-mode contract the next poll() throws
     * ISE if any record from the previous poll is unacked. Wait for workers to finish
     * THEN drain — calling commitPendingAcks before all records have been ack'd appears
     * to close the batch and makes subsequent acknowledge() calls fail with "The record
     * cannot be acknowledged."
     *
     * <p>Long-handler caveat: if a single handler runs longer than the broker's
     * group.share.record.lock.duration.ms the broker will re-deliver the record and
     * our subsequent terminal ack lands on a now-released entry (force-RELEASE covers
     * this case). The fix is operator-side: raise the broker lock duration or shorten
     * the handler.
     */
    private void drainAndCommit() {
        boolean drained = registry.awaitDrain(effectiveBarrier());
        if (!drained && registry.currentInFlight() > 0) {
            forceReleaseStuckRecords();
        }
        coordinator.commitPendingAcks(consumer);
    }

    /**
     * Drain-barrier timeout for this poll cycle. Flat {@code barrierTimeout} when adaptive
     * is disabled or warming up; otherwise clamp(p&lt;percentile&gt; × multiplier,
     * max(FLOOR, pollTimeout), barrierTimeout). Ceiling pinned to barrierTimeout means
     * adaptive can only LOWER the give-up point — preserving the local&lt;broker lock invariant.
     *
     * <p>Package-private (not private) so unit tests can assert the clamp logic directly
     * without driving the whole loop. Called once per poll cycle from {@link #drainAndCommit}.
     */
    Duration effectiveBarrier() {
        if (latencyWindow == null || adaptiveConfig == null
            || latencyWindow.sampleCount() < ADAPTIVE_MIN_SAMPLES) {
            effectiveBarrierMillis = barrierTimeout.toMillis();
            return barrierTimeout;
        }
        double rawMs = latencyWindow.percentileMillis(adaptiveConfig.percentile())
            * adaptiveConfig.multiplier();
        Duration raw = Duration.ofMillis((long) Math.ceil(rawMs));
        Duration floor = ADAPTIVE_FLOOR.compareTo(pollTimeout) >= 0 ? ADAPTIVE_FLOOR : pollTimeout;
        // Apply the floor, THEN cap at the ceiling (barrierTimeout == lockDuration) LAST so
        // the ceiling always wins — even in the pathological case of a sub-floor lockDuration
        // (e.g. lockDuration < 1s). This guarantees the load-bearing invariant: the adaptive
        // barrier can only ever LOWER the give-up point, never exceed lockDuration.
        Duration result = raw.compareTo(floor) < 0 ? floor : raw;
        if (result.compareTo(barrierTimeout) > 0) {
            result = barrierTimeout;
        }
        effectiveBarrierMillis = result.toMillis();
        return result;
    }

    /** Current effective barrier in millis — backs the plurima.consumer.barrier.timeout_ms gauge. */
    long currentBarrierMillis() {
        return effectiveBarrierMillis;
    }

    public void shutdown() {
        running = false;
        consumer.wakeup();
    }

    @Override
    public void close() {
        shutdown();
    }

    private void forceReleaseStuckRecords() {
        var stuck = registry.activeRecords();
        if (stuck.isEmpty()) return;
        log.warn("Drain barrier timed out with {} record(s) in flight; force-RELEASE", stuck.size());
        for (var r : stuck) {
            coordinator.queueAck(r, org.apache.kafka.clients.consumer.AcknowledgeType.RELEASE);
            // From Plurima's perspective we have ABANDONED this record: RELEASE goes to the broker,
            // which may redeliver it on the next poll. Identity-aware complete() removes ONLY if
            // this exact InFlightRecord is still registered (it should be — we just read it from
            // activeRecords). The orphan worker's eventual complete(record) call will return
            // false because we removed our record here, OR because a redelivery has replaced it.
            //
            // Whoever's call to registry.complete() actually removes the entry is also
            // responsible for releasing the matching backpressure permit (see worker finally
            // block in UnorderedDispatcher). If we don't release here when
            // we abandon, the next poll cycle will block on gate.acquire(1) — that's the bug
            // where a fully-stuck consumer (concurrency=1, all permits held) cannot poll the
            // redelivered record and gets stuck for as long as the orphan worker runs.
            if (registry.complete(r)) {
                gate.release(1);
            }
        }
    }

    private void drainAndClose() {
        // NOTE: the shutdown drain deliberately uses the flat shutdownDrainTimeout, NOT
        // effectiveBarrier(). The adaptive barrier exists to abandon stragglers EARLY
        // during steady-state polling; at shutdown we instead want the full configured
        // budget to let in-flight workers finish and commit. The two drains are distinct
        // by design.
        // End-to-end budget: drain + close together must complete within
        // shutdownDrainTimeout so ShareConsumerRuntime's join (shutdownDrainTimeout + 5s)
        // sees the poll thread finish. Previously awaitDrain could consume the full
        // timeout AND consumer.close was given another full timeout — that's 2 ×
        // shutdownDrainTimeout worst case, exceeding the join budget. Use a deadline:
        // awaitDrain uses what it needs (up to shutdownDrainTimeout); consumer.close
        // gets whatever's left, with a floor so the broker always sees some attempt.
        long deadlineNanos = System.nanoTime() + shutdownDrainTimeout.toNanos();

        boolean drained = registry.awaitDrain(shutdownDrainTimeout);
        if (!drained && registry.currentInFlight() > 0) {
            forceReleaseStuckRecords();
        }
        // Final commit must NOT prevent consumer.close — at shutdown a network/broker
        // error here would otherwise propagate and leak the underlying KafkaShareConsumer.
        try {
            coordinator.commitPendingAcks(consumer);
        } catch (Exception e) {
            log.warn("Final commitPendingAcks at shutdown raised — closing consumer anyway", e);
        }

        Duration closeBudget = remainingBudget(deadlineNanos);
        try {
            consumer.close(closeBudget);
        } catch (Exception e) {
            log.warn("Consumer close raised", e);
        }
    }

    /**
     * Time remaining until {@code deadlineNanos}, floored at {@link #CLOSE_FLOOR}
     * so the broker always sees some close attempt even when awaitDrain consumed
     * the entire shutdown budget. The floor is small enough to stay within the
     * runtime's join padding (5s by convention).
     */
    private static Duration remainingBudget(long deadlineNanos) {
        long remainingNanos = deadlineNanos - System.nanoTime();
        if (remainingNanos < CLOSE_FLOOR.toNanos()) return CLOSE_FLOOR;
        return Duration.ofNanos(remainingNanos);
    }

    private static final Duration CLOSE_FLOOR = Duration.ofSeconds(2);
}
