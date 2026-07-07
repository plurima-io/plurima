package io.plurima.kafka.internal;

import io.plurima.kafka.annotation.Internal;
import io.plurima.kafka.metrics.AckOutcome;
import io.plurima.kafka.metrics.PlurimaMetrics;
import io.plurima.kafka.metrics.ProcessResult;
import org.apache.kafka.clients.consumer.AcknowledgeType;
import org.apache.kafka.clients.consumer.AcknowledgementCommitCallback;
import org.apache.kafka.clients.consumer.ShareConsumer;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicIdPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

@Internal
public final class AckCoordinator implements AcknowledgementCommitCallback {

    private static final Logger log = LoggerFactory.getLogger(AckCoordinator.class);

    private final Queue<AckRequest> pending = new ConcurrentLinkedQueue<>();
    private final InFlightRegistry registry;
    private final PlurimaMetrics metrics;
    private final SlownessReleaseTracker slownessTracker; // nullable — null = not tracked
    /**
     * Coords whose terminal ACCEPT/REJECT has been queued but whose broker commit has not yet
     * been confirmed. Their slowness count is cleared only once {@link #onComplete} reports the
     * commit succeeded — NOT at queue time — so that if the terminal ack/commit fails and the
     * record is redelivered, the redelivery still benefits from the earlier slowness subtraction
     * (and is not prematurely exhausted/DLT'd). Empty/unused when {@code slownessTracker} is null.
     */
    private final Set<RecordCoord> pendingSlownessClear = ConcurrentHashMap.newKeySet();

    public AckCoordinator(InFlightRegistry registry) {
        this(registry, PlurimaMetrics.noOp(), null);
    }

    public AckCoordinator(InFlightRegistry registry, PlurimaMetrics metrics) {
        this(registry, metrics, null);
    }

    public AckCoordinator(InFlightRegistry registry, PlurimaMetrics metrics,
                          SlownessReleaseTracker slownessTracker) {
        this.registry = registry;
        this.metrics = metrics;
        this.slownessTracker = slownessTracker;
    }

    @Override
    public void onComplete(Map<TopicIdPartition, Set<Long>> offsets, Exception exception) {
        // Resolve slowness-clear marks for the offsets in this commit. The mark is consumed
        // (removed) either way, but the tracker is cleared ONLY on commit success:
        //  - success → the terminal ack durably landed; forget the slowness count.
        //  - failure → the record will be redelivered; KEEP the count, but still drop the mark so a
        //    later non-terminal (RELEASE) commit of the same coord can't spuriously clear it later.
        // Only terminal acks (ACCEPT/REJECT) are ever in the set — RELEASEs are never marked, so a
        // committed RELEASE never matches and its count survives for the redelivery.
        if (slownessTracker != null && !pendingSlownessClear.isEmpty()) {
            for (var e : offsets.entrySet()) {
                String topic = e.getKey().topicPartition().topic();
                int partition = e.getKey().topicPartition().partition();
                for (Long offset : e.getValue()) {
                    RecordCoord coord = new RecordCoord(topic, partition, offset);
                    if (pendingSlownessClear.remove(coord) && exception == null) {
                        slownessTracker.clear(coord);
                    }
                }
            }
        }
        if (exception != null) {
            for (TopicIdPartition tip : offsets.keySet()) {
                log.warn("Async commit failed for partition {}: {}", tip, exception.getMessage());
                metrics.ackCommitFailed(tip.topicPartition().topic(), exception.getClass().getSimpleName());
            }
        }
    }

    public void queueAck(InFlightRecord<?, ?> r, AcknowledgeType type) {
        // Drop stale acks: if this exact InFlightRecord is no longer the current registered
        // value for its coord, an orphan worker is trying to ACCEPT/REJECT/RELEASE a record
        // we've already abandoned via forceReleaseStuckRecords. Submitting the ack would
        // either no-op at the broker (record already released and possibly redelivered) or
        // worse, inadvertently terminal-ack the redelivery — a stale ACCEPT from an orphan
        // could acknowledge the redelivered record. Drop silently; ISE swallow path also
        // covers any that slip through (e.g. queued before force-RELEASE ran).
        if (!registry.isCurrent(r)) {
            log.debug("Dropping stale {} ack for abandoned record {}", type, r.coord());
            return;
        }
        // First-wins terminal ack. Without this, a worker's just-queued ACCEPT could be
        // followed by forceReleaseStuckRecords queueing RELEASE for the same record (the
        // worker hasn't reached its finally yet, so the record is still "current"). Both
        // would land in pending and commitPendingAcks would drain both BEFORE commitAsync,
        // so the broker — which stores one ack type per offset before send — keeps the
        // later RELEASE and redelivers an already-successfully-processed record.
        if (!r.markTerminalAckQueued()) {
            log.debug("Dropping duplicate terminal {} ack for {} — first terminal already queued",
                type, r.coord());
            return;
        }
        pending.offer(new AckRequest(r, type));
        metrics.ackQueued(toAckOutcome(type));
        // Centralized recordsProcessed emission. Previously only WorkerProcessor's auto-ack
        // success path fired this metric, so manual-ack ACCEPT/REJECT and ListenerInvoker's
        // auto-RELEASE-on-no-ack were invisible.
        metrics.recordsProcessed(r.coord().topic(), toProcessResult(type));
        // Terminal outcome (ACCEPT/REJECT) → the coord will be done once its commit confirms. Mark
        // it for slowness-clear, but DON'T clear yet: the clear happens in onComplete on commit
        // success, so a failed commit (record redelivered) keeps its slowness subtraction. RELEASE
        // is non-terminal — never marked, so its count survives for the redelivery.
        if (slownessTracker != null
            && (type == AcknowledgeType.ACCEPT || type == AcknowledgeType.REJECT)) {
            pendingSlownessClear.add(r.coord());
        }
    }

    /**
     * RELEASE a record because its handler was too slow (drain-barrier / shutdown force-RELEASE),
     * recording the release as <em>slowness</em> rather than failure so {@link RetryEngine} does
     * not count the resulting broker redelivery toward retry exhaustion. Used only by
     * {@link PollLoop#forceReleaseStuckRecords()}; failure-driven retries go through
     * {@link #queueAck} with {@code RELEASE} and are (correctly) NOT recorded here.
     *
     * <p>The slowness count is recorded optimistically before the first-wins guard in
     * {@code queueAck}; if the RELEASE is dropped (the worker won the race with a terminal ACCEPT),
     * the record completes successfully and its stale count is never read again (and is evicted by
     * the tracker's cap) — harmless, and erring on the safe side (never toward premature DLT).
     */
    public void queueReleaseForSlowness(InFlightRecord<?, ?> r) {
        if (slownessTracker != null) {
            slownessTracker.recordRelease(r.coord());
        }
        queueAck(r, AcknowledgeType.RELEASE);
    }

    /**
     * Called only on the poll thread. Drains the pending-ack queue and, if anything was
     * actually drained, calls {@link org.apache.kafka.clients.consumer.ShareConsumer#commitAsync()}.
     *
     * <p>The "only if drained" guard avoids issuing a no-op commit on every empty poll
     * cycle (Plurima polls every {@code pollTimeout}, often returning zero records). It
     * also reduces the shutdown-time blast radius: if the final shutdown commit would
     * have only been a spurious commitAsync, we skip it entirely.
     *
     * <p>Note: {@link InFlightRegistry#complete(InFlightRecord)} is the <em>worker's</em>
     * responsibility (called in the worker's {@code finally} block) for the normal path.
     * This method retains the {@code registry.complete} call only in the recovery paths
     * for a rejected acknowledge (the invalid-record-state / lease-expired case and the
     * broader broker-side {@code KafkaException} family, plus the
     * {@code IllegalStateException} record-not-waiting case), where the worker may
     * already have exited without clearing the record.
     *
     * <p><b>Exceptions that are NOT recovered:</b> {@code WakeupException} and
     * {@code InterruptException} (shutdown control flow) and
     * {@code AuthenticationException} / {@code AuthorizationException} (persistent,
     * fatal per Kafka client conventions) are rethrown so the poll loop's fatal/shutdown
     * machinery engages instead of silently redelivering forever — see the catch blocks
     * below.
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    public void commitPendingAcks(ShareConsumer<?, ?> consumer) {
        boolean anyDrained = false;
        AckRequest req;
        while ((req = pending.poll()) != null) {
            anyDrained = true;
            try {
                // Use the ConsumerRecord-based acknowledge form (the 4-arg topic/partition/offset
                // form is documented for poison-pill records only and is rejected by the broker
                // for normal records — "The record cannot be acknowledged.").
                ((ShareConsumer) consumer).acknowledge(req.record().consumerRecord(), req.type());
                metrics.ackCommitted(req.coord().topic(), toAckOutcome(req.type()));
            } catch (org.apache.kafka.common.errors.WakeupException e) {
                // WakeupException extends KafkaException, so it would otherwise be
                // swallowed by the broader catch below and treated as a completed ack
                // failure — silently absorbing the shutdown signal PollLoop.shutdown()
                // relies on. ShareConsumer.acknowledge() is local bookkeeping (no network
                // call) and does not throw this in practice, but we rethrow explicitly
                // rather than depend on that implementation detail; a future client
                // version that makes acknowledge() wakeup-aware must not have its wakeup
                // absorbed here.
                throw e;
            } catch (org.apache.kafka.common.errors.InterruptException
                     | org.apache.kafka.common.errors.AuthenticationException
                     | org.apache.kafka.common.errors.AuthorizationException fatal) {
                // These KafkaException subtypes must NOT fall into the complete-and-
                // continue recovery below:
                //  - InterruptException is the client's unchecked wrapper for thread
                //    interruption — shutdown CONTROL FLOW, not a per-record ack failure.
                //    Swallowing it would absorb the stop signal (worker-pool shutdownNow /
                //    poll-thread interrupt) the shutdown path relies on.
                //  - Authentication/AuthorizationException are persistent per Kafka client
                //    conventions: every subsequent acknowledge fails identically, so
                //    complete-and-continue would degrade into infinite redelivery with the
                //    consumer still reporting RUNNING and onFatalError never firing.
                // Rethrow so PollLoop.run()'s fatal path (FAILED state transition + the
                // user's onFatalError callback) engages fast.
                throw fatal;
            } catch (KafkaException e) {
                // Broker/lease-side acknowledge failure — e.g. an invalid-record-state
                // error (lease expired) or any other KafkaException subtype the broker or
                // client raises for this specific record (timeouts, disconnects, etc.).
                // Previously only the invalid-record-state case was caught here; any other
                // KafkaException subtype propagated out of this loop, past drainAndCommit,
                // and into PollLoop.run()'s outer catch(Throwable) — which treats it as
                // FATAL and stops the ENTIRE poll thread over what is really a
                // single-record ack failure. Catching the broader KafkaException hierarchy
                // applies the same complete-and-continue recovery uniformly — EXCEPT the
                // control-flow / fatal-by-convention subtypes rethrown above: by the time
                // commitPendingAcks runs, the worker (or forceReleaseStuckRecords) has
                // already completed the registry entry, so the complete() call here is a
                // no-op safety net. We don't need to release a permit because the rightful
                // owner already did so when they won the identity-aware complete race. A
                // truly unknown Throwable (not a KafkaException) is NOT caught here — it
                // still propagates and is correctly treated as fatal.
                log.warn("Acknowledge failed (broker/lease error) for {}: {}",
                    req.coord(), e.getMessage());
                dropPendingSlownessClear(req);
                registry.complete(req.record());
            } catch (IllegalStateException e) {
                // The record is no longer in the consumer's current batch — most commonly
                // because PollLoop.forceReleaseStuckRecords() already RELEASE'd it after
                // the drain barrier timed out, and a (formerly stuck) worker has now finally
                // returned and queued its own ACCEPT. The broker has handed the record back
                // to the share group; redelivery handles it. Swallow + continue.
                log.warn("Acknowledge dropped (record not waiting — likely after force-RELEASE) for {}: {}",
                    req.coord(), e.getMessage());
                dropPendingSlownessClear(req);
                registry.complete(req.record());
            }
        }
        if (anyDrained) {
            consumer.commitAsync();
        }
    }

    /**
     * The terminal ack for {@code req} never reached the broker (lease expired / record not
     * waiting), so drop its slowness-clear mark — otherwise a later successful commit of the same
     * (redelivered) coord could spuriously clear a still-live slowness count. No-op for
     * non-terminal (RELEASE) acks, which are never marked.
     */
    private void dropPendingSlownessClear(AckRequest req) {
        if (slownessTracker != null) {
            pendingSlownessClear.remove(req.coord());
        }
    }

    /**
     * Maps the broker-facing {@link AcknowledgeType} to the metrics-facing
     * {@link AckOutcome}. Package-private so {@link PollLoop#acknowledgeDirectly} can
     * reuse it for its own {@code ackCommitted} emission on the duplicate-coord path.
     * {@code RENEW} never reaches Plurima's ack-queue/commit metrics — it is a
     * lock-renewal signal, not a terminal or queueable outcome.
     */
    static AckOutcome toAckOutcome(AcknowledgeType type) {
        return switch (type) {
            case ACCEPT -> AckOutcome.ACCEPT;
            case RELEASE -> AckOutcome.RELEASE;
            case REJECT -> AckOutcome.REJECT;
            case RENEW -> throw new IllegalArgumentException(
                "RENEW is not an ack outcome Plurima queues or commits: " + type);
        };
    }

    /** Maps the broker-facing {@link AcknowledgeType} to the metrics-facing {@link ProcessResult}. */
    private static ProcessResult toProcessResult(AcknowledgeType type) {
        return switch (type) {
            case ACCEPT -> ProcessResult.ACCEPT;
            case RELEASE -> ProcessResult.RELEASE;
            case REJECT -> ProcessResult.REJECT;
            case RENEW -> throw new IllegalArgumentException(
                "RENEW is not a process result Plurima records: " + type);
        };
    }
}
