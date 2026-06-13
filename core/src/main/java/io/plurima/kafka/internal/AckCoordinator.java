package io.plurima.kafka.internal;

import io.plurima.kafka.annotation.Internal;
import io.plurima.kafka.metrics.PlurimaMetrics;
import org.apache.kafka.clients.consumer.AcknowledgeType;
import org.apache.kafka.clients.consumer.AcknowledgementCommitCallback;
import org.apache.kafka.clients.consumer.ShareConsumer;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.errors.InvalidRecordStateException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Locale;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;

@Internal
public final class AckCoordinator implements AcknowledgementCommitCallback {

    private static final Logger log = LoggerFactory.getLogger(AckCoordinator.class);

    private final Queue<AckRequest> pending = new ConcurrentLinkedQueue<>();
    private final InFlightRegistry registry;
    private final PlurimaMetrics metrics;

    public AckCoordinator(InFlightRegistry registry) {
        this(registry, PlurimaMetrics.noOp());
    }

    public AckCoordinator(InFlightRegistry registry, PlurimaMetrics metrics) {
        this.registry = registry;
        this.metrics = metrics;
    }

    @Override
    public void onComplete(Map<TopicIdPartition, Set<Long>> offsets, Exception exception) {
        if (exception == null) {
            return;
        }
        for (TopicIdPartition tip : offsets.keySet()) {
            String topic = tip.topicPartition().topic();
            log.warn("Async commit failed for partition {}: {}", tip, exception.getMessage());
            metrics.ackCommitFailed(topic, exception.getClass().getSimpleName());
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
        metrics.ackQueued(type.name());
        // Centralized recordsProcessed emission. Previously only WorkerProcessor's auto-ack
        // success path fired this metric, so manual-ack ACCEPT/REJECT and ListenerInvoker's
        // auto-RELEASE-on-no-ack were invisible.
        metrics.recordsProcessed(r.coord().topic(), type.name().toLowerCase(Locale.ROOT));
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
     * This method retains the {@code registry.complete} call only in the
     * {@link InvalidRecordStateException} recovery path, where the worker may already
     * have exited without clearing the record.
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
                metrics.ackCommitted(req.coord().topic(), req.type().name());
            } catch (InvalidRecordStateException e) {
                // Lease expired broker-side. By the time commitPendingAcks runs, the worker
                // (or forceReleaseStuckRecords) has already completed the registry entry — so
                // the complete() call here is a no-op safety net. We don't need to release a
                // permit because the rightful owner already did so when they won the
                // identity-aware complete race.
                log.warn("Acknowledge failed (lease expired) for {}: {}",
                    req.coord(), e.getMessage());
                registry.complete(req.record());
            } catch (IllegalStateException e) {
                // The record is no longer in the consumer's current batch — most commonly
                // because PollLoop.forceReleaseStuckRecords() already RELEASE'd it after
                // the drain barrier timed out, and a (formerly stuck) worker has now finally
                // returned and queued its own ACCEPT. The broker has handed the record back
                // to the share group; redelivery handles it. Swallow + continue.
                log.warn("Acknowledge dropped (record not waiting — likely after force-RELEASE) for {}: {}",
                    req.coord(), e.getMessage());
                registry.complete(req.record());
            }
        }
        if (anyDrained) {
            consumer.commitAsync();
        }
    }
}
