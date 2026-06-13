package io.plurima.kafka.internal;

import io.plurima.kafka.annotation.Internal;
import io.plurima.kafka.metrics.PlurimaMetrics;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

/**
 * Rebalance listener for the CLASSIC_BASIC engine.
 *
 * <p>Two concerns on {@code onPartitionsRevoked}:
 *
 * <ol>
 *   <li><b>Commit before handoff.</b> For each revoked partition, drain its
 *       {@link CommitFrontier} and {@code commitSync} the result so the next owner of
 *       the partition picks up where we left off.</li>
 *   <li><b>Stop touching revoked-partition state.</b> Remove the frontier from the
 *       map (so any late worker completions are dropped via the identity check in
 *       {@link ClassicPollLoop#markComplete}), purge queued KEY-mode shard entries
 *       (which would otherwise process records the new owner is about to redeliver),
 *       and drop revoked partitions from the backpressure-paused set.</li>
 * </ol>
 *
 * <p>In-flight records on revoked partitions <i>continue to run</i> on their worker
 * threads — the continuous-poll model doesn't interrupt them. Their {@code markComplete}
 * calls drop silently because the frontier they captured is no longer the same instance
 * as what's in the map. This is at-least-once with possible side-effect overlap during
 * the rebalance window — see UserGuide § CLASSIC_BASIC.
 *
 * <p>{@code onPartitionsAssigned} is informational only — the new partitions get
 * fresh frontiers lazily in {@link ClassicPollLoop#dispatchBatchAsync} on first poll
 * after assignment.
 */
@Internal
final class ClassicRebalanceListener implements ConsumerRebalanceListener {

    private static final Logger log = LoggerFactory.getLogger(ClassicRebalanceListener.class);

    private final KafkaConsumer<byte[], byte[]> consumer;
    private final ConcurrentMap<TopicPartition, CommitFrontier> frontiers;
    private final ClassicKeyShardDispatcher keyShardDispatcher;  // nullable
    private final Set<TopicPartition> pausedByBackpressure;
    private final Runnable clearBackpressureActiveIfEmpty;
    private final PlurimaMetrics metrics;
    /**
     * Shared retry buffer of failed async commits keyed by partition. On revoke,
     * any buffered entry for a revoked partition is folded into the commitSync
     * snapshot so the synchronous commit reconciles the last failed async attempt
     * before handing the partition off. Always removed for revoked partitions at
     * the end of {@link #onPartitionsRevoked} so a later async drain cannot try to
     * commit offsets for a partition we no longer own.
     */
    private final ConcurrentMap<TopicPartition, OffsetAndMetadata> pendingFailedCommits;
    /**
     * Shared per-partition high-water of successfully-committed offsets. Updated
     * here on revoke-time commitSync success so the regression guard in
     * {@link ClassicPollLoop#handleAsyncCommitFailure} rejects any stale older
     * async failure callbacks that arrive after revoke + reassign.
     */
    private final ConcurrentMap<TopicPartition, Long> committedHighWater;
    /**
     * Shared per-partition tracking of commitAsync offsets whose callbacks have
     * not yet fired. Folded into the revoke commitSync snapshot so an in-flight
     * async commit racing the revoke can't be lost when the partition hands off
     * (and the old owner's async callback fires too late to retry).
     */
    private final ConcurrentMap<TopicPartition, OffsetAndMetadata> inFlightAsyncCommits;

    ClassicRebalanceListener(
        KafkaConsumer<byte[], byte[]> consumer,
        ConcurrentMap<TopicPartition, CommitFrontier> frontiers,
        ClassicKeyShardDispatcher keyShardDispatcher,
        Set<TopicPartition> pausedByBackpressure,
        Runnable clearBackpressureActiveIfEmpty,
        PlurimaMetrics metrics,
        ConcurrentMap<TopicPartition, OffsetAndMetadata> pendingFailedCommits,
        ConcurrentMap<TopicPartition, Long> committedHighWater,
        ConcurrentMap<TopicPartition, OffsetAndMetadata> inFlightAsyncCommits) {
        this.consumer = consumer;
        this.frontiers = frontiers;
        this.keyShardDispatcher = keyShardDispatcher;
        this.pausedByBackpressure = pausedByBackpressure;
        this.clearBackpressureActiveIfEmpty = clearBackpressureActiveIfEmpty;
        this.metrics = metrics;
        this.pendingFailedCommits = pendingFailedCommits;
        this.committedHighWater = committedHighWater;
        this.inFlightAsyncCommits = inFlightAsyncCommits;
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        if (partitions.isEmpty()) return;

        Map<TopicPartition, OffsetAndMetadata> commits = collectPendingCommits(partitions);
        if (keyShardDispatcher != null) {
            keyShardDispatcher.purgePartitions(partitions);
        }
        pausedByBackpressure.removeAll(partitions);
        clearBackpressureActiveIfEmpty.run();

        try {
            if (!commits.isEmpty()) {
                consumer.commitSync(commits);
                for (var e : commits.entrySet()) {
                    TopicPartition tp = e.getKey();
                    metrics.ackCommitted(tp.topic(), "accept");
                    // High-water records the broker-confirmed ceiling so any later
                    // stale async failure callback for this partition is ignored
                    // (see ClassicPollLoop.handleAsyncCommitFailure).
                    committedHighWater.merge(tp, e.getValue().offset(), Math::max);
                }
                log.info("Committed pending offsets on revoke for {} partitions: {}",
                    commits.size(), commits);
            } else {
                log.debug("No pending commits to flush on revoke of {}", partitions);
            }
        } catch (Exception e) {
            log.error("commitSync on revoke FAILED for {}; next owner of these partitions may "
                + "re-process records (at-least-once duplicate). Cause: {}",
                commits.keySet(), e.toString());
            for (TopicPartition tp : commits.keySet()) {
                metrics.ackCommitFailed(tp.topic(), e.getClass().getSimpleName());
            }
        } finally {
            // ALWAYS drop buffered failed-async commits AND in-flight async tracking
            // for revoked partitions, regardless of whether the revoke-time commitSync
            // succeeded. A later drainPendingCommits running on the old owner could
            // otherwise commit offsets for a partition the new owner now controls —
            // concurrent commits from two consumers in the same group is the classic
            // recipe for offset corruption. On revoke-commitSync failure the in-flight
            // workers' next-attempt records are simply re-delivered to the new owner
            // from the last broker-side committed offset (at-least-once duplicate),
            // which is documented behaviour.
            for (TopicPartition tp : partitions) {
                pendingFailedCommits.remove(tp);
                inFlightAsyncCommits.remove(tp);
            }
        }
    }

    /**
     * Build the revoke commit snapshot from three sources, newest-per-partition
     * wins, all filtered by {@link #committedHighWater}:
     * <ol>
     *   <li>Live frontier drain for the revoked partition.</li>
     *   <li>Any buffered failed-async commit for the partition.</li>
     *   <li>Any in-flight async commit whose callback hasn't fired yet — without
     *       this, a commitAsync racing the revoke would be invisible to the
     *       sync snapshot (frontier already advanced past it, failure callback
     *       not yet seen) and the new owner would redeliver already-processed
     *       records.</li>
     * </ol>
     * The synchronous commitSync that follows is the consumer's last chance to
     * persist these offsets before handoff.
     */
    private Map<TopicPartition, OffsetAndMetadata> collectPendingCommits(
        Collection<TopicPartition> partitions) {
        Map<TopicPartition, OffsetAndMetadata> commits = new HashMap<>();
        for (TopicPartition tp : partitions) {
            CommitFrontier f = frontiers.remove(tp);
            if (f != null) {
                OptionalLong commit = f.drainCommittable();
                commit.ifPresent(off -> commits.put(tp, new OffsetAndMetadata(off)));
            }
            mergeIfNotStale(commits, tp, pendingFailedCommits.get(tp));
            mergeIfNotStale(commits, tp, inFlightAsyncCommits.get(tp));
        }
        return filterStaleEntries(commits);
    }

    private void mergeIfNotStale(
        Map<TopicPartition, OffsetAndMetadata> commits, TopicPartition tp, OffsetAndMetadata candidate) {
        if (candidate == null) return;
        commits.merge(tp, candidate,
            (live, buf) -> live.offset() >= buf.offset() ? live : buf);
    }

    private Map<TopicPartition, OffsetAndMetadata> filterStaleEntries(
        Map<TopicPartition, OffsetAndMetadata> commits) {
        // High-water defence: drop any candidate ≤ recorded committed offset to
        // avoid regressing the broker's commit on revoke commitSync.
        commits.entrySet().removeIf(e -> {
            Long highWater = committedHighWater.get(e.getKey());
            return highWater != null && e.getValue().offset() <= highWater;
        });
        return commits;
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        if (!partitions.isEmpty()) {
            log.info("Assigned {} partitions: {}", partitions.size(), partitions);
        }
    }
}
