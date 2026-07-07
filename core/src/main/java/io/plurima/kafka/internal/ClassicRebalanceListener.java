package io.plurima.kafka.internal;

import io.plurima.kafka.annotation.Internal;
import io.plurima.kafka.metrics.AckOutcome;
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
 *       and drop revoked partitions from the backpressure-paused AND DLT-failure-paused
 *       sets (an orphaned worker still retrying a DLT publish notices the frontier
 *       swap and stops on its own; this just keeps the desired-pause bookkeeping
 *       consistent with the handed-off assignment).</li>
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
 *
 * <p>{@code onPartitionsLost} performs the same local cleanup as revoke — via the
 * shared {@link #releaseLocalState} / {@link #clearCommitBuffers} helpers — but
 * deliberately skips the commit: see its javadoc for why committing offsets for
 * already-reassigned partitions is unsafe.
 */
@Internal
final class ClassicRebalanceListener implements ConsumerRebalanceListener {

    private static final Logger log = LoggerFactory.getLogger(ClassicRebalanceListener.class);

    private final KafkaConsumer<byte[], byte[]> consumer;
    private final ConcurrentMap<TopicPartition, CommitFrontier> frontiers;
    private final ClassicKeyShardDispatcher keyShardDispatcher;  // nullable
    private final Set<TopicPartition> pausedByBackpressure;
    /**
     * Shared key-set view over {@link ClassicPollLoop}'s per-partition DLT-pause
     * refcount holds (one hold per worker still retrying a failed DLT publish).
     * Removing a revoked partition here drops its entry — ALL of its workers' holds —
     * entirely: revoke ends the generation, the orphan retryers notice the frontier
     * swap and abort, and their own generation-guarded releases then no-op. This keeps
     * the desired-pause state consistent with the (now handed-off) assignment so a
     * later poll iteration doesn't keep a revoked partition paused. Independent of
     * {@link #pausedByBackpressure}.
     */
    private final Set<TopicPartition> pausedByDltFailure;
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
        Set<TopicPartition> pausedByDltFailure,
        Runnable clearBackpressureActiveIfEmpty,
        PlurimaMetrics metrics,
        ConcurrentMap<TopicPartition, OffsetAndMetadata> pendingFailedCommits,
        ConcurrentMap<TopicPartition, Long> committedHighWater,
        ConcurrentMap<TopicPartition, OffsetAndMetadata> inFlightAsyncCommits) {
        this.consumer = consumer;
        this.frontiers = frontiers;
        this.keyShardDispatcher = keyShardDispatcher;
        this.pausedByBackpressure = pausedByBackpressure;
        this.pausedByDltFailure = pausedByDltFailure;
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
        // Frontier removal already happened above (collectPendingCommits drains-and-removes
        // it to build the commit snapshot); releaseLocalState's own frontier removal is then
        // just a harmless no-op for this path. See onPartitionsLost for the case where
        // nothing has removed the frontier yet and this call does the real work.
        releaseLocalState(partitions);

        try {
            if (!commits.isEmpty()) {
                consumer.commitSync(commits);
                for (var e : commits.entrySet()) {
                    TopicPartition tp = e.getKey();
                    metrics.ackCommitted(tp.topic(), AckOutcome.ACCEPT);
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
            clearCommitBuffers(partitions);
        }
    }

    /**
     * The default {@link ConsumerRebalanceListener#onPartitionsLost} delegates to
     * {@link #onPartitionsRevoked}, which is wrong for this listener: "lost" only
     * fires once the group has <b>already</b> reassigned these partitions to another
     * member (session timeout, fatal group error, failed rejoin) — there was no
     * orderly handoff, so unlike revoke, we never get a last chance to commit before
     * someone else starts consuming. Calling {@code commitSync} here anyway would
     * race the new owner: our commit could land after the new owner has already
     * committed further ahead (silently regressing the broker's offset for that
     * partition), or simply be redundant, plus it tends to surface as a confusing
     * commit-failure log line during a rebalance the consumer no longer participates
     * in. So this override performs the identical local bookkeeping cleanup as
     * {@link #onPartitionsRevoked} (frontier removal, shard purge, pause-set and
     * commit-buffer cleanup) but skips the commit: the new owner simply starts from
     * whatever offset the broker last saw committed and re-processes at-least-once,
     * the same in-flight-overlap window already documented for a normal revoke.
     */
    @Override
    public void onPartitionsLost(Collection<TopicPartition> partitions) {
        if (partitions.isEmpty()) return;

        releaseLocalState(partitions);
        clearCommitBuffers(partitions);
        log.warn("Lost {} partitions (already reassigned by the group; not committing): {}",
            partitions.size(), partitions);
    }

    /**
     * Local (non-broker) cleanup shared by {@link #onPartitionsRevoked} and
     * {@link #onPartitionsLost}: drop the frontier (so late worker completions are
     * dropped via the identity check in {@link ClassicPollLoop#markComplete}), purge
     * queued KEY-mode shard entries, and drop the partitions from both independent
     * pause sets. For revoke this frontier removal is a harmless no-op — the commit
     * snapshot in {@link #collectPendingCommits} already removed it while draining —
     * but for lost it's the only place that happens.
     */
    private void releaseLocalState(Collection<TopicPartition> partitions) {
        for (TopicPartition tp : partitions) {
            frontiers.remove(tp);
        }
        if (keyShardDispatcher != null) {
            keyShardDispatcher.purgePartitions(partitions);
        }
        pausedByBackpressure.removeAll(partitions);
        // Drop DLT-failure pause intent too. The orphan DLT-retry worker (if any) also
        // self-clears when it notices the frontier is gone, but doing it here guarantees
        // the poll thread's applyDltFailurePause won't keep a handed-off/lost partition
        // paused, and mirrors the pausedByBackpressure cleanup above.
        pausedByDltFailure.removeAll(partitions);
        clearBackpressureActiveIfEmpty.run();
    }

    /**
     * Drop buffered failed-async commits AND in-flight async-commit tracking for
     * {@code partitions}. Shared by {@link #onPartitionsRevoked} (always, regardless
     * of whether the revoke-time commitSync succeeded) and {@link #onPartitionsLost}
     * (which has no commit attempt to gate this on at all) — in both cases, a later
     * {@code drainPendingCommits} running on this consumer must not try to commit
     * offsets for a partition it no longer owns.
     */
    private void clearCommitBuffers(Collection<TopicPartition> partitions) {
        for (TopicPartition tp : partitions) {
            pendingFailedCommits.remove(tp);
            inFlightAsyncCommits.remove(tp);
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
