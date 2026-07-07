package io.plurima.kafka.internal;

import io.plurima.kafka.OrderingMode;
import io.plurima.kafka.RecordListener;
import io.plurima.kafka.annotation.Internal;
import io.plurima.kafka.deserializer.RecordDeserializer;
import io.plurima.kafka.metrics.AckOutcome;
import io.plurima.kafka.metrics.BackpressureEvent;
import io.plurima.kafka.metrics.PlurimaMetrics;
import io.plurima.kafka.metrics.ProcessResult;
import org.apache.kafka.clients.consumer.CloseOptions;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiPredicate;
import java.util.function.Consumer;

/**
 * Poll loop for the CLASSIC_BASIC engine. Continuous-poll model with intra-partition
 * key-shard parallelism (KEY mode), out-of-order commit frontier (KEY mode), and
 * pause/resume backpressure to keep the consumer heartbeating under load.
 *
 * <h3>Threading model</h3>
 * <ul>
 *   <li><b>Poll thread.</b> Runs {@link #run()}; never blocks beyond {@code pollTimeout}.
 *       Each iteration: apply backpressure (pause/resume based on in-flight count),
 *       poll the broker, dispatch any returned records asynchronously, drain advanced
 *       commit frontiers. The poll thread heartbeats every iteration regardless of
 *       worker progress — the broker's {@code max.poll.interval.ms} cannot fence us.</li>
 *   <li><b>Worker threads.</b> Run {@code processOne} per record (with retry / DLT
 *       semantics) and call {@link #markComplete} on success / reject / DLT terminal.
 *       Each completion decrements {@link #inFlight}; the poll thread observes it on
 *       the next iteration.</li>
 * </ul>
 *
 * <h3>Backpressure</h3>
 * In-flight count is bounded only loosely (a single {@code poll()} can return up to
 * {@code max.poll.records} records, all of which we dispatch). We use the count to
 * decide whether to pause the consumer for the NEXT poll. When in-flight ≥
 * {@code concurrency} we pause all assigned partitions; when in-flight ≤
 * {@code concurrency / 2} we resume. Paused partitions return zero records on poll,
 * which keeps the heartbeat alive without filling up workers further.
 *
 * <h3>Commit semantics</h3>
 * Each partition has a {@link CommitFrontier} tracking the smallest offset NOT yet
 * completed. Workers call {@code markComplete(tp, offset)} when their record reaches
 * a terminal state. The poll thread drains advanced frontiers between polls and emits
 * {@code commitAsync()}. On rebalance revoke, frontiers for the revoked partitions
 * are drained and committed synchronously before reassignment.
 *
 * <h3>Retry / DLT (preserved from prior version)</h3>
 * <ul>
 *   <li>Success → frontier.complete(offset).</li>
 *   <li>Inline retry (delay ≤ 1s) → sleep on worker, retry.</li>
 *   <li>Delayed retry (delay &gt; 1s) → sleep on worker. The continuous-poll model
 *       means the worker can sleep as long as the per-retry cap allows without
 *       affecting heartbeat — no partition pause needed for retry.</li>
 *   <li>Reject → frontier.complete (skip).</li>
 *   <li>Exhausted + DLT → produce to DLT, then frontier.complete on success.
 *       On DLT publish failure the frontier is NOT advanced (commit stalls at the
 *       failing offset until DLT is healthy; restart redelivers), AND the partition
 *       is paused (independently of backpressure — see {@link #pausedByDltFailure})
 *       while the failing worker retries the publish with capped exponential
 *       backoff. This bounds {@code completedAhead} growth during a DLT outage
 *       instead of letting it grow unboundedly while later offsets keep completing.
 *       Alert on {@code plurima.consumer.dlt.failures}.</li>
 *   <li>Exhausted, no DLT → log ERROR + frontier.complete (lossy by configuration —
 *       record dropped, {@code plurima.consumer.records.processed{result=reject}}).</li>
 * </ul>
 */
@Internal
final class ClassicPollLoop<K, V> implements Runnable, AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(ClassicPollLoop.class);

    private final KafkaConsumer<byte[], byte[]> consumer;
    private final String topic;
    /** Consumer group id, for the {@code group_id} tag on {@code plurima.consumer.poll.duration}. */
    private final String groupId;
    private final RecordListener<K, V> listener;
    private final RecordDeserializer<K> keyDeser;
    private final RecordDeserializer<V> valueDeser;
    private final OrderingMode ordering;
    private final RetryEngine retryEngine;
    private final DltRouter dltRouter;          // nullable
    private final Duration pollTimeout;
    private final PlurimaMetrics metrics;
    private final WorkerLauncher launcher;
    /**
     * True when both key and value deserializers are the cached
     * {@link RecordDeserializer#IDENTITY_BYTES} singleton — the common
     * bytes-in/bytes-out case. We skip the {@code new ConsumerRecord<>(...)}
     * allocation in {@link #deserialize} and return the raw record cast to
     * {@code ConsumerRecord<K, V>}, which is safe because {@code K = V = byte[]}
     * is the only way both can be identity.
     */
    private final boolean identityDeserializers;
    /**
     * Callback invoked from the poll thread's {@code finally} block when the loop exits.
     * Wired to {@link ClassicBasicRuntime#close()} so the runtime closes its underlying
     * KafkaConsumer + DltRouter + WorkerLauncher after the loop drains. Idempotent and
     * self-join-safe (must not join the poll thread back to itself).
     */
    private final Runnable onLoopExit;
    /**
     * Fatal-error hook (B6). Invoked from {@link #run()}'s finally block ONLY when the
     * loop exited via the generic {@code catch (Throwable t)} branch below — i.e. an
     * unrecoverable error, not a normal shutdown — and only in place of (never in
     * addition to) {@link #onLoopExit}. {@code null} for legacy/test constructors that
     * don't wire one up; in that case {@link #onLoopExit} still fires on the fatal path
     * so cleanup is never skipped, just without the state-transition/callback semantics
     * {@code ClassicBasicRuntime.handleFatal} adds. Mirrors {@code PollLoop.onFatal} —
     * see there for the SHARE-engine analogue.
     */
    private final Consumer<Throwable> onFatal;

    /**
     * Per-partition commit frontier. Each frontier tracks the smallest offset NOT yet
     * completed for its partition; the poll thread drains advanced frontiers between
     * polls and issues {@code commitAsync()}. Out-of-order completion is supported —
     * required for KEY mode where intra-partition key-shard parallelism lets later
     * offsets finish before earlier ones. For UNORDERED and PARTITION modes (in-order
     * completion within each partition) the frontier still works, advancing linearly.
     */
    private final ConcurrentMap<TopicPartition, CommitFrontier> frontiers = new ConcurrentHashMap<>();

    /**
     * Failed-commit retry buffer. When {@code commitAsync}'s callback reports an
     * exception for a partition, the offset we tried to commit lands here so the
     * next drain re-issues it. Without this, {@link CommitFrontier#drainCommittable}
     * has already advanced {@code lastDrained} past the failed offset and won't
     * re-emit it — a transient broker hiccup followed by shutdown/rebalance would
     * produce avoidable duplicate processing of records the frontier had already
     * marked complete but never successfully persisted broker-side.
     *
     * <p>Semantics: keyed by partition, holds the LATEST offset we failed to commit.
     * On the next drain we merge this map into the snapshot (failed offset is used
     * only if {@link CommitFrontier} has no newer offset to emit — newer subsumes
     * older). On commitAsync success the entry is cleared. On final shutdown +
     * revoke commitSync, this map is included so the synchronous commit reconciles
     * the last failed attempt.
     */
    private final ConcurrentMap<TopicPartition, OffsetAndMetadata> pendingFailedCommits =
        new ConcurrentHashMap<>();

    /**
     * Per-partition high-water of successfully-committed offsets — the highest value
     * we have proof the broker accepted. <b>Monotonic non-decreasing</b>. Two layers
     * of regression protection feed off this:
     *
     * <ol>
     *   <li>A {@code commitAsync} failure callback NEVER buffers an offset
     *       {@code ≤ highWater}. Kafka does not protect offset commits from going
     *       backwards — if newer commit 20 succeeds and older commit 10 fails (or
     *       its callback fires out of order with the success), unconditionally
     *       buffering 10 lets the next drain re-commit it. That would regress the
     *       group offset and produce a duplicate window covering 11..19. The
     *       high-water filter rejects the stale buffering.</li>
     *   <li>{@link #collectCommitSnapshot} also filters the buffered map by
     *       high-water defensively in case anything stale lingers (e.g. from a
     *       prior generation before high-water was updated).</li>
     * </ol>
     */
    private final ConcurrentMap<TopicPartition, Long> committedHighWater = new ConcurrentHashMap<>();

    /**
     * Offsets sent via {@code commitAsync} for which no callback has yet fired —
     * i.e. commits the broker may or may not have already persisted. {@link CommitFrontier#drainCommittable}
     * advances {@code lastDrained} immediately on emit, so once an offset is in
     * flight via commitAsync the frontier will not re-emit it on subsequent drains.
     *
     * <p>If shutdown or revoke fires while an async commit is still pending, the
     * subsequent commitSync would normally see an empty snapshot (frontier already
     * drained, failure callback hasn't fired yet) and skip the partition. We then
     * close the consumer; the in-flight commit's callback may later report failure
     * with no opportunity to retry, and restart redelivers records the consumer
     * had already processed.
     *
     * <p>This map plugs the gap: every commitAsync writes the offset here, every
     * callback (success OR failure) clears it, and {@link #collectCommitSnapshot}
     * merges it into the snapshot (filtered through {@link #committedHighWater}).
     * Shutdown's {@link #drainPendingCommitsSync} and the rebalance listener's
     * commitSync both pick it up — the sync commit either succeeds and resolves
     * the uncertainty, or fails and lands in the buffered-retry path the same way
     * any other commit failure does.
     */
    private final ConcurrentMap<TopicPartition, OffsetAndMetadata> inFlightAsyncCommits =
        new ConcurrentHashMap<>();

    /**
     * Configured {@code shutdownDrainTimeout} in milliseconds. On {@code close()}, the
     * poll thread's finally block waits up to this long for in-flight records to finish
     * before tearing down. Honors UserGuide § Shutdown semantics — handlers shorter than
     * the configured drain timeout complete and their offsets commit; longer handlers
     * are abandoned with a WARN and their records redeliver to the next partition owner.
     */
    private final long shutdownDrainTimeoutMs;

    /**
     * Builder-configured concurrency. Used as the backpressure pause/resume threshold:
     * pause all assigned partitions when {@code inFlight ≥ concurrency}; resume when
     * {@code inFlight ≤ concurrency / 2}.
     */
    private final int concurrency;

    /** Count of dispatched-but-not-yet-completed records. Drives backpressure decisions. */
    private final AtomicInteger inFlight = new AtomicInteger(0);

    /** Partitions we explicitly paused for backpressure. We resume these specifically. */
    private final Set<TopicPartition> pausedByBackpressure = ConcurrentHashMap.newKeySet();

    /** Whether we have actively paused for backpressure. Avoids re-pausing every iteration. */
    private volatile boolean backpressureActive = false;

    /**
     * Per-partition DLT-pause holds: a partition is paused for DLT failure while ANY
     * worker is retrying a failed DLT publish for it, so the value is a per-worker
     * refcount scoped to the partition's frontier generation. <b>Deliberately independent
     * of {@link #pausedByBackpressure}.</b> A single partition may be paused for BOTH
     * reasons at once, and each reason has its own resume trigger:
     * <ul>
     *   <li>Backpressure resumes when the in-flight count drains — but its resume path
     *       ({@link #applyBackpressure}) explicitly excludes anything still held here,
     *       so a drop in in-flight can never resume a partition whose DLT is still down.</li>
     *   <li>A DLT-failure pause resumes only when the LAST retrying worker's publish
     *       succeeds (or the partition is revoked/lost), which removes the entry here;
     *       {@link #applyDltFailurePause} then issues the {@code consumer.resume}.</li>
     * </ul>
     *
     * <p><b>Why a refcount, not plain set membership.</b> With UNORDERED (or KEY)
     * ordering, several workers on the SAME partition can be inside
     * {@link #pauseAndRetryDlt}'s retry loop concurrently. A plain set entry would be
     * removed by the FIRST worker to succeed, resuming the partition while the others are
     * still retrying — and they never re-add it, so {@code completedAhead} would grow
     * unboundedly for the rest of the outage (the exact condition this pause exists to
     * prevent). Each worker instead increments on retry-loop entry and decrements exactly
     * once on exit (success, abort, shutdown); the entry — and thus the pause — only
     * clears at refcount zero.
     *
     * <p><b>Why generation-scoped.</b> Revoke (or lost) clears a partition's entry
     * ENTIRELY via {@link #pausedByDltFailure}{@code .removeAll} — all of the old
     * generation's holds vanish at once, matching the frontier removal that makes those
     * workers abort. If the partition is then reassigned and a NEW generation's worker
     * re-acquires a hold, a straggling old-generation worker's late decrement must not
     * release the new generation's pause: {@link #releaseDltPause} only decrements when
     * the hold's recorded frontier is identical to the caller's, and
     * {@link #acquireDltPause} only replaces a stale different-generation hold when the
     * CALLER is the live generation — a stale caller leaves an existing (possibly newer,
     * live) hold untouched instead of clobbering it.
     *
     * <p><b>Threading.</b> Mutated by WORKER threads via {@link #acquireDltPause} /
     * {@link #releaseDltPause}; cleared wholesale by the rebalance listener (poll thread)
     * and {@code run()}'s shutdown finally. Read by the POLL thread in
     * {@link #applyDltFailurePause} and {@link #applyBackpressure} through the
     * {@link #pausedByDltFailure} key-set view. Only the poll thread ever calls
     * {@code consumer.pause/resume} — workers never touch the (non-thread-safe) consumer.
     */
    private final ConcurrentMap<TopicPartition, DltPauseHold> dltPauseHolds = new ConcurrentHashMap<>();

    /**
     * Live key-set view of {@link #dltPauseHolds} — the "desired DLT-paused" partitions.
     * The poll thread's pause/resume reconciliation ({@link #applyDltFailurePause},
     * {@link #applyBackpressure}) reads it; {@link ClassicRebalanceListener} and
     * {@link #simulateRevoke} call {@code removeAll} on it, which drops the underlying
     * refcount entries entirely (revoke releases every worker's hold at once — their own
     * generation-guarded releases then no-op). Never added to directly: workers acquire
     * holds through {@link #acquireDltPause} only.
     */
    private final Set<TopicPartition> pausedByDltFailure = dltPauseHolds.keySet();

    /**
     * One partition's aggregate DLT-pause hold: how many workers of {@code generation}
     * (the frontier instance captured at their dispatch) are currently inside the DLT
     * retry loop. Immutable — {@link #acquireDltPause}/{@link #releaseDltPause} swap
     * whole instances inside atomic {@code compute} operations.
     */
    private record DltPauseHold(CommitFrontier generation, int count) {}

    /**
     * Register this worker's DLT-pause hold for {@code tp} (increment the refcount).
     * A leftover hold from a DIFFERENT generation is replaced ONLY IF the caller itself
     * is the live generation ({@code frontiers.get(tp) == frontier}): that is the normal
     * revoke+reassign race, where the old-generation owners are aborting and their
     * generation-guarded releases will skip the new hold. A STALE caller (its own
     * generation no longer live) must NOT clobber whatever hold is there — most
     * dangerously, a live NEWER generation's hold — because a stale caller has no
     * standing to mutate a pause it doesn't own; it just leaves the existing hold
     * untouched and falls through to its own (no-op) release later. This mirrors
     * {@link #releaseDltPause}'s generation guard: both acquire and release must be
     * safe against stale-generation stragglers waking up after reassignment.
     */
    private void acquireDltPause(TopicPartition tp, CommitFrontier frontier) {
        dltPauseHolds.compute(tp, (k, hold) -> {
            if (hold != null && hold.generation() == frontier) {
                return new DltPauseHold(frontier, hold.count() + 1);
            }
            if (hold != null && frontiers.get(tp) != frontier) {
                // Caller is stale (not the live generation) — leave the existing hold
                // (whoever owns it) untouched rather than clobbering it.
                return hold;
            }
            return new DltPauseHold(frontier, 1);
        });
    }

    /**
     * Release this worker's DLT-pause hold (decrement-and-remove-at-zero). No-ops when
     * the entry is gone (revoke already cleared it) or belongs to a NEWER generation
     * (revoke cleared ours, reassign + a new failure re-acquired it) — a stale worker
     * must never lift a pause it no longer owns.
     */
    private void releaseDltPause(TopicPartition tp, CommitFrontier frontier) {
        dltPauseHolds.computeIfPresent(tp, (k, hold) -> {
            if (hold.generation() != frontier) return hold;  // not our hold — leave it
            return hold.count() <= 1 ? null : new DltPauseHold(frontier, hold.count() - 1);
        });
    }

    /**
     * Poll-thread-ONLY mirror of which partitions {@link #applyDltFailurePause} has
     * actually issued {@code consumer.pause} for. The reconcile step diffs this against
     * {@link #pausedByDltFailure}: entries present there but not here are newly-failed
     * (pause them); entries here but no longer there had their worker recover (resume
     * them). Plain {@link HashSet} — never touched off the poll thread.
     */
    private final Set<TopicPartition> dltPauseApplied = new HashSet<>();

    /**
     * DLT-publish retry backoff, capped exponential with jitter. Defaults per design:
     * 1s start, 30s cap. {@code volatile} + package-private so
     * {@code ClassicDltFailureBackpressureTest} can shrink them for fast, deterministic
     * runs without a broker. Never mutated in production.
     */
    private volatile long dltRetryBaseMs = 1_000L;
    private volatile long dltRetryCapMs = 30_000L;

    /**
     * Per-attempt budget {@link #publishToDlt} waits on {@code route.future().get(...)}
     * before giving up and treating the attempt as a caller-side timeout. 30s in
     * production (matches the client's default request timeout order of magnitude);
     * {@code volatile} + package-private purely as a test seam — see
     * {@link #setDltPublishBudgetForTest} — so tests can force the caller-side
     * give-up path deterministically without actually blocking 30 real seconds. Never
     * mutated in production.
     */
    private volatile long dltPublishBudgetMs = 30_000L;

    private volatile boolean running = true;

    /**
     * Dispatch strategy chosen per {@code ordering}:
     * <ul>
     *   <li>UNORDERED → {@link ClassicUnorderedDispatcher}: one worker per record;
     *       same-partition records run concurrently.</li>
     *   <li>PARTITION → {@link ClassicPartitionSerialDispatcher}: one worker per
     *       partition; in-partition records run in offset order.</li>
     *   <li>KEY → {@link ClassicKeyShardDispatcher}: intra-partition parallelism via
     *       {@code (tp, hash(key) % shardCount)} shards.</li>
     * </ul>
     */
    private final ClassicDispatcher dispatcher;

    /**
     * Optional reference to the key-shard dispatcher when {@code ordering == KEY}.
     * Used by the rebalance revoke path to purge shards for revoked partitions so a
     * later reassignment to the same consumer starts clean. Null in other modes.
     */
    private final ClassicKeyShardDispatcher keyShardDispatcher;

    ClassicPollLoop(
        KafkaConsumer<byte[], byte[]> consumer,
        String topic,
        String groupId,
        RecordListener<K, V> listener,
        RecordDeserializer<K> keyDeser,
        RecordDeserializer<V> valueDeser,
        OrderingMode ordering,
        RetryEngine retryEngine,
        DltRouter dltRouter,
        Duration pollTimeout,
        long shutdownDrainTimeoutMs,
        int concurrency,
        int shardCount,
        PlurimaMetrics metrics,
        WorkerLauncher launcher,
        Runnable onLoopExit) {
        this(consumer, topic, groupId, listener, keyDeser, valueDeser, ordering, retryEngine, dltRouter,
            pollTimeout, shutdownDrainTimeoutMs, concurrency, shardCount, metrics, launcher,
            onLoopExit, null);
    }

    /**
     * Full constructor including the fatal-error hook (B6). {@code onFatal} is wired by
     * {@code ClassicBasicRuntime} to its {@code handleFatal} method; see {@link #onFatal}
     * for exactly when it fires relative to {@link #onLoopExit}.
     */
    ClassicPollLoop(
        KafkaConsumer<byte[], byte[]> consumer,
        String topic,
        String groupId,
        RecordListener<K, V> listener,
        RecordDeserializer<K> keyDeser,
        RecordDeserializer<V> valueDeser,
        OrderingMode ordering,
        RetryEngine retryEngine,
        DltRouter dltRouter,
        Duration pollTimeout,
        long shutdownDrainTimeoutMs,
        int concurrency,
        int shardCount,
        PlurimaMetrics metrics,
        WorkerLauncher launcher,
        Runnable onLoopExit,
        Consumer<Throwable> onFatal) {
        this.consumer = consumer;
        this.topic = topic;
        this.groupId = groupId;
        this.listener = listener;
        this.keyDeser = keyDeser;
        this.valueDeser = valueDeser;
        this.identityDeserializers =
            keyDeser == RecordDeserializer.IDENTITY_BYTES
            && valueDeser == RecordDeserializer.IDENTITY_BYTES;
        this.ordering = ordering;
        this.retryEngine = retryEngine;
        this.dltRouter = dltRouter;
        this.pollTimeout = pollTimeout;
        this.shutdownDrainTimeoutMs = shutdownDrainTimeoutMs;
        this.concurrency = concurrency;
        this.metrics = metrics;
        this.launcher = launcher;
        this.onLoopExit = onLoopExit;
        this.onFatal = onFatal;
        // stillOwned: returns true iff the partition's commit frontier is still the
        // same instance we captured at dispatch time. If revoke happened (frontier
        // removed) or revoke+reassign happened (a new frontier installed for the
        // same TopicPartition), this returns false. Used by PartitionSerial to break
        // out of a mid-batch loop, and by Unordered to skip launching listener work
        // for stale records.
        BiPredicate<TopicPartition, CommitFrontier> stillOwned = (tp, f) -> frontiers.get(tp) == f;
        ClassicKeyShardDispatcher ks = ordering == OrderingMode.KEY
            ? new ClassicKeyShardDispatcher(shardCount, launcher, this::processOne, () -> running)
            : null;
        this.keyShardDispatcher = ks;
        this.dispatcher = switch (ordering) {
            case KEY -> ks;
            // UNORDERED launches each record into its own worker thread regardless
            // of partition — within a partition records run concurrently, giving
            // the throughput their lack-of-ordering buys. CommitFrontier handles
            // the out-of-order completion arithmetic safely.
            case UNORDERED -> new ClassicUnorderedDispatcher(
                launcher, this::processOne, () -> running, stillOwned);
            // PARTITION routes every record through a persistent per-partition worker
            // chain: one worker at a time drains the partition's queue, so records
            // process serially in offset order ACROSS poll batches (cross-batch FIFO),
            // while distinct partitions still run concurrently. Cross-cluster
            // per-partition FIFO comes from classic consumer-group exclusive partition
            // ownership.
            case PARTITION -> new ClassicPartitionSerialDispatcher(
                launcher, this::processOne, () -> running, stillOwned);
        };
    }

    @Override
    public void run() {
        Throwable fatal = null;
        try {
            while (running) {
                applyBackpressure();
                // Reconcile DLT-failure pauses AFTER backpressure so a DLT pause always
                // has the final say for this iteration (backpressure's resume path
                // deliberately leaves DLT-paused partitions paused; this step also
                // resumes any whose worker just recovered).
                applyDltFailurePause();

                ConsumerRecords<byte[], byte[]> batch;
                long pollStartNanos = System.nanoTime();
                try {
                    batch = consumer.poll(pollTimeout);
                } catch (WakeupException e) {
                    // Only the shutdown path issues wakeup() (see shutdown()).
                    if (running) throw e;
                    break;
                }
                metrics.recordPollDuration(
                    topic, groupId, Duration.ofNanos(System.nanoTime() - pollStartNanos));

                if (!batch.isEmpty()) {
                    for (TopicPartition tp : batch.partitions()) {
                        metrics.recordsPolled(tp.topic(), batch.records(tp).size());
                    }
                    dispatchBatchAsync(batch);
                }

                drainPendingCommits();
            }
        } catch (WakeupException e) {
            if (running) {
                log.warn("Unexpected WakeupException in ClassicPollLoop", e);
            }
        } catch (Throwable t) {
            log.error("Fatal error in ClassicPollLoop; consumer will stop", t);
            fatal = t;
        } finally {
            // Shared close deadline — mirrors PollLoop.drainAndClose() (the SHARE engine's
            // analogous shutdown path). Drain, final commit, AND consumer.close() together
            // must not exceed shutdownDrainTimeoutMs, matching the budget
            // ClassicBasicRuntime.close() waits on when it joins this thread
            // (shutdownDrainTimeout + 5s of padding). Previously each phase drew its OWN
            // full budget: shutdownDrain() computed a fresh shutdownDrainTimeoutMs-long
            // deadline, then drainPendingCommitsSync() blocked on the client's UNBOUNDED
            // default commitSync timeout, then consumer.close() blocked on the client's
            // UNBOUNDED default close timeout. Worst case that's the configured drain
            // budget PLUS two independent unbounded blocking calls — easily exceeding the
            // runtime's join padding and leaving the non-daemon poll thread running in the
            // background after the runtime's close() had already returned.
            //
            // Both exit paths must converge on running == false BEFORE the drain below.
            // A normal shutdown() already flipped it; the fatal path (generic
            // catch (Throwable) above) reaches here with running still TRUE — the flag is
            // otherwise only flipped later, by handleFatal → doClose → shutdown(), which
            // runs AFTER this whole finally block. Without this write, a worker parked in
            // pauseAndRetryDlt's while (running) retry loop (or processOne's retry loop)
            // keeps retrying and pins inFlight for the entire drain slice, starving the
            // final commit down to its floor. Flipping it here lets every worker observe
            // shutdown at its next check and unwind promptly. Nothing below this point
            // reads running as "still live": shutdownDrain only watches inFlight, and the
            // commit/close phases don't consult the flag at all.
            running = false;
            // One overall deadline computed once. The TAIL phases are reserved UP FRONT:
            // the drain may consume at most (overall − COMMIT_RESERVE − CLOSE_FLOOR), so
            // even a drain that exhausts its whole slice leaves the final commitSync a
            // meaningful ~COMMIT_RESERVE budget rather than starving it to the 2s close
            // floor (a routinely slow drain would otherwise turn every such shutdown's
            // final commit into a coin flip → avoidable duplicates on restart). Each
            // subsequent phase then gets whatever's left of the shared deadline, floored
            // so the broker always sees some attempt — see remainingBudget.
            long deadlineNanos = System.nanoTime() + shutdownDrainTimeoutMs * 1_000_000L;
            shutdownDrain(deadlineNanos - COMMIT_RESERVE.toNanos() - CLOSE_FLOOR.toNanos());
            // Drop DLT-pause bookkeeping on the way out (close clears the set, per design).
            // Workers still in a DLT retry loop observe running==false and stop on their
            // own; clearing here just keeps the state tidy for any post-shutdown inspection.
            pausedByDltFailure.clear();
            dltPauseApplied.clear();
            // SYNC commit on shutdown so a transient broker hiccup on the FINAL commit
            // can't silently leave the last batch unpersisted and cause avoidable
            // duplicates on restart. drainPendingCommits (async) is fine for steady-
            // state — newer commits subsume older — but the last call has nothing
            // following to subsume a failed earlier attempt. Bounded by what's left of
            // the shared deadline (minus the CLOSE_FLOOR still owed to consumer.close
            // below) rather than the client's default; floored at COMMIT_RESERVE so the
            // up-front reservation above is honored even when the drain overshoots its
            // slice by a scheduling tick.
            drainPendingCommitsSync(
                remainingBudget(deadlineNanos - CLOSE_FLOOR.toNanos(), COMMIT_RESERVE));
            // CRITICAL: close the consumer HERE on the poll thread. KafkaConsumer is
            // not thread-safe; closing it from the user thread (via the runtime's
            // close()) while this thread is still inside commitSync would race the
            // consumer's own thread-safety guard and could leak the underlying
            // socket. By closing on the poll thread we guarantee no concurrent
            // access. The runtime's close() just waits for this thread to finish.
            // Bounded by what's LEFT of the shared deadline after the drain + commit
            // phases above — not a fresh budget of its own.
            RuntimeCleanup.logIfRaised("KafkaConsumer",
                () -> consumer.close(CloseOptions.timeout(remainingBudget(deadlineNanos, CLOSE_FLOOR))));
            if (fatal != null && onFatal != null) {
                // onFatal (ClassicBasicRuntime.handleFatal) performs the equivalent of
                // onLoopExit's cleanup itself (it shares the same doClose() body), plus
                // the FAILED-state transition and the user's onFatalError callback, in
                // that order. Calling onLoopExit here too would be redundant (and would
                // race the state transition — see the class-level note on onFatal).
                Throwable f = fatal;
                RuntimeCleanup.logIfRaised("onFatal callback", () -> onFatal.accept(f));
            } else if (onLoopExit != null) {
                RuntimeCleanup.logIfRaised("onLoopExit callback", onLoopExit::run);
            }
        }
    }

    /**
     * Time remaining until {@code deadlineNanos}, floored at {@code floor} so the broker
     * always sees some attempt even when an earlier phase consumed the entire shutdown
     * budget. Mirrors {@code PollLoop.remainingBudget} for the SHARE engine, generalized
     * to a caller-supplied floor: the final commit floors at {@link #COMMIT_RESERVE}
     * (matching its up-front reservation), the consumer close at {@link #CLOSE_FLOOR}.
     */
    private static Duration remainingBudget(long deadlineNanos, Duration floor) {
        long remainingNanos = deadlineNanos - System.nanoTime();
        if (remainingNanos < floor.toNanos()) return floor;
        return Duration.ofNanos(remainingNanos);
    }

    /**
     * Minimum budget reserved UP FRONT for the final shutdown {@code commitSync}: the
     * drain phase's deadline is shortened by this (plus {@link #CLOSE_FLOOR}) so a drain
     * that exhausts its whole slice can never starve the last commit down to the close
     * floor. 5s is deliberately generous relative to {@code CLOSE_FLOOR} — a failed final
     * commit costs duplicate processing on restart, a slightly shorter drain merely
     * abandons still-running handlers a few seconds earlier (they redeliver either way).
     * For configured drain budgets smaller than {@code COMMIT_RESERVE + CLOSE_FLOOR} the
     * drain slice degenerates to zero and the two floors dominate the total shutdown
     * time, mirroring how {@code CLOSE_FLOOR} already behaved.
     */
    private static final Duration COMMIT_RESERVE = Duration.ofSeconds(5);

    private static final Duration CLOSE_FLOOR = Duration.ofSeconds(2);

    /**
     * Pause assigned partitions when in-flight count saturates; resume when it drains.
     * Heartbeats are preserved because the poll thread still calls {@code poll()} on each
     * iteration — paused partitions just return no records.
     */
    private void applyBackpressure() {
        int n = inFlight.get();
        if (n >= concurrency) {
            // Pause ALL currently-assigned partitions that aren't already paused. This
            // covers two cases:
            //   1. Initial transition (backpressureActive == false): pause everything.
            //   2. Already-active backpressure + rebalance assigned new partitions: pause
            //      them too. Without this catch-up, new partitions would deliver records
            //      while we're already over the concurrency cap, defeating the gate.
            Set<TopicPartition> assignment = consumer.assignment();
            if (assignment.isEmpty()) return;
            Set<TopicPartition> toPause = new HashSet<>(assignment);
            toPause.removeAll(pausedByBackpressure);
            if (toPause.isEmpty()) {
                return;  // already paused everything we own; nothing to do
            }
            consumer.pause(toPause);
            pausedByBackpressure.addAll(toPause);
            if (!backpressureActive) {
                backpressureActive = true;
                metrics.backpressureEvent(topic, BackpressureEvent.PAUSED);
                log.debug("backpressure: paused {} partitions (inFlight={} >= concurrency={})",
                    toPause.size(), n, concurrency);
            } else {
                // Mid-active catch-up — don't fire another "paused" metric event (that's
                // counted per transition, not per partition). Just log so operators can
                // see the new partitions being absorbed.
                log.debug("backpressure: catching up — paused {} newly-assigned partitions "
                    + "(inFlight={}, total paused={})",
                    toPause.size(), n, pausedByBackpressure.size());
            }
        } else if (backpressureActive && n <= concurrency / 2) {
            if (!pausedByBackpressure.isEmpty()) {
                // Resume everything backpressure paused EXCEPT partitions a DLT-publish
                // failure is still holding down. The two pause reasons are independent:
                // a partition paused for both must stay paused until BOTH clear, and only
                // the DLT worker's success (or a revoke) is allowed to lift the DLT pause
                // — never a drop in the in-flight count. applyDltFailurePause tracks those
                // held-down partitions in dltPauseApplied and resumes them when the worker
                // recovers, so excluding them here can't strand them paused forever.
                Set<TopicPartition> toResume = new HashSet<>(pausedByBackpressure);
                pausedByBackpressure.clear();
                toResume.removeAll(pausedByDltFailure);
                if (!toResume.isEmpty()) {
                    consumer.resume(toResume);
                }
            }
            backpressureActive = false;
            log.debug("backpressure: resumed (inFlight={} <= concurrency/2={})", n, concurrency / 2);
            metrics.backpressureEvent(topic, BackpressureEvent.RESUMED);
        }
    }

    /**
     * Reconcile DLT-failure pauses on the poll thread. {@link #pausedByDltFailure} is the
     * desired-paused set written by worker threads; {@link #dltPauseApplied} mirrors what
     * this method has actually paused. The diff drives two actions:
     *
     * <ul>
     *   <li><b>New failures</b> — in {@code pausedByDltFailure} but not yet in
     *       {@code dltPauseApplied}: pause the partition (idempotent even if backpressure
     *       already paused it) and record it as applied.</li>
     *   <li><b>Recoveries</b> — in {@code dltPauseApplied} but no longer in
     *       {@code pausedByDltFailure} (the worker published successfully, or the partition
     *       was revoked): drop the mirror entry and resume the partition, but ONLY if
     *       backpressure isn't still holding it paused and we still own it. Resuming a
     *       backpressure-paused partition here would defeat the backpressure gate.</li>
     * </ul>
     *
     * <p>Only the poll thread calls {@code consumer.pause/resume} — KafkaConsumer is not
     * thread-safe, and workers only ever mutate the concurrent {@code pausedByDltFailure}
     * set, never the consumer.
     */
    private void applyDltFailurePause() {
        if (pausedByDltFailure.isEmpty() && dltPauseApplied.isEmpty()) {
            return;  // fast path — no DLT trouble in progress
        }
        Set<TopicPartition> assignment = consumer.assignment();
        // Pause newly-failed partitions.
        for (TopicPartition tp : pausedByDltFailure) {
            if (assignment.contains(tp) && dltPauseApplied.add(tp)) {
                consumer.pause(List.of(tp));
                log.warn("DLT publish failing for {} — paused partition; a worker is retrying "
                    + "the publish with backoff. Commits hold at the failing offset until it "
                    + "recovers (alert on plurima.consumer.dlt.failures).", tp);
            }
        }
        // Resume partitions whose worker recovered (or that were revoked).
        if (!dltPauseApplied.isEmpty()) {
            Iterator<TopicPartition> it = dltPauseApplied.iterator();
            while (it.hasNext()) {
                TopicPartition tp = it.next();
                if (pausedByDltFailure.contains(tp)) {
                    continue;  // still failing — leave paused
                }
                it.remove();
                if (!pausedByBackpressure.contains(tp) && assignment.contains(tp)) {
                    consumer.resume(List.of(tp));
                    log.info("DLT publish recovered for {} — resumed partition.", tp);
                }
            }
        }
    }

    // Package-private (was private) as a test seam: ClassicDltFailureBackpressureTest
    // dispatches a real batch to drive a worker through processOne → handleExhaustion
    // without spinning up the whole poll thread. Production callers are all in-class.
    void dispatchBatchAsync(ConsumerRecords<byte[], byte[]> batch) {
        for (TopicPartition tp : batch.partitions()) {
            List<ConsumerRecord<byte[], byte[]>> records = batch.records(tp);
            // Register EVERY record's offset with the frontier before dispatching the
            // batch. The frontier matches completions against the offsets that were
            // actually DELIVERED — not against arithmetic successors — because offsets
            // are not dense: compacted topics drop records and read_committed skips
            // transaction markers / aborted batches. Observing only the first offset
            // would leave the frontier waiting forever for a phantom "next" offset
            // that no worker will ever complete, permanently stalling commits.
            //
            // Ordering matters twice over: (a) observe() must see offsets in delivery
            // order (the frontier's delivered-queue relies on it), which batch.records(tp)
            // guarantees; (b) ALL observe() calls must precede dispatch, so a worker's
            // complete() can never arrive for an offset the frontier hasn't seen
            // delivered. Both observe() and this loop run on the poll thread only.
            //
            // We hold the frontier REFERENCE (not just the TopicPartition) and pass it to
            // the dispatcher. Workers carry the reference through to markComplete, which
            // identity-checks against frontiers.get(tp). If revoke + reassign happens
            // mid-flight, a new CommitFrontier instance replaces this one in the map, and
            // any completions from old-generation workers are dropped at the identity check.
            CommitFrontier frontier = frontiers.computeIfAbsent(tp, k -> new CommitFrontier());
            for (ConsumerRecord<byte[], byte[]> r : records) {
                frontier.observe(r.offset());
            }
            inFlight.addAndGet(records.size());
            // dispatch is contractually throw-free: each dispatcher catches its own
            // launcher failures and fires onRecordDone for every record (success OR
            // launch-rejection path) so inFlight stays balanced. A throw here would
            // be a genuine bug — log and continue without rollback, because rolling
            // back the whole batch would double-decrement for any records that DID
            // launch into workers before the throw (see ClassicKeyShardDispatcher.
            // launchNext's per-entry recovery path).
            try {
                dispatcher.dispatch(tp, frontier, records, inFlight::decrementAndGet);
            } catch (Throwable t) {
                log.error("dispatcher.dispatch threw unexpectedly for {} (batch of {} records); "
                    + "this is a bug — inFlight may now be inaccurate",
                    tp, records.size(), t);
            }
        }
    }

    /**
     * Wait for in-flight workers to drain, bounded by the shared close deadline computed
     * once in {@code run()}'s finally block (see the comment there). NOT computing its
     * own fresh {@code shutdownDrainTimeoutMs}-long deadline here is the load-bearing
     * part of the L4 fix: the caller passes the SAME deadline that also governs the
     * final commit and {@code consumer.close()} below it, so this phase can't silently
     * consume a budget larger than the one the runtime's join is waiting on.
     */
    private void shutdownDrain(long deadlineNanos) {
        long startNanos = System.nanoTime();
        while (inFlight.get() > 0 && System.nanoTime() < deadlineNanos) {
            try { Thread.sleep(50); }
            catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        int remaining = inFlight.get();
        long elapsedMs = (System.nanoTime() - startNanos) / 1_000_000L;
        if (remaining > 0) {
            log.warn("shutdown drain window ({}ms elapsed of shutdownDrainTimeout={}ms; the "
                + "tail is reserved for the final commit + consumer close) reached with {} "
                + "record(s) still in flight; abandoning. Records that hadn't yet committed "
                + "will be redelivered to the next owner of their partitions (at-least-once "
                + "duplicate, per-partition ordering preserved).",
                elapsedMs, shutdownDrainTimeoutMs, remaining);
        } else if (elapsedMs > 0) {
            log.info("Shutdown drain completed in {}ms.", elapsedMs);
        }
    }

    /**
     * Process a single record with inline-retry / delayed-retry / DLT semantics.
     * Loops on the worker thread for inline retries and delayed retries (after
     * sleep-and-retry); exits when the record either succeeds, is rejected, exhausts
     * retries, or DLT-routes (success or failure).
     *
     * <p>The {@code frontier} parameter is the per-partition commit-frontier instance
     * captured at dispatch time. It's checked before invoking the listener (early-skip
     * if revoked) and again at every {@link #markComplete} call (identity-check so a
     * stale completion can't pollute a new-generation frontier).
     */
    private void processOne(CommitFrontier frontier, TopicPartition tp, ConsumerRecord<byte[], byte[]> raw) {
        // InFlightRecord is reused from share for the retry-engine signature; the
        // share-specific terminalAckQueued field goes unused here (classic never
        // races force-RELEASE vs worker terminal).
        InFlightRecord<byte[], byte[]> in = new InFlightRecord<>(raw);
        while (running) {
            // Per-attempt ownership re-check. The listener can sleep arbitrarily (retry
            // delays, long handlers); revoke can fire during that window. Checking only
            // ONCE at processOne entry would re-invoke the listener after revoke when a
            // retry loop continues. Re-checking before EACH attempt means an orphan
            // worker stops calling the listener as soon as it notices revoke.
            if (frontiers.get(tp) != frontier) {
                log.debug("Aborting {}@{} (attempt {}): frontier identity changed since dispatch",
                    tp, raw.offset(), in.attempt());
                return;
            }
            Throwable failure = invokeListener(frontier, tp, raw, in);
            if (failure == null) return;                                   // success
            if (!handleProcessingFailure(frontier, tp, raw, in, failure)) return; // terminal
            // else: retry decided — loop, ownership re-check above protects the next call
        }
    }

    /**
     * Run the user listener once and emit the accept-path metrics. Returns {@code null}
     * on success; on failure, returns the thrown cause so the caller can decide
     * retry/reject/DLT. Listener failures are confined to this method — the retry
     * decision logic lives in {@link #handleProcessingFailure}.
     */
    private @Nullable Throwable invokeListener(
        CommitFrontier frontier, TopicPartition tp,
        ConsumerRecord<byte[], byte[]> raw, InFlightRecord<byte[], byte[]> in) {
        long startNanos = System.nanoTime();
        try {
            ConsumerRecord<K, V> typed = deserialize(raw);
            listener.onRecord(typed, new ClassicConsumerContext(in, ordering));
            metrics.recordProcessDuration(raw.topic(), Duration.ofNanos(System.nanoTime() - startNanos));
            metrics.recordsProcessed(raw.topic(), ProcessResult.ACCEPT);
            markComplete(frontier, tp, raw.offset());
            return null;
        } catch (Throwable t) {
            metrics.recordProcessDuration(raw.topic(), Duration.ofNanos(System.nanoTime() - startNanos));
            metrics.recordsFailed(raw.topic(), t.getClass().getName());
            return t;
        }
    }

    /**
     * Decide what happens after a listener failure. Returns {@code true} when the
     * caller should retry (inline or delayed — the sleep happens inside), {@code false}
     * when the record is terminal (rejected, exhausted, or shutting down).
     */
    private boolean handleProcessingFailure(
        CommitFrontier frontier, TopicPartition tp,
        ConsumerRecord<byte[], byte[]> raw, InFlightRecord<byte[], byte[]> in, Throwable cause) {
        RetryDecision decision = retryEngine.evaluate(in, cause);
        return switch (decision) {
            case RetryDecision.RetryInline inline -> retryAfter(inline.delay(), raw, in);
            // Classic delayed retry: sleep on this worker thread. With the continuous-poll
            // model the poll thread keeps heartbeating regardless, so a long sleep doesn't
            // fence the consumer. Backpressure pauses the partitions to prevent new
            // records from piling on; once workers finish, partitions resume.
            case RetryDecision.RetryDelayed delayed -> retryAfter(delayed.delay(), raw, in);
            case RetryDecision.Reject reject -> {
                log.warn("Rejecting non-retriable {}@{} on CLASSIC_BASIC: {}",
                    tp, raw.offset(), reject.cause().toString());
                metrics.recordsProcessed(raw.topic(), ProcessResult.REJECT);
                markComplete(frontier, tp, raw.offset());
                yield false;
            }
            case RetryDecision.Exhausted exh -> {
                handleExhaustion(frontier, tp, in, exh.cause());
                yield false;
            }
        };
    }

    /** Sleep for the retry delay, bump the attempt counter, emit metrics. Returns false on shutdown/interrupt. */
    private boolean retryAfter(Duration delay, ConsumerRecord<byte[], byte[]> raw, InFlightRecord<byte[], byte[]> in) {
        if (!sleepInterruptible(delay)) return false;
        in.incrementAttempt();
        metrics.retryAttempt(raw.topic(), in.attempt());
        return true;
    }

    private void handleExhaustion(
        CommitFrontier frontier, TopicPartition tp, InFlightRecord<byte[], byte[]> in, Throwable cause) {
        ConsumerRecord<byte[], byte[]> raw = in.consumerRecord();
        // Ownership re-check before DLT route. Retry exhaustion may have happened across
        // multiple seconds of retry sleeps; revoke could easily have fired. Routing to
        // DLT after revoke would produce a side-effect (a record published to the DLT
        // topic by an orphan worker) for a record the new owner will redeliver and
        // possibly exhaust into DLT itself. Skip the publish + markComplete entirely.
        if (frontiers.get(tp) != frontier) {
            log.debug("Aborting exhaustion handling for {}@{}: frontier identity changed since dispatch",
                tp, raw.offset());
            return;
        }
        if (dltRouter == null) {
            log.error("Retry exhausted for {}@{} (no DLT configured); committing past (lossy): {}",
                tp, raw.offset(), cause.toString());
            metrics.recordsProcessed(raw.topic(), ProcessResult.REJECT);
            markComplete(frontier, tp, raw.offset());
            return;
        }
        log.warn("Retry exhausted for {}@{}, routing to DLT: {}", tp, raw.offset(), cause.toString());
        if (publishToDlt(tp, in, cause)) {
            completeDltSuccess(frontier, tp, raw);
            return;
        }
        // First DLT publish failed. Rather than leave the poll loop fetching new records
        // for this partition (parking their completions in completedAhead UNBOUNDEDLY while
        // nothing commits during the outage), pause the partition and retry the publish on
        // THIS worker with capped exponential backoff until it recovers, shutdown fires, or
        // the partition is revoked.
        pauseAndRetryDlt(frontier, tp, in, cause);
    }

    /**
     * Attempt a single DLT publish, blocking up to the per-attempt budget. Returns
     * {@code true} on success. On failure the appropriate {@code dltFailed} metric is
     * emitted (via the {@link DltRouter.DltRoute} CAS guard for the caller-side give-up;
     * {@link DltRouter} already emitted it for an async producer failure) and {@code false}
     * is returned so the caller can decide whether to retry.
     *
     * <p>An {@link InterruptedException} while waiting re-sets the thread's interrupt flag
     * and returns {@code false}; the retry loop's next interruptible sleep then observes it
     * and stops — so a shutdown (worker-pool {@code shutdownNow}) unwinds promptly.
     */
    private boolean publishToDlt(TopicPartition tp, InFlightRecord<byte[], byte[]> in, Throwable cause) {
        ConsumerRecord<byte[], byte[]> raw = in.consumerRecord();
        DltRouter.DltRoute route = dltRouter.route(in, cause);
        try {
            route.future().get(dltPublishBudgetMs, TimeUnit.MILLISECONDS);
            return true;
        } catch (java.util.concurrent.TimeoutException | InterruptedException callerSide) {
            // Caller-side give-up. CAS metricEmitted so the eventual producer callback
            // (which may still fire later) does NOT also emit a metric — exactly one
            // outcome per route call, no double-counting and no "dltFailed then dltRouted"
            // sequence under a slow-but-eventually-successful DLT broker.
            if (callerSide instanceof InterruptedException) Thread.currentThread().interrupt();
            if (route.metricEmitted().compareAndSet(false, true)) {
                metrics.dltFailed(raw.topic(), callerSide.getClass().getSimpleName());
            }
            log.error("DLT routing did not complete within budget for {}@{}. Alert on "
                + "plurima.consumer.dlt.failures.", tp, raw.offset(), callerSide);
            return false;
        } catch (Exception ex) {
            // ExecutionException wraps the producer's async failure — DltRouter already
            // CAS-emitted dltFailed inside its callback.
            log.error("DLT routing failed for {}@{}. Alert on plurima.consumer.dlt.failures.",
                tp, raw.offset(), ex);
            return false;
        }
    }

    /** Emit the accept metric and advance the frontier for a successful DLT publish. */
    private void completeDltSuccess(CommitFrontier frontier, TopicPartition tp, ConsumerRecord<byte[], byte[]> raw) {
        metrics.recordsProcessed(raw.topic(), ProcessResult.ACCEPT);
        markComplete(frontier, tp, raw.offset());
    }

    /**
     * Pause the partition (by publishing intent into {@link #pausedByDltFailure} — the poll
     * thread applies the actual {@code consumer.pause} in {@link #applyDltFailurePause}) and
     * retry the DLT publish on THIS worker thread with capped exponential backoff + jitter
     * (1s start, 30s cap by default). The sleep runs on the worker, NEVER the poll thread,
     * so the consumer keeps heartbeating throughout the outage.
     *
     * <h4>Concurrency invariants</h4>
     * <ul>
     *   <li><b>Ownership.</b> Re-checked before every sleep and every publish. A revoke (or
     *       revoke+reassign) swaps the frontier instance in the map; the moment this worker
     *       sees a different instance it stops — WITHOUT advancing the frontier — because the
     *       new owner will redeliver from the last committed offset and re-attempt DLT
     *       itself. Advancing here would falsely commit past a record the new owner still
     *       owes.</li>
     *   <li><b>Shutdown.</b> The loop condition is {@code running}, and every sleep is
     *       interruptible. When {@code running} flips false (or the worker pool interrupts
     *       this thread on {@code shutdownNow}) the loop exits and the frontier is left
     *       unadvanced — the record redelivers after restart, which is correct at-least-once
     *       behaviour.</li>
     *   <li><b>Resume ordering.</b> On success the frontier is advanced FIRST, then this
     *       worker's hold on {@code dltPauseHolds} is released (in the finally below);
     *       the poll thread only resumes the partition after the LAST hold clears, by
     *       which point the committable offset is already in place.</li>
     *   <li><b>Concurrent retryers (refcount).</b> With UNORDERED/KEY ordering several
     *       workers on the same partition can run this loop at once. Each acquires its
     *       own hold on entry and releases it exactly once on exit (success, abort,
     *       shutdown, interrupt — the finally guarantees it); the partition resumes only
     *       when the LAST retryer finishes, never when the first one does. See
     *       {@link #dltPauseHolds} for the full invariant, including why releases are
     *       generation-guarded.</li>
     * </ul>
     */
    private void pauseAndRetryDlt(
        CommitFrontier frontier, TopicPartition tp, InFlightRecord<byte[], byte[]> in, Throwable cause) {
        ConsumerRecord<byte[], byte[]> raw = in.consumerRecord();
        // Signal the poll thread to pause this partition next iteration (refcounted —
        // one hold per retrying worker). Independent of pausedByBackpressure so a
        // backpressure resume can never lift this pause.
        acquireDltPause(tp, frontier);
        log.warn("DLT publish failed for {}@{}; pausing partition and retrying on this worker "
            + "with capped exponential backoff. Frontier WILL NOT advance until the DLT "
            + "recovers.", tp, raw.offset());
        long backoffMs = dltRetryBaseMs;
        try {
            while (running) {
                // Ownership re-check BEFORE sleeping so a revoke that already happened stops us
                // immediately without burning a backoff interval.
                if (frontiers.get(tp) != frontier) {
                    logDltRetryAbort(tp, raw, "before backoff");
                    return;
                }
                if (!sleepInterruptible(Duration.ofMillis(jitteredBackoff(backoffMs)))) {
                    // running flipped false, or the thread was interrupted (pool shutdownNow).
                    // Stop retrying; leave the frontier unadvanced — redeliver on restart.
                    log.warn("DLT retry for {}@{} stopped by shutdown/interrupt; frontier NOT "
                        + "advanced, record redelivers on restart.", tp, raw.offset());
                    return;
                }
                // Ownership can change during the sleep too.
                if (frontiers.get(tp) != frontier) {
                    logDltRetryAbort(tp, raw, "after backoff");
                    return;
                }
                if (publishToDlt(tp, in, cause)) {
                    completeDltSuccess(frontier, tp, raw);
                    log.info("DLT publish recovered for {}@{} after retry; partition will "
                        + "resume once no other worker is still retrying.", tp, raw.offset());
                    return;
                }
                backoffMs = Math.min(backoffMs * 2, dltRetryCapMs);
            }
            // running == false without a successful publish: shutdown between attempts.
            log.warn("DLT retry loop for {}@{} stopped: consumer shutting down. Frontier NOT "
                + "advanced; record redelivers on restart.", tp, raw.offset());
        } finally {
            // Release THIS worker's hold exactly once, on every exit path. Ordering: on
            // the success path this runs AFTER completeDltSuccess, so the frontier holds
            // a committable offset before the poll thread can resume the partition. The
            // release is generation-guarded and decrement-at-zero: it lifts the pause
            // only when no other worker of this generation still holds it, and it
            // no-ops entirely if revoke already cleared the entry (or a newer
            // generation re-acquired it).
            releaseDltPause(tp, frontier);
        }
    }

    /** Common abort logging for the DLT retry loop when the partition is no longer ours. */
    private void logDltRetryAbort(TopicPartition tp, ConsumerRecord<byte[], byte[]> raw, String when) {
        // No pause-hold cleanup here: the revoke path already cleared the partition's
        // entry wholesale, and this worker's own (generation-guarded) release runs in
        // pauseAndRetryDlt's finally regardless.
        log.debug("Aborting DLT retry for {}@{} ({}): frontier identity changed "
            + "(revoked/reassigned); new owner redelivers and re-attempts DLT.",
            tp, raw.offset(), when);
    }

    /**
     * Equal-jitter backoff: half the (capped) delay fixed, half random. Keeps a sensible
     * floor while decorrelating retries across partitions and instances so a recovering
     * DLT broker isn't hit by a synchronized thundering herd.
     */
    private long jitteredBackoff(long backoffMs) {
        long capped = Math.min(backoffMs, dltRetryCapMs);
        if (capped <= 1L) {
            return capped;
        }
        long half = capped / 2L;
        return half + ThreadLocalRandom.current().nextLong(half + 1L);
    }

    private boolean sleepInterruptible(Duration delay) {
        try {
            Thread.sleep(delay.toMillis());
            return running;
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    /**
     * Mark a record's offset as completed in its partition's frontier. Workers pass the
     * frontier reference they captured at dispatch time; we identity-check against the
     * current frontier map and apply the completion only when the references match.
     *
     * <p><b>Revoke + reassign race.</b> Three scenarios this must handle:
     *
     * <ol>
     *   <li>Partition still owned ({@code frontiers.get(tp) == frontier}). Normal path:
     *       apply completion to the frontier.</li>
     *   <li>Partition revoked, not reassigned ({@code frontiers.get(tp) == null}).
     *       Drop silently. The new owner redelivers from the last committed offset.</li>
     *   <li>Partition revoked AND reassigned to us before this completion landed
     *       ({@code frontiers.get(tp) == newFrontier ≠ frontier}). This is the case the
     *       previous "just check null" fix MISSED. The new frontier is for a fresh
     *       batch dispatched on a new generation; if we applied this completion to it,
     *       we would pollute it with an offset from the old generation — and in KEY
     *       mode where completions can be out-of-order, that offset could be HIGHER
     *       than the new generation's nextExpected, falsely advancing the commit past
     *       records the new owner has not yet processed. Drop silently.</li>
     * </ol>
     *
     * <p>Dropping is safe in all the revoke/reassign cases because the new owner of
     * the partition (which may be this consumer's new generation) redelivers from the
     * last successfully committed offset (at-least-once with a duplicate of this
     * record). The orphan worker's onRecordDone — invoked by the dispatcher AFTER
     * processOne returns — still decrements inFlight, so backpressure stays accurate.
     */
    void markComplete(CommitFrontier frontier, TopicPartition tp, long offset) {
        CommitFrontier current = frontiers.get(tp);
        if (current != frontier) {
            log.debug("Dropping completion for {}@{}: frontier generation changed "
                + "(revoked or reassigned since dispatch); new owner redelivers.",
                tp, offset);
            return;
        }
        frontier.complete(offset);
    }

    void drainPendingCommits() {
        // Steady-state path: do NOT include inFlightAsyncCommits. If a slow async
        // callback hasn't fired yet, re-emitting the same offset would spam
        // commitAsync every poll cycle until the broker eventually responds —
        // wasteful broker traffic under coordinator slowness. The in-flight entry
        // is already protecting the data path (shutdown/revoke sync snapshots
        // include it via collectCommitSnapshot(true)); steady drains only need
        // frontier + buffered failures.
        Map<TopicPartition, OffsetAndMetadata> snapshot = collectCommitSnapshot(/* includeInFlightAsync */ false);
        if (snapshot.isEmpty()) return;
        // Record in-flight before commitAsync so a shutdown/revoke racing the
        // callback can still pick up this offset for its commitSync. The merge
        // keeps the higher of any concurrent attempts (callbacks fire on the poll
        // thread so concurrent attempts are sequential, but the merge is cheap
        // and self-documenting).
        for (var e : snapshot.entrySet()) {
            inFlightAsyncCommits.merge(e.getKey(), e.getValue(),
                (oldOff, newOff) -> oldOff.offset() >= newOff.offset() ? oldOff : newOff);
        }
        consumer.commitAsync(snapshot, (offsets, ex) -> {
            // Clear in-flight tracking on EVERY callback — success means the offset
            // is now reflected in committedHighWater; failure routes through
            // handleAsyncCommitFailure into pendingFailedCommits. Either way, the
            // shutdown-merge path no longer needs to consider this entry.
            for (var e : offsets.entrySet()) {
                inFlightAsyncCommits.remove(e.getKey(), e.getValue());
            }
            if (ex != null) {
                handleAsyncCommitFailure(offsets, ex);
            } else {
                handleAsyncCommitSuccess(offsets);
            }
        });
    }

    private void handleAsyncCommitFailure(Map<TopicPartition, OffsetAndMetadata> offsets, Exception ex) {
        for (var e : offsets.entrySet()) {
            TopicPartition tp = e.getKey();
            long failed = e.getValue().offset();
            Long highWater = committedHighWater.get(tp);
            // Regression guard: if a newer commit has ALREADY succeeded for this
            // partition (commit 20 → success → highWater=20, then older commit 10
            // fails or its callback arrives late), DO NOT buffer the stale failure.
            // Re-committing 10 would regress the group offset and produce a
            // duplicate window from 11 to highWater on restart/rebalance.
            if (highWater != null && failed <= highWater) {
                log.debug("Ignoring stale commitAsync failure for {}@{} (highWater={})",
                    tp, failed, highWater);
                continue;
            }
            // Use merge with max so an even-later success callback doesn't overwrite
            // a fresher buffered failure.
            pendingFailedCommits.merge(tp, e.getValue(),
                (oldOff, newOff) -> oldOff.offset() >= newOff.offset() ? oldOff : newOff);
            metrics.ackCommitFailed(tp.topic(), ex.getClass().getSimpleName());
        }
        log.warn("commitAsync failed in ClassicPollLoop ({} partitions); buffered for retry: {}",
            offsets.size(), ex.getMessage());
    }

    private void handleAsyncCommitSuccess(Map<TopicPartition, OffsetAndMetadata> offsets) {
        for (var e : offsets.entrySet()) {
            TopicPartition tp = e.getKey();
            long committed = e.getValue().offset();
            // High-water is monotonic — never decrease (out-of-order success
            // callbacks of older commits must not regress the recorded ceiling).
            committedHighWater.merge(tp, committed, Math::max);
            // Successful commit at offset N supersedes any buffered failure at
            // offset ≤ N. Remove only when the buffered offset is no newer.
            pendingFailedCommits.compute(tp, (k, buffered) ->
                buffered == null || buffered.offset() <= committed ? null : buffered);
            metrics.ackCommitted(tp.topic(), AckOutcome.ACCEPT);
        }
    }

    /**
     * Synchronous variant of {@link #drainPendingCommits} used on the final shutdown
     * drain, using the client's own default commit timeout. {@code commitAsync}'s
     * callback may not fire before the consumer closes, which is exactly when a
     * transient failure becomes "the last commit before shutdown" and turns into
     * avoidable duplicates on restart. commitSync blocks until the broker confirms so we
     * know for certain whether the offsets landed.
     *
     * <p>Visible for testing — exercised directly by {@code ClassicPollLoopCommitRetryTest}
     * without a shared shutdown deadline in play. Production shutdown goes through
     * {@link #drainPendingCommitsSync(Duration)} instead, so the final commit shares the
     * same deadline as the drain phase before it and {@code consumer.close()} after it.
     */
    void drainPendingCommitsSync() {
        drainPendingCommitsSync(null);
    }

    /**
     * Deadline-bound variant used by {@code run()}'s shutdown finally block: {@code budget}
     * is the time remaining on the shared close deadline (see that method's comment) after
     * {@link #shutdownDrain}, so the final commit can't independently consume the client's
     * unbounded default timeout on top of an already-exhausted drain phase. {@code null}
     * preserves the no-arg overload's original unbounded-default behavior.
     */
    void drainPendingCommitsSync(Duration budget) {
        // Shutdown path: include in-flight async commits. This is the consumer's
        // last chance to confirm an offset whose async callback hasn't fired yet;
        // skipping it would mean restart redelivers records the consumer had
        // already processed.
        Map<TopicPartition, OffsetAndMetadata> snapshot = collectCommitSnapshot(/* includeInFlightAsync */ true);
        if (snapshot.isEmpty()) return;
        try {
            // shutdown() sets running=false THEN calls consumer.wakeup(). If the poll
            // thread is between polls at that moment (already past its last poll(),
            // now running the shutdown drain), the armed wakeup has nowhere to land
            // until the next blocking consumer call — which is this commitSync. That
            // throws WakeupException, and without special handling it would fall into
            // the generic catch below and be treated as a genuine commit failure,
            // silently dropping the last batch of offsets. wakeup() only arms a
            // single interrupt (KafkaConsumer clears the flag when it's consumed), so
            // retrying exactly once is guaranteed to get a real attempt through with
            // no armed wakeup left to interrupt it.
            try {
                commitSyncBounded(snapshot, budget);
            } catch (WakeupException wakeup) {
                log.debug("armed shutdown wakeup consumed before final commit", wakeup);
                commitSyncBounded(snapshot, budget);
            }
            for (var e : snapshot.entrySet()) {
                TopicPartition tp = e.getKey();
                committedHighWater.merge(tp, e.getValue().offset(), Math::max);
                pendingFailedCommits.remove(tp);
                // Sync commit confirms the offset — any in-flight async tracking for
                // this partition is now resolved (the async callback may still fire
                // later, but its outcome is irrelevant since the sync has authority).
                inFlightAsyncCommits.remove(tp);
                metrics.ackCommitted(tp.topic(), AckOutcome.ACCEPT);
            }
        } catch (Exception ex) {
            // Final-shutdown commitSync failure: the partition's last committed offset
            // stays where the broker left it; restart redelivers from there with
            // at-least-once duplicates. Log loudly so operators can alert. This also
            // catches a (theoretically impossible) second WakeupException from the
            // retry above, falling back to the same generic handling as any other
            // commit failure.
            log.error("Final commitSync at shutdown FAILED for {}; partition will redeliver "
                + "from the last successfully-committed offset on restart (at-least-once "
                + "duplicate). Cause: {}", snapshot.keySet(), ex.toString());
            for (TopicPartition tp : snapshot.keySet()) {
                metrics.ackCommitFailed(tp.topic(), ex.getClass().getSimpleName());
            }
        }
    }

    /**
     * Issues the final commitSync, using the client's default timeout when {@code budget}
     * is {@code null} (the no-arg {@link #drainPendingCommitsSync()} test entry point) or
     * the explicit shared-deadline remainder otherwise (production shutdown).
     */
    private void commitSyncBounded(Map<TopicPartition, OffsetAndMetadata> snapshot, Duration budget) {
        if (budget != null) {
            consumer.commitSync(snapshot, budget);
        } else {
            consumer.commitSync(snapshot);
        }
    }

    /**
     * Build the next commit snapshot by combining sources, newer always winning
     * per partition. Always-included sources:
     * <ol>
     *   <li>Live frontier drain ({@code drainCommittable}) — the normal path.</li>
     *   <li>{@link #pendingFailedCommits} — async failures we still need to retry.</li>
     * </ol>
     * Opt-in via {@code includeInFlightAsync}:
     * <ol start="3">
     *   <li>{@link #inFlightAsyncCommits} — offsets currently committed via
     *       commitAsync whose callback hasn't fired yet. Only included on
     *       shutdown/revoke sync paths; including on steady-state drains
     *       would re-issue the same offset every poll cycle and spam the
     *       broker under coordinator slowness.</li>
     * </ol>
     * All sources are filtered by {@link #committedHighWater} — anything ≤ the
     * recorded high-water is stale and dropped (regression guard).
     */
    private Map<TopicPartition, OffsetAndMetadata> collectCommitSnapshot(boolean includeInFlightAsync) {
        Map<TopicPartition, OffsetAndMetadata> snapshot = new HashMap<>();
        for (var entry : frontiers.entrySet()) {
            OptionalLong commit = entry.getValue().drainCommittable();
            commit.ifPresent(off -> snapshot.put(entry.getKey(), new OffsetAndMetadata(off)));
        }
        mergeFilteredByHighWater(snapshot, pendingFailedCommits, /* dropStale */ true);
        if (includeInFlightAsync) {
            // DO NOT remove stale entries from inFlightAsyncCommits here — the
            // async callback owns its lifecycle. If the callback arrives later
            // with success it'll clear the entry then; if with failure it'll
            // move it into pendingFailedCommits where the next cycle handles it.
            mergeFilteredByHighWater(snapshot, inFlightAsyncCommits, /* dropStale */ false);
        }
        return snapshot;
    }

    private void mergeFilteredByHighWater(
        Map<TopicPartition, OffsetAndMetadata> snapshot,
        ConcurrentMap<TopicPartition, OffsetAndMetadata> source,
        boolean dropStale) {
        for (var entry : source.entrySet()) {
            TopicPartition tp = entry.getKey();
            OffsetAndMetadata candidate = entry.getValue();
            Long highWater = committedHighWater.get(tp);
            if (highWater != null && candidate.offset() <= highWater) {
                if (dropStale) source.remove(tp, candidate);
                continue;
            }
            snapshot.merge(tp, candidate,
                (live, buf) -> live.offset() >= buf.offset() ? live : buf);
        }
    }

    @SuppressWarnings("unchecked")
    private ConsumerRecord<K, V> deserialize(ConsumerRecord<byte[], byte[]> raw) {
        // Fast path: identity bytes-in/bytes-out. The user took the default
        // RecordDeserializer.bytes() for both key and value, so K == V == byte[].
        // Building a new ConsumerRecord just to copy the same byte[] references is
        // wasted allocation on the worker hot path; the cast is safe because both
        // type parameters resolve to byte[].
        if (identityDeserializers) {
            return (ConsumerRecord<K, V>) raw;
        }
        K key = keyDeser.deserialize(raw.topic(), raw.key());
        V value = valueDeser.deserialize(raw.topic(), raw.value());
        return new ConsumerRecord<>(
            raw.topic(),
            raw.partition(),
            raw.offset(),
            raw.timestamp(),
            raw.timestampType(),
            raw.serializedKeySize(),
            raw.serializedValueSize(),
            key,
            value,
            raw.headers(),
            raw.leaderEpoch(),
            raw.deliveryCount());
    }

    /**
     * Called from a caller thread (not the poll thread) to request shutdown.
     * Setting {@code running = false} first means the poll thread's own
     * {@code while (running)} check can exit cleanly if it's between iterations;
     * {@code wakeup()} then interrupts a {@code poll()} that's already blocked. But
     * these two steps race with where the poll thread currently is: if it's already
     * PAST its last poll and into the shutdown drain, the wakeup has nothing to
     * interrupt there and arms itself for the next blocking consumer call instead
     * — which is the final {@code commitSync} in {@link #drainPendingCommitsSync}.
     * That method retries once specifically to absorb this race; see its comment.
     */
    void shutdown() {
        running = false;
        consumer.wakeup();
    }

    @Override
    public void close() {
        shutdown();
    }

    /**
     * Current in-flight record count. Used by {@link ClassicBasicRuntime} to back the
     * {@code plurima.consumer.records.in_flight} gauge, and by tests to assert the
     * backpressure counter is balanced after various scenarios.
     */
    int inFlightCount() {
        return inFlight.get();
    }

    /** Test/diagnostic — exposes the backpressure flag. */
    boolean isBackpressureActive() {
        return backpressureActive;
    }

    /** Test/diagnostic — true if the partition has a live commit frontier. */
    boolean hasFrontier(TopicPartition tp) {
        return frontiers.containsKey(tp);
    }

    /** Test/diagnostic — true if a DLT-publish failure is currently pausing the partition. */
    boolean isPausedByDltFailure(TopicPartition tp) {
        return pausedByDltFailure.contains(tp);
    }

    /**
     * Test-only — shrink the DLT-retry backoff so tests exercise the failing-then-
     * succeeding retry loop in milliseconds rather than seconds. Never called in
     * production; the {@code volatile} fields default to 1s/30s per design.
     */
    void setDltRetryBackoffForTest(long baseMs, long capMs) {
        this.dltRetryBaseMs = baseMs;
        this.dltRetryCapMs = capMs;
    }

    /**
     * Test-only — shrink {@link #dltPublishBudgetMs} so a DLT producer that never
     * completes a send hits the caller-side timeout path in {@link #publishToDlt} in
     * milliseconds rather than the production 30s. Never called in production.
     */
    void setDltPublishBudgetForTest(long budgetMs) {
        this.dltPublishBudgetMs = budgetMs;
    }

    /**
     * Test-only — drives {@link #acquireDltPause} directly. In production the acquire
     * call sits immediately after {@code handleExhaustion}'s ownership check with no
     * injectable delay, so the stale-generation-straggler race it guards against (a
     * worker descheduled between that check and this call, waking after reassignment)
     * is a single-instruction window that real concurrency can't be relied on to hit
     * deterministically. This seam lets tests call it directly with an arbitrary
     * (possibly non-live) frontier to exercise the generation guard itself.
     */
    void acquireDltPauseForTest(TopicPartition tp, CommitFrontier frontier) {
        acquireDltPause(tp, frontier);
    }

    /** Test-only — drives {@link #releaseDltPause} directly; see {@link #acquireDltPauseForTest}. */
    void releaseDltPauseForTest(TopicPartition tp, CommitFrontier frontier) {
        releaseDltPause(tp, frontier);
    }

    /** Test/diagnostic — returns the current frontier reference (or null). */
    @Nullable CommitFrontier frontier(TopicPartition tp) {
        return frontiers.get(tp);
    }

    /**
     * Test/diagnostic — installs a frontier pinned at {@code firstOffset}. Unlike
     * {@code dispatchBatchAsync}, this observes only the FIRST offset; tests that
     * complete a dense range from there exercise the frontier's defensive dense
     * fallback (see {@link CommitFrontier#observe}).
     */
    CommitFrontier installFrontier(TopicPartition tp, long firstOffset) {
        CommitFrontier f = frontiers.computeIfAbsent(tp, k -> new CommitFrontier());
        f.observe(firstOffset);
        return f;
    }

    /** Test/diagnostic — simulates the rebalance revoke handler without a broker. */
    void simulateRevoke(Collection<TopicPartition> partitions) {
        // Mirror onPartitionsRevoked's frontier + shard-purge work, but skip the
        // commitSync (no real consumer in tests).
        for (TopicPartition tp : partitions) {
            frontiers.remove(tp);
        }
        pausedByDltFailure.removeAll(partitions);
        if (keyShardDispatcher != null) {
            keyShardDispatcher.purgePartitions(partitions);
        }
    }

    /**
     * Rebalance hook for the classic engine. Delegates to {@link ClassicRebalanceListener}
     * which handles the revoke-time commit + state cleanup. Visible for use by
     * {@link ClassicBasicRuntime} when calling {@code consumer.subscribe(topic, listener)}.
     */
    ConsumerRebalanceListener rebalanceListener() {
        return new ClassicRebalanceListener(
            consumer, frontiers, keyShardDispatcher,
            pausedByBackpressure,
            pausedByDltFailure,
            () -> { if (pausedByBackpressure.isEmpty()) backpressureActive = false; },
            metrics,
            pendingFailedCommits,
            committedHighWater,
            inFlightAsyncCommits);
    }

}
