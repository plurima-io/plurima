package io.plurima.kafka.internal;

import io.plurima.kafka.OrderingMode;
import io.plurima.kafka.RecordListener;
import io.plurima.kafka.annotation.Internal;
import io.plurima.kafka.deserializer.RecordDeserializer;
import io.plurima.kafka.metrics.PlurimaMetrics;
import io.plurima.kafka.retry.RetryDecision;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiPredicate;

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
 *       failing offset until DLT is healthy; restart redelivers). Alert on
 *       {@code plurima.consumer.dlt.failures}.</li>
 *   <li>Exhausted, no DLT → log ERROR + frontier.complete (lossy by configuration —
 *       record dropped, {@code plurima.consumer.records.processed{result=reject}}).</li>
 * </ul>
 */
@Internal
final class ClassicPollLoop<K, V> implements Runnable, AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(ClassicPollLoop.class);

    private final KafkaConsumer<byte[], byte[]> consumer;
    private final String topic;
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
        this.consumer = consumer;
        this.topic = topic;
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
            // PARTITION dispatches a single worker per partition's batch; records
            // process in offset order within the partition. Cross-cluster per-partition
            // FIFO comes from classic consumer-group exclusive partition ownership.
            case PARTITION -> new ClassicPartitionSerialDispatcher(
                launcher, this::processOne, () -> running, stillOwned);
        };
    }

    @Override
    public void run() {
        try {
            while (running) {
                applyBackpressure();

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
                    Duration.ofNanos(System.nanoTime() - pollStartNanos));

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
        } finally {
            // Shutdown drain: wait up to shutdownDrainTimeout for in-flight workers to
            // finish so their offsets can commit. Handlers within the drain budget land
            // their commits; handlers exceeding it get abandoned with a WARN and their
            // records redeliver to the next owner of the partitions.
            shutdownDrain();
            // SYNC commit on shutdown so a transient broker hiccup on the FINAL commit
            // can't silently leave the last batch unpersisted and cause avoidable
            // duplicates on restart. drainPendingCommits (async) is fine for steady-
            // state — newer commits subsume older — but the last call has nothing
            // following to subsume a failed earlier attempt.
            drainPendingCommitsSync();
            // CRITICAL: close the consumer HERE on the poll thread. KafkaConsumer is
            // not thread-safe; closing it from the user thread (via the runtime's
            // close()) while this thread is still inside commitSync would race the
            // consumer's own thread-safety guard and could leak the underlying
            // socket. By closing on the poll thread we guarantee no concurrent
            // access. The runtime's close() just waits for this thread to finish.
            RuntimeCleanup.logIfRaised("KafkaConsumer", consumer::close);
            if (onLoopExit != null) RuntimeCleanup.logIfRaised("onLoopExit callback", onLoopExit::run);
        }
    }

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
                metrics.backpressureEvent(topic, "paused");
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
                consumer.resume(pausedByBackpressure);
                pausedByBackpressure.clear();
            }
            backpressureActive = false;
            log.debug("backpressure: resumed (inFlight={} <= concurrency/2={})", n, concurrency / 2);
            metrics.backpressureEvent(topic, "resumed");
        }
    }

    private void dispatchBatchAsync(ConsumerRecords<byte[], byte[]> batch) {
        for (TopicPartition tp : batch.partitions()) {
            List<ConsumerRecord<byte[], byte[]>> records = batch.records(tp);
            // Pin the frontier's starting offset to the first record we're about to dispatch
            // for this partition. Without observe(), a partition that's never seen a record
            // before would have an uninitialised frontier; the first complete() call would
            // initialise it to the just-completed offset, which is correct only when there
            // are no gaps below — true in classic-consumer batch ordering, but observe() is
            // the explicit contract.
            //
            // We hold the frontier REFERENCE (not just the TopicPartition) and pass it to
            // the dispatcher. Workers carry the reference through to markComplete, which
            // identity-checks against frontiers.get(tp). If revoke + reassign happens
            // mid-flight, a new CommitFrontier instance replaces this one in the map, and
            // any completions from old-generation workers are dropped at the identity check.
            CommitFrontier frontier = frontiers.computeIfAbsent(tp, k -> new CommitFrontier());
            frontier.observe(records.get(0).offset());
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

    private void shutdownDrain() {
        long startNanos = System.nanoTime();
        long deadlineNanos = startNanos + shutdownDrainTimeoutMs * 1_000_000L;
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
            log.warn("shutdownDrainTimeout={}ms reached with {} record(s) still in flight; "
                + "abandoning. Records that hadn't yet committed will be redelivered to the "
                + "next owner of their partitions (at-least-once duplicate, per-partition "
                + "ordering preserved).", shutdownDrainTimeoutMs, remaining);
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
    private Throwable invokeListener(
        CommitFrontier frontier, TopicPartition tp,
        ConsumerRecord<byte[], byte[]> raw, InFlightRecord<byte[], byte[]> in) {
        long startNanos = System.nanoTime();
        try {
            ConsumerRecord<K, V> typed = deserialize(raw);
            listener.onRecord(typed, new ClassicConsumerContext(in, ordering));
            metrics.recordProcessDuration(raw.topic(), Duration.ofNanos(System.nanoTime() - startNanos));
            metrics.recordsProcessed(raw.topic(), "accept");
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
                metrics.recordsProcessed(raw.topic(), "reject");
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
            metrics.recordsProcessed(raw.topic(), "reject");
            markComplete(frontier, tp, raw.offset());
            return;
        }
        log.warn("Retry exhausted for {}@{}, routing to DLT: {}", tp, raw.offset(), cause.toString());
        DltRouter.DltRoute route = dltRouter.route(in, cause);
        try {
            route.future().get(30, TimeUnit.SECONDS);
            metrics.recordsProcessed(raw.topic(), "accept");
            markComplete(frontier, tp, raw.offset());
        } catch (java.util.concurrent.TimeoutException | InterruptedException callerSide) {
            // Caller-side give-up. CAS metricEmitted so the eventual producer callback
            // (which may still fire later) does NOT also emit a metric — exactly one
            // outcome per route call, no double-counting and no "dltFailed then
            // dltRouted" sequence under a slow-but-eventually-successful DLT broker.
            if (callerSide instanceof InterruptedException) Thread.currentThread().interrupt();
            if (route.metricEmitted().compareAndSet(false, true)) {
                metrics.dltFailed(raw.topic(), callerSide.getClass().getSimpleName());
            }
            log.error("DLT routing did not complete within budget for {}@{}; frontier "
                + "WILL NOT advance — restart redelivers once DLT is healthy. Alert on "
                + "plurima.consumer.dlt.failures.", tp, raw.offset(), callerSide);
        } catch (Exception ex) {
            // ExecutionException wraps the producer's async failure — DltRouter already
            // CAS-emitted dltFailed inside its callback. Same outcome: do NOT markComplete.
            // The frontier stalls at this offset; later records on the same partition
            // complete into completedAhead but never advance the commit past this gap.
            // On the next restart (or rebalance), the broker redelivers from the last
            // committed offset and we try DLT again. Worker accounting still happens via
            // the dispatcher's onRecordDone in its own finally block, so inFlight stays
            // balanced and backpressure recovers.
            log.error("DLT routing failed for {}@{}; frontier WILL NOT advance — restart "
                + "redelivers from this offset once DLT is healthy. Alert on "
                + "plurima.consumer.dlt.failures.", tp, raw.offset(), ex);
        }
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
            metrics.ackCommitted(tp.topic(), "accept");
        }
    }

    /**
     * Synchronous variant of {@link #drainPendingCommits} used on the final shutdown
     * drain. {@code commitAsync}'s callback may not fire before the consumer closes,
     * which is exactly when a transient failure becomes "the last commit before
     * shutdown" and turns into avoidable duplicates on restart. commitSync blocks
     * until the broker confirms — bounded by the consumer's default timeout — so we
     * know for certain whether the offsets landed.
     */
    private void drainPendingCommitsSync() {
        // Shutdown path: include in-flight async commits. This is the consumer's
        // last chance to confirm an offset whose async callback hasn't fired yet;
        // skipping it would mean restart redelivers records the consumer had
        // already processed.
        Map<TopicPartition, OffsetAndMetadata> snapshot = collectCommitSnapshot(/* includeInFlightAsync */ true);
        if (snapshot.isEmpty()) return;
        try {
            consumer.commitSync(snapshot);
            for (var e : snapshot.entrySet()) {
                TopicPartition tp = e.getKey();
                committedHighWater.merge(tp, e.getValue().offset(), Math::max);
                pendingFailedCommits.remove(tp);
                // Sync commit confirms the offset — any in-flight async tracking for
                // this partition is now resolved (the async callback may still fire
                // later, but its outcome is irrelevant since the sync has authority).
                inFlightAsyncCommits.remove(tp);
                metrics.ackCommitted(tp.topic(), "accept");
            }
        } catch (Exception ex) {
            // Final-shutdown commitSync failure: the partition's last committed offset
            // stays where the broker left it; restart redelivers from there with
            // at-least-once duplicates. Log loudly so operators can alert.
            log.error("Final commitSync at shutdown FAILED for {}; partition will redeliver "
                + "from the last successfully-committed offset on restart (at-least-once "
                + "duplicate). Cause: {}", snapshot.keySet(), ex.toString());
            for (TopicPartition tp : snapshot.keySet()) {
                metrics.ackCommitFailed(tp.topic(), ex.getClass().getSimpleName());
            }
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

    /** Test/diagnostic — returns the current frontier reference (or null). */
    CommitFrontier frontier(TopicPartition tp) {
        return frontiers.get(tp);
    }

    /** Test/diagnostic — installs a frontier as if dispatchBatchAsync had run. */
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
            () -> { if (pausedByBackpressure.isEmpty()) backpressureActive = false; },
            metrics,
            pendingFailedCommits,
            committedHighWater,
            inFlightAsyncCommits);
    }

}
