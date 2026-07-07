package io.plurima.kafka.internal;

import io.plurima.kafka.annotation.Internal;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiPredicate;
import java.util.function.BooleanSupplier;

/**
 * Partition-serial dispatch: one worker chain per partition processes that partition's
 * records in offset order across ALL poll batches. Used by the {@code PARTITION}
 * ordering mode.
 *
 * <p>Same-partition records flow through one persistent chain, so cross-cluster
 * per-partition FIFO holds when combined with classic consumer-group exclusive
 * partition ownership. For UNORDERED workloads that don't need any FIFO contract, see
 * {@link ClassicUnorderedDispatcher} which launches each record into its own worker
 * for higher intra-partition throughput.
 *
 * <h3>Why a persistent chain and not one worker per batch</h3>
 * The pre-fix design launched a NEW worker for every {@code dispatch(tp, ...)} call.
 * The poll loop can dispatch a second batch for the same partition while the first
 * batch's worker is still running (e.g. blocked in a slow listener), so two workers
 * for the same partition ran concurrently — silently violating per-partition FIFO,
 * the entire point of PARTITION mode. Global backpressure only masked it at
 * saturation. The fix mirrors {@link ClassicKeyShardDispatcher}: a persistent
 * per-partition chain that queues batch entries and drains them through a single
 * worker at a time, so overlapping batches for one partition serialize while
 * distinct partitions still run concurrently.
 *
 * <h3>Concurrency model — lock-free</h3>
 * Each partition holds a {@link ConcurrentLinkedQueue} (FIFO, lock-free) and an
 * {@link AtomicBoolean} "busy" flag. Dispatch (single-producer, the poll thread) for
 * each record:
 * <ol>
 *   <li>Append the record's {@link Entry} to the partition's queue. Records are added
 *       in offset order (the poll thread hands us {@code batch.records(tp)} in offset
 *       order), and {@code ConcurrentLinkedQueue} preserves FIFO, so the single worker
 *       drains them in offset order — the per-partition FIFO invariant, now upheld
 *       across batch boundaries too.</li>
 *   <li>CAS {@code busy} {@code false → true}. On success this producer owns the chain
 *       and calls {@link #launchNext}. On failure a worker (or another producer) is
 *       already draining and will pick up this entry on its next {@code poll()}.</li>
 * </ol>
 * On completion the worker calls {@link #launchNext} again: poll the next entry and
 * launch a fresh worker for it, or release {@code busy} and double-check the queue for
 * the producer-vs-release race. This is the standard publisher / single-consumer
 * lock-free chain pattern, mirroring {@link ClassicKeyShardDispatcher} exactly —
 * including its iterative (stack-safe) {@code launchNext} drain loop.
 *
 * <p><b>Generation guard.</b> Each {@link Entry} carries the {@link CommitFrontier}
 * reference captured at dispatch time; before processing every record the worker checks
 * {@code stillOwned.test(tp, frontier)}. If the partition was revoked (or revoked +
 * reassigned, which installs a NEW frontier for the same TopicPartition), the worker
 * skips the remaining records — they would be re-processed by the new owner anyway, and
 * continuing to invoke the listener risks double-side-effects. We still fire
 * {@code onRecordDone} for each skipped record to keep the backpressure counter
 * balanced.
 *
 * <p><b>No purge hook on revoke.</b> Unlike {@link ClassicKeyShardDispatcher} — whose
 * worker has no per-record ownership re-check and therefore MUST have its queued entries
 * purged on revoke to avoid processing records the new owner will redeliver — this
 * dispatcher re-checks {@code stillOwned} on every record. When a partition is revoked
 * the frontier is removed from the poll loop's map, so {@code stillOwned} returns false
 * for every still-queued entry: the draining worker skips processing and only fires
 * {@code onRecordDone}, keeping {@code inFlight} balanced. Queued entries thus drain
 * naturally; no {@code purgePartitions} hook is needed or wired for PARTITION mode.
 */
@Internal
final class ClassicPartitionSerialDispatcher implements ClassicDispatcher {

    private static final Logger log = LoggerFactory.getLogger(ClassicPartitionSerialDispatcher.class);

    private final WorkerLauncher launcher;
    private final ClassicRecordProcessor processor;
    private final BooleanSupplier running;
    private final BiPredicate<TopicPartition, CommitFrontier> stillOwned;
    private final ConcurrentMap<TopicPartition, PartitionChain> chains = new ConcurrentHashMap<>();

    ClassicPartitionSerialDispatcher(
        WorkerLauncher launcher,
        ClassicRecordProcessor processor,
        BooleanSupplier running,
        BiPredicate<TopicPartition, CommitFrontier> stillOwned) {
        this.launcher = launcher;
        this.processor = processor;
        this.running = running;
        this.stillOwned = stillOwned;
    }

    @Override
    public void dispatch(
        TopicPartition tp,
        CommitFrontier frontier,
        List<ConsumerRecord<byte[], byte[]>> records,
        Runnable onRecordDone) {
        // Single-producer (poll thread): append each record to the partition's chain in
        // offset order, then try to claim the worker chain. If the CAS fails, a worker
        // (from this or a prior batch) is already draining and will pick up our entry on
        // its next poll() — this is exactly what serializes overlapping batches for the
        // same partition. dispatch never blocks on worker completion (interface contract).
        PartitionChain chain = chains.computeIfAbsent(tp, PartitionChain::new);
        for (ConsumerRecord<byte[], byte[]> raw : records) {
            chain.queue.add(new Entry(raw, frontier, onRecordDone));
            if (chain.busy.compareAndSet(false, true)) {
                launchNext(chain);
            }
        }
    }

    /**
     * Pop the next entry and launch a worker for it. If the queue is empty, release
     * {@code busy} and double-check for the producer-vs-release race: a producer that
     * adds between our {@code poll() == null} and {@code busy.set(false)} would have
     * seen {@code busy == true} and not launched anything, so the queue would stall.
     * After releasing, peek the queue; if non-empty, try to reclaim {@code busy} and
     * resume the chain.
     *
     * <p>Iterative, not recursive — the same shape as {@link ClassicKeyShardDispatcher}'s
     * {@code launchNext}: both the busy-release double-check and the per-entry
     * launch-rejection recovery (see the catch below) loop back to the top instead of
     * calling {@code launchNext} again. A deep queue drained entirely through the
     * rejection path — e.g. every launch failing during shutdown — would otherwise
     * recurse once per queued entry on a single thread, an unbounded stack-depth hazard.
     * The loop makes that drain O(1) in stack depth regardless of queue size, with
     * identical behavior on every path: {@code onRecordDone} still fires exactly once
     * per record.
     */
    private void launchNext(PartitionChain chain) {
        while (true) {
            Entry next = chain.queue.poll();
            if (next == null) {
                chain.busy.set(false);
                if (chain.queue.isEmpty() || !chain.busy.compareAndSet(false, true)) {
                    return;
                }
                // Reclaimed busy after the double-check race — loop back and poll
                // again instead of recursing.
                continue;
            }
            try {
                launcher.launch(() -> processThenAdvance(chain, next));
                return;
            } catch (Throwable t) {
                // Launcher rejected the task (typically RejectedExecutionException on
                // shutdown or worker pool saturation). We already polled the Entry from
                // the queue; if we returned here without further action the entry would
                // be lost AND its onRecordDone would never fire — ClassicPollLoop's
                // inFlight counter would stay too high and backpressure would lock up.
                //
                // Recovery (mirrors ClassicKeyShardDispatcher.launchNext):
                //   1. Fire onRecordDone for THIS entry only (decrements inFlight by 1).
                //   2. Loop back to try the next queued entry. If the next launch also
                //      fails we keep looping; eventually the queue drains and busy is
                //      released. Bounded by queue size, but iteratively — not via
                //      recursion — so depth never grows with the backlog.
                //
                // We deliberately do NOT throw — the dispatch caller (ClassicPollLoop)
                // must NOT roll back inFlight for the whole batch, because earlier
                // entries that DID launch into workers will fire their own onRecordDone.
                log.error("Worker launch rejected for {} (likely shutdown or pool saturation); "
                    + "dropping record and continuing with next queued entry", chain.tp, t);
                next.onRecordDone().run();
                // fall through to the top of the loop to try the next entry
            }
        }
    }

    private void processThenAdvance(PartitionChain chain, Entry entry) {
        try {
            if (!running.getAsBoolean()) {
                // Loop is shutting down — skip processing but still fire onRecordDone
                // below so the batch-completion accounting (inFlight) doesn't deadlock.
                return;
            }
            if (!stillOwned.test(chain.tp, entry.frontier())) {
                // Partition was revoked (or revoked + reassigned, which installs a NEW
                // frontier instance) since dispatch. Skip processing — the new owner
                // redelivers from the last committed offset. Per-record check means we
                // stop AS SOON AS revoke is detected; remaining queued entries for this
                // partition also fail this check and drain here (see class javadoc on
                // "No purge hook on revoke").
                return;
            }
            try {
                processor.process(entry.frontier(), chain.tp, entry.raw());
            } catch (Throwable t) {
                log.error("processOne threw for {}@{}; skipping record and continuing chain",
                    chain.tp, entry.raw().offset(), t);
            }
        } finally {
            // onRecordDone fires exactly once for EVERY record on EVERY path (success,
            // shutdown skip, revoke skip, listener throw), keeping inFlight balanced.
            entry.onRecordDone().run();
            // Advance the chain. launchNext handles its own launcher failures internally,
            // so this try/catch is purely belt-and-suspenders against an unexpected
            // exception in the queue-poll / busy bookkeeping above launcher.launch.
            try {
                launchNext(chain);
            } catch (Throwable t) {
                chain.busy.set(false);
                throw t;
            }
        }
    }

    /** Test/diagnostic — number of partitions with a live chain. */
    int activeChainCount() {
        return chains.size();
    }

    private record Entry(
        ConsumerRecord<byte[], byte[]> raw,
        CommitFrontier frontier,
        Runnable onRecordDone) {}

    private static final class PartitionChain {
        final TopicPartition tp;
        final ConcurrentLinkedQueue<Entry> queue = new ConcurrentLinkedQueue<>();
        final AtomicBoolean busy = new AtomicBoolean();

        PartitionChain(TopicPartition tp) {
            this.tp = tp;
        }
    }
}
