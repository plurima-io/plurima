package io.plurima.kafka.internal;

import io.plurima.kafka.annotation.Internal;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;

/**
 * Intra-partition key-shard dispatch for KEY ordering mode on the CLASSIC_BASIC engine.
 *
 * <p>Records are sharded by {@code (TopicPartition, hash(key) % shardCount)}. Records
 * mapped to the same shard run serially in offset order; distinct shards run
 * concurrently. The result is per-key FIFO with intra-partition parallelism — the
 * pattern Confluent Parallel Consumer's {@code ProcessingOrder.KEY} delivers.
 *
 * <p><b>Why the shard key includes the partition.</b> With Kafka's default key-aware
 * partitioner, same-key records always land on the same partition, so within any
 * single consumer's assignment, "(partition, key-hash)" and "(topic, key-hash)" produce
 * the same shard layout. Including the partition makes the structure trivially
 * partition-scoped: revoking a partition lets us drop its shards without touching others.
 *
 * <p><b>What about a custom non-key-aware partitioner?</b> If a producer routes same-key
 * records to different partitions, KEY mode no longer guarantees cross-cluster per-key
 * FIFO — that's a producer-side contract violation Plurima cannot mask. The user guide
 * documents KEY mode requires the default partitioner.
 *
 * <h3>Concurrency model — lock-free</h3>
 * Each shard holds a {@link ConcurrentLinkedQueue} (FIFO, lock-free) and an
 * {@link AtomicBoolean} "busy" flag. Dispatch:
 * <ol>
 *   <li>Append the entry to the shard's queue.</li>
 *   <li>CAS {@code busy} from {@code false} to {@code true}. On success, this producer
 *       owns the shard's worker chain and calls {@link #launchNext}. On failure, a
 *       previously-running worker is already draining; it will see this entry on its
 *       next {@code poll()}.</li>
 * </ol>
 * On completion, the worker calls {@link #launchNext} again — same flow: poll the next
 * entry and launch a fresh worker for it, or set {@code busy = false} and double-check
 * the queue for the race where a producer added between our poll and our set. This is
 * the standard publisher / single-consumer-per-key lock-free pattern.
 *
 * <p>Compared to the prior {@code ReentrantLock} version, workers never take a lock at
 * all; producers only do CAS ops; the lock is no longer held across
 * {@code launcher.launch(...)}.
 */
@Internal
final class ClassicKeyShardDispatcher implements ClassicDispatcher {

    private static final Logger log = LoggerFactory.getLogger(ClassicKeyShardDispatcher.class);

    private final int shardCount;
    private final WorkerLauncher launcher;
    private final ClassicRecordProcessor processor;
    private final BooleanSupplier running;
    private final ConcurrentMap<ShardKey, Shard> shards = new ConcurrentHashMap<>();

    ClassicKeyShardDispatcher(
        int shardCount,
        WorkerLauncher launcher,
        ClassicRecordProcessor processor,
        BooleanSupplier running) {
        if (shardCount <= 0) {
            throw new IllegalArgumentException("shardCount must be > 0, was " + shardCount);
        }
        this.shardCount = shardCount;
        this.launcher = launcher;
        this.processor = processor;
        this.running = running;
    }

    @Override
    public void dispatch(
        TopicPartition tp,
        CommitFrontier frontier,
        List<ConsumerRecord<byte[], byte[]>> records,
        Runnable onRecordDone) {
        // Dispatch is single-producer (poll thread); records are added in their offset
        // order. ConcurrentLinkedQueue preserves FIFO so same-shard records process in
        // dispatch order, which is offset order — the per-key FIFO invariant.
        //
        // The captured frontier reference goes onto every Entry; the worker passes it to
        // processOne which identity-checks before applying markComplete. If the partition
        // is revoked + reassigned between dispatch and worker completion, a NEW Shard
        // (the shards map gets purged on revoke) and a NEW frontier are created for the
        // new generation; old-generation completions fail the identity check and are
        // dropped silently.
        for (ConsumerRecord<byte[], byte[]> raw : records) {
            int idx = shardIndexFor(raw.key());
            ShardKey key = new ShardKey(tp, idx);
            Shard shard = shards.computeIfAbsent(key, k -> new Shard(tp));
            shard.queue.add(new Entry(raw, frontier, onRecordDone));
            // Claim the worker chain if it isn't running. If the CAS fails another
            // producer (or a worker about to finish) already owns the chain; it will
            // pick up our entry on its next poll().
            if (shard.busy.compareAndSet(false, true)) {
                launchNext(shard);
            }
        }
    }

    int shardIndexFor(byte[] key) {
        // Math.floorMod handles negative hashes correctly. Arrays.hashCode handles null
        // keys — all null-keyed records collapse onto one shard.
        return Math.floorMod(Arrays.hashCode(key), shardCount);
    }

    /**
     * Pop the next entry and launch a worker for it. If the queue is empty, release
     * {@code busy} and double-check for the producer-vs-release race: a producer that
     * adds between our {@code poll() == null} and {@code busy.set(false)} would have
     * seen {@code busy == true} and not launched anything, so the queue would stall.
     * After releasing, peek the queue; if non-empty, try to reclaim {@code busy} and
     * resume the chain.
     *
     * <p>Iterative, not recursive: both the busy-release double-check and the
     * per-entry launch-rejection recovery (see the catch below) loop back to the top
     * instead of calling {@code launchNext} again. A deep queue drained entirely
     * through the rejection path — e.g. every launch failing during shutdown — would
     * otherwise recurse once per queued entry on a single thread, an unbounded
     * stack-depth hazard. The loop makes that drain O(1) in stack depth regardless of
     * queue size, with identical behavior on every path: {@code onRecordDone} still
     * fires exactly once per record.
     */
    private void launchNext(Shard shard) {
        while (true) {
            Entry next = shard.queue.poll();
            if (next == null) {
                shard.busy.set(false);
                if (shard.queue.isEmpty() || !shard.busy.compareAndSet(false, true)) {
                    return;
                }
                // Reclaimed busy after the double-check race — loop back and poll
                // again instead of recursing.
                continue;
            }
            try {
                launcher.launch(() -> processThenAdvance(shard, next));
                return;
            } catch (Throwable t) {
                // Launcher rejected the task (typically RejectedExecutionException on
                // shutdown or worker pool saturation). We already polled the Entry from
                // the queue; if we returned here without further action the entry would
                // be lost AND its onRecordDone would never fire — ClassicPollLoop's
                // inFlight counter would stay too high and backpressure would lock up.
                //
                // Recovery:
                //   1. Fire onRecordDone for THIS entry only (decrements inFlight by 1).
                //   2. Loop back to try the next queued entry. If the next launch also
                //      fails we keep looping; eventually the queue drains and busy is
                //      released. Bounded by queue size, but iteratively — not via
                //      recursion — so depth never grows with the backlog.
                //
                // We deliberately do NOT throw — the dispatch caller (ClassicPollLoop)
                // must NOT roll back inFlight for the whole batch, because earlier
                // records on OTHER shards (or this shard's earlier successful launches)
                // are already running and will fire their own onRecordDone.
                log.error("Worker launch rejected for {} (likely shutdown or pool saturation); "
                    + "dropping record and continuing with next queued entry",
                    shard.tp, t);
                next.onRecordDone.run();
                // fall through to the top of the loop to try the next entry
            }
        }
    }

    private void processThenAdvance(Shard shard, Entry entry) {
        try {
            if (running.getAsBoolean()) {
                processor.process(entry.frontier, shard.tp, entry.raw);
            }
        } finally {
            entry.onRecordDone.run();
            // launchNext now handles its own launcher failures internally, so this
            // try/catch is purely a belt-and-suspenders against an unexpected
            // exception in the queue-poll / busy-set bookkeeping above launcher.launch.
            try {
                launchNext(shard);
            } catch (Throwable t) {
                shard.busy.set(false);
                throw t;
            }
        }
    }

    /**
     * Drop all shards for the given partitions. Called on revoke so a partition
     * reassigned back to us starts with empty shards.
     *
     * <p>For each revoked shard we MUST also drain queued entries — if we only removed
     * the map entry, an in-flight worker on that shard would call {@link #launchNext}
     * after completion, polling and processing the queued (now-revoked) records. That's
     * wasted work AND it leaks inFlight: the queued records were counted in inFlight
     * by {@code ClassicPollLoop.dispatchBatchAsync} and only the per-record
     * {@code onRecordDone} decrements it.
     *
     * <p>So for every queued entry we explicitly fire {@code onRecordDone.run()} —
     * the backpressure counter goes back to zero promptly, the new owner of the
     * partition handles redelivery from the last committed offset, and the in-flight
     * worker's eventual {@link #launchNext} sees an empty queue and exits cleanly.
     *
     * <p>Records ALREADY launched into worker threads (i.e. polled from the queue
     * before this purge ran) continue to run to completion — we can't take them back.
     * Their {@code markComplete} calls drop silently via {@link ClassicPollLoop#markComplete}
     * because the corresponding frontier has been removed.
     */
    void purgePartitions(Collection<TopicPartition> partitions) {
        if (partitions.isEmpty()) return;
        Set<TopicPartition> set = new HashSet<>(partitions);
        shards.entrySet().removeIf(e -> {
            if (!set.contains(e.getKey().tp)) return false;
            // Drain queued entries: fire onRecordDone for each so inFlight stays
            // consistent. Worker that's currently RUNNING a record from this shard
            // will fire onRecordDone via its own finally block; nothing extra here.
            Shard shard = e.getValue();
            Entry queued;
            while ((queued = shard.queue.poll()) != null) {
                queued.onRecordDone.run();
            }
            return true;
        });
    }

    /** Test/diagnostic. */
    int shardCount() {
        return shardCount;
    }

    /** Test/diagnostic. */
    int activeShardCount() {
        return shards.size();
    }

    private record ShardKey(TopicPartition tp, int shardIdx) {}

    private record Entry(
        ConsumerRecord<byte[], byte[]> raw,
        CommitFrontier frontier,
        Runnable onRecordDone) {}

    private static final class Shard {
        final TopicPartition tp;
        final ConcurrentLinkedQueue<Entry> queue = new ConcurrentLinkedQueue<>();
        final AtomicBoolean busy = new AtomicBoolean();

        Shard(TopicPartition tp) {
            this.tp = tp;
        }
    }
}
