package io.plurima.kafka.internal;

import io.plurima.kafka.annotation.Internal;

import java.util.OptionalLong;
import java.util.concurrent.ConcurrentSkipListSet;

/**
 * Per-partition commit frontier supporting out-of-order completion.
 *
 * <p>The classic engine's KEY mode lets multiple records on the same partition run
 * concurrently — different keys map to different shards, and different shards run on
 * different workers. As a consequence records can complete out of offset order: offset
 * 7 may finish before offset 3 if their keys hash to different shards and shard 7 had
 * less work in front of it.
 *
 * <p>This class tracks the safe-to-commit offset for a partition: the smallest offset
 * we have NOT yet seen completed. We commit that value — committing it tells the
 * broker "everything below this has been processed; redeliver starting from here on
 * restart/rebalance." Records that have completed but sit above an incomplete one are
 * remembered in a {@link ConcurrentSkipListSet}; as soon as the gap closes the
 * frontier advances past them in one sweep.
 *
 * <h3>Lifecycle per partition</h3>
 * <ol>
 *   <li>{@link #observe(long)} called with the first record's offset before any
 *       worker starts. Pins the starting next-expected.</li>
 *   <li>{@link #complete(long)} called from each worker as its record finishes.
 *       <b>Lock-free</b> — just an add to the concurrent set.</li>
 *   <li>{@link #drainCommittable()} called from the poll thread between polls. Absorbs
 *       contiguous completions, advances the frontier, returns the new commit offset
 *       if it moved.</li>
 * </ol>
 *
 * <h3>Concurrency model</h3>
 * <ul>
 *   <li>{@link #complete} runs on worker threads. It only touches the concurrent set —
 *       no synchronization, no monitor contention between workers.</li>
 *   <li>{@link #observe} and {@link #drainCommittable} are called from the poll thread
 *       only. They mutate {@code nextExpected} and {@code lastDrained}.</li>
 *   <li>{@link #absorbContiguous} is the one mutator of {@code nextExpected} from
 *       worker-adjacent code paths (drain + test getters); it's the only synchronized
 *       method, contended only between the poll thread and (rare) test reads.</li>
 * </ul>
 * Workers therefore never block each other on this class — the prior version had every
 * {@code complete()} taking the {@code synchronized this} monitor.
 */
@Internal
final class CommitFrontier {

    /**
     * The smallest offset NOT yet completed. When we commit, we commit THIS value as the
     * "next to consume" offset — i.e. the broker will redeliver from here.
     *
     * <p>Mutated only by {@link #observe} and {@link #absorbContiguous}, both called
     * from the poll thread (or, for tests, the test thread). {@code volatile} so reads
     * from test threads see fresh values.
     *
     * <p>{@code -1} means "uninitialised" — no record observed yet for this partition.
     */
    private volatile long nextExpected = -1L;

    /**
     * Last offset returned by {@link #drainCommittable()}; we only re-emit when
     * {@link #nextExpected} has moved past it. Poll-thread-only.
     */
    private long lastDrained = -1L;

    /**
     * Completed offsets parked here by worker threads via {@link #complete}. The set is
     * lock-free; workers do not block each other on add. The poll thread drains it on
     * each {@link #drainCommittable} call.
     */
    private final ConcurrentSkipListSet<Long> completedAhead = new ConcurrentSkipListSet<>();

    /**
     * Mark the partition's starting offset. Idempotent within a partition lifetime — only
     * the FIRST call has effect. Poll-thread-only by contract; no synchronization needed
     * because subsequent {@code observe()} calls just short-circuit on the same thread.
     */
    void observe(long firstOffsetInBatch) {
        if (nextExpected < 0) {
            nextExpected = firstOffsetInBatch;
        }
    }

    /**
     * Record completion of an offset. <b>Lock-free.</b> Workers just add to the
     * concurrent set; absorption into {@link #nextExpected} happens later in
     * {@link #drainCommittable}. Duplicate / stale offsets are absorbed and discarded
     * silently at drain time.
     */
    void complete(long offset) {
        completedAhead.add(offset);
    }

    /**
     * Return the next-to-commit offset IF the frontier has advanced since the last
     * drain; otherwise empty. Called from the poll thread.
     */
    OptionalLong drainCommittable() {
        absorbContiguous();
        long current = nextExpected;
        if (current < 0 || current == lastDrained) {
            return OptionalLong.empty();
        }
        lastDrained = current;
        return OptionalLong.of(current);
    }

    /**
     * Pop contiguous offsets from {@link #completedAhead} that close the gap with
     * {@link #nextExpected}. Synchronized because it mutates {@code nextExpected}; the
     * lock is only ever contended between the poll thread and (rare) test reads, never
     * between workers. Workers' {@link #complete} path stays lock-free.
     *
     * <p>If {@code nextExpected} is uninitialised and the set has entries, we bootstrap
     * from the smallest observed completion — defensive against a programming error
     * where {@code observe} was missed.
     */
    private synchronized void absorbContiguous() {
        while (true) {
            Long first = completedAhead.pollFirst();
            if (first == null) break;
            long f = first;
            if (nextExpected < 0) {
                nextExpected = f;  // defensive bootstrap when observe() was skipped
            }
            if (f < nextExpected) continue;        // stale completion, drop
            if (f == nextExpected) {
                nextExpected++;
                continue;
            }
            // f > nextExpected: gap not closed. Put back and stop.
            completedAhead.add(f);
            break;
        }
    }

    /** Test/diagnostic only — calls {@link #absorbContiguous} so the value reflects all completions seen so far. */
    long nextExpected() {
        absorbContiguous();
        return nextExpected;
    }

    /** Test/diagnostic only. */
    int gapSize() {
        absorbContiguous();
        return completedAhead.size();
    }
}
