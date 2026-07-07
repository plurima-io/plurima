package io.plurima.kafka.internal;

import io.plurima.kafka.annotation.Internal;

import java.util.ArrayDeque;
import java.util.OptionalLong;
import java.util.concurrent.ConcurrentSkipListSet;

/**
 * Per-partition commit frontier supporting out-of-order completion AND sparse
 * (gappy) offset sequences.
 *
 * <p>The classic engine's KEY mode lets multiple records on the same partition run
 * concurrently — different keys map to different shards, and different shards run on
 * different workers. As a consequence records can complete out of offset order: offset
 * 7 may finish before offset 3 if their keys hash to different shards and shard 7 had
 * less work in front of it.
 *
 * <p>Offsets are NOT dense: compacted topics drop records, and {@code read_committed}
 * consumers skip transaction-marker slots and aborted batches. A frontier that waits
 * for "offset N+1" after absorbing N stalls forever when N+1 was never delivered. So
 * the frontier tracks the offsets that were ACTUALLY DELIVERED — {@link #observe(long)}
 * appends each dispatched record's offset to the {@link #pendingDelivered} FIFO — and
 * matches completions against that queue, never against arithmetic successors.
 *
 * <p>This class tracks the safe-to-commit offset for a partition: one past the newest
 * delivered offset whose completion — and every earlier delivered offset's completion —
 * has been absorbed. We commit that value; committing it tells the broker "everything
 * below this has been processed; redeliver starting from here on restart/rebalance."
 * Records that have completed but sit behind an incomplete delivered offset are
 * remembered in a {@link ConcurrentSkipListSet}; as soon as the gap closes the
 * frontier advances past them in one sweep.
 *
 * <h3>Lifecycle per partition</h3>
 * <ol>
 *   <li>{@link #observe(long)} called with EVERY record's offset, in delivery order,
 *       before any worker starts on the batch. The first call also pins the starting
 *       commit floor.</li>
 *   <li>{@link #complete(long)} called from each worker as its record finishes.
 *       <b>Lock-free</b> — just an add to the concurrent set.</li>
 *   <li>{@link #drainCommittable()} called from the poll thread between polls. Absorbs
 *       completions of delivered offsets in delivery order, advances the frontier,
 *       returns the new commit offset if it moved.</li>
 * </ol>
 *
 * <h3>Concurrency model</h3>
 * <ul>
 *   <li>{@link #complete} runs on worker threads. It only touches the concurrent set —
 *       no synchronization, no monitor contention between workers.</li>
 *   <li>{@link #observe} and {@link #drainCommittable} are called from the poll thread
 *       only. They mutate {@code pendingDelivered}, {@code nextExpected} and
 *       {@code lastDrained}.</li>
 *   <li>{@link #observe} and {@link #absorbContiguous} are synchronized because both
 *       touch {@code pendingDelivered} and {@code nextExpected}; the monitor is only
 *       ever contended between the poll thread and (rare) test reads, never between
 *       workers. Workers' {@link #complete} path stays lock-free.</li>
 * </ul>
 * Workers therefore never block each other on this class — the prior version had every
 * {@code complete()} taking the {@code synchronized this} monitor.
 *
 * <h3>Memory bound</h3>
 * {@code pendingDelivered} holds only delivered-but-not-yet-absorbed offsets — bounded
 * by the engine's in-flight cap (backpressure pauses partitions long before this could
 * grow). Entries leave the queue as their completions are absorbed. Stale
 * {@code completedAhead} entries below the queue head (duplicate completions, or
 * completions for offsets that were never delivered) are purged on every absorb, so
 * neither structure grows without bound.
 */
@Internal
final class CommitFrontier {

    /**
     * The commit floor: the "next to consume" offset we would commit right now — i.e.
     * the broker will redeliver from here. Advanced by {@link #absorbContiguous} to one
     * past each delivered offset whose completion has been matched in delivery order.
     * Because delivery can be sparse, this is NOT necessarily a delivered offset itself.
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
     * Offsets that were actually delivered (dispatched to workers), in delivery order,
     * and whose completions have not yet been absorbed. The head is the oldest
     * outstanding delivered offset — the one gating the frontier. Guarded by
     * {@code this} monitor (see class doc); never touched by worker threads.
     */
    private final ArrayDeque<Long> pendingDelivered = new ArrayDeque<>();

    /**
     * Completed offsets parked here by worker threads via {@link #complete}. The set is
     * lock-free; workers do not block each other on add. The poll thread drains it on
     * each {@link #drainCommittable} call.
     */
    private final ConcurrentSkipListSet<Long> completedAhead = new ConcurrentSkipListSet<>();

    /**
     * Register a delivered record's offset. Called for EVERY record of a batch, in
     * delivery order, before the batch is dispatched to workers. The first call pins
     * the starting commit floor. Poll-thread-only by contract; synchronized anyway
     * because {@link #absorbContiguous} (reachable from test getters) reads
     * {@code pendingDelivered} under the same monitor.
     *
     * <p>Offsets must arrive strictly increasing (Kafka delivers a partition in order,
     * and this frontier instance is replaced — never reused — across rebalances).
     * Non-increasing or already-absorbed offsets are dropped defensively: a duplicate
     * queue entry could never be matched twice by the completion SET and would stall
     * the frontier forever.
     */
    synchronized void observe(long offset) {
        if (nextExpected < 0) {
            nextExpected = offset;
        }
        Long newest = pendingDelivered.peekLast();
        if ((newest != null && offset <= newest) || offset < nextExpected) {
            return;  // duplicate/stale delivery — defensive, see javadoc
        }
        pendingDelivered.addLast(offset);
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
     * Match parked completions against the delivered-offset queue and advance
     * {@link #nextExpected} past every delivered offset whose completion has arrived,
     * in delivery order. Stops at the first delivered offset still incomplete — that
     * offset gates the frontier regardless of how many later completions are parked.
     * Synchronized because it mutates {@code pendingDelivered} and {@code nextExpected};
     * the lock is only ever contended between the poll thread and (rare) test reads,
     * never between workers. Workers' {@link #complete} path stays lock-free.
     *
     * <p>Completions below the queue head can never match a delivered offset (delivery
     * order is strictly increasing) — they are duplicates or replays and are purged so
     * {@code completedAhead} cannot accumulate garbage.
     *
     * <p>If the queue is empty (nothing delivered, or everything delivered already
     * absorbed) we fall back to the pre-queue DENSE absorption from the commit floor —
     * defensive against a programming error where {@code observe} was missed for some
     * records. In that mode an uninitialised floor bootstraps from the smallest parked
     * completion, mirroring the historical complete-without-observe behavior.
     */
    private synchronized void absorbContiguous() {
        while (!pendingDelivered.isEmpty()) {
            long head = pendingDelivered.peekFirst();
            completedAhead.headSet(head).clear();  // purge stale entries below the head
            if (!completedAhead.remove(head)) {
                return;  // oldest delivered offset still incomplete — frontier holds here
            }
            pendingDelivered.pollFirst();
            nextExpected = head + 1;
        }

        // Defensive dense fallback (queue empty): see javadoc.
        if (nextExpected < 0) {
            Long smallest = completedAhead.pollFirst();
            if (smallest == null) {
                return;
            }
            nextExpected = smallest + 1;  // bootstrap when observe() was skipped entirely
        }
        completedAhead.headSet(nextExpected).clear();  // purge duplicates below the floor
        while (completedAhead.remove(nextExpected)) {
            nextExpected++;
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
