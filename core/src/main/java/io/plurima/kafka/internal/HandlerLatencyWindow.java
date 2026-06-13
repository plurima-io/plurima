package io.plurima.kafka.internal;

import io.plurima.kafka.annotation.Internal;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;

/**
 * Fixed-size ring of recent handler-completion latencies (nanos), used by the
 * SHARE engine's adaptive drain barrier to estimate a give-up point.
 *
 * <p>Writers are worker threads ({@link #record}); the single reader is the poll
 * thread ({@link #percentileMillis}, called once per poll cycle). Writes are
 * lock-free; a reader may observe a partially-overwritten window, which is
 * acceptable for a statistical estimate — {@link AtomicLongArray} guarantees each
 * individual slot is read/written atomically (no torn longs).
 */
@Internal
final class HandlerLatencyWindow {

    private final int windowSize;
    private final AtomicLongArray values;
    private final AtomicLong cursor = new AtomicLong(0);
    private final AtomicLong total = new AtomicLong(0);

    HandlerLatencyWindow(int windowSize) {
        if (windowSize <= 0) {
            throw new IllegalArgumentException("windowSize must be > 0, was " + windowSize);
        }
        this.windowSize = windowSize;
        this.values = new AtomicLongArray(windowSize);
    }

    /** Record a handler completion latency in nanoseconds. Lock-free; worker-thread call. */
    void record(long nanos) {
        int idx = (int) Math.floorMod(cursor.getAndIncrement(), (long) windowSize);
        // ORDERING INVARIANT: write the slot BEFORE bumping total. percentileMillis derives
        // its snapshot length n from total, then reads slots [0, n) — so total must lag the
        // slot write, never lead it, or the snapshot could read an unwritten slot. Do not
        // reorder these two lines.
        values.set(idx, nanos);
        total.incrementAndGet();
    }

    /** Samples recorded so far, saturating at {@code windowSize}. */
    int sampleCount() {
        return (int) Math.min(total.get(), (long) windowSize);
    }

    /**
     * Quantile of recorded latencies in milliseconds. {@code q} in (0, 1]. Returns 0
     * when no samples. Snapshots the filled portion, sorts a copy, picks the index
     * {@code ceil(q*n)-1} clamped to {@code [0, n-1]}.
     */
    double percentileMillis(double q) {
        int n = sampleCount();
        if (n == 0) return 0.0;
        long[] snap = new long[n];
        for (int i = 0; i < n; i++) snap[i] = values.get(i);
        Arrays.sort(snap);
        int idx = (int) Math.ceil(q * n) - 1;
        if (idx < 0) idx = 0;
        if (idx >= n) idx = n - 1;
        return snap[idx] / 1_000_000.0;
    }
}
