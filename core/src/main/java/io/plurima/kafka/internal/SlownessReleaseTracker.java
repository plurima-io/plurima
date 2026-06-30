package io.plurima.kafka.internal;

import io.plurima.kafka.annotation.Internal;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Counts, per record coordinate, how many times Plurima has RELEASEd a record because its
 * handler was <em>too slow</em> (drain-barrier / shutdown force-RELEASE) rather than because
 * it <em>failed</em>.
 *
 * <p><b>Why this exists.</b> The broker increments a record's {@code deliveryCount} on every
 * redelivery — whether the redelivery was caused by a genuine failure-driven retry
 * (WorkerProcessor RetryDelayed → RELEASE) or by Plurima abandoning a slow-but-healthy record
 * at the drain barrier ({@link PollLoop#forceReleaseStuckRecords()} → RELEASE). {@link RetryEngine}
 * derives the effective attempt from {@code deliveryCount}, so without this tracker a record
 * that is merely slow would burn its retry budget and be wrongly routed to the DLT. By
 * subtracting the slowness-release count from {@code deliveryCount}, only genuine failures count
 * toward exhaustion.
 *
 * <p><b>Lifecycle &amp; bounding.</b> A coord's count is {@link #clear cleared} when the record
 * terminally completes (ACCEPT / REJECT), so entries do not accumulate for finished records — in
 * steady state the map holds only coords that have been force-RELEASEd and are awaiting
 * redelivery + a terminal outcome, which is bounded by the in-flight + recently-released set
 * (≈ concurrency). The access-ordered, size-capped LRU map is therefore a memory <em>backstop</em>
 * sized well above that working set; in normal operation it never evicts a live (uncleared) entry.
 *
 * <p><b>Residual guarantee.</b> Under pathological sustained slow-record churn whose live
 * working set exceeds the cap, the LRU may evict a still-relevant count; that coord's later
 * failure would then count its slowness redeliveries as genuine attempts (possibly premature
 * DLT). This is the documented limit of the "slow ≠ failure" guarantee — raise the cap (scale it
 * with {@code concurrency}) if you run pathologically many concurrently-stuck records.
 *
 * <p>Thread-safety: the poll thread writes ({@link #recordRelease}); worker threads read
 * ({@link #releaseCount}) from {@link RetryEngine}/{@link DltRouter}. All access is synchronized
 * via {@link Collections#synchronizedMap}; contention is negligible (writes happen only on the
 * rare force-RELEASE path).
 */
@Internal
public final class SlownessReleaseTracker {

    private final Map<RecordCoord, Integer> counts;

    public SlownessReleaseTracker(int maxEntries) {
        if (maxEntries <= 0) {
            throw new IllegalArgumentException("maxEntries must be > 0, was " + maxEntries);
        }
        this.counts = Collections.synchronizedMap(new LinkedHashMap<>(16, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<RecordCoord, Integer> eldest) {
                return size() > maxEntries;
            }
        });
    }

    /** Record one slowness-driven RELEASE for this coord (poll thread). */
    public void recordRelease(RecordCoord coord) {
        counts.merge(coord, 1, Integer::sum);
    }

    /** Slowness-driven RELEASE count for this coord, or 0 if none (worker thread). */
    public int releaseCount(RecordCoord coord) {
        Integer v = counts.get(coord);
        return v == null ? 0 : v;
    }

    /**
     * Forget this coord's slowness count — called when the record terminally completes
     * (ACCEPT / REJECT) so live entries don't accumulate and can't be LRU-evicted while still
     * relevant. A no-op if the coord has no recorded slowness.
     */
    public void clear(RecordCoord coord) {
        counts.remove(coord);
    }
}
