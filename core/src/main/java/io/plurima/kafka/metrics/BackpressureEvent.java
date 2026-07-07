package io.plurima.kafka.metrics;

import io.plurima.kafka.annotation.Stable;

import java.util.Locale;

/**
 * Event tag for {@link PlurimaMetrics#backpressureEvent(String, BackpressureEvent)} —
 * {@code plurima.consumer.backpressure.events}. Replaces the previously stringly-typed
 * {@code event} parameter (formerly one of {@code "paused"}, {@code "resumed"}).
 * Emitted only by the CLASSIC_BASIC engine; SHARE uses semaphore-based backpressure
 * inside the poll thread, not pause/resume.
 *
 * <p>{@link #toString()} renders the lower-case tag value so the Micrometer adapter
 * emits the exact same tag values pre- and post-hardening.
 */
@Stable(since = "0.3.0")
public enum BackpressureEvent {
    /** In-flight count reached the concurrency threshold; assigned partitions were paused. */
    PAUSED,
    /** In-flight count drained below the resume threshold; paused partitions were resumed. */
    RESUMED;

    @Override
    public String toString() {
        return name().toLowerCase(Locale.ROOT);
    }
}
