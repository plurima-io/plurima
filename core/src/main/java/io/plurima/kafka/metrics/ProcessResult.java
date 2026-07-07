package io.plurima.kafka.metrics;

import io.plurima.kafka.annotation.Stable;

import java.util.Locale;

/**
 * Outcome tag for {@link PlurimaMetrics#recordsProcessed(String, ProcessResult)} —
 * {@code plurima.consumer.records.processed}. Replaces the previously stringly-typed
 * {@code result} parameter (formerly one of {@code "accept"}, {@code "release"},
 * {@code "reject"}).
 *
 * <p>{@link #toString()} renders the lower-case tag value (e.g. {@code "accept"}) so
 * {@code metrics/}'s Micrometer adapter emits the exact same tag values pre- and
 * post-hardening.
 */
@Stable(since = "0.3.0")
public enum ProcessResult {
    /** The record was processed successfully (including a successful DLT route). */
    ACCEPT,
    /** The record was released back to the broker for redelivery (not a failure). */
    RELEASE,
    /** The record was rejected — processed terminally without success. */
    REJECT;

    @Override
    public String toString() {
        return name().toLowerCase(Locale.ROOT);
    }
}
