package io.plurima.kafka.metrics;

import io.plurima.kafka.annotation.Stable;

import java.util.Locale;

/**
 * Ack-type tag for {@link PlurimaMetrics#ackQueued(AckOutcome)} —
 * {@code plurima.consumer.ack.queued} — and {@link PlurimaMetrics#ackCommitted(String, AckOutcome)}
 * — {@code plurima.consumer.ack.committed}. Replaces the previously stringly-typed
 * {@code type} parameter, which — before this hardening — was inconsistently emitted:
 * upper-case (from {@code AcknowledgeType.name()}) on the SHARE engine's ack paths and
 * lower-case literals on the CLASSIC_BASIC engine's. {@link #toString()} renders the
 * lower-case tag value uniformly, matching {@link ProcessResult} and the CLASSIC_BASIC
 * convention.
 *
 * <p>Mirrors {@code org.apache.kafka.clients.consumer.AcknowledgeType}'s
 * {@code ACCEPT}/{@code RELEASE}/{@code REJECT} values (its fourth value, {@code RENEW},
 * is a lock-renewal signal never surfaced through this metric).
 */
@Stable(since = "0.3.0")
public enum AckOutcome {
    /** The record was acknowledged as successfully consumed. */
    ACCEPT,
    /** The record was released back to the broker for redelivery (not a failure). */
    RELEASE,
    /** The record was acknowledged as rejected — not consumed, not redelivered. */
    REJECT;

    @Override
    public String toString() {
        return name().toLowerCase(Locale.ROOT);
    }
}
