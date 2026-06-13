package io.plurima.kafka.retry;

import io.plurima.kafka.annotation.Stable;

import java.time.Duration;

/**
 * Outcome of {@link io.plurima.kafka.internal.RetryEngine#evaluate} for a failed record.
 * Sealed: callers must handle all four cases. Engine-specific execution semantics are
 * documented per record below.
 */
@Stable(since = "0.1.0")
public sealed interface RetryDecision {

    /**
     * Retry on the same worker thread after sleeping {@link #delay()}. Both engines
     * honour the delay exactly — the sleep happens on the worker before the listener
     * is re-invoked. Used when the computed backoff is short enough to keep on the
     * worker (≤ 1 s by default).
     *
     * @param delay sleep duration before the next listener invocation
     */
    @Stable(since = "0.1.0")
    record RetryInline(Duration delay) implements RetryDecision {}

    /**
     * Retry with a longer-than-inline delay. Execution semantics differ by engine:
     *
     * <ul>
     *   <li><b>SHARE engine</b>: the record is {@code RELEASE}d immediately via
     *       {@code AcknowledgeType.RELEASE}; the broker decides when to redeliver.
     *       Apache Kafka through 4.2 has no broker-side "redeliver after N seconds"
     *       primitive — once RELEASE'd the broker re-acquires the record as soon as a
     *       share-group member polls. The {@link #delay()} value is <b>informational
     *       only</b> on this engine; a future broker-side KIP could honour it, but
     *       today the actual interval before redelivery is the broker's fetch cadence
     *       (typically tens of milliseconds).</li>
     *
     *   <li><b>CLASSIC_BASIC engine</b>: the {@link #delay()} is honoured exactly —
     *       the worker {@code Thread.sleep}s for that duration before re-invoking the
     *       listener. The continuous-poll loop heartbeats independently of worker
     *       progress, so the sleep does not fence the consumer regardless of length.
     *       Backpressure pauses the partition while the worker sleeps.</li>
     * </ul>
     *
     * @param delay backoff duration; honoured exactly on CLASSIC_BASIC,
     *     informational on SHARE (see above)
     */
    @Stable(since = "0.1.0")
    record RetryDelayed(Duration delay) implements RetryDecision {}

    /**
     * Classifier deemed the exception non-retriable; record will be REJECTed.
     *
     * @param cause the listener exception that triggered the rejection
     */
    @Stable(since = "0.1.0")
    record Reject(Throwable cause) implements RetryDecision {}

    /**
     * All retries used up. If a {@link io.plurima.kafka.dlt.DltConfig} is configured,
     * the record is routed to the DLT topic; otherwise REJECTed with an ERROR log.
     *
     * @param cause the listener exception that ended the retry attempts
     */
    @Stable(since = "0.1.0")
    record Exhausted(Throwable cause) implements RetryDecision {}
}
