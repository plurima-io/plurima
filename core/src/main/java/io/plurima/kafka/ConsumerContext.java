package io.plurima.kafka;

import io.plurima.kafka.annotation.Stable;

/** Per-record context passed to {@link RecordListener#onRecord} on every invocation. */
@Stable(since = "0.1.0")
public interface ConsumerContext {

    /**
     * Delivery count for the in-flight record. The semantics differ by engine:
     *
     * <ul>
     *   <li>{@link ConsumerEngine#SHARE}: <b>broker-derived</b>, the KIP-932 counter
     *       on {@link org.apache.kafka.clients.consumer.ConsumerRecord} (natively a
     *       {@code short}, widened here to {@code int}). Survives cross-instance
     *       redelivery: a record acquired by another consumer in the share group,
     *       RELEASEd, then redelivered to us reports the cumulative broker-tracked
     *       count.</li>
     *   <li>{@link ConsumerEngine#CLASSIC_BASIC}: <b>in-process</b>, Plurima's own
     *       retry-attempt counter (1 on the first invocation, incremented on each
     *       inline / delayed retry). Classic consumer groups have no broker-side
     *       counter, so this value resets to 1 on any redelivery from the last
     *       committed offset (e.g. after rebalance or restart).</li>
     * </ul>
     *
     * Always present (never zero) — exposed as primitive {@code int} so callers never
     * need to unwrap an {@code Optional} or cast down from Kafka's KIP-932 {@code short}
     * wire shape just to compare against a retry budget.
     *
     * @return current delivery / attempt count for this record (≥ 1)
     */
    int deliveryCount();

    /**
     * Current ordering mode this consumer is operating in.
     *
     * @return the configured {@link OrderingMode}
     */
    OrderingMode orderingMode();
}
