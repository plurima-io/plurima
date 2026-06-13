package io.plurima.kafka;

import io.plurima.kafka.annotation.Stable;

import java.util.Optional;

/** Per-record context passed to {@link RecordListener#onRecord} on every invocation. */
@Stable(since = "0.1.0")
public interface ConsumerContext {

    /**
     * Delivery count for the in-flight record. The semantics differ by engine:
     *
     * <ul>
     *   <li>{@link ConsumerEngine#SHARE}: <b>broker-derived</b>, the KIP-932 counter
     *       on {@link org.apache.kafka.clients.consumer.ConsumerRecord}. Survives
     *       cross-instance redelivery: a record acquired by another consumer in the
     *       share group, RELEASEd, then redelivered to us reports the cumulative
     *       broker-tracked count.</li>
     *   <li>{@link ConsumerEngine#CLASSIC_BASIC}: <b>in-process</b>, Plurima's own
     *       retry-attempt counter (1 on the first invocation, incremented on each
     *       inline / delayed retry). Classic consumer groups have no broker-side
     *       counter, so this value resets to 1 on any redelivery from the last
     *       committed offset (e.g. after rebalance or restart).</li>
     * </ul>
     *
     * Always present (never zero) — exposed as primitive {@code short} for
     * ergonomic use; the {@link #deliveryCountOptional()} variant matches the KIP-932
     * native shape if you prefer that style.
     *
     * @return current delivery / attempt count for this record (≥ 1)
     */
    short deliveryCount();

    /**
     * Same value as {@link #deliveryCount()} but in the KIP-932 native shape.
     *
     * @return the delivery count wrapped in an {@link Optional}; never empty
     */
    Optional<Short> deliveryCountOptional();

    /**
     * Current ordering mode this consumer is operating in.
     *
     * @return the configured {@link OrderingMode}
     */
    OrderingMode orderingMode();
}
