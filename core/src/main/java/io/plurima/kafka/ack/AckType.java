package io.plurima.kafka.ack;

import io.plurima.kafka.annotation.Stable;

/**
 * Terminal acknowledgement outcome for a single record, passed to {@link AckContext#acknowledge}
 * and {@link AckMessage#acknowledge}. Replaces Kafka's
 * {@code org.apache.kafka.clients.consumer.AcknowledgeType} on Plurima's public surface: that
 * type also exposes {@code RENEW}, which Plurima rejects at runtime (lock renewal is not
 * exposed — see the per-constant note below), and forces handler code to depend on the Kafka
 * client just to name an ack type. {@code AckType} has no {@code RENEW} constant, so the invalid
 * value is now a compile error instead of a runtime warning.
 *
 * <p>Internally, each constant maps 1:1 onto the corresponding Kafka {@code AcknowledgeType} at
 * the {@code ShareConsumer.acknowledge} boundary.
 */
@Stable(since = "0.3.0")
public enum AckType {

    /** Processed successfully — the broker permanently removes the record from the share group. */
    ACCEPT,

    /**
     * Hand the record back to the broker for redelivery. Non-terminal from the broker's
     * perspective: the record becomes available for delivery (to this or another consumer)
     * again, subject to the broker's redelivery/lease policy.
     */
    RELEASE,

    /**
     * Terminal acknowledgement — the record will not be redelivered. When configured with a
     * dead-letter topic, Plurima's retry engine routes exhausted records there instead of
     * rejecting them (a library feature, not a broker capability).
     */
    REJECT
}
