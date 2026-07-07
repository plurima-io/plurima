package io.plurima.kafka.ack;

import io.plurima.kafka.Message;
import io.plurima.kafka.annotation.Stable;

/**
 * A {@link Message} that also carries explicit acknowledgement, for
 * {@link MessageAckListener}. Payload, metadata, and ack live on one object, so a complex
 * handler can branch and dispose of a record without juggling separate context/ack handles.
 *
 * @param <K> deserialized key type
 * @param <V> deserialized value type
 */
@Stable(since = "0.2.0")
public interface AckMessage<K, V> extends Message<K, V> {

    /**
     * Acknowledge this record with a terminal type. First call wins; later calls are no-ops.
     *
     * @param type {@code ACCEPT}, {@code REJECT}, or {@code RELEASE}
     */
    void acknowledge(AckType type);

    /** Shorthand for {@code acknowledge(ACCEPT)} — processed successfully. */
    default void accept() { acknowledge(AckType.ACCEPT); }

    /** Shorthand for {@code acknowledge(RELEASE)} — hand back to the broker for redelivery. */
    default void release() { acknowledge(AckType.RELEASE); }

    /** Shorthand for {@code acknowledge(REJECT)} — give up on this record. */
    default void reject() { acknowledge(AckType.REJECT); }
}
