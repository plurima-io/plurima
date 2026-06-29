package io.plurima.kafka.ack;

import io.plurima.kafka.annotation.Stable;

/**
 * Explicit-ack listener over an {@link AckMessage}. The handler MUST call one of
 * {@link AckMessage#acknowledge}, {@link AckMessage#accept()}, {@link AckMessage#release()},
 * or {@link AckMessage#reject()} (a return without acking auto-RELEASEs, same as
 * {@link ManualAckListener}). A thrown exception is routed through the retry/DLT pipeline.
 *
 * @param <K> deserialized key type
 * @param <V> deserialized value type
 */
@Stable(since = "0.2.0")
@FunctionalInterface
public interface MessageAckListener<K, V> {
    /**
     * Process a single message and acknowledge it explicitly.
     *
     * @param message payload + metadata + ack handle for the record
     * @throws Exception any thrown exception is handed to the retry pipeline
     */
    void onMessage(AckMessage<K, V> message) throws Exception;
}
