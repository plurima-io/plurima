package io.plurima.kafka;

import io.plurima.kafka.annotation.Stable;

/**
 * Auto-ack listener over a {@link Message}. Recommended default for application handlers:
 * the handler works with a single Kafka-decoupled object, and acknowledgement is implicit —
 * a normal return ACCEPTs the record; a thrown exception is handed to the retry/DLT pipeline
 * (identical semantics to {@link RecordListener}, which this is adapted onto internally).
 *
 * <p>For explicit per-record disposition, use {@link io.plurima.kafka.ack.MessageAckListener}.
 *
 * @param <K> deserialized key type
 * @param <V> deserialized value type
 */
@Stable(since = "0.2.0")
@FunctionalInterface
public interface MessageListener<K, V> {
    /**
     * Process a single message. Normal return ⇒ ACCEPT; a thrown exception is routed through
     * the configured {@link io.plurima.kafka.retry.RetryPolicy} (retry / DLT).
     *
     * @param message payload + metadata for the record
     * @throws Exception any thrown exception is handed to the retry pipeline
     */
    void onMessage(Message<K, V> message) throws Exception;
}
