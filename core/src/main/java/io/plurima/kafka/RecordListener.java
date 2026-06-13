package io.plurima.kafka;

import io.plurima.kafka.annotation.Stable;
import org.apache.kafka.clients.consumer.ConsumerRecord;

@Stable(since = "0.1.0")
@FunctionalInterface
public interface RecordListener<K, V> {
    /**
     * Called for each record. Normal return ⇒ implicit ACCEPT (the runtime queues the
     * terminal ack on SHARE / advances the commit frontier on CLASSIC_BASIC). A thrown
     * exception is handed to the configured {@link io.plurima.kafka.retry.RetryPolicy};
     * see {@link io.plurima.kafka.retry.RetryDecision} for the resulting paths.
     */
    void onRecord(ConsumerRecord<K, V> record, ConsumerContext ctx) throws Exception;
}
