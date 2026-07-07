package io.plurima.kafka;

import io.plurima.kafka.annotation.Stable;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * The lowest-level per-record handler: receives the raw Kafka {@link ConsumerRecord} plus a
 * {@link ConsumerContext} carrying delivery metadata (attempt count, ordering mode). Prefer
 * the higher-level {@link MessageListener} (via {@link PlurimaConsumerBuilder#onMessage}) for
 * application code that doesn't need direct access to Kafka client types.
 *
 * <p>Registered via {@link PlurimaConsumerBuilder#listener}. Exactly one handler — this,
 * {@link io.plurima.kafka.ack.ManualAckListener}, {@link MessageListener}, or
 * {@link io.plurima.kafka.ack.MessageAckListener} — may be configured per consumer.
 *
 * @param <K> deserialized key type
 * @param <V> deserialized value type
 */
@Stable(since = "0.1.0")
@FunctionalInterface
public interface RecordListener<K, V> {
    /**
     * Called for each record. Normal return ⇒ implicit ACCEPT (the runtime queues the
     * terminal ack on SHARE / advances the commit frontier on CLASSIC_BASIC). A thrown
     * exception is handed to the configured {@link io.plurima.kafka.retry.RetryPolicy},
     * which classifies it into an inline retry, a delayed retry, a rejection, or — once
     * retries are exhausted — a route to the DLT (if configured).
     */
    void onRecord(ConsumerRecord<K, V> record, ConsumerContext ctx) throws Exception;
}
