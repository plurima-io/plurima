package io.plurima.kafka.ack;

import io.plurima.kafka.annotation.Stable;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * Listener variant that requires the user to call {@link AckContext#acknowledge}
 * explicitly. If a manual-ack listener throws, the standard retry/DLT pipeline runs
 * as usual; only the success path differs (no auto-ACCEPT).
 *
 * <p>returning without acknowledging auto-RELEASEs; the first acknowledgement wins and
 * subsequent calls are no-ops.
 *
 * @param <K> deserialized key type
 * @param <V> deserialized value type
 */
@Stable(since = "0.1.0")
@FunctionalInterface
public interface ManualAckListener<K, V> {
    /**
     * Process a single record. The listener MUST call {@code ctx.acknowledge(...)}
     * exactly once with the desired terminal {@link AckType}.
     *
     * @param record the polled record
     * @param ctx ack handle for this record
     * @throws Exception any thrown exception is handed to the retry pipeline
     */
    void onRecord(ConsumerRecord<K, V> record, AckContext ctx) throws Exception;
}
