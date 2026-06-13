package io.plurima.kafka.internal;

import io.plurima.kafka.annotation.Internal;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;

/**
 * Per-record processing callback for classic dispatchers. The CommitFrontier reference
 * is passed through so the processor can identity-check against the live frontier map
 * before invoking the listener and again at {@code markComplete} time — closing the
 * "old worker completes a record on a partition that was revoked and reassigned"
 * race.
 *
 * <p>Java's stock {@link java.util.function.BiConsumer} only takes two arguments;
 * this 3-arg interface saves us from boxing the frontier into a record-context type.
 */
@Internal
@FunctionalInterface
interface ClassicRecordProcessor {
    void process(
        CommitFrontier frontier,
        TopicPartition tp,
        ConsumerRecord<byte[], byte[]> raw);
}
