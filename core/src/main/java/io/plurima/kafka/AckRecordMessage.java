package io.plurima.kafka;

import io.plurima.kafka.ack.AckContext;
import io.plurima.kafka.ack.AckMessage;
import io.plurima.kafka.ack.AckType;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.Objects;

/**
 * Package-private {@link AckMessage} implementation: a {@link RecordMessage} whose
 * {@link #acknowledge} delegates to the engine's {@link AckContext}. Constructed by the
 * builder's {@code onMessageAck} adapter; users receive it only through the {@link AckMessage}
 * interface.
 */
final class AckRecordMessage<K, V> extends RecordMessage<K, V> implements AckMessage<K, V> {

    private final AckContext ack;

    AckRecordMessage(ConsumerRecord<K, V> record, AckContext ack) {
        super(record, ack.deliveryCount(), ack.orderingMode());
        this.ack = Objects.requireNonNull(ack, "ack");
    }

    @Override
    public void acknowledge(AckType type) {
        ack.acknowledge(type);
    }
}
