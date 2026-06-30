package io.plurima.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;

import java.time.Instant;
import java.util.Objects;
import java.util.Optional;

/**
 * Package-private {@link Message} implementation backed by a {@link ConsumerRecord} plus the
 * per-record delivery count and ordering mode (sourced from the engine's {@code ConsumerContext}
 * at runtime, or supplied directly by {@link Messages} in tests). Not part of the public API —
 * users receive it only through the {@link Message} interface.
 *
 * <p>Non-final so {@link AckRecordMessage} can extend it for the explicit-ack variant.
 */
class RecordMessage<K, V> implements Message<K, V> {

    private final ConsumerRecord<K, V> record;
    private final short deliveryCount;
    private final OrderingMode orderingMode;

    RecordMessage(ConsumerRecord<K, V> record, short deliveryCount, OrderingMode orderingMode) {
        this.record = Objects.requireNonNull(record, "record");
        this.deliveryCount = deliveryCount;
        this.orderingMode = Objects.requireNonNull(orderingMode, "orderingMode");
    }

    @Override public K key() { return record.key(); }
    @Override public V value() { return record.value(); }
    @Override public String topic() { return record.topic(); }
    @Override public int partition() { return record.partition(); }
    @Override public long offset() { return record.offset(); }
    @Override public Instant timestamp() { return Instant.ofEpochMilli(record.timestamp()); }
    @Override public Headers headers() { return record.headers(); }

    @Override
    public Optional<byte[]> header(String name) {
        Header h = record.headers().lastHeader(name);
        return h == null ? Optional.empty() : Optional.ofNullable(h.value());
    }

    @Override public short deliveryCount() { return deliveryCount; }
    @Override public Optional<Short> deliveryCountOptional() { return Optional.of(deliveryCount); }
    @Override public OrderingMode orderingMode() { return orderingMode; }
}
