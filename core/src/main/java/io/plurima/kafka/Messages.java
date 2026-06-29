package io.plurima.kafka;

import io.plurima.kafka.annotation.Stable;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;

import java.util.Optional;

/**
 * Factory for building {@link Message} instances in tests, with no Kafka broker or
 * {@code ConsumerRecord} construction required. Lets a {@link MessageListener} (or a class that
 * implements it) be unit-tested directly:
 *
 * <pre>{@code
 * new OrderHandler(orderService).onMessage(Messages.of("k1", order));
 * }</pre>
 *
 * <p>Not used by the runtime — production messages are built internally from the polled record.
 */
@Stable(since = "0.2.0")
public final class Messages {

    private Messages() {}

    /** A message carrying only {@code value} (null key), deliveryCount 1. */
    public static <V> Message<byte[], V> of(V value) {
        return Messages.<byte[], V>builder(null, value).build();
    }

    /** A message carrying {@code key} and {@code value}, deliveryCount 1. */
    public static <K, V> Message<K, V> of(K key, V value) {
        return builder(key, value).build();
    }

    /** A message with an explicit {@code deliveryCount} (e.g. to test redelivery branches). */
    public static <K, V> Message<K, V> of(K key, V value, short deliveryCount) {
        return builder(key, value).deliveryCount(deliveryCount).build();
    }

    /** A builder for full control over topic/partition/offset/timestamp/headers/deliveryCount. */
    public static <K, V> Builder<K, V> builder(K key, V value) {
        return new Builder<>(key, value);
    }

    /** Fluent builder for test {@link Message}s. */
    @Stable(since = "0.2.0")
    public static final class Builder<K, V> {
        private final K key;
        private final V value;
        private String topic = "test-topic";
        private int partition = 0;
        private long offset = 0L;
        private long timestampMillis = 0L;
        private short deliveryCount = 1;
        private OrderingMode orderingMode = OrderingMode.UNORDERED;
        private final Headers headers = new RecordHeaders();

        Builder(K key, V value) {
            this.key = key;
            this.value = value;
        }

        public Builder<K, V> topic(String topic) { this.topic = topic; return this; }
        public Builder<K, V> partition(int partition) { this.partition = partition; return this; }
        public Builder<K, V> offset(long offset) { this.offset = offset; return this; }
        public Builder<K, V> timestampMillis(long ms) { this.timestampMillis = ms; return this; }
        public Builder<K, V> deliveryCount(short deliveryCount) { this.deliveryCount = deliveryCount; return this; }
        public Builder<K, V> orderingMode(OrderingMode orderingMode) { this.orderingMode = orderingMode; return this; }
        public Builder<K, V> header(String name, byte[] value) { this.headers.add(name, value); return this; }

        public Message<K, V> build() {
            ConsumerRecord<K, V> record = new ConsumerRecord<>(
                topic, partition, offset,
                timestampMillis, TimestampType.CREATE_TIME,
                ConsumerRecord.NULL_SIZE, ConsumerRecord.NULL_SIZE,
                key, value,
                headers,
                Optional.empty(),                 // leaderEpoch
                Optional.of(deliveryCount));
            return new RecordMessage<>(record, deliveryCount, orderingMode);
        }
    }
}
