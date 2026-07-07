package io.plurima.kafka;

import io.plurima.kafka.annotation.Stable;
import org.jspecify.annotations.Nullable;

import java.time.Instant;
import java.util.Optional;

/**
 * A read-only, Kafka-decoupled view of a single delivered record — payload and metadata in one
 * object. Passed to a {@link MessageListener} (auto-ack) or, as {@link io.plurima.kafka.ack.AckMessage},
 * to a {@link io.plurima.kafka.ack.MessageAckListener} (explicit ack).
 *
 * <p>Compared to the lower-level {@link RecordListener} (which receives a raw
 * {@link org.apache.kafka.clients.consumer.ConsumerRecord} plus a separate
 * {@link ConsumerContext}), {@code Message} keeps business code free of Kafka client types and
 * unifies payload + delivery metadata, so handlers are easier to structure and to unit-test
 * (see {@link Messages} for building one without a broker).
 *
 * <p>Extends {@link ConsumerContext}, so {@link #deliveryCount()} / {@link #orderingMode()} are
 * available here too.
 *
 * @param <K> deserialized key type
 * @param <V> deserialized value type
 */
@Stable(since = "0.2.0")
public interface Message<K, V> extends ConsumerContext {

    /** The deserialized key, or {@code null} (e.g. a tombstone). */
    @Nullable K key();

    /** The deserialized value, or {@code null} (e.g. a tombstone). */
    @Nullable V value();

    /** Source topic. */
    String topic();

    /** Source partition. */
    int partition();

    /** Record offset within its partition. */
    long offset();

    /** Record timestamp as an {@link Instant} (a record with no timestamp yields the epoch-relative instant). */
    Instant timestamp();

    /**
     * The last header value for {@code name}, if present. For multi-valued headers or full
     * access use {@link #headers()}.
     */
    Optional<byte[]> header(String name);

    /** All record headers, as a Kafka-decoupled {@link MessageHeaders} view. */
    MessageHeaders headers();
}
