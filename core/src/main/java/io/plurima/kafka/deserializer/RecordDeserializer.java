package io.plurima.kafka.deserializer;

import io.plurima.kafka.annotation.Stable;
import org.jspecify.annotations.Nullable;

/**
 * Converts a raw byte array (as delivered by the Kafka broker) into a typed value.
 * Implementations may consult the topic name for routing decisions; most ignore it.
 *
 * <p>Plurima defaults to a byte-identity deserializer when none is supplied — the
 * bytes-in/bytes-out fast path the runtime checks for via reference equality with
 * {@link #IDENTITY_BYTES}.
 *
 * @param <T> the type produced by this deserializer
 */
@Stable(since = "0.1.0")
@FunctionalInterface
public interface RecordDeserializer<T> {
    /**
     * Decode a single record's bytes into a typed value.
     *
     * @param topic the topic the record was delivered from — implementations may
     *     consult it for routing decisions; most ignore it
     * @param data raw bytes from the broker, may be {@code null}
     * @return the deserialized value, or {@code null} if {@code data} was {@code null}
     */
    @Nullable T deserialize(String topic, byte @Nullable [] data);

    /**
     * Cached identity-deserializer singleton. Internal code can compare by reference
     * ({@code keyDeser == RecordDeserializer.IDENTITY_BYTES}) to detect the
     * bytes-in/bytes-out fast path and skip the per-record allocation.
     *
     * <p>Public types use {@link #bytes()} instead so the API surface still hands out
     * a typed {@code RecordDeserializer<byte[]>}.
     */
    RecordDeserializer<byte[]> IDENTITY_BYTES = (topic, data) -> data;

    /** Identity deserializer: returns the input bytes unchanged. */
    static RecordDeserializer<byte[]> bytes() {
        return IDENTITY_BYTES;
    }

    /** UTF-8 string deserializer. {@code null} bytes become {@code null}. */
    static RecordDeserializer<String> utf8String() {
        return (topic, data) -> data == null ? null : new String(data, java.nio.charset.StandardCharsets.UTF_8);
    }
}
