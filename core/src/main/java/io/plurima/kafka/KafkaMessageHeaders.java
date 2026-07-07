package io.plurima.kafka;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * Package-private lazy adapter from Kafka's own {@link Headers} multi-map to the Kafka-decoupled
 * {@link MessageHeaders} view. Not part of the public API — {@link RecordMessage} and
 * {@link AckRecordMessage} expose it only through the {@link MessageHeaders} interface.
 *
 * <p>Wraps the record's {@link Headers} by reference and re-derives each query on demand; no
 * eager copy into a Plurima-owned structure happens at construction. The {@code byte[]} arrays
 * returned by {@link #values} and {@link #lastValue} are the live Kafka buffers backing the
 * record (not defensively copied) — the same contract {@link Header#value()} itself exposes.
 * Callers must treat them as read-only.
 */
final class KafkaMessageHeaders implements MessageHeaders {

    private final Headers headers;

    KafkaMessageHeaders(Headers headers) {
        this.headers = Objects.requireNonNull(headers, "headers");
    }

    @Override
    public List<byte[]> values(String name) {
        List<byte[]> values = new ArrayList<>();
        for (Header header : headers.headers(name)) {
            values.add(header.value());
        }
        return values;
    }

    @Override
    public Optional<byte[]> lastValue(String name) {
        Header header = headers.lastHeader(name);
        return header == null ? Optional.empty() : Optional.ofNullable(header.value());
    }

    @Override
    public Set<String> names() {
        Set<String> names = new LinkedHashSet<>();
        for (Header header : headers) {
            names.add(header.key());
        }
        return names;
    }
}
