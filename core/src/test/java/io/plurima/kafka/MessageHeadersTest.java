package io.plurima.kafka;

import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit coverage for {@link KafkaMessageHeaders}, the package-private adapter from Kafka's own
 * {@code Headers} multi-map to the Kafka-decoupled {@link MessageHeaders} view.
 */
class MessageHeadersTest {

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    @Test
    void valuesReturnsAllValuesForNameInDeliveryOrder() {
        RecordHeaders raw = new RecordHeaders();
        raw.add("trace", bytes("first"));
        raw.add("trace", bytes("second"));
        raw.add("trace", bytes("third"));

        MessageHeaders headers = new KafkaMessageHeaders(raw);

        assertThat(headers.values("trace"))
            .extracting(b -> new String(b, StandardCharsets.UTF_8))
            .containsExactly("first", "second", "third");
    }

    @Test
    void valuesReturnsEmptyListWhenNameAbsent() {
        MessageHeaders headers = new KafkaMessageHeaders(new RecordHeaders());

        assertThat(headers.values("absent")).isEmpty();
    }

    @Test
    void lastValueReturnsMostRecentlyAddedValue() {
        RecordHeaders raw = new RecordHeaders();
        raw.add("trace", bytes("first"));
        raw.add("trace", bytes("second"));

        MessageHeaders headers = new KafkaMessageHeaders(raw);

        assertThat(headers.lastValue("trace"))
            .map(b -> new String(b, StandardCharsets.UTF_8))
            .contains("second");
    }

    @Test
    void lastValueIsEmptyWhenNameAbsent() {
        MessageHeaders headers = new KafkaMessageHeaders(new RecordHeaders());

        assertThat(headers.lastValue("absent")).isEmpty();
    }

    @Test
    void namesReturnsDistinctHeaderNames() {
        RecordHeaders raw = new RecordHeaders();
        raw.add("trace", bytes("a"));
        raw.add("trace", bytes("b"));
        raw.add("tenant", bytes("acme"));

        MessageHeaders headers = new KafkaMessageHeaders(raw);

        assertThat(headers.names()).containsExactlyInAnyOrder("trace", "tenant");
    }

    @Test
    void namesIsEmptyWhenNoHeaders() {
        MessageHeaders headers = new KafkaMessageHeaders(new RecordHeaders());

        assertThat(headers.names()).isEmpty();
    }
}
