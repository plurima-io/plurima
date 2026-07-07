package io.plurima.kafka;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit coverage for the {@link Messages} factory and the {@link Message} accessors it produces. */
class MessagesTest {

    @Test
    void ofValueOnlyHasNullKeyAndDeliveryCountOne() {
        Message<byte[], String> m = Messages.of("payload");
        assertThat(m.key()).isNull();
        assertThat(m.value()).isEqualTo("payload");
        assertThat(m.deliveryCount()).isEqualTo(1);
        assertThat(m.orderingMode()).isEqualTo(OrderingMode.UNORDERED);
    }

    @Test
    void ofKeyValueExposesBoth() {
        Message<String, String> m = Messages.of("k", "v");
        assertThat(m.key()).isEqualTo("k");
        assertThat(m.value()).isEqualTo("v");
        assertThat(m.topic()).isEqualTo("test-topic");
        assertThat(m.partition()).isZero();
        assertThat(m.offset()).isZero();
    }

    @Test
    void explicitDeliveryCountIsCarried() {
        Message<String, String> m = Messages.of("k", "v", 4);
        assertThat(m.deliveryCount()).isEqualTo(4);
    }

    @Test
    void deliveryCountBeyondShortRangeIsRejected() {
        // The test builder still round-trips through a ConsumerRecord, whose native
        // deliveryCount field is a short (KIP-932 wire shape) — fail fast on overflow
        // instead of silently truncating.
        org.assertj.core.api.Assertions.assertThatThrownBy(
                () -> Messages.builder("k", "v").deliveryCount(Short.MAX_VALUE + 1).build())
            .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void nullValueIsAllowed() {
        Message<String, String> m = Messages.of("k", null);
        assertThat(m.value()).isNull();
        assertThat(m.key()).isEqualTo("k");
    }

    @Test
    void builderSetsAllFields() {
        Message<String, String> m = Messages.builder("k", "v")
            .topic("orders").partition(3).offset(42L)
            .timestampMillis(1_000L)
            .deliveryCount(2)
            .orderingMode(OrderingMode.KEY)
            .header("x-tenant", "acme".getBytes(StandardCharsets.UTF_8))
            .build();

        assertThat(m.topic()).isEqualTo("orders");
        assertThat(m.partition()).isEqualTo(3);
        assertThat(m.offset()).isEqualTo(42L);
        assertThat(m.timestamp()).isEqualTo(Instant.ofEpochMilli(1_000L));
        assertThat(m.deliveryCount()).isEqualTo(2);
        assertThat(m.orderingMode()).isEqualTo(OrderingMode.KEY);
    }

    @Test
    void headerLookupReturnsLastValueOrEmpty() {
        Message<String, String> m = Messages.builder("k", "v")
            .header("h", "first".getBytes(StandardCharsets.UTF_8))
            .header("h", "second".getBytes(StandardCharsets.UTF_8))   // last wins
            .build();

        assertThat(m.header("h")).map(b -> new String(b, StandardCharsets.UTF_8)).contains("second");
        assertThat(m.header("absent")).isEqualTo(Optional.empty());
        assertThat(m.headers()).isNotNull();
    }

    @Test
    void builderHeaderAccumulationRoundTripsThroughMessageHeadersView() {
        Message<String, String> m = Messages.builder("k", "v")
            .header("trace", "first".getBytes(StandardCharsets.UTF_8))
            .header("trace", "second".getBytes(StandardCharsets.UTF_8))
            .header("tenant", "acme".getBytes(StandardCharsets.UTF_8))
            .build();

        MessageHeaders headers = m.headers();

        assertThat(headers.values("trace"))
            .extracting(b -> new String(b, StandardCharsets.UTF_8))
            .containsExactly("first", "second");
        assertThat(headers.lastValue("trace"))
            .map(b -> new String(b, StandardCharsets.UTF_8))
            .contains("second");
        assertThat(headers.names()).containsExactlyInAnyOrder("trace", "tenant");
    }
}
