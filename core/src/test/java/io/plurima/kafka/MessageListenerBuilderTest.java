package io.plurima.kafka;

import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Builder wiring/validation for the Tier-2 {@code onMessage} / {@code onMessageAck} methods. */
class MessageListenerBuilderTest {

    private static Properties props() {
        Properties p = new Properties();
        p.put("bootstrap.servers", "localhost:9092");
        p.put("group.id", "g");
        return p;
    }

    @Test
    void onMessageBuildsSuccessfully() {
        PlurimaConsumer<byte[], byte[]> c = PlurimaConsumer.builder()
            .kafkaProperties(props())
            .topic("t")
            .onMessage(msg -> { /* handle */ })
            .build();
        assertThat(c).isNotNull();
    }

    @Test
    void onMessageAckBuildsSuccessfully() {
        PlurimaConsumer<byte[], byte[]> c = PlurimaConsumer.builder()
            .kafkaProperties(props())
            .topic("t")
            .onMessageAck(AckMessageHelper::acceptAll)
            .build();
        assertThat(c).isNotNull();
    }

    @Test
    void onMessageAndOnMessageAckAreMutuallyExclusive() {
        // onMessage sets the auto-ack listener, onMessageAck sets the manual-ack listener →
        // build() rejects having both (same guard as listener + manualAckListener).
        assertThatThrownBy(() -> PlurimaConsumer.builder()
            .kafkaProperties(props())
            .topic("t")
            .onMessage(msg -> { })
            .onMessageAck(AckMessageHelper::acceptAll)
            .build())
            .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void duplicateHandlerInSameAutoBucketIsRejectedImmediately() {
        // listener + onMessage both target the auto-ack bucket; setting both must be rejected
        // (exactly one handler), not silently overwrite.
        assertThatThrownBy(() -> PlurimaConsumer.builder()
            .topic("t")
            .listener((r, ctx) -> { })
            .onMessage(msg -> { }))
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("already configured");
    }

    @Test
    void listenerThenManualAckIsRejectedAtTheSecondCall() {
        assertThatThrownBy(() -> PlurimaConsumer.builder()
            .topic("t")
            .listener((r, ctx) -> { })
            .manualAckListener((r, ack) -> { }))
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("already configured");
    }

    @Test
    void onMessageRejectsNull() {
        assertThatThrownBy(() -> PlurimaConsumer.builder().onMessage(null))
            .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> PlurimaConsumer.builder().onMessageAck(null))
            .isInstanceOf(NullPointerException.class);
    }

    @Test
    void listenerAndManualAckListenerRejectNull() {
        assertThatThrownBy(() -> PlurimaConsumer.builder().listener(null))
            .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> PlurimaConsumer.builder().manualAckListener(null))
            .isInstanceOf(NullPointerException.class);
    }

    @Test
    void rejectedNullListenerDoesNotWedgeTheBuilder() {
        // Null is rejected BEFORE the handler-configured flag is set, so a real handler can still
        // be configured afterwards (the builder isn't left half-configured / "already configured").
        PlurimaConsumerBuilder<byte[], byte[]> b = PlurimaConsumer.builder()
            .kafkaProperties(props()).topic("t");
        try { b.listener(null); } catch (NullPointerException expected) { /* rejected pre-mark */ }
        assertThat(b.onMessage(msg -> { }).build()).isNotNull();
    }

    private static final class AckMessageHelper {
        static <K, V> void acceptAll(io.plurima.kafka.ack.AckMessage<K, V> m) { m.accept(); }
    }
}
