package io.plurima.kafka;

import io.plurima.kafka.ack.AckMessage;
import io.plurima.kafka.ack.AckType;
import io.plurima.kafka.ack.MessageAckListener;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Demonstrates (and verifies) the Tier-2 ergonomics goal: a complex handler is written as a
 * class implementing {@link MessageListener}/{@link MessageAckListener} with injected
 * dependencies, free of Kafka client types, and is unit-tested directly via {@link Messages} —
 * no broker, no {@code ConsumerRecord} construction.
 */
class MessageHandlerExampleTest {

    /** A "service" the handler depends on (would be a real bean in production). */
    private static final class OrderService {
        final List<String> processed = new ArrayList<>();
        void process(String order) { processed.add(order); }
    }

    /** A complex handler as a class with a constructor-injected dependency. */
    static final class OrderHandler implements MessageListener<String, String> {
        private final OrderService service;
        OrderHandler(OrderService service) { this.service = service; }

        @Override
        public void onMessage(Message<String, String> msg) {
            if (msg.header("skip").isPresent()) return;   // business rule using metadata
            service.process(msg.value());
        }
    }

    @Test
    void autoAckHandlerProcessesValue() throws Exception {
        OrderService svc = new OrderService();
        OrderHandler handler = new OrderHandler(svc);

        handler.onMessage(Messages.of("k1", "order-1"));

        assertThat(svc.processed).containsExactly("order-1");
    }

    @Test
    void autoAckHandlerHonorsHeaderBasedRule() throws Exception {
        OrderService svc = new OrderService();
        OrderHandler handler = new OrderHandler(svc);

        handler.onMessage(Messages.builder("k2", "order-2")
            .header("skip", "true".getBytes(StandardCharsets.UTF_8)).build());

        assertThat(svc.processed).isEmpty();   // skipped via header rule
    }

    /** An explicit-ack handler exercised with a recording AckMessage. */
    @Test
    void explicitAckHandlerCanAcceptAndReject() throws Exception {
        MessageAckListener<String, String> handler = msg -> {
            if (msg.deliveryCount() > 3) msg.reject();
            else msg.accept();
        };

        RecordingAckMessage<String, String> first = new RecordingAckMessage<>("k", "v", 1);
        RecordingAckMessage<String, String> retriedTooMuch = new RecordingAckMessage<>("k", "v", 4);

        handler.onMessage(first);
        handler.onMessage(retriedTooMuch);

        assertThat(first.acked).isEqualTo(AckType.ACCEPT);
        assertThat(retriedTooMuch.acked).isEqualTo(AckType.REJECT);
    }

    /** Minimal test double for AckMessage so explicit-ack handlers are unit-testable too. */
    private static final class RecordingAckMessage<K, V> implements AckMessage<K, V> {
        private final K key; private final V value; private final int dc;
        AckType acked;
        RecordingAckMessage(K key, V value, int dc) { this.key = key; this.value = value; this.dc = dc; }
        @Override public void acknowledge(AckType type) { this.acked = type; }
        @Override public K key() { return key; }
        @Override public V value() { return value; }
        @Override public String topic() { return "t"; }
        @Override public int partition() { return 0; }
        @Override public long offset() { return 0; }
        @Override public java.time.Instant timestamp() { return java.time.Instant.EPOCH; }
        @Override public java.util.Optional<byte[]> header(String name) { return java.util.Optional.empty(); }
        @Override public MessageHeaders headers() {
            return new MessageHeaders() {
                @Override public List<byte[]> values(String name) { return List.of(); }
                @Override public java.util.Optional<byte[]> lastValue(String name) { return java.util.Optional.empty(); }
                @Override public java.util.Set<String> names() { return java.util.Set.of(); }
            };
        }
        @Override public int deliveryCount() { return dc; }
        @Override public OrderingMode orderingMode() { return OrderingMode.UNORDERED; }
    }
}
