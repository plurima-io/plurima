package io.plurima.kafka;

import io.plurima.kafka.ack.AckContext;
import io.plurima.kafka.ack.AckMessage;
import org.apache.kafka.clients.consumer.AcknowledgeType;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit coverage for {@link AckRecordMessage} (package-private impl of {@link AckMessage}):
 * payload/metadata come from the record, and acknowledge/accept/release/reject delegate to the
 * underlying {@link AckContext}.
 */
class AckMessageTest {

    /** Records the last acknowledge type and supplies metadata. */
    private static final class FakeAckContext implements AckContext {
        AcknowledgeType last;
        int calls;
        @Override public void acknowledge(AcknowledgeType type) { last = type; calls++; }
        @Override public short deliveryCount() { return 2; }
        @Override public Optional<Short> deliveryCountOptional() { return Optional.of((short) 2); }
        @Override public OrderingMode orderingMode() { return OrderingMode.UNORDERED; }
    }

    private static AckRecordMessage<String, String> msg(FakeAckContext ack) {
        ConsumerRecord<String, String> r = new ConsumerRecord<>("t", 0, 5L, "k", "v");
        return new AckRecordMessage<>(r, ack);
    }

    @Test
    void exposesPayloadAndMetadataFromRecordAndContext() {
        AckMessage<String, String> m = msg(new FakeAckContext());
        assertThat(m.key()).isEqualTo("k");
        assertThat(m.value()).isEqualTo("v");
        assertThat(m.offset()).isEqualTo(5L);
        assertThat(m.deliveryCount()).isEqualTo((short) 2);
        assertThat(m.orderingMode()).isEqualTo(OrderingMode.UNORDERED);
    }

    @Test
    void acceptDelegatesAccept() {
        FakeAckContext ack = new FakeAckContext();
        msg(ack).accept();
        assertThat(ack.last).isEqualTo(AcknowledgeType.ACCEPT);
    }

    @Test
    void releaseDelegatesRelease() {
        FakeAckContext ack = new FakeAckContext();
        msg(ack).release();
        assertThat(ack.last).isEqualTo(AcknowledgeType.RELEASE);
    }

    @Test
    void rejectDelegatesReject() {
        FakeAckContext ack = new FakeAckContext();
        msg(ack).reject();
        assertThat(ack.last).isEqualTo(AcknowledgeType.REJECT);
    }

    @Test
    void rawAcknowledgeDelegates() {
        FakeAckContext ack = new FakeAckContext();
        msg(ack).acknowledge(AcknowledgeType.RELEASE);
        assertThat(ack.last).isEqualTo(AcknowledgeType.RELEASE);
        assertThat(ack.calls).isEqualTo(1);
    }
}
