package io.plurima.kafka;

import io.plurima.kafka.ack.AckContext;
import io.plurima.kafka.ack.AckMessage;
import io.plurima.kafka.ack.AckType;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit coverage for {@link AckRecordMessage} (package-private impl of {@link AckMessage}):
 * payload/metadata come from the record, and acknowledge/accept/release/reject delegate to the
 * underlying {@link AckContext}.
 */
class AckMessageTest {

    /** Records the last acknowledge type and supplies metadata. */
    private static final class FakeAckContext implements AckContext {
        AckType last;
        int calls;
        @Override public void acknowledge(AckType type) { last = type; calls++; }
        @Override public int deliveryCount() { return 2; }
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
        assertThat(m.deliveryCount()).isEqualTo(2);
        assertThat(m.orderingMode()).isEqualTo(OrderingMode.UNORDERED);
    }

    @Test
    void acceptDelegatesAccept() {
        FakeAckContext ack = new FakeAckContext();
        msg(ack).accept();
        assertThat(ack.last).isEqualTo(AckType.ACCEPT);
    }

    @Test
    void releaseDelegatesRelease() {
        FakeAckContext ack = new FakeAckContext();
        msg(ack).release();
        assertThat(ack.last).isEqualTo(AckType.RELEASE);
    }

    @Test
    void rejectDelegatesReject() {
        FakeAckContext ack = new FakeAckContext();
        msg(ack).reject();
        assertThat(ack.last).isEqualTo(AckType.REJECT);
    }

    @Test
    void rawAcknowledgeDelegates() {
        FakeAckContext ack = new FakeAckContext();
        msg(ack).acknowledge(AckType.RELEASE);
        assertThat(ack.last).isEqualTo(AckType.RELEASE);
        assertThat(ack.calls).isEqualTo(1);
    }

    @Test
    void acknowledgeAckTypeAcceptBehavesAsAccept() {
        FakeAckContext viaAccept = new FakeAckContext();
        msg(viaAccept).accept();

        FakeAckContext viaAcknowledge = new FakeAckContext();
        msg(viaAcknowledge).acknowledge(AckType.ACCEPT);

        assertThat(viaAcknowledge.last).isEqualTo(viaAccept.last);
        assertThat(viaAcknowledge.calls).isEqualTo(viaAccept.calls).isEqualTo(1);
    }
}
