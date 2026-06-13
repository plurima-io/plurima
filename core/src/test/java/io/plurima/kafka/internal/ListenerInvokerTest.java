package io.plurima.kafka.internal;

import io.plurima.kafka.ConsumerContext;
import io.plurima.kafka.OrderingMode;
import io.plurima.kafka.RecordListener;
import io.plurima.kafka.ack.AckContext;
import io.plurima.kafka.ack.ManualAckListener;
import io.plurima.kafka.deserializer.RecordDeserializer;
import org.apache.kafka.clients.consumer.AcknowledgeType;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ShareConsumer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class ListenerInvokerTest {

    private InFlightRegistry registry;
    private AckCoordinator coordinator;
    private ShareConsumer<byte[], byte[]> consumer;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        registry = new InFlightRegistry();
        coordinator = new AckCoordinator(registry);
        consumer = mock(ShareConsumer.class);
    }

    private InFlightRecord<byte[], byte[]> rec(long offset, byte[] key, byte[] value) {
        InFlightRecord<byte[], byte[]> r = new InFlightRecord<>(new ConsumerRecord<>("t", 0, offset, key, value));
        registry.register(r);
        return r;
    }

    private ConsumerContext ctxFor(InFlightRecord<byte[], byte[]> r) {
        return new ConsumerContext() {
            @Override public short deliveryCount() { return 1; }
            @Override public Optional<Short> deliveryCountOptional() { return Optional.of((short) 1); }
            @Override public OrderingMode orderingMode() { return OrderingMode.UNORDERED; }
        };
    }

    @Test
    void implicitInvokerDeserializesAndCallsListener() throws Throwable {
        AtomicReference<String> seenKey = new AtomicReference<>();
        AtomicReference<String> seenValue = new AtomicReference<>();
        RecordListener<String, String> listener = (r, ctx) -> {
            seenKey.set(r.key());
            seenValue.set(r.value());
        };
        ListenerInvoker invoker = ListenerInvoker.forImplicit(
            listener, RecordDeserializer.utf8String(), RecordDeserializer.utf8String());

        InFlightRecord<byte[], byte[]> r = rec(1, "k".getBytes(), "v".getBytes());
        invoker.invoke(r, ctxFor(r), coordinator);

        assertThat(seenKey.get()).isEqualTo("k");
        assertThat(seenValue.get()).isEqualTo("v");
        assertThat(invoker.isManualAck()).isFalse();
    }

    @Test
    void implicitInvokerDoesNotAckOnReturn() throws Throwable {
        RecordListener<byte[], byte[]> listener = (r, ctx) -> { /* ok */ };
        ListenerInvoker invoker = ListenerInvoker.forImplicit(
            listener, RecordDeserializer.bytes(), RecordDeserializer.bytes());

        InFlightRecord<byte[], byte[]> r = rec(2, new byte[0], new byte[0]);
        invoker.invoke(r, ctxFor(r), coordinator);

        // The invoker itself does NOT queue an ack — that's WorkerProcessor's job. So when
        // commitPendingAcks drains, the queue is empty and (post the "skip commitAsync when
        // nothing drained" change) no commitAsync is issued. The original asserting-commit
        // behavior here was incidental.
        coordinator.commitPendingAcks(consumer);
        verify(consumer, org.mockito.Mockito.never())
            .acknowledge(
                org.mockito.ArgumentMatchers.<ConsumerRecord<byte[], byte[]>>any(),
                any(AcknowledgeType.class));
        verify(consumer, org.mockito.Mockito.never()).commitAsync();
    }

    @Test
    void manualInvokerExposesAckContext() throws Throwable {
        AtomicReference<AckContext> seenAck = new AtomicReference<>();
        ManualAckListener<String, String> listener = (r, ack) -> {
            seenAck.set(ack);
            ack.acknowledge(AcknowledgeType.ACCEPT);
        };
        ListenerInvoker invoker = ListenerInvoker.forManual(
            listener, RecordDeserializer.utf8String(), RecordDeserializer.utf8String());

        InFlightRecord<byte[], byte[]> r = rec(3, "k".getBytes(), "v".getBytes());
        invoker.invoke(r, ctxFor(r), coordinator);

        assertThat(seenAck.get()).isNotNull();
        assertThat(invoker.isManualAck()).isTrue();

        coordinator.commitPendingAcks(consumer);
        verify(consumer).acknowledge(
            argThat(cr -> cr.topic().equals("t") && cr.partition() == 0 && cr.offset() == 3L),
            eq(AcknowledgeType.ACCEPT));
    }

    @Test
    void manualAckContextIsIdempotent() throws Throwable {
        ManualAckListener<byte[], byte[]> listener = (r, ack) -> {
            ack.acknowledge(AcknowledgeType.ACCEPT);
            ack.acknowledge(AcknowledgeType.REJECT); // second call ignored
        };
        ListenerInvoker invoker = ListenerInvoker.forManual(
            listener, RecordDeserializer.bytes(), RecordDeserializer.bytes());

        InFlightRecord<byte[], byte[]> r = rec(4, new byte[0], new byte[0]);
        invoker.invoke(r, ctxFor(r), coordinator);

        coordinator.commitPendingAcks(consumer);
        verify(consumer).acknowledge(
            argThat(cr -> cr.topic().equals("t") && cr.partition() == 0 && cr.offset() == 4L),
            eq(AcknowledgeType.ACCEPT));
        verify(consumer).commitAsync();
        org.mockito.Mockito.verifyNoMoreInteractions(consumer);
    }

    @Test
    void manualInvokerPropagatesListenerException() {
        ManualAckListener<byte[], byte[]> listener = (r, ack) -> {
            throw new RuntimeException("boom");
        };
        ListenerInvoker invoker = ListenerInvoker.forManual(
            listener, RecordDeserializer.bytes(), RecordDeserializer.bytes());

        InFlightRecord<byte[], byte[]> r = rec(5, new byte[0], new byte[0]);
        org.junit.jupiter.api.Assertions.assertThrows(RuntimeException.class,
            () -> invoker.invoke(r, ctxFor(r), coordinator));
    }

    @Test
    void manualInvokerAutoReleasesWhenListenerForgetsToAck() throws Throwable {
        ManualAckListener<byte[], byte[]> listener = (r, ack) -> { /* forgot to call ack.acknowledge */ };
        ListenerInvoker invoker = ListenerInvoker.forManual(
            listener, RecordDeserializer.bytes(), RecordDeserializer.bytes());

        InFlightRecord<byte[], byte[]> r = rec(6, new byte[0], new byte[0]);
        invoker.invoke(r, ctxFor(r), coordinator);

        coordinator.commitPendingAcks(consumer);
        org.mockito.Mockito.verify(consumer).acknowledge(
            org.mockito.ArgumentMatchers.<ConsumerRecord<byte[], byte[]>>argThat(
                cr -> cr.topic().equals("t") && cr.offset() == 6L),
            org.mockito.ArgumentMatchers.eq(AcknowledgeType.RELEASE));
    }

    @Test
    void manualInvokerDropsRenewAckWithWarnLog() throws Throwable {
        java.util.concurrent.atomic.AtomicBoolean listenerCompleted = new java.util.concurrent.atomic.AtomicBoolean(false);
        ManualAckListener<byte[], byte[]> listener = (r, ack) -> {
            ack.acknowledge(AcknowledgeType.RENEW); // should be silently dropped
            listenerCompleted.set(true);
        };
        ListenerInvoker invoker = ListenerInvoker.forManual(
            listener, RecordDeserializer.bytes(), RecordDeserializer.bytes());

        InFlightRecord<byte[], byte[]> r = rec(7, new byte[0], new byte[0]);
        invoker.invoke(r, ctxFor(r), coordinator);

        // RENEW was dropped, so wasAcked() is still false — auto-RELEASE should follow
        coordinator.commitPendingAcks(consumer);
        // Only the auto-RELEASE (queued by forManual because listener didn't ack) arrives
        org.mockito.Mockito.verify(consumer).acknowledge(
            org.mockito.ArgumentMatchers.<ConsumerRecord<byte[], byte[]>>argThat(
                cr -> cr.topic().equals("t") && cr.offset() == 7L),
            org.mockito.ArgumentMatchers.eq(AcknowledgeType.RELEASE));
        assertThat(listenerCompleted.get()).isTrue();
    }

    @Test
    void manualInvokerLateAsyncAckIsSwallowedAfterAutoRelease() throws Throwable {
        AtomicReference<AckContext> escaped = new AtomicReference<>();
        ManualAckListener<byte[], byte[]> listener = (r, ack) -> {
            escaped.set(ack);  // capture and return without acking
        };
        ListenerInvoker invoker = ListenerInvoker.forManual(
            listener, RecordDeserializer.bytes(), RecordDeserializer.bytes());

        InFlightRecord<byte[], byte[]> r = rec(8, new byte[0], new byte[0]);
        invoker.invoke(r, ctxFor(r), coordinator);

        // Auto-RELEASE was queued. Now the user "remembers" and calls ack late:
        escaped.get().acknowledge(AcknowledgeType.ACCEPT);

        coordinator.commitPendingAcks(consumer);
        // Only ONE ack should have been delivered to the consumer (the auto-RELEASE).
        org.mockito.Mockito.verify(consumer, org.mockito.Mockito.times(1)).acknowledge(
            org.mockito.ArgumentMatchers.<ConsumerRecord<byte[], byte[]>>any(),
            org.mockito.ArgumentMatchers.any(AcknowledgeType.class));
        // Specifically, only the RELEASE was queued — the late ACCEPT was dropped.
        org.mockito.Mockito.verify(consumer).acknowledge(
            org.mockito.ArgumentMatchers.<ConsumerRecord<byte[], byte[]>>argThat(
                cr -> cr.topic().equals("t") && cr.offset() == 8L),
            org.mockito.ArgumentMatchers.eq(AcknowledgeType.RELEASE));
    }
}
