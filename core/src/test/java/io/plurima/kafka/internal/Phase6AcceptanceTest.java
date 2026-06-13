package io.plurima.kafka.internal;

import io.plurima.kafka.OrderingMode;
import io.plurima.kafka.RecordListener;
import io.plurima.kafka.ack.ManualAckListener;
import io.plurima.kafka.deserializer.RecordDeserializer;
import io.plurima.kafka.retry.RetryPolicy;
import org.apache.kafka.clients.consumer.AcknowledgeType;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

class Phase6AcceptanceTest {

    @Test
    void typedDeserializationDeliversStringRecord() throws Exception {
        FakeShareConsumer consumer = new FakeShareConsumer();
        consumer.subscribe(List.of("t"));
        consumer.enqueue(new ConsumerRecord<>(
            "t", 0, 1L, "hello".getBytes(), "world".getBytes()));

        AtomicReference<String> seenKey = new AtomicReference<>();
        AtomicReference<String> seenValue = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);
        RecordListener<String, String> listener = (r, ctx) -> {
            seenKey.set(r.key());
            seenValue.set(r.value());
            latch.countDown();
        };

        InFlightRegistry registry = new InFlightRegistry();
        AckCoordinator coordinator = new AckCoordinator(registry);
        WorkerLauncher launcher = new WorkerLauncher();
        BackpressureGate gate = new BackpressureGate(10);
        ListenerInvoker invoker = ListenerInvoker.forImplicit(
            listener,
            RecordDeserializer.utf8String(),
            RecordDeserializer.utf8String());
        WorkerProcessor processor = new WorkerProcessor(
            invoker, new RetryEngine(RetryPolicy.noRetry()), coordinator);
        UnorderedDispatcher dispatcher = new UnorderedDispatcher(processor, registry, coordinator, launcher, gate, OrderingMode.UNORDERED);

        PollLoop loop = new PollLoop(
            consumer, dispatcher, coordinator, registry, gate,
            Duration.ofMillis(50), Duration.ofSeconds(30), Duration.ofSeconds(5));

        Thread t = new Thread(loop, "deserializer-acceptance-loop");
        t.start();
        try {
            assertThat(latch.await(3, TimeUnit.SECONDS)).isTrue();
            assertThat(seenKey.get()).isEqualTo("hello");
            assertThat(seenValue.get()).isEqualTo("world");

            long start = System.nanoTime();
            while (consumer.acks().isEmpty()
                && (System.nanoTime() - start) < TimeUnit.SECONDS.toNanos(2)) {
                Thread.sleep(20);
            }
            assertThat(consumer.acks())
                .extracting(FakeShareConsumer.AckCall::type)
                .containsOnly(AcknowledgeType.ACCEPT);
        } finally {
            loop.shutdown();
            t.join(5_000);
            launcher.close();
        }
    }

    @Test
    void manualAckListenerControlsAck() throws Exception {
        FakeShareConsumer consumer = new FakeShareConsumer();
        consumer.subscribe(List.of("t"));
        consumer.enqueue(new ConsumerRecord<>("t", 0, 2L, "k".getBytes(), "v".getBytes()));
        consumer.enqueue(new ConsumerRecord<>("t", 0, 3L, "k".getBytes(), "v".getBytes()));

        CountDownLatch latch = new CountDownLatch(2);
        ManualAckListener<String, String> listener = (r, ack) -> {
            // Offset 2 → ACCEPT. Offset 3 → REJECT.
            if (r.offset() == 2L) ack.acknowledge(AcknowledgeType.ACCEPT);
            else ack.acknowledge(AcknowledgeType.REJECT);
            latch.countDown();
        };

        InFlightRegistry registry = new InFlightRegistry();
        AckCoordinator coordinator = new AckCoordinator(registry);
        WorkerLauncher launcher = new WorkerLauncher();
        BackpressureGate gate = new BackpressureGate(10);
        ListenerInvoker invoker = ListenerInvoker.forManual(
            listener,
            RecordDeserializer.utf8String(),
            RecordDeserializer.utf8String());
        WorkerProcessor processor = new WorkerProcessor(
            invoker, new RetryEngine(RetryPolicy.noRetry()), coordinator);
        UnorderedDispatcher dispatcher = new UnorderedDispatcher(processor, registry, coordinator, launcher, gate, OrderingMode.UNORDERED);

        PollLoop loop = new PollLoop(
            consumer, dispatcher, coordinator, registry, gate,
            Duration.ofMillis(50), Duration.ofSeconds(30), Duration.ofSeconds(5));

        Thread t = new Thread(loop, "manual-ack-acceptance-loop");
        t.start();
        try {
            assertThat(latch.await(3, TimeUnit.SECONDS)).isTrue();

            long start = System.nanoTime();
            while (consumer.acks().size() < 2
                && (System.nanoTime() - start) < TimeUnit.SECONDS.toNanos(2)) {
                Thread.sleep(20);
            }

            assertThat(consumer.acks())
                .extracting(c -> c.offset() + ":" + c.type())
                .containsExactlyInAnyOrder("2:accept", "3:reject");
        } finally {
            loop.shutdown();
            t.join(5_000);
            launcher.close();
        }
    }
}
