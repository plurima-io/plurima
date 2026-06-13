package io.plurima.kafka.internal;

import io.plurima.kafka.OrderingMode;
import io.plurima.kafka.RecordListener;
import io.plurima.kafka.deserializer.RecordDeserializer;
import io.plurima.kafka.retry.RetryPolicy;
import org.apache.kafka.clients.consumer.AcknowledgeType;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

class RetryAcceptanceTest {

    @Test
    void inlineRetrySucceedsAndAcceptsRecord() throws Exception {
        FakeShareConsumer consumer = new FakeShareConsumer();
        consumer.subscribe(List.of("t"));
        consumer.enqueue(new ConsumerRecord<>("t", 0, 1L, new byte[0], new byte[0]));

        AtomicInteger attempts = new AtomicInteger();
        CountDownLatch acceptLatch = new CountDownLatch(1);

        RecordListener<byte[], byte[]> listener = (r, ctx) -> {
            int n = attempts.incrementAndGet();
            if (n < 3) throw new IOException("transient");
            acceptLatch.countDown();
        };

        InFlightRegistry registry = new InFlightRegistry();
        AckCoordinator coordinator = new AckCoordinator(registry);
        WorkerLauncher launcher = new WorkerLauncher();
        BackpressureGate gate = new BackpressureGate(10);
        ListenerInvoker invoker = ListenerInvoker.forImplicit(listener, RecordDeserializer.bytes(), RecordDeserializer.bytes());
        WorkerProcessor processor = new WorkerProcessor(
            invoker,
            new RetryEngine(RetryPolicy.exponential()
                .maxAttempts(5)
                .initialDelay(Duration.ofMillis(30))
                .multiplier(1.0)
                .jitter(0.0)
                .retryOn(IOException.class)
                .build()),
            coordinator);
        UnorderedDispatcher dispatcher = new UnorderedDispatcher(processor, registry, coordinator, launcher, gate, OrderingMode.UNORDERED);

        PollLoop loop = new PollLoop(
            consumer, dispatcher, coordinator, registry, gate,
            Duration.ofMillis(50), Duration.ofSeconds(30), Duration.ofSeconds(5));

        Thread t = new Thread(loop, "retry-acceptance-loop");
        t.start();
        try {
            assertThat(acceptLatch.await(5, TimeUnit.SECONDS)).isTrue();

            long start = System.nanoTime();
            while (consumer.acks().isEmpty()
                && (System.nanoTime() - start) < TimeUnit.SECONDS.toNanos(3)) {
                Thread.sleep(20);
            }

            assertThat(attempts.get()).isEqualTo(3);
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
    void nonRetriableExceptionImmediatelyRejects() throws Exception {
        FakeShareConsumer consumer = new FakeShareConsumer();
        consumer.subscribe(List.of("t"));
        consumer.enqueue(new ConsumerRecord<>("t", 0, 2L, new byte[0], new byte[0]));

        CountDownLatch ran = new CountDownLatch(1);
        RecordListener<byte[], byte[]> listener = (r, ctx) -> {
            ran.countDown();
            throw new IllegalArgumentException("perm");
        };

        InFlightRegistry registry = new InFlightRegistry();
        AckCoordinator coordinator = new AckCoordinator(registry);
        WorkerLauncher launcher = new WorkerLauncher();
        BackpressureGate gate = new BackpressureGate(10);
        ListenerInvoker invoker2 = ListenerInvoker.forImplicit(listener, RecordDeserializer.bytes(), RecordDeserializer.bytes());
        WorkerProcessor processor = new WorkerProcessor(
            invoker2,
            new RetryEngine(RetryPolicy.exponential()
                .maxAttempts(5)
                .initialDelay(Duration.ofMillis(30))
                .retryOn(IOException.class)
                .build()),
            coordinator);
        UnorderedDispatcher dispatcher = new UnorderedDispatcher(processor, registry, coordinator, launcher, gate, OrderingMode.UNORDERED);

        PollLoop loop = new PollLoop(
            consumer, dispatcher, coordinator, registry, gate,
            Duration.ofMillis(50), Duration.ofSeconds(30), Duration.ofSeconds(5));

        Thread t = new Thread(loop, "retry-reject-loop");
        t.start();
        try {
            assertThat(ran.await(2, TimeUnit.SECONDS)).isTrue();
            long start = System.nanoTime();
            while (consumer.acks().isEmpty()
                && (System.nanoTime() - start) < TimeUnit.SECONDS.toNanos(2)) {
                Thread.sleep(20);
            }
            assertThat(consumer.acks())
                .extracting(FakeShareConsumer.AckCall::type)
                .containsOnly(AcknowledgeType.REJECT);
        } finally {
            loop.shutdown();
            t.join(5_000);
            launcher.close();
        }
    }
}
