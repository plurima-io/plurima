package io.plurima.kafka.internal;

import io.plurima.kafka.OrderingMode;
import io.plurima.kafka.RecordListener;
import io.plurima.kafka.deserializer.RecordDeserializer;
import io.plurima.kafka.dlt.DltConfig;
import io.plurima.kafka.retry.RetryPolicy;
import org.apache.kafka.clients.consumer.AcknowledgeType;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

class DltAcceptanceTest {

    @Test
    void exhaustedRetryRoutesToDltAndAcceptsOriginal() throws Exception {
        FakeShareConsumer consumer = new FakeShareConsumer();
        consumer.subscribe(List.of("orders"));
        consumer.enqueue(new ConsumerRecord<>(
            "orders", 0, 11L, "k".getBytes(), "v".getBytes()));

        AtomicInteger attempts = new AtomicInteger();
        CountDownLatch ran = new CountDownLatch(2); // initial + 1 retry
        RecordListener<byte[], byte[]> listener = (r, ctx) -> {
            attempts.incrementAndGet();
            ran.countDown();
            throw new IOException("always");
        };

        MockProducer<byte[], byte[]> dltProducer = new MockProducer<>(
            true, null, new ByteArraySerializer(), new ByteArraySerializer());
        Properties pp = new Properties();
        pp.put("bootstrap.servers", "ignored");
        DltConfig dltCfg = DltConfig.builder().producerProperties(pp).build();
        DltRouter dlt = new DltRouter(dltProducer, dltCfg);

        InFlightRegistry registry = new InFlightRegistry();
        AckCoordinator coordinator = new AckCoordinator(registry);
        WorkerLauncher launcher = new WorkerLauncher();
        BackpressureGate gate = new BackpressureGate(10);
        ListenerInvoker invoker = ListenerInvoker.forImplicit(listener, RecordDeserializer.bytes(), RecordDeserializer.bytes());
        WorkerProcessor processor = new WorkerProcessor(
            invoker,
            new RetryEngine(RetryPolicy.exponential()
                .maxAttempts(1)
                .initialDelay(Duration.ofMillis(20))
                .multiplier(1.0)
                .jitter(0.0)
                .retryOn(IOException.class)
                .build()),
            coordinator, dlt);
        UnorderedDispatcher dispatcher = new UnorderedDispatcher(processor, registry, coordinator, launcher, gate, OrderingMode.UNORDERED);

        PollLoop loop = new PollLoop(
            consumer, dispatcher, coordinator, registry, gate,
            Duration.ofMillis(50), Duration.ofSeconds(30), Duration.ofSeconds(5));

        Thread t = new Thread(loop, "dlt-acceptance-loop");
        t.start();
        try {
            assertThat(ran.await(5, TimeUnit.SECONDS)).isTrue();

            long start = System.nanoTime();
            while (dltProducer.history().isEmpty()
                && (System.nanoTime() - start) < TimeUnit.SECONDS.toNanos(3)) {
                Thread.sleep(20);
            }
            assertThat(dltProducer.history()).hasSize(1);
            assertThat(dltProducer.history().get(0).topic()).isEqualTo("orders.DLT");

            long start2 = System.nanoTime();
            while (consumer.acks().isEmpty()
                && (System.nanoTime() - start2) < TimeUnit.SECONDS.toNanos(3)) {
                Thread.sleep(20);
            }
            assertThat(consumer.acks())
                .extracting(FakeShareConsumer.AckCall::type)
                .containsOnly(AcknowledgeType.ACCEPT);
        } finally {
            loop.shutdown();
            t.join(5_000);
            launcher.close();
            dlt.close();
        }

        // Sanity: the DLT record carries the canonical headers
        var sent = dltProducer.history().get(0);
        java.util.Map<String, String> headers = new java.util.HashMap<>();
        sent.headers().forEach(h -> headers.put(h.key(), new String(h.value(), StandardCharsets.UTF_8)));
        assertThat(headers)
            .containsEntry("plurima-dlt-original-topic", "orders")
            .containsEntry("plurima-dlt-original-partition", "0")
            .containsEntry("plurima-dlt-original-offset", "11")
            .containsEntry("plurima-dlt-failure-class", "java.io.IOException");
    }
}
