package io.plurima.kafka.integration;

import io.plurima.kafka.PlurimaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Real-traffic load test: a high volume of records across multiple partitions, processed
 * concurrently via the Tier-2 {@code onMessage} API, must all be delivered with no loss. Exercises
 * the SHARE engine under sustained throughput (fetch → dispatch → ack pipeline, backpressure gate,
 * commit batching) rather than the small fixed counts of the focused tests.
 *
 * <p>Tagged {@code integration} (needs a real 4.2 broker at localhost:9092).
 */
@Tag("integration")
class ShareHighVolumeIntegrationTest {

    @Test
    void highVolumeAcrossPartitionsProcessesEveryRecordWithNoLoss() throws Exception {
        String topic = KafkaIntegrationSupport.createUniqueTopic("plurima-highvol", 4);
        String groupId = "plurima-highvol-group-" + java.util.UUID.randomUUID();
        int total = 3000;

        Set<String> distinct = ConcurrentHashMap.newKeySet();
        AtomicInteger invocations = new AtomicInteger();
        AtomicInteger maxDeliveryCount = new AtomicInteger();
        CountDownLatch done = new CountDownLatch(total);
        Properties props = KafkaIntegrationSupport.consumerProps(groupId);

        PlurimaConsumer<byte[], byte[]> consumer = PlurimaConsumer.builder()
            .kafkaProperties(props)
            .topic(topic)
            .concurrency(50)
            .pollTimeout(Duration.ofMillis(200))
            .onMessage(msg -> {
                invocations.incrementAndGet();
                maxDeliveryCount.accumulateAndGet(msg.deliveryCount(), Math::max);
                // A touch of realistic per-record work so concurrency/backpressure actually engage.
                Thread.sleep(1);
                if (distinct.add(new String(msg.value(), StandardCharsets.UTF_8))) done.countDown();
            })
            .build();

        try {
            consumer.start();
            KafkaIntegrationSupport.waitForAssignment(groupId, topic);

            long startNanos = System.nanoTime();
            try (KafkaProducer<byte[], byte[]> producer = KafkaIntegrationSupport.byteProducer()) {
                for (int i = 0; i < total; i++) {
                    producer.send(new ProducerRecord<>(topic,
                        ("k" + (i % 500)).getBytes(StandardCharsets.UTF_8),
                        ("v" + i).getBytes(StandardCharsets.UTF_8)));
                }
                producer.flush();
            }

            boolean all = done.await(120, TimeUnit.SECONDS);
            long ms = (System.nanoTime() - startNanos) / 1_000_000L;
            System.out.printf("[high-volume] %d/%d distinct in %d ms (%.0f rec/s); invocations=%d dups=%d maxDeliveryCount=%d%n",
                distinct.size(), total, ms, total * 1000.0 / Math.max(1, ms),
                invocations.get(), invocations.get() - total, maxDeliveryCount.get());

            assertThat(all).as("every one of %d records processed (no loss)", total).isTrue();
            assertThat(distinct).hasSize(total);
        } finally {
            consumer.close();
            KafkaIntegrationSupport.deleteTopicQuietly(topic);
        }
    }
}
