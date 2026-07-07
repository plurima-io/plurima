package io.plurima.kafka.integration;

import io.plurima.kafka.PlurimaConsumer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static io.plurima.kafka.integration.KafkaIntegrationSupport.*;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that {@link PlurimaConsumer#close()} shuts down cleanly even when workers are
 * mid-flight, completing within a reasonable wall-clock budget without hanging or throwing.
 */
@Tag("integration")
class ShutdownIntegrationTest {

    @Test
    void closeDuringInflightWorkersCompletesGracefully() throws Exception {
        String topic = createUniqueTopic("plurima-int-shutdown");
        String groupId = "plurima-int-shutdown-group-" + UUID.randomUUID();

        AtomicInteger processed = new AtomicInteger();
        CountDownLatch firstProcessed = new CountDownLatch(1);

        PlurimaConsumer<byte[], byte[]> consumer = PlurimaConsumer.builder()
            .kafkaProperties(consumerProps(groupId))
            .topic(topic)
            .pollTimeout(Duration.ofMillis(100))
            .concurrency(2)
            .listener((r, ctx) -> {
                firstProcessed.countDown();
                try {
                    Thread.sleep(500);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                }
                processed.incrementAndGet();
            })
            .build();
        consumer.start();

        try {
            waitForAssignment(groupId, topic);

            try (var producer = byteProducer()) {
                for (int i = 0; i < 4; i++) {
                    producer.send(new ProducerRecord<>(
                        topic, ("k" + i).getBytes(), "v".getBytes())).get();
                }
            }

            // Wait for the first record to enter the handler before initiating close.
            assertThat(firstProcessed.await(60, TimeUnit.SECONDS))
                .as("at least one record must start processing")
                .isTrue();

            long start = System.nanoTime();
            consumer.close();
            long elapsedMs = (System.nanoTime() - start) / 1_000_000;

            // close() should complete well within the 30 s shutdownDrainTimeout default.
            // Allow 20 s to account for broker load in combined test runs.
            assertThat(elapsedMs)
                .as("close must complete within 20 s (normal drain, not timeout)")
                .isLessThan(20_000);

            // At least the one record we know started must have finished.
            assertThat(processed.get())
                .as("at least one record must have completed processing")
                .isGreaterThanOrEqualTo(1);
        } finally {
            // Guard: if test fails before close(), try closing and then clean up.
            try { consumer.close(); } catch (Exception ignored) {}
            deleteTopicQuietly(topic);
        }
    }
}
