package io.plurima.kafka.integration;

import io.plurima.kafka.PlurimaConsumer;
import io.plurima.kafka.retry.RetryPolicy;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static io.plurima.kafka.integration.KafkaIntegrationSupport.*;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that inline retry actually retries until success against a real broker.
 * <p>
 * The listener throws {@link IOException} on the first two attempts and succeeds on the third.
 * The test asserts that exactly 3 attempts were made.
 */
@Tag("integration")
class RetryIntegrationTest {

    @Test
    void inlineRetrySucceedsAfterTransientFailures() throws Exception {
        String topic = createUniqueTopic("plurima-int-retry");
        String groupId = "plurima-int-retry-group-" + UUID.randomUUID();

        AtomicInteger attempts = new AtomicInteger();
        CountDownLatch succeeded = new CountDownLatch(1);

        try (PlurimaConsumer<byte[], byte[]> consumer = PlurimaConsumer.builder()
                .kafkaProperties(consumerProps(groupId))
                .topic(topic)
                .pollTimeout(Duration.ofMillis(200))
                .retry(RetryPolicy.exponential()
                    .maxAttempts(5)
                    .initialDelay(Duration.ofMillis(20))
                    .multiplier(1.0)
                    .jitter(0.0)
                    .retryOn(IOException.class)
                    .build())
                .listener((r, ctx) -> {
                    int n = attempts.incrementAndGet();
                    if (n < 3) throw new IOException("transient failure " + n);
                    succeeded.countDown();
                })
                .build()) {
            consumer.start();
            waitForAssignment(groupId, topic);

            try (var producer = byteProducer()) {
                producer.send(new ProducerRecord<>(
                    topic, "k".getBytes(), "v".getBytes())).get();
            }

            assertThat(succeeded.await(90, TimeUnit.SECONDS))
                .as("listener must succeed within timeout (attempts so far: %d)", attempts.get())
                .isTrue();
            assertThat(attempts.get())
                .as("exactly 3 attempts: 2 failures + 1 success")
                .isEqualTo(3);
        } finally {
            deleteTopicQuietly(topic);
        }
    }
}
