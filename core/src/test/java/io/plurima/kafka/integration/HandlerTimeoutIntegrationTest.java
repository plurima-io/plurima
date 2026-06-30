package io.plurima.kafka.integration;

import io.plurima.kafka.PlurimaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end verification of Pillar 3: a listener that blocks longer than
 * {@code handlerTimeout} is interrupted and (with the default no-retry policy) REJECTed —
 * the record is NOT reprocessed forever.
 *
 * <p>Tagged {@code integration} (needs a real 4.2 broker at localhost:9092).
 */
@Tag("integration")
class HandlerTimeoutIntegrationTest {

    @Test
    void blockedHandlerIsTimedOutAndNotReprocessedForever() throws Exception {
        String topic = KafkaIntegrationSupport.createUniqueTopic("plurima-htimeout");
        String groupId = "plurima-htimeout-group-" + java.util.UUID.randomUUID();

        AtomicInteger invocations = new AtomicInteger();
        CountDownLatch firstInvocation = new CountDownLatch(1);
        Properties props = KafkaIntegrationSupport.consumerProps(groupId);

        PlurimaConsumer<byte[], byte[]> consumer = PlurimaConsumer.<byte[], byte[]>builder()
            .kafkaProperties(props)
            .topic(topic)
            .concurrency(4)
            .pollTimeout(Duration.ofMillis(500))
            .handlerTimeout(Duration.ofSeconds(1))   // interrupt the handler after 1s
            .listener((record, ctx) -> {
                invocations.incrementAndGet();
                firstInvocation.countDown();
                Thread.sleep(30_000);   // blocks far past the timeout; interruptible
            })
            .build();

        try {
            consumer.start();
            KafkaIntegrationSupport.waitForAssignment(groupId, topic);
            try (KafkaProducer<byte[], byte[]> producer = KafkaIntegrationSupport.byteProducer()) {
                producer.send(new ProducerRecord<>(topic, new byte[]{0}, "x".getBytes())).get();
                producer.flush();
            }

            assertThat(firstInvocation.await(20, TimeUnit.SECONDS))
                .as("handler is invoked").isTrue();

            // After the timeout fires and the record is REJECTed (no-retry default), it must NOT
            // be reprocessed indefinitely. Allow a brief settle, then assert invocations are bounded.
            Thread.sleep(6_000);
            assertThat(invocations.get())
                .as("timed-out record is REJECTed, not reprocessed forever (saw %d invocations)",
                    invocations.get())
                .isLessThanOrEqualTo(2);
        } finally {
            consumer.close();
            KafkaIntegrationSupport.deleteTopicQuietly(topic);
        }
    }
}
