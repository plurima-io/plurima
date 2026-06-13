package io.plurima.kafka.integration;

import io.plurima.kafka.PlurimaConsumer;
import io.plurima.kafka.deserializer.RecordDeserializer;
import org.apache.kafka.clients.consumer.AcknowledgeType;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static io.plurima.kafka.integration.KafkaIntegrationSupport.*;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that a {@code ManualAckListener} can drive explicit ACCEPT and REJECT
 * acknowledgements against a real broker.
 * <p>
 * Produces two records: one prefixed {@code "good-"} (ACCEPT) and one prefixed
 * {@code "bad-"} (REJECT). Asserts both listeners fire and the observations list
 * contains exactly two entries.
 */
@Tag("integration")
class ManualAckIntegrationTest {

    @Test
    void manualAckListenerControlsAck() throws Exception {
        String topic = createUniqueTopic("plurima-int-manual");
        String groupId = "plurima-int-manual-group-" + UUID.randomUUID();

        CountDownLatch latch = new CountDownLatch(2);
        ConcurrentLinkedQueue<String> observations = new ConcurrentLinkedQueue<>();

        try (PlurimaConsumer<byte[], String> consumer = PlurimaConsumer.<byte[], String>builder()
                .kafkaProperties(consumerProps(groupId))
                .topic(topic)
                .valueDeserializer(RecordDeserializer.utf8String())
                .pollTimeout(Duration.ofMillis(200))
                .manualAckListener((r, ack) -> {
                    observations.add(r.value());
                    if (r.value() != null && r.value().startsWith("good-")) {
                        ack.acknowledge(AcknowledgeType.ACCEPT);
                    } else {
                        ack.acknowledge(AcknowledgeType.REJECT);
                    }
                    latch.countDown();
                })
                .build()) {
            consumer.start();
            waitForAssignment(groupId, topic);

            try (var producer = stringValueProducer()) {
                producer.send(new ProducerRecord<>(topic, "k1".getBytes(), "good-1")).get();
                producer.send(new ProducerRecord<>(topic, "k2".getBytes(), "bad-1")).get();
            }

            assertThat(latch.await(60, TimeUnit.SECONDS))
                .as("both records must be processed within timeout")
                .isTrue();
            assertThat(observations)
                .as("both records must have been observed")
                .hasSize(2)
                .containsExactlyInAnyOrder("good-1", "bad-1");
        } finally {
            deleteTopicQuietly(topic);
        }
    }
}
