package io.plurima.kafka.integration;

import io.plurima.kafka.PlurimaConsumer;
import io.plurima.kafka.deserializer.RecordDeserializer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static io.plurima.kafka.integration.KafkaIntegrationSupport.*;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that the typed UTF-8 string deserializer delivers correctly-decoded key and value
 * records to the listener against a real broker.
 */
@Tag("integration")
class DeserializerIntegrationTest {

    @Test
    void stringDeserializerDeliversTypedRecord() throws Exception {
        String topic = createUniqueTopic("plurima-int-deser");
        String groupId = "plurima-int-deser-group-" + UUID.randomUUID();

        AtomicReference<String> seenKey = new AtomicReference<>();
        AtomicReference<String> seenValue = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);

        try (PlurimaConsumer<String, String> consumer = PlurimaConsumer.<String, String>builder()
                .kafkaProperties(consumerProps(groupId))
                .topic(topic)
                .keyDeserializer(RecordDeserializer.utf8String())
                .valueDeserializer(RecordDeserializer.utf8String())
                .pollTimeout(Duration.ofMillis(200))
                .listener((r, ctx) -> {
                    seenKey.set(r.key());
                    seenValue.set(r.value());
                    latch.countDown();
                })
                .build()) {
            consumer.start();
            waitForAssignment(groupId, topic);

            // stringValueProducer uses ByteArraySerializer for key and StringSerializer for value.
            // We send "hello" as raw bytes for the key and "world" as a String value.
            try (var producer = stringValueProducer()) {
                producer.send(new ProducerRecord<>(topic, "hello".getBytes(), "world")).get();
            }

            assertThat(latch.await(60, TimeUnit.SECONDS))
                .as("record must be processed within timeout")
                .isTrue();
            assertThat(seenKey.get())
                .as("key must be decoded as UTF-8 string")
                .isEqualTo("hello");
            assertThat(seenValue.get())
                .as("value must be decoded as UTF-8 string")
                .isEqualTo("world");
        } finally {
            deleteTopicQuietly(topic);
        }
    }
}
