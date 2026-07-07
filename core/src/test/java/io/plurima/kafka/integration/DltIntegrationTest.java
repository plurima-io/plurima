package io.plurima.kafka.integration;

import io.plurima.kafka.PlurimaConsumer;
import io.plurima.kafka.dlt.DltConfig;
import io.plurima.kafka.retry.RetryPolicy;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static io.plurima.kafka.integration.KafkaIntegrationSupport.*;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that a listener whose retries are exhausted routes the record to the {@code .DLT}
 * topic with the canonical Plurima headers, and that the original record is ACCEPTed (no
 * redelivery).
 */
@Tag("integration")
class DltIntegrationTest {

    @Test
    void exhaustedRetryRoutesToDltAndAcceptsOriginal() throws Exception {
        String topic = createUniqueTopic("plurima-int-dlt");
        String dltTopic = topic + ".DLT";

        // Pre-create the DLT topic so the producer does not auto-create it with broker defaults.
        try (AdminClient admin = AdminClient.create(adminProps())) {
            admin.createTopics(List.of(new NewTopic(dltTopic, 1, (short) 1)))
                .all().get(15, TimeUnit.SECONDS);
        }

        String groupId = "plurima-int-dlt-group-" + UUID.randomUUID();
        AtomicInteger attempts = new AtomicInteger();

        Properties dltProducerProps = new Properties();
        dltProducerProps.put("bootstrap.servers", BOOTSTRAP);

        try (PlurimaConsumer<byte[], byte[]> consumer = PlurimaConsumer.builder()
                .kafkaProperties(consumerProps(groupId))
                .topic(topic)
                .pollTimeout(Duration.ofMillis(200))
                .retry(RetryPolicy.exponential()
                    .maxAttempts(2)
                    .initialDelay(Duration.ofMillis(20))
                    .multiplier(1.0)
                    .jitter(0.0)
                    .retryOn(IOException.class)
                    .build())
                .deadLetter(DltConfig.builder()
                    .producerProperties(dltProducerProps)
                    .build())
                .listener((r, ctx) -> {
                    attempts.incrementAndGet();
                    throw new IOException("always fails");
                })
                .build()) {
            consumer.start();
            waitForAssignment(groupId, topic);

            try (var producer = byteProducer()) {
                producer.send(new ProducerRecord<>(
                    topic, "k".getBytes(), "v".getBytes())).get();
            }

            // Use a plain KafkaConsumer (not share consumer) to read the DLT topic.
            Properties dltConsProps = new Properties();
            dltConsProps.put("bootstrap.servers", BOOTSTRAP);
            dltConsProps.put("group.id", "plurima-int-dlt-verifier-" + UUID.randomUUID());
            dltConsProps.put("key.deserializer", ByteArrayDeserializer.class.getName());
            dltConsProps.put("value.deserializer", ByteArrayDeserializer.class.getName());
            dltConsProps.put("auto.offset.reset", "earliest");
            dltConsProps.put("enable.auto.commit", "false");

            ConsumerRecord<byte[], byte[]> dltRecord = null;
            try (KafkaConsumer<byte[], byte[]> dltVerifier = new KafkaConsumer<>(dltConsProps)) {
                dltVerifier.subscribe(List.of(dltTopic));
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
                while (System.nanoTime() < deadline) {
                    var records = dltVerifier.poll(Duration.ofMillis(500));
                    if (!records.isEmpty()) {
                        dltRecord = records.iterator().next();
                        break;
                    }
                }
            }

            assertThat(dltRecord)
                .as("a record must appear on the DLT topic within 60 s")
                .isNotNull();
            assertThat(new String(dltRecord.value()))
                .as("DLT record value must match original")
                .isEqualTo("v");

            Map<String, String> headers = headerMap(dltRecord);
            assertThat(headers)
                .as("DLT headers must carry canonical Plurima metadata")
                .containsEntry("plurima-dlt-original-topic", topic)
                .containsEntry("plurima-dlt-failure-class", "java.io.IOException");
        } finally {
            deleteTopicQuietly(topic);
            deleteTopicQuietly(dltTopic);
        }
    }

    private static Map<String, String> headerMap(ConsumerRecord<byte[], byte[]> r) {
        Map<String, String> m = new HashMap<>();
        for (var h : r.headers()) {
            m.put(h.key(), new String(h.value(), StandardCharsets.UTF_8));
        }
        return m;
    }
}
