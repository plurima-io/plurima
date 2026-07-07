package io.plurima.kafka.integration;

import io.plurima.kafka.ConsumerEngine;
import io.plurima.kafka.OrderingMode;
import io.plurima.kafka.PlurimaConsumer;
import io.plurima.kafka.dlt.DltConfig;
import io.plurima.kafka.retry.RetryPolicy;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static io.plurima.kafka.integration.KafkaIntegrationSupport.BOOTSTRAP;
import static io.plurima.kafka.integration.KafkaIntegrationSupport.byteProducer;
import static io.plurima.kafka.integration.KafkaIntegrationSupport.consumerProps;
import static io.plurima.kafka.integration.KafkaIntegrationSupport.createUniqueTopic;
import static io.plurima.kafka.integration.KafkaIntegrationSupport.deleteTopicQuietly;
import static io.plurima.kafka.integration.KafkaIntegrationSupport.waitForAssignment;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * SHARE engine counterpart to {@link ClassicBasicNoMessageLossIntegrationTest}.
 * Drives a mix of fates through {@code KafkaShareConsumer} + KIP-932 explicit-mode
 * acks and asserts the global no-loss invariant:
 *
 * <pre>
 *   |processed-successfully| + |routed-to-DLT| == |produced|
 *   union(processed, DLT)   == produced
 * </pre>
 *
 * <p>SHARE's redelivery model is different from CLASSIC_BASIC: failed records are
 * RELEASEd to the broker which may hand them to any consumer in the share group on
 * the next poll. Inline retry (sub-{@code lockDuration} delay) happens on the worker
 * thread; delayed retry queues a RELEASE and the broker schedules redelivery. Either
 * way the record stays accounted for — this test holds the SHARE path to the same
 * end-to-end no-loss bar as the classic engine.
 */
@Tag("integration")
class ShareNoMessageLossIntegrationTest {

    @Test
    void shareEngineEveryProducedRecordEndsUpInExactlyOneTerminalState() throws Exception {
        int total = 100;
        String topic = createUniqueTopic("plurima-int-share-noloss", 1);
        String dltTopic = topic + ".DLT";
        try (var admin = org.apache.kafka.clients.admin.AdminClient.create(adminProps())) {
            admin.createTopics(java.util.List.of(
                new org.apache.kafka.clients.admin.NewTopic(dltTopic, 1, (short) 1)))
                .all().get(10, TimeUnit.SECONDS);
        }

        String groupId = "plurima-int-share-noloss-" + UUID.randomUUID();

        ConcurrentMap<String, AtomicInteger> attemptsByValue = new ConcurrentHashMap<>();
        Set<String> processedSuccessfully = ConcurrentHashMap.newKeySet();

        DltConfig dlt = DltConfig.builder()
            .namingStrategy(t -> t + ".DLT")
            .producerProperties(producerProps())
            .build();

        // maxAttempts=2 → attempts 0, 1 retry; attempt 2 exhausts → DLT.
        // SHARE's delayedRetry queues a RELEASE; the broker redelivers. inline-retry
        // stays on the worker (sub-lockDuration delay).
        RetryPolicy retry = RetryPolicy.exponential()
            .maxAttempts(2)
            .initialDelay(Duration.ofMillis(20))
            .multiplier(1.5)
            .jitter(0.0)
            .retryOn(RuntimeException.class)
            .build();

        try (PlurimaConsumer<byte[], byte[]> consumer = PlurimaConsumer.builder()
            .kafkaProperties(consumerProps(groupId))
            .topic(topic)
            .engine(ConsumerEngine.SHARE)
            .ordering(OrderingMode.UNORDERED)
            .retry(retry)
            .deadLetter(dlt)
            .concurrency(8)
            .pollTimeout(Duration.ofMillis(200))
            .shutdownDrainTimeout(Duration.ofSeconds(15))
            .listener((rec, ctx) -> {
                String value = new String(rec.value());
                int n = Integer.parseInt(value.substring(1));
                int attempt = attemptsByValue.computeIfAbsent(value,
                    k -> new AtomicInteger()).incrementAndGet();

                // SHARE has broker-side delivery counting + cross-instance redelivery,
                // so a record's "attempt" counter as seen by this listener may include
                // attempts from a prior consumer instance. The fate mapping below is
                // chosen to be robust to that:
                //   n % 3 == 0: always succeed
                //   n % 3 == 1: fail once on attempt 1, succeed thereafter
                //   n % 3 == 2: always fail → DLT route
                switch (n % 3) {
                    case 0:
                        break;
                    case 1:
                        if (attempt == 1) throw new RuntimeException("transient-" + value);
                        break;
                    case 2:
                        throw new RuntimeException("always-fails-" + value);
                }
                processedSuccessfully.add(value);
            })
            .build()) {

            consumer.start();
            waitForAssignment(groupId, topic);

            // Produce AFTER assignment — SHARE's offset reset is broker-side LATEST by
            // default; records published before subscription would not be delivered.
            try (var producer = byteProducer()) {
                for (int n = 0; n < total; n++) {
                    producer.send(new ProducerRecord<>(topic,
                        ("k" + n).getBytes(), ("v" + n).getBytes()));
                }
                producer.flush();
            }

            int expectedDlt = expectedDltCount(total);          // n % 3 == 2
            int expectedProcessed = total - expectedDlt;

            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(90);
            int dltSeen = 0;
            while (System.nanoTime() < deadline) {
                if (processedSuccessfully.size() >= expectedProcessed) {
                    dltSeen = countRecordsInTopic(dltTopic, expectedDlt, Duration.ofSeconds(5));
                    if (dltSeen >= expectedDlt) break;
                }
                Thread.sleep(200);
            }

            Set<String> expectedSuccessSet = new HashSet<>();
            for (int n = 0; n < total; n++) {
                if (n % 3 != 2) expectedSuccessSet.add("v" + n);
            }
            assertThat(processedSuccessfully)
                .as("SHARE: every non-DLT record must be processed successfully at least once")
                .containsExactlyInAnyOrderElementsOf(expectedSuccessSet);

            Set<String> dltPayloads = drainDltPayloads(dltTopic, expectedDlt, Duration.ofSeconds(15));
            Set<String> expectedDltSet = new HashSet<>();
            for (int n = 0; n < total; n++) {
                if (n % 3 == 2) expectedDltSet.add("v" + n);
            }
            assertThat(dltPayloads)
                .as("SHARE: DLT must hold exactly the n%%3==2 payloads")
                .containsExactlyInAnyOrderElementsOf(expectedDltSet);

            // Global no-loss invariant: union(processed, DLT) == produced.
            Set<String> union = new HashSet<>(processedSuccessfully);
            union.addAll(dltPayloads);
            Set<String> produced = new HashSet<>();
            for (int n = 0; n < total; n++) produced.add("v" + n);
            assertThat(union)
                .as("SHARE: union(processed, DLT) must equal the produced set — no record lost")
                .isEqualTo(produced);

        } finally {
            deleteTopicQuietly(topic);
            deleteTopicQuietly(dltTopic);
        }
    }

    private static int expectedDltCount(int total) {
        int count = 0;
        for (int n = 0; n < total; n++) if (n % 3 == 2) count++;
        return count;
    }

    private static int countRecordsInTopic(String topic, int expected, Duration timeout) {
        Properties p = simpleConsumerProps("plurima-int-share-noloss-counter-" + UUID.randomUUID());
        try (KafkaConsumer<byte[], byte[]> c = new KafkaConsumer<>(p)) {
            c.subscribe(java.util.List.of(topic));
            int count = 0;
            long deadline = System.nanoTime() + timeout.toNanos();
            while (count < expected && System.nanoTime() < deadline) {
                ConsumerRecords<byte[], byte[]> batch = c.poll(Duration.ofMillis(500));
                count += batch.count();
            }
            return count;
        }
    }

    private static Set<String> drainDltPayloads(String topic, int expected, Duration timeout) {
        Set<String> payloads = ConcurrentHashMap.newKeySet();
        Properties p = simpleConsumerProps("plurima-int-share-noloss-dlt-reader-" + UUID.randomUUID());
        try (KafkaConsumer<byte[], byte[]> c = new KafkaConsumer<>(p)) {
            c.subscribe(java.util.List.of(topic));
            long deadline = System.nanoTime() + timeout.toNanos();
            int emptyAfterExpected = 0;
            while (System.nanoTime() < deadline) {
                ConsumerRecords<byte[], byte[]> batch = c.poll(Duration.ofMillis(500));
                if (batch.isEmpty()) {
                    if (payloads.size() >= expected) emptyAfterExpected++;
                    if (emptyAfterExpected >= 2) break;
                    continue;
                }
                emptyAfterExpected = 0;
                for (ConsumerRecord<byte[], byte[]> r : batch) {
                    payloads.add(new String(r.value()));
                }
            }
        }
        return payloads;
    }

    private static Properties simpleConsumerProps(String groupId) {
        Properties p = new Properties();
        p.put("bootstrap.servers", BOOTSTRAP);
        p.put("group.id", groupId);
        p.put("auto.offset.reset", "earliest");
        p.put("enable.auto.commit", "false");
        p.put("key.deserializer", ByteArrayDeserializer.class.getName());
        p.put("value.deserializer", ByteArrayDeserializer.class.getName());
        return p;
    }

    private static Properties adminProps() {
        Properties p = new Properties();
        p.put("bootstrap.servers", BOOTSTRAP);
        return p;
    }

    private static Properties producerProps() {
        Properties p = new Properties();
        p.put("bootstrap.servers", BOOTSTRAP);
        return p;
    }
}
