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
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static io.plurima.kafka.integration.KafkaIntegrationSupport.BOOTSTRAP;
import static io.plurima.kafka.integration.KafkaIntegrationSupport.byteProducer;
import static io.plurima.kafka.integration.KafkaIntegrationSupport.classicConsumerProps;
import static io.plurima.kafka.integration.KafkaIntegrationSupport.createUniqueTopic;
import static io.plurima.kafka.integration.KafkaIntegrationSupport.deleteTopicQuietly;
import static io.plurima.kafka.integration.KafkaIntegrationSupport.waitForClassicAssignment;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that the CLASSIC_BASIC engine does NOT lose records when DLT publishing
 * fails. The contract documented in {@code UserGuide.md § DLT failure handling}:
 *
 * <ul>
 *   <li>The partition's commit frontier MUST NOT advance past the failing offset.</li>
 *   <li>Later records on the same partition still process (their completions park
 *       in {@code completedAhead}) but the commit stays pinned.</li>
 *   <li>On restart / rebalance, the broker redelivers from the failing offset.</li>
 * </ul>
 *
 * <p>Strategy: configure the DLT producer to point at a closed port with tight
 * timeouts. Every DLT publish fails fast. The listener always throws so every record
 * exhausts retries and hits the DLT-failure path. After the workload, we read the
 * broker's committed offset directly via {@code KafkaConsumer.committed} and assert
 * it is &lt; the produced records' starting offset — proving the frontier never
 * advanced.
 */
@Tag("integration")
class ClassicBasicDltFailureNoLossIntegrationTest {

    @Test
    void dltPublishFailureKeepsFrontierStuckSoRestartRedelivers() throws Exception {
        int total = 5;
        String topic = createUniqueTopic("plurima-int-dlt-fail-noloss", 1);
        String groupId = "plurima-int-dlt-fail-noloss-" + UUID.randomUUID();

        try (var producer = byteProducer()) {
            for (int i = 0; i < total; i++) {
                producer.send(new ProducerRecord<>(
                    topic, ("k" + i).getBytes(), ("v" + i).getBytes())).get();
            }
        }

        // DLT producer pointed at a closed port so every send fails fast.
        Properties dltProps = new Properties();
        dltProps.put("bootstrap.servers", "127.0.0.1:1");      // closed port
        dltProps.put("max.block.ms", "500");
        dltProps.put("request.timeout.ms", "500");
        dltProps.put("delivery.timeout.ms", "1000");
        dltProps.put("retries", "0");

        DltConfig brokenDlt = DltConfig.builder()
            .namingStrategy(t -> t + ".DLT")
            .producerProperties(dltProps)
            .build();

        // Single attempt → exhaust immediately on the first throw. Keeps the test
        // fast (no retry sleeps).
        RetryPolicy retry = RetryPolicy.exponential()
            .maxAttempts(0)
            .initialDelay(Duration.ofMillis(10))
            .multiplier(1.0)
            .jitter(0.0)
            .retryOn(RuntimeException.class)
            .build();

        AtomicInteger listenerInvocations = new AtomicInteger();
        Set<String> seenValues = new HashSet<>();

        try (PlurimaConsumer<byte[], byte[]> consumer = PlurimaConsumer.<byte[], byte[]>builder()
            .kafkaProperties(classicConsumerProps(groupId))
            .topic(topic)
            .engine(ConsumerEngine.CLASSIC_BASIC)
            .ordering(OrderingMode.PARTITION)         // one worker per partition → predictable order
            .retry(retry)
            .deadLetterTopic(brokenDlt)
            .pollTimeout(Duration.ofMillis(200))
            .shutdownDrainTimeout(Duration.ofSeconds(15))
            .listener((rec, ctx) -> {
                listenerInvocations.incrementAndGet();
                seenValues.add(new String(rec.value()));
                throw new RuntimeException("always-fails-" + new String(rec.value()));
            })
            .build()) {

            consumer.start();
            waitForClassicAssignment(groupId, 1, 1);

            // Wait until every record has been invoked at least once.
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(45);
            while (seenValues.size() < total && System.nanoTime() < deadline) {
                Thread.sleep(200);
            }
            assertThat(seenValues)
                .as("each record must be delivered to the listener at least once")
                .hasSize(total);
        } finally {
            // close happens in try-with-resources above — consumer shuts down here
        }

        // After shutdown, query the broker for the committed offset on this partition.
        // The first record was at offset 0; if DLT-failure-was-lossy (frontier advanced),
        // the committed offset would be `total` (== 5). Under the new contract, the
        // commit must stay at <= 0 (the failing offset) so a restart redelivers.
        long committed = readCommittedOffset(topic, groupId);
        assertThat(committed)
            .as("commit must NOT advance past the first failing offset — restart must "
                + "redeliver from offset 0 (observed committed=%d, expected <= 0)", committed)
            .isLessThanOrEqualTo(0L);

        deleteTopicQuietly(topic);
    }

    /**
     * Read the committed offset for partition 0 of {@code topic} on {@code groupId}.
     * Returns 0 if no commit exists yet (the broker's "no committed offset" signal).
     */
    private static long readCommittedOffset(String topic, String groupId) {
        Properties p = new Properties();
        p.put("bootstrap.servers", BOOTSTRAP);
        p.put("group.id", groupId);
        p.put("auto.offset.reset", "earliest");
        p.put("enable.auto.commit", "false");
        p.put("key.deserializer", ByteArrayDeserializer.class.getName());
        p.put("value.deserializer", ByteArrayDeserializer.class.getName());

        try (KafkaConsumer<byte[], byte[]> c = new KafkaConsumer<>(p)) {
            TopicPartition tp = new TopicPartition(topic, 0);
            var meta = c.committed(Set.of(tp));
            var offset = meta.get(tp);
            return offset == null ? 0L : offset.offset();
        }
    }

    /**
     * Sanity check: even with DLT broken, the consumer can later resume with a healthy
     * DLT and the stuck records re-route correctly. Proves the "restart redelivers"
     * promise actually works end-to-end.
     */
    @Test
    void afterRestartWithHealthyDltAllStuckRecordsRouteToDlt() throws Exception {
        int total = 3;
        String topic = createUniqueTopic("plurima-int-dlt-fail-recovery", 1);
        String dltTopic = topic + ".DLT";
        String groupId = "plurima-int-dlt-fail-recovery-" + UUID.randomUUID();

        try (var admin = org.apache.kafka.clients.admin.AdminClient.create(adminProps())) {
            admin.createTopics(List.of(
                new org.apache.kafka.clients.admin.NewTopic(dltTopic, 1, (short) 1)))
                .all().get(10, TimeUnit.SECONDS);
        }

        try (var producer = byteProducer()) {
            for (int i = 0; i < total; i++) {
                producer.send(new ProducerRecord<>(
                    topic, ("k" + i).getBytes(), ("v" + i).getBytes())).get();
            }
        }

        Properties brokenDltProps = new Properties();
        brokenDltProps.put("bootstrap.servers", "127.0.0.1:1");
        brokenDltProps.put("max.block.ms", "500");
        brokenDltProps.put("request.timeout.ms", "500");
        brokenDltProps.put("delivery.timeout.ms", "1000");
        brokenDltProps.put("retries", "0");

        Properties healthyDltProps = new Properties();
        healthyDltProps.put("bootstrap.servers", BOOTSTRAP);

        RetryPolicy retry = RetryPolicy.exponential()
            .maxAttempts(0)
            .initialDelay(Duration.ofMillis(10))
            .multiplier(1.0)
            .jitter(0.0)
            .retryOn(RuntimeException.class)
            .build();

        // First run: broken DLT — frontier stays stuck.
        AtomicInteger firstRunInvocations = new AtomicInteger();
        try (PlurimaConsumer<byte[], byte[]> consumer1 = PlurimaConsumer.<byte[], byte[]>builder()
            .kafkaProperties(classicConsumerProps(groupId))
            .topic(topic)
            .engine(ConsumerEngine.CLASSIC_BASIC)
            .ordering(OrderingMode.PARTITION)
            .retry(retry)
            .deadLetterTopic(DltConfig.builder()
                .namingStrategy(t -> t + ".DLT")
                .producerProperties(brokenDltProps)
                .build())
            .pollTimeout(Duration.ofMillis(200))
            .shutdownDrainTimeout(Duration.ofSeconds(15))
            .listener((rec, ctx) -> {
                firstRunInvocations.incrementAndGet();
                throw new RuntimeException("first-run-throw");
            })
            .build()) {

            consumer1.start();
            waitForClassicAssignment(groupId, 1, 1);

            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(45);
            while (firstRunInvocations.get() < 1 && System.nanoTime() < deadline) {
                Thread.sleep(200);
            }
            assertThat(firstRunInvocations.get())
                .as("first run must invoke the listener at least once")
                .isGreaterThanOrEqualTo(1);
        }

        // Broker should NOT have any committed offset for this group's partition 0
        // (or it should be 0). Hand the same group a HEALTHY DLT and verify all
        // total records route to DLT this time.
        AtomicInteger secondRunInvocations = new AtomicInteger();
        try (PlurimaConsumer<byte[], byte[]> consumer2 = PlurimaConsumer.<byte[], byte[]>builder()
            .kafkaProperties(classicConsumerProps(groupId))
            .topic(topic)
            .engine(ConsumerEngine.CLASSIC_BASIC)
            .ordering(OrderingMode.PARTITION)
            .retry(retry)
            .deadLetterTopic(DltConfig.builder()
                .namingStrategy(t -> t + ".DLT")
                .producerProperties(healthyDltProps)
                .build())
            .pollTimeout(Duration.ofMillis(200))
            .shutdownDrainTimeout(Duration.ofSeconds(15))
            .listener((rec, ctx) -> {
                secondRunInvocations.incrementAndGet();
                throw new RuntimeException("second-run-throw");
            })
            .build()) {

            consumer2.start();
            waitForClassicAssignment(groupId, 1, 1);

            // Each record gets one attempt (maxAttempts=0) then DLT. With healthy DLT,
            // every record routes successfully → commit advances.
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
            while (secondRunInvocations.get() < total && System.nanoTime() < deadline) {
                Thread.sleep(200);
            }
            assertThat(secondRunInvocations.get())
                .as("second run with healthy DLT must invoke the listener for every "
                    + "originally-stuck record (frontier was pinned, broker redelivered)")
                .isGreaterThanOrEqualTo(total);
        }

        // Verify all records made it to the DLT topic on the second run.
        Set<String> dltPayloads = drainDltPayloads(dltTopic, total, Duration.ofSeconds(15));
        Set<String> expected = new HashSet<>();
        for (int i = 0; i < total; i++) expected.add("v" + i);
        assertThat(dltPayloads)
            .as("with healthy DLT, every stuck record routes to DLT — no loss")
            .containsAll(expected);

        deleteTopicQuietly(topic);
        deleteTopicQuietly(dltTopic);
    }

    private static Set<String> drainDltPayloads(String topic, int expected, Duration timeout) {
        Set<String> payloads = new HashSet<>();
        Properties p = new Properties();
        p.put("bootstrap.servers", BOOTSTRAP);
        p.put("group.id", "plurima-int-dlt-fail-reader-" + UUID.randomUUID());
        p.put("auto.offset.reset", "earliest");
        p.put("enable.auto.commit", "false");
        p.put("key.deserializer", ByteArrayDeserializer.class.getName());
        p.put("value.deserializer", ByteArrayDeserializer.class.getName());

        try (KafkaConsumer<byte[], byte[]> c = new KafkaConsumer<>(p)) {
            c.subscribe(List.of(topic));
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

    private static Properties adminProps() {
        Properties p = new Properties();
        p.put("bootstrap.servers", BOOTSTRAP);
        return p;
    }
}
