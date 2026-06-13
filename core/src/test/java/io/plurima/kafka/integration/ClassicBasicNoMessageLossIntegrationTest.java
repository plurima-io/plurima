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
import java.util.concurrent.CopyOnWriteArrayList;
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
 * Comprehensive no-loss verification for the CLASSIC_BASIC engine. Drives every
 * non-trivial failure mode in one workload and asserts that every produced record
 * lands in exactly one terminal state — successful processing OR the DLT — with
 * no record disappearing into the void.
 *
 * <h3>Why this test exists</h3>
 * Plurima's per-path no-loss claims are individually testable elsewhere
 * (retry/exhaust/DLT routing in {@link ClassicBasicRetryDltIntegrationTest},
 * rebalance handoff in {@link ClassicBasicRebalanceIntegrationTest}, launcher
 * rejection in unit tests). This test composes them — a single workload where
 * different records hit different fates — and verifies the global invariant:
 *
 * <pre>
 *   |processed-successfully| + |routed-to-DLT| == |produced|
 *   union(processed, DLT)   == produced
 * </pre>
 *
 * <h3>Mix of fates engineered into the workload</h3>
 * For each record's payload "v{n}" we choose a fate by {@code n mod 4}:
 * <ul>
 *   <li>{@code n % 4 == 0}: succeeds on first attempt (happy path).</li>
 *   <li>{@code n % 4 == 1}: throws once, succeeds on retry (inline-retry path).</li>
 *   <li>{@code n % 4 == 2}: throws every attempt → exhausts retries → DLT route.</li>
 *   <li>{@code n % 4 == 3}: succeeds on first attempt with a brief simulated delay
 *       (concurrent with retrying records; exercises the dispatcher's parallel
 *       launch/complete bookkeeping).</li>
 * </ul>
 *
 * <p>Configured for UNORDERED on CLASSIC_BASIC so every record gets its own worker
 * (the broadest dispatcher coverage — one-worker-per-record means launch/complete
 * paths fire {@code total} times, maximising the chance of finding a leak).
 */
@Tag("integration")
class ClassicBasicNoMessageLossIntegrationTest {

    @Test
    void everyProducedRecordEndsUpInExactlyOneTerminalState() throws Exception {
        int total = 200;
        String topic = createUniqueTopic("plurima-int-noloss", 4);
        String dltTopic = topic + ".DLT";
        // Create the DLT topic up-front so the simple KafkaConsumer we use to verify can
        // attach without timing out on metadata refresh.
        try (var admin = org.apache.kafka.clients.admin.AdminClient.create(adminProps())) {
            admin.createTopics(java.util.List.of(
                new org.apache.kafka.clients.admin.NewTopic(dltTopic, 1, (short) 1)))
                .all().get(10, TimeUnit.SECONDS);
        }

        String groupId = "plurima-int-noloss-" + UUID.randomUUID();

        try (var producer = byteProducer()) {
            for (int n = 0; n < total; n++) {
                int partition = n % 4;
                producer.send(new ProducerRecord<>(
                    topic, partition, ("k" + n).getBytes(), ("v" + n).getBytes()));
            }
            producer.flush();
        }

        // Per-record attempt counter so "fails on first try, succeeds on retry" knows
        // whether the current attempt is the first or a retry. Keyed by full payload
        // value so we don't confuse records with the same partition/offset across
        // partitions.
        ConcurrentMap<String, AtomicInteger> attemptsByValue = new ConcurrentHashMap<>();
        Set<String> processedSuccessfully = ConcurrentHashMap.newKeySet();
        CopyOnWriteArrayList<String> processedLog = new CopyOnWriteArrayList<>();

        DltConfig dlt = DltConfig.builder()
            .namingStrategy(t -> t + ".DLT")
            .producerProperties(producerPropsForDlt())
            .build();

        RetryPolicy retry = RetryPolicy.exponential()
            .maxAttempts(2)                     // attempts 0,1 retry; 2 exhausts
            .initialDelay(Duration.ofMillis(20))
            .multiplier(1.5)
            .jitter(0.0)
            .retryOn(RuntimeException.class)
            .build();

        try (PlurimaConsumer<byte[], byte[]> consumer = PlurimaConsumer.<byte[], byte[]>builder()
            .kafkaProperties(classicConsumerProps(groupId))
            .topic(topic)
            .engine(ConsumerEngine.CLASSIC_BASIC)
            .ordering(OrderingMode.UNORDERED)
            .retry(retry)
            .deadLetterTopic(dlt)
            .concurrency(16)
            .pollTimeout(Duration.ofMillis(200))
            .shutdownDrainTimeout(Duration.ofSeconds(15))
            .listener((rec, ctx) -> {
                String value = new String(rec.value());
                int n = Integer.parseInt(value.substring(1));    // strip "v"
                int attempt = attemptsByValue.computeIfAbsent(value,
                    k -> new AtomicInteger()).incrementAndGet();

                switch (n % 4) {
                    case 0:
                        // happy path
                        break;
                    case 1:
                        // fail once, succeed on retry
                        if (attempt == 1) throw new RuntimeException("transient-" + value);
                        break;
                    case 2:
                        // always fail → exhausts retries → DLT
                        throw new RuntimeException("always-fails-" + value);
                    case 3:
                        // happy path with brief stagger
                        try { Thread.sleep(5); } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                        break;
                }
                processedSuccessfully.add(value);
                processedLog.add(value);
            })
            .build()) {

            consumer.start();
            waitForClassicAssignment(groupId, 1, 4);

            // Wait until every record has either succeeded or hit DLT. Polling the
            // processed set + DLT counter is more reliable than a CountDownLatch: a
            // CountDownLatch can't tell if a record was re-delivered (counted twice).
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(90);
            int expectedDlt = expectedDltCount(total);          // n % 4 == 2 → DLT
            int expectedProcessed = total - expectedDlt;
            int dltSeen = 0;
            while (System.nanoTime() < deadline) {
                if (processedSuccessfully.size() >= expectedProcessed) {
                    // Recheck DLT — count records on the DLT topic. This pause is small;
                    // most of the wait is in the consumer doing actual work above.
                    dltSeen = countRecordsInTopic(dltTopic, expectedDlt, Duration.ofSeconds(5));
                    if (dltSeen >= expectedDlt) break;
                }
                Thread.sleep(200);
            }

            assertThat(processedSuccessfully)
                .as("every non-DLT record must be processed successfully at least once "
                    + "(observed %d / %d expected)", processedSuccessfully.size(), expectedProcessed)
                .hasSize(expectedProcessed);

            // Verify the exact set of values: all v{n} where n % 4 != 2.
            Set<String> expectedSuccessSet = new HashSet<>();
            for (int n = 0; n < total; n++) {
                if (n % 4 != 2) expectedSuccessSet.add("v" + n);
            }
            assertThat(processedSuccessfully)
                .as("processed set must contain every non-DLT payload value")
                .containsExactlyInAnyOrderElementsOf(expectedSuccessSet);

            // DLT must hold exactly the n % 4 == 2 records.
            Set<String> dltPayloads = drainDltPayloads(dltTopic, expectedDlt, Duration.ofSeconds(15));
            Set<String> expectedDltSet = new HashSet<>();
            for (int n = 0; n < total; n++) {
                if (n % 4 == 2) expectedDltSet.add("v" + n);
            }
            assertThat(dltPayloads)
                .as("DLT topic must hold exactly the n%%4==2 payloads")
                .containsExactlyInAnyOrderElementsOf(expectedDltSet);

            // Global no-loss invariant: union(processed, DLT) == every produced value.
            // The mathematical statement of "no record disappeared into the void."
            Set<String> union = new HashSet<>(processedSuccessfully);
            union.addAll(dltPayloads);
            Set<String> produced = new HashSet<>();
            for (int n = 0; n < total; n++) produced.add("v" + n);
            assertThat(union)
                .as("union(processed, DLT) must equal the produced set — no record lost")
                .isEqualTo(produced);

            // Attempt-count sanity: every n%4==2 record was tried exactly maxAttempts+1=3
            // times before DLT routing. Confirms the retry path was actually exercised,
            // not just a happy-path test in disguise.
            int dltRecordAttemptSum = 0;
            for (int n = 0; n < total; n++) {
                if (n % 4 != 2) continue;
                AtomicInteger ai = attemptsByValue.get("v" + n);
                assertThat(ai).as("v%d (DLT record) must have been attempted", n).isNotNull();
                dltRecordAttemptSum += ai.get();
            }
            // 3 attempts per record × expectedDlt records, but ALLOW some over-count from
            // at-least-once redelivery on rebalance or restart (none expected here, no
            // rebalance fired). Floor: 3 × expectedDlt.
            assertThat(dltRecordAttemptSum)
                .as("DLT records must have been attempted at least 3× (maxAttempts=2 → 3 tries)")
                .isGreaterThanOrEqualTo(3 * expectedDlt);

        } finally {
            deleteTopicQuietly(topic);
            deleteTopicQuietly(dltTopic);
        }
    }

    // ────── Helpers ──────

    private static int expectedDltCount(int total) {
        int count = 0;
        for (int n = 0; n < total; n++) if (n % 4 == 2) count++;
        return count;
    }

    private static int countRecordsInTopic(String topic, int expected, Duration timeout) {
        Properties p = simpleConsumerProps("plurima-int-noloss-counter-" + UUID.randomUUID());
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
        Properties p = simpleConsumerProps("plurima-int-noloss-dlt-reader-" + UUID.randomUUID());
        try (KafkaConsumer<byte[], byte[]> c = new KafkaConsumer<>(p)) {
            c.subscribe(java.util.List.of(topic));
            long deadline = System.nanoTime() + timeout.toNanos();
            // Two consecutive empty polls after we've seen the expected count are our
            // "no more records" signal — at-least-once means we might see duplicates,
            // so we can't just stop at the first match.
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

    private static Properties producerPropsForDlt() {
        Properties p = new Properties();
        p.put("bootstrap.servers", BOOTSTRAP);
        return p;
    }
}
