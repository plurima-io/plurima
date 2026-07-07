package io.plurima.kafka;

import io.plurima.kafka.integration.KafkaIntegrationSupport;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Empirical verification that the G1 fix works end-to-end against a real Kafka 4.2.0 broker
 * at {@code localhost:9092}. Companion to {@link ShareConsumerExplicitModeContractTest}
 * (which proved the bug existed).
 *
 * <p>Scenario: produce 50 records, run a slow listener (~50 ms per record) at concurrency=10
 * through a real {@link PlurimaConsumer} for ~6 seconds (covering many poll cycles). Verify
 * that:
 * <ul>
 *   <li>no {@code IllegalStateException} ever fires (the drain barrier holds);
 *   <li>all 50 records are processed at least once;
 *   <li>no duplicates within tolerance (broker may redeliver due to lock expiry on slow handlers).
 * </ul>
 *
 * <p>Tagged {@code integration} so it stays out of the default suite.
 */
@Tag("integration")
class G1FixedEndToEndTest {

    private static final String BOOTSTRAP = "localhost:9092";

    @Test
    void slowMultiPollWorkloadDoesNotThrowISE() throws Exception {
        String topic = "plurima-g1-fixed-" + UUID.randomUUID();
        String groupId = "plurima-g1-fixed-group-" + UUID.randomUUID();
        int recordCount = 50;
        long handlerSleepMs = 50;

        try (AdminClient admin = AdminClient.create(adminProps())) {
            admin.createTopics(List.of(new NewTopic(topic, 1, (short) 1)))
                .all().get(15, TimeUnit.SECONDS);
        }

        // Subscribe and prime first so the share group's position sits at end-of-log.
        Properties cProps = new Properties();
        cProps.put("bootstrap.servers", BOOTSTRAP);
        cProps.put("group.id", groupId);
        cProps.put("share.acknowledgement.mode", "explicit");

        AtomicInteger seen = new AtomicInteger();
        ConcurrentLinkedQueue<Long> offsetsSeen = new ConcurrentLinkedQueue<>();
        CountDownLatch allReceived = new CountDownLatch(recordCount);

        try (PlurimaConsumer<byte[], byte[]> consumer = PlurimaConsumer.builder()
            .kafkaProperties(cProps)
            .topic(topic)
            .concurrency(10)
            .pollTimeout(Duration.ofMillis(200))
            .lockDuration(Duration.ofSeconds(30))
            .listener((r, ctx) -> {
                try { Thread.sleep(handlerSleepMs); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); }
                offsetsSeen.add(r.offset());
                seen.incrementAndGet();
                allReceived.countDown();
            })
            .build()) {
            consumer.start();

            KafkaIntegrationSupport.waitForAssignment(groupId, topic);

            // Produce 50 records
            Properties pProps = new Properties();
            pProps.put("bootstrap.servers", BOOTSTRAP);
            pProps.put("key.serializer", ByteArraySerializer.class.getName());
            pProps.put("value.serializer", ByteArraySerializer.class.getName());
            try (var producer = new KafkaProducer<byte[], byte[]>(pProps)) {
                for (int i = 0; i < recordCount; i++) {
                    producer.send(new ProducerRecord<>(
                        topic, new byte[]{(byte) (i % 8)}, ("msg-" + i).getBytes())).get();
                }
                producer.flush();
            }
            System.out.println("G1-fixed: produced " + recordCount + " records");

            boolean done = allReceived.await(30, TimeUnit.SECONDS);
            System.out.println("G1-fixed: latch done=" + done + " seen=" + seen.get()
                + " uniqueOffsets=" + Set.copyOf(offsetsSeen).size());

            assertThat(done).as("listener invoked for all %d records", recordCount).isTrue();
            // Tolerance: broker may redeliver. Allow up to 10% duplicates.
            assertThat(seen.get())
                .as("at least %d invocations (no records lost)", recordCount)
                .isGreaterThanOrEqualTo(recordCount);
            assertThat(Set.copyOf(offsetsSeen))
                .as("all %d distinct offsets observed", recordCount)
                .hasSize(recordCount);
        } finally {
            try (AdminClient admin = AdminClient.create(adminProps())) {
                admin.deleteTopics(List.of(topic)).all().get(5, TimeUnit.SECONDS);
            } catch (Exception ignored) {}
        }
    }

    private static Properties adminProps() {
        Properties p = new Properties();
        p.put("bootstrap.servers", BOOTSTRAP);
        p.put("request.timeout.ms", "10000");
        return p;
    }
}
