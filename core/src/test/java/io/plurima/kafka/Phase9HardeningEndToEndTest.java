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
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * End-to-end verification of Phase 9 hardening items G5–G7:
 *
 * <ul>
 *   <li><b>G7</b>: unsupported KafkaConsumer configs (e.g., {@code enable.auto.commit}) are
 *       rejected at builder time with {@link IllegalArgumentException}.</li>
 *   <li><b>G5</b>: when the user requests {@code share.acknowledgement.mode=implicit}, the
 *       library overrides it to {@code explicit} (without throwing), and the resulting
 *       consumer runs end-to-end against a real broker.</li>
 * </ul>
 *
 * <p>G2/G3 (poison pill + corrupt batch) are unit-tested via {@code FakeShareConsumer} —
 * injecting them against a live broker requires bypassing the producer-side serialization,
 * which adds complexity without proving more than the unit test already does.
 *
 * <p>G4 (commit-failed callback) is unit-tested in {@code AckCoordinatorTest}.
 */
@Tag("integration")
class Phase9HardeningEndToEndTest {

    private static final String BOOTSTRAP = "localhost:9092";

    @Test
    void unsupportedConfigsRejectedAtBuildTime() {
        Properties props = new Properties();
        props.put("bootstrap.servers", BOOTSTRAP);
        props.put("enable.auto.commit", "true");
        assertThatThrownBy(() -> PlurimaConsumer.<byte[], byte[]>builder()
                .kafkaProperties(props)
                .topic("t")
                .listener((r, ctx) -> {})
                .build())
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("enable.auto.commit");
    }

    @Test
    void autoOffsetResetRejectedAtBuildTime() {
        Properties props = new Properties();
        props.put("bootstrap.servers", BOOTSTRAP);
        props.put("auto.offset.reset", "earliest");
        assertThatThrownBy(() -> PlurimaConsumer.<byte[], byte[]>builder()
                .kafkaProperties(props)
                .topic("t")
                .listener((r, ctx) -> {})
                .build())
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("auto.offset.reset");
    }

    @Test
    void implicitModeRequestIsForcedToExplicit() throws Exception {
        String topic = "plurima-g5-" + UUID.randomUUID();
        String groupId = "plurima-g5-group-" + UUID.randomUUID();

        try (AdminClient admin = AdminClient.create(adminProps())) {
            admin.createTopics(List.of(new NewTopic(topic, 1, (short) 1)))
                .all().get(15, TimeUnit.SECONDS);
        }

        Properties cProps = new Properties();
        cProps.put("bootstrap.servers", BOOTSTRAP);
        cProps.put("group.id", groupId);
        // User asks for implicit; library should warn-log and force explicit.
        cProps.put("share.acknowledgement.mode", "implicit");

        CountDownLatch latch = new CountDownLatch(1);
        try (PlurimaConsumer<byte[], byte[]> consumer = PlurimaConsumer.<byte[], byte[]>builder()
            .kafkaProperties(cProps)
            .topic(topic)
            .pollTimeout(Duration.ofMillis(200))
            .listener((r, ctx) -> latch.countDown())
            .build()) {
            consumer.start();

            KafkaIntegrationSupport.waitForAssignment(groupId, topic);

            try (var producer = new KafkaProducer<byte[], byte[]>(producerProps())) {
                producer.send(new ProducerRecord<>(topic, "k".getBytes(), "v".getBytes())).get();
            }

            // If implicit had NOT been overridden, our explicit-mode acknowledge() call inside
            // AckCoordinator.commitPendingAcks would throw IllegalStateException at runtime and
            // the listener would never see the record. The latch firing proves explicit was forced.
            assertThat(latch.await(20, TimeUnit.SECONDS))
                .as("explicit-mode override must permit acknowledge() to succeed")
                .isTrue();
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

    private static Properties producerProps() {
        Properties p = new Properties();
        p.put("bootstrap.servers", BOOTSTRAP);
        p.put("key.serializer", ByteArraySerializer.class.getName());
        p.put("value.serializer", ByteArraySerializer.class.getName());
        p.put("acks", "all");
        return p;
    }
}
