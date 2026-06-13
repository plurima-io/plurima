package io.plurima.kafka;

import io.plurima.kafka.integration.KafkaIntegrationSupport;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.AcknowledgeType;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaShareConsumer;
import org.apache.kafka.clients.consumer.ShareConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Empirical verification of design § 17 G1 (KIP-932 explicit-mode polling contract).
 *
 * <p>KIP-932 states: "If the application calls KafkaShareConsumer.poll(Duration)
 * without having acknowledged all records, an IllegalStateException is thrown."
 *
 * <p>Plurima's design originally tried to handwave this away ("the wiki text describes
 * an older design that was relaxed during implementation"). Empirical verification on
 * Kafka 4.2.0 confirmed the wiki was correct — ISE IS thrown. That's why
 * {@link io.plurima.kafka.internal.PollLoop} has the drain barrier
 * (design § 9 I9 / § 17.5 G1).
 *
 * <p>This test now actively asserts that confirmed behavior. If a future Kafka release
 * relaxes the contract (so the second poll succeeds without ISE), this test fails loudly,
 * which is the correct signal to revisit the drain barrier.
 *
 * <p>Tagged {@code integration} so it does not run on every push.
 */
@Tag("integration")
class ShareConsumerExplicitModeContractTest {

    private static final String BOOTSTRAP = "localhost:9092";

    @Test
    void secondPollWithoutFullAckThrowsIllegalStateException() throws Exception {
        String topic = "plurima-g1-verification-" + UUID.randomUUID();
        String groupId = "plurima-g1-group-" + UUID.randomUUID();

        try (AdminClient admin = AdminClient.create(adminProps())) {
            admin.createTopics(List.of(new NewTopic(topic, 1, (short) 1)))
                .all().get(15, java.util.concurrent.TimeUnit.SECONDS);
        }
        System.out.println("G1: created topic " + topic);

        try (ShareConsumer<byte[], byte[]> consumer =
                 new KafkaShareConsumer<>(consumerProps(groupId))) {
            consumer.subscribe(List.of(topic));

            // The raw KafkaShareConsumer doesn't register itself with the broker until it
            // calls poll() at least once. Drive empty polls in parallel with AdminClient
            // probes until the broker reports the assignment, with a hard 30s deadline.
            // Replaces the historical "subscribe + sleep 2s and hope" prime that produced
            // the inconclusive-pass behavior the reviewer flagged.
            assignWithPolling(consumer, groupId, topic, Duration.ofSeconds(30));
            System.out.println("G1: subscribed and assigned " + groupId);

            // Produce 50 records — enough to ensure a single poll() returns at least 2,
            // which is the precondition for exercising the unacked-second-poll path.
            try (var producer = new KafkaProducer<byte[], byte[]>(producerProps())) {
                for (int i = 0; i < 50; i++) {
                    producer.send(new ProducerRecord<>(
                        topic, new byte[]{(byte) i}, ("msg-" + i).getBytes())).get();
                }
                producer.flush();
            }
            System.out.println("G1: produced 50 records");

            // Poll repeatedly until we get a single batch with >= 2 records — the precondition
            // for exercising the unacked-second-poll path. To avoid prematurely tripping the
            // very ISE we're trying to verify, we FULLY ACK every undersized batch (acking the
            // entire previous batch satisfies the explicit-mode contract so the next poll is
            // legal). Hard deadline → test failure (no inconclusive exits).
            Duration deadline = Duration.ofSeconds(30);
            long deadlineNanos = System.nanoTime() + deadline.toNanos();
            ConsumerRecords<byte[], byte[]> first;
            while (true) {
                first = consumer.poll(Duration.ofSeconds(2));
                int got = first.count();
                if (got >= 2) break;
                if (got == 1) {
                    // Fully ack so the next poll() is contract-legal.
                    for (ConsumerRecord<byte[], byte[]> r : first) {
                        consumer.acknowledge(r, AcknowledgeType.ACCEPT);
                    }
                }
                if (System.nanoTime() > deadlineNanos) {
                    throw new AssertionError(
                        "G1 cannot be exercised: no single poll() returned >= 2 records within "
                        + deadline + ". Cannot test the unacked-second-poll path with fewer "
                        + "than 2 records in a batch.");
                }
            }
            int batchSize = first.count();
            System.out.println("G1: first poll returned " + batchSize + " records");

            List<ConsumerRecord<byte[], byte[]>> received = new ArrayList<>();
            for (ConsumerRecord<byte[], byte[]> r : first) received.add(r);

            // Ack all but the last record. With >= 2 records in the batch, this leaves
            // exactly one unacknowledged — sufficient to trigger the contract.
            int ackCount = batchSize - 1;
            for (int i = 0; i < ackCount; i++) {
                consumer.acknowledge(received.get(i), AcknowledgeType.ACCEPT);
            }
            System.out.println("G1: ACKed " + ackCount + " of " + batchSize
                + " records (1 left unacked)");

            // The contract assertion: a second poll with unacked records MUST throw ISE.
            // This is the KIP-932 explicit-mode contract that Plurima's drain barrier exists
            // to honor.
            ConsumerRecords<byte[], byte[]> second = null;
            IllegalStateException observed = null;
            try {
                second = consumer.poll(Duration.ofSeconds(2));
            } catch (IllegalStateException ise) {
                observed = ise;
            }

            Set<Long> firstOffsets = new HashSet<>();
            for (ConsumerRecord<byte[], byte[]> r : received) firstOffsets.add(r.offset());
            System.out.println("G1: first-poll offsets  = " + firstOffsets);
            if (second != null) {
                Set<Long> secondOffsets = new HashSet<>();
                for (ConsumerRecord<byte[], byte[]> r : second) secondOffsets.add(r.offset());
                System.out.println("G1: second-poll offsets = " + secondOffsets);
            }

            assertThat(observed)
                .as("KIP-932 explicit-mode contract: a second poll() with unacked records "
                    + "from the previous batch must throw IllegalStateException. If this "
                    + "assertion fails, Kafka has relaxed the contract and Plurima's drain "
                    + "barrier (design § 9 I9 / § 17.5 G1) may no longer be necessary.")
                .isNotNull()
                .hasMessageContaining("acknowledg");

            System.out.println("G1: OBSERVATION → ILLEGAL_STATE_EXCEPTION: " + observed.getMessage());
        } finally {
            try (AdminClient admin = AdminClient.create(adminProps())) {
                admin.deleteTopics(List.of(topic)).all().get(5, java.util.concurrent.TimeUnit.SECONDS);
            } catch (Exception ignored) {}
        }
    }

    /**
     * Repeatedly poll(short timeout) on the consumer (which is what makes the share-group
     * coordinator see it as a live member) while polling AdminClient.describeShareGroups
     * for the assignment to appear. Throws AssertionError on deadline.
     *
     * <p>Empty polls are safe here because no records have been produced yet — they don't
     * accumulate unacked records, so the explicit-mode contract is not engaged.
     */
    private static void assignWithPolling(
        ShareConsumer<byte[], byte[]> consumer,
        String groupId,
        String topic,
        Duration timeout) throws Exception {
        long deadlineNanos = System.nanoTime() + timeout.toNanos();
        try (AdminClient admin = AdminClient.create(adminProps())) {
            while (System.nanoTime() < deadlineNanos) {
                consumer.poll(Duration.ofMillis(500));  // drives heartbeat
                try {
                    var described = admin.describeShareGroups(List.of(groupId))
                        .all().get(2, java.util.concurrent.TimeUnit.SECONDS);
                    var d = described.get(groupId);
                    if (d != null) {
                        for (var m : d.members()) {
                            for (var tp : m.assignment().topicPartitions()) {
                                if (tp.topic().equals(topic)) return;
                            }
                        }
                    }
                } catch (Exception ignored) {
                    // group not yet visible to coordinator
                }
            }
        }
        throw new AssertionError("Share-group '" + groupId + "' did not get '" + topic
            + "' assigned within " + timeout);
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

    private static Properties consumerProps(String groupId) {
        Properties p = new Properties();
        p.put("bootstrap.servers", BOOTSTRAP);
        p.put("group.id", groupId);
        p.put("key.deserializer", ByteArrayDeserializer.class.getName());
        p.put("value.deserializer", ByteArrayDeserializer.class.getName());
        p.put("share.acknowledgement.mode", "explicit");
        // Encourage the broker to return a large batch on a single poll so we can leave at
        // least one record unacked. Without this, the broker may dribble records out one at
        // a time and we'd have no way to retry the poll (a second poll without acking would
        // itself trigger the very ISE we're trying to verify on the contract test poll).
        p.put("max.poll.records", "50");
        // Note: legacy auto.offset.reset is REJECTED by KafkaShareConsumer (ConfigException);
        // share.auto.offset.reset is a broker-side dynamic group config, not a client property.
        // The test compensates by subscribing before producing, so the group's initial position
        // sits at the (then-empty) end of log and naturally consumes the subsequent records.
        return p;
    }
}
