package io.plurima.kafka;

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
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * Characterization of Kafka 4.2's {@code acknowledge(record, RENEW)} semantics for the SHARE
 * consumer. <b>Plurima does NOT expose lock renewal</b> (see {@code AckContext#acknowledge} and the
 * UserGuide) — these tests pin the broker/client behavior that <em>informed that decision</em>, so
 * a future Kafka change surfaces here rather than silently shifting the contract.
 *
 * <p>The three tests build up the full picture, which is internally consistent once you separate
 * "the immediate next poll" from "subsequent polls":
 * <ol>
 *   <li>{@link #renewSatisfiesPollContractAndIsNotRedeliveredOnImmediateNextPoll()} — a RENEW
 *       satisfies the explicit-mode poll contract (the very next {@code poll()} does NOT throw
 *       {@code IllegalStateException}), and the renewed record is not re-delivered on that
 *       <em>immediate</em> poll (the renewal round-trip hasn't completed yet).</li>
 *   <li>{@link #doesRenewRedeliverTheRecordOnLaterPolls()} — but once the renewal round-trips,
 *       <em>later</em> polls DO re-deliver the record (same {@code deliveryCount}). This is the
 *       finding that makes a "renew in the background while the worker keeps running" design
 *       non-viable, and why Plurima does not offer renewal.</li>
 *   <li>{@link #acceptFailsInRenewingWindowButSucceedsOnRedelivery()} — a terminal ACCEPT throws
 *       in the renewing window but succeeds against the re-delivered instance.</li>
 * </ol>
 *
 * <p>Tagged {@code integration} so it does not run on every push (needs a real 4.2 broker).
 */
@Tag("integration")
class ShareConsumerRenewContractTest {

    private static final String BOOTSTRAP = "localhost:9092";

    @Test
    void renewSatisfiesPollContractAndIsNotRedeliveredOnImmediateNextPoll() throws Exception {
        String topic = "plurima-renew-verification-" + UUID.randomUUID();
        String groupId = "plurima-renew-group-" + UUID.randomUUID();

        try (AdminClient admin = AdminClient.create(adminProps())) {
            admin.createTopics(List.of(new NewTopic(topic, 1, (short) 1)))
                .all().get(15, java.util.concurrent.TimeUnit.SECONDS);
        }

        try (ShareConsumer<byte[], byte[]> consumer =
                 new KafkaShareConsumer<>(consumerProps(groupId))) {
            consumer.subscribe(List.of(topic));
            assignWithPolling(consumer, groupId, topic, Duration.ofSeconds(30));

            try (var producer = new KafkaProducer<byte[], byte[]>(producerProps())) {
                for (int i = 0; i < 50; i++) {
                    producer.send(new ProducerRecord<>(
                        topic, new byte[]{(byte) i}, ("msg-" + i).getBytes())).get();
                }
                producer.flush();
            }

            // Poll until we get a batch with >= 2 records (need one to RENEW and the rest to ACCEPT).
            ConsumerRecords<byte[], byte[]> first;
            long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
            while (true) {
                first = consumer.poll(Duration.ofSeconds(2));
                if (first.count() >= 2) break;
                // Fully ack undersized batches so the next poll stays contract-legal.
                for (ConsumerRecord<byte[], byte[]> r : first) {
                    consumer.acknowledge(r, AcknowledgeType.ACCEPT);
                }
                if (System.nanoTime() > deadline) {
                    throw new AssertionError("could not get a batch of >= 2 records to exercise RENEW");
                }
            }

            List<ConsumerRecord<byte[], byte[]>> batch = new ArrayList<>();
            for (ConsumerRecord<byte[], byte[]> r : first) batch.add(r);
            ConsumerRecord<byte[], byte[]> renewed = batch.get(0);
            long renewedOffset = renewed.offset();

            // RENEW the first record; terminally ACCEPT the rest so the all-acknowledged gate
            // is satisfied entirely by [RENEW(renewed), ACCEPT(others)].
            consumer.acknowledge(renewed, AcknowledgeType.RENEW);
            for (int i = 1; i < batch.size(); i++) {
                consumer.acknowledge(batch.get(i), AcknowledgeType.ACCEPT);
            }

            // (1) Second poll must NOT throw the explicit-mode ISE — RENEW satisfied the gate.
            ConsumerRecords<byte[], byte[]> second =
                assertSecondPollDoesNotThrow(consumer);

            // (2) On the IMMEDIATE next poll the renewed offset is not re-delivered (the renewal
            // round-trip hasn't completed yet). NOTE: this is only true for this immediate poll —
            // doesRenewRedeliverTheRecordOnLaterPolls() shows it IS re-delivered on subsequent polls.
            Set<Long> redelivered = new HashSet<>();
            for (ConsumerRecord<byte[], byte[]> r : second) redelivered.add(r.offset());
            // Accept the freshly delivered records so we stay contract-legal for the cycle below.
            for (ConsumerRecord<byte[], byte[]> r : second) {
                consumer.acknowledge(r, AcknowledgeType.ACCEPT);
            }
            assertThat(redelivered)
                .as("renewed record is not re-delivered on the immediate next poll (round-trip pending)")
                .doesNotContain(renewedOffset);

            // (3) Full cycle: after the renewal round-trips through the broker, the record must
            // still be terminally acknowledgeable. Drive a few polls to let the renewal confirm
            // and the record return to the in-flight set, then ACCEPT it.
            assertThatCode(() -> {
                boolean acked = false;
                long cycleDeadline = System.nanoTime() + Duration.ofSeconds(15).toNanos();
                while (!acked && System.nanoTime() < cycleDeadline) {
                    try {
                        consumer.acknowledge(renewed, AcknowledgeType.ACCEPT);
                        acked = true;
                    } catch (IllegalStateException transientWindow) {
                        // Record is mid-renewal (renewing window) — poll to advance, then retry.
                        ConsumerRecords<byte[], byte[]> more = consumer.poll(Duration.ofMillis(500));
                        for (ConsumerRecord<byte[], byte[]> r : more) {
                            consumer.acknowledge(r, AcknowledgeType.ACCEPT);
                        }
                    }
                }
                if (!acked) throw new AssertionError("renewed record never became acknowledgeable again");
            }).doesNotThrowAnyException();
        } finally {
            try (AdminClient admin = AdminClient.create(adminProps())) {
                admin.deleteTopics(List.of(topic)).all().get(5, java.util.concurrent.TimeUnit.SECONDS);
            } catch (Exception ignored) {}
        }
    }

    /**
     * Definitive probe for the load-bearing question: does a RENEW'd-but-not-terminally-acked
     * record get RE-DELIVERED to the application on subsequent polls? RENEW one record, never
     * terminally ack it, and poll many times — recording whether its offset reappears. The
     * outcome decides whether background lock renewal (worker keeps running) is even possible.
     */
    @Test
    void doesRenewRedeliverTheRecordOnLaterPolls() throws Exception {
        String topic = "plurima-renew-redeliver-" + UUID.randomUUID();
        String groupId = "plurima-renew-redeliver-group-" + UUID.randomUUID();
        try (AdminClient admin = AdminClient.create(adminProps())) {
            admin.createTopics(List.of(new NewTopic(topic, 1, (short) 1)))
                .all().get(15, java.util.concurrent.TimeUnit.SECONDS);
        }
        try (ShareConsumer<byte[], byte[]> consumer =
                 new KafkaShareConsumer<>(consumerProps(groupId))) {
            consumer.subscribe(List.of(topic));
            assignWithPolling(consumer, groupId, topic, Duration.ofSeconds(30));
            try (var producer = new KafkaProducer<byte[], byte[]>(producerProps())) {
                producer.send(new ProducerRecord<>(topic, new byte[]{0}, "only".getBytes())).get();
                producer.flush();
            }

            // Get the single record and RENEW it (never ACCEPT it).
            ConsumerRecord<byte[], byte[]> r = null;
            long deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
            while (r == null && System.nanoTime() < deadline) {
                for (ConsumerRecord<byte[], byte[]> c : consumer.poll(Duration.ofSeconds(1))) r = c;
            }
            assertThat(r).as("produced record delivered").isNotNull();
            long off = r.offset();
            consumer.acknowledge(r, AcknowledgeType.RENEW);

            // Poll repeatedly. Record how many times the same offset is re-delivered, and the
            // deliveryCount the broker reports each time.
            int redeliveries = 0;
            List<Short> deliveryCounts = new ArrayList<>();
            for (int i = 0; i < 15; i++) {
                ConsumerRecords<byte[], byte[]> batch = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<byte[], byte[]> c : batch) {
                    if (c.offset() == off) {
                        redeliveries++;
                        deliveryCounts.add(c.deliveryCount().orElse((short) -1));
                        consumer.acknowledge(c, AcknowledgeType.RENEW);  // keep renewing
                    }
                }
            }
            // CHARACTERIZATION (verified on Kafka 4.2.1): acknowledge(RENEW) RE-DELIVERS the
            // record to the application on subsequent polls (here 7×), each time with the SAME
            // deliveryCount=1 — i.e. it is NOT a broker redelivery and NOT a transparent
            // background lock extension. takeRenewals() puts the renewed record back into the
            // consumer's in-flight set, so poll() hands it to the app again.
            //
            // CONSEQUENCE: Plurima's lock-renewal design ("renew in the background while the
            // worker keeps running, ACCEPT when it finishes") is NOT viable on this semantic —
            // the record bounces back every poll and the terminal ACCEPT never escapes the
            // renewing window. See ShareRenewalIntegrationTest (@Disabled) for the end-to-end
            // failure this produces. If a future Kafka release stops re-delivering RENEW'd
            // records, this test will fail (redeliveries == 0) — the signal to revisit the design.
            assertThat(redeliveries)
                .as("RENEW re-delivered offset %d with deliveryCounts=%s — documents the "
                    + "re-delivery semantic that blocks background lock renewal", off, deliveryCounts)
                .isGreaterThan(0);
        } finally {
            try (AdminClient admin = AdminClient.create(adminProps())) {
                admin.deleteTopics(List.of(topic)).all().get(5, java.util.concurrent.TimeUnit.SECONDS);
            } catch (Exception ignored) {}
        }
    }

    /**
     * Pinpoints WHY a terminal ACCEPT fails after a RENEW, and whether it can ever succeed.
     * Hypothesis (from {@code ShareInFlightBatch} bytecode): {@code acknowledge()} requires the
     * offset to be present in the consumer's {@code inFlightRecords}; a RENEW evicts it (into
     * {@code renewingRecords}) until a later poll's {@code takeRenewals()} re-adds it — and that
     * same poll RE-DELIVERS it. So:
     * <ul>
     *   <li>ACCEPT issued in the parked window (offset not in inFlightRecords) → ISE.</li>
     *   <li>ACCEPT issued against the RE-DELIVERED instance (offset back in inFlightRecords) →
     *       succeeds and terminates the record.</li>
     * </ul>
     * If both hold, lock renewal is salvageable — but only by acking the re-delivery, not by
     * acking the original delivery from a background worker.
     */
    @Test
    void acceptFailsInRenewingWindowButSucceedsOnRedelivery() throws Exception {
        String topic = "plurima-renew-accept-" + UUID.randomUUID();
        String groupId = "plurima-renew-accept-group-" + UUID.randomUUID();
        try (AdminClient admin = AdminClient.create(adminProps())) {
            admin.createTopics(List.of(new NewTopic(topic, 1, (short) 1)))
                .all().get(15, java.util.concurrent.TimeUnit.SECONDS);
        }
        try (ShareConsumer<byte[], byte[]> consumer =
                 new KafkaShareConsumer<>(consumerProps(groupId))) {
            consumer.subscribe(List.of(topic));
            assignWithPolling(consumer, groupId, topic, Duration.ofSeconds(30));
            try (var producer = new KafkaProducer<byte[], byte[]>(producerProps())) {
                producer.send(new ProducerRecord<>(topic, new byte[]{0}, "x".getBytes())).get();
                producer.flush();
            }

            ConsumerRecord<byte[], byte[]> original = null;
            long deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
            while (original == null && System.nanoTime() < deadline) {
                for (ConsumerRecord<byte[], byte[]> c : consumer.poll(Duration.ofSeconds(1))) original = c;
            }
            assertThat(original).isNotNull();
            long off = original.offset();

            // RENEW evicts the offset from inFlightRecords on the next commit/poll.
            consumer.acknowledge(original, AcknowledgeType.RENEW);
            consumer.poll(Duration.ofMillis(300));   // drive the RENEW commit → takeAcknowledgedRecords

            // (A) In the parked (renewing) window, ACCEPT on the original must throw ISE.
            IllegalStateException parked = null;
            try {
                consumer.acknowledge(original, AcknowledgeType.ACCEPT);
            } catch (IllegalStateException e) {
                parked = e;
            }

            // (B) Poll until the record is re-delivered, then ACCEPT the re-delivered instance.
            ConsumerRecord<byte[], byte[]> redelivered = null;
            deadline = System.nanoTime() + Duration.ofSeconds(15).toNanos();
            while (redelivered == null && System.nanoTime() < deadline) {
                for (ConsumerRecord<byte[], byte[]> c : consumer.poll(Duration.ofMillis(500))) {
                    if (c.offset() == off) redelivered = c;
                }
            }
            assertThat(redelivered).as("renewed record is re-delivered").isNotNull();
            // ACCEPT the re-delivered instance — should succeed (offset is back in inFlightRecords).
            consumer.acknowledge(redelivered, AcknowledgeType.ACCEPT);

            // (C) After ACCEPT on the re-delivery, the record must NOT come back.
            int afterAccept = 0;
            for (int i = 0; i < 8; i++) {
                for (ConsumerRecord<byte[], byte[]> c : consumer.poll(Duration.ofMillis(500))) {
                    if (c.offset() == off) afterAccept++;
                }
            }

            assertThat(parked)
                .as("ACCEPT in the renewing window throws ISE (offset evicted from inFlightRecords)")
                .isNotNull()
                .hasMessageContaining("acknowledg");
            assertThat(afterAccept)
                .as("ACCEPT on the re-delivered instance terminates the record (no further re-delivery)")
                .isZero();
        } finally {
            try (AdminClient admin = AdminClient.create(adminProps())) {
                admin.deleteTopics(List.of(topic)).all().get(5, java.util.concurrent.TimeUnit.SECONDS);
            } catch (Exception ignored) {}
        }
    }

    private static ConsumerRecords<byte[], byte[]> assertSecondPollDoesNotThrow(
        ShareConsumer<byte[], byte[]> consumer) {
        try {
            return consumer.poll(Duration.ofSeconds(2));
        } catch (IllegalStateException ise) {
            throw new AssertionError(
                "RENEW did not satisfy the explicit-mode poll contract — second poll threw ISE: "
                + ise.getMessage() + ". If this fires, Kafka treats RENEW differently than 4.2.0 "
                + "and the lock-renewal design must be revisited.", ise);
        }
    }

    private static void assignWithPolling(
        ShareConsumer<byte[], byte[]> consumer, String groupId, String topic, Duration timeout)
        throws Exception {
        long deadlineNanos = System.nanoTime() + timeout.toNanos();
        try (AdminClient admin = AdminClient.create(adminProps())) {
            while (System.nanoTime() < deadlineNanos) {
                consumer.poll(Duration.ofMillis(500));
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
                    // group not yet visible
                }
            }
        }
        throw new AssertionError("share-group '" + groupId + "' did not get '" + topic + "' assigned");
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
        p.put("max.poll.records", "50");
        return p;
    }
}
