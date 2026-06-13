package io.plurima.kafka.integration;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.ShareGroupDescription;
import org.apache.kafka.clients.admin.ShareMemberDescription;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Shared static helpers for Plurima feature integration tests.
 * All tests using this support run against a real Kafka 4.2.0 broker at {@code localhost:9092}.
 *
 * <p>Public so tests outside the {@code io.plurima.kafka.integration} package
 * (e.g. {@code Phase9HardeningEndToEndTest}, {@code G1FixedEndToEndTest} in the parent
 * package) can call {@link #waitForAssignment} too.
 */
public final class KafkaIntegrationSupport {

    public static final String BOOTSTRAP = "localhost:9092";

    /**
     * Defensive lower bound for {@link #waitForAssignment} — even if AdminClient reports
     * the share-group member has the topic assigned, we give the consumer this many
     * milliseconds afterwards to settle (the first {@code poll()} may still need a moment).
     * Reduced from 6 000 to 500 because the wait itself is now deterministic.
     */
    public static final long PRIME_MS = 500;

    /** Maximum total wait for {@link #waitForAssignment} before giving up. */
    public static final Duration ASSIGNMENT_TIMEOUT = Duration.ofSeconds(30);

    private KafkaIntegrationSupport() {}

    /**
     * Block until the broker reports that the given share group has at least one member
     * assigned to the given topic, then return. Replaces the historical fixed
     * {@code Thread.sleep(6_000)} prime that hid real release regressions behind sleep luck.
     *
     * <p>Uses {@link AdminClient#describeShareGroups(java.util.Collection)} (KIP-932). Polls
     * every 200 ms up to {@link #ASSIGNMENT_TIMEOUT}, returning as soon as a member with the
     * topic appears in the group's assignment, then sleeps an additional {@link #PRIME_MS}
     * (500 ms) to let the consumer's first {@code poll()} settle.
     *
     * @throws AssertionError if no assignment appears within {@link #ASSIGNMENT_TIMEOUT}.
     */
    public static void waitForAssignment(String groupId, String topic) throws Exception {
        long deadlineNanos = System.nanoTime() + ASSIGNMENT_TIMEOUT.toNanos();
        try (AdminClient admin = AdminClient.create(adminProps())) {
            while (System.nanoTime() < deadlineNanos) {
                try {
                    Map<String, ShareGroupDescription> described =
                        admin.describeShareGroups(List.of(groupId)).all().get(5, TimeUnit.SECONDS);
                    ShareGroupDescription d = described.get(groupId);
                    if (d != null) {
                        boolean assigned = false;
                        for (ShareMemberDescription m : d.members()) {
                            for (var tp : m.assignment().topicPartitions()) {
                                if (tp.topic().equals(topic)) { assigned = true; break; }
                            }
                            if (assigned) break;
                        }
                        if (assigned) {
                            // Brief settle for the consumer's first poll() to land.
                            Thread.sleep(PRIME_MS);
                            return;
                        }
                    }
                } catch (Exception swallow) {
                    // Group not yet known to coordinator, etc. — retry until deadline.
                }
                Thread.sleep(200);
            }
        }
        throw new AssertionError(
            "Share-group '" + groupId + "' did not get '" + topic + "' assigned within "
            + ASSIGNMENT_TIMEOUT);
    }

    /**
     * Classic-consumer-group counterpart to {@link #waitForAssignment}. Blocks until the
     * specified number of group members all have non-empty partition assignment AND the
     * total assigned partition count equals {@code expectedTotalPartitions}. Replaces
     * fixed-sleep "let the rebalance settle" idioms in multi-consumer classic tests —
     * fixed sleeps flake under broker load.
     *
     * <p>Used by {@code ClassicBasicCrossClusterOrderingIntegrationTest}; safe to use for
     * any classic-engine test that needs a stable rebalance state before driving traffic.
     *
     * @param groupId the consumer group ID
     * @param expectedMembers number of consumers expected in the group
     * @param expectedTotalPartitions total partition count across all assignments
     * @throws AssertionError if the steady-state isn't reached within
     *     {@link #ASSIGNMENT_TIMEOUT}.
     */
    public static void waitForClassicAssignment(
        String groupId, int expectedMembers, int expectedTotalPartitions) throws Exception {
        long deadlineNanos = System.nanoTime() + ASSIGNMENT_TIMEOUT.toNanos();
        try (AdminClient admin = AdminClient.create(adminProps())) {
            while (System.nanoTime() < deadlineNanos) {
                try {
                    Map<String, ConsumerGroupDescription> described =
                        admin.describeConsumerGroups(List.of(groupId)).all()
                            .get(5, TimeUnit.SECONDS);
                    ConsumerGroupDescription d = described.get(groupId);
                    if (d != null && d.members().size() == expectedMembers) {
                        int totalAssigned = 0;
                        boolean allHaveSome = true;
                        for (MemberDescription m : d.members()) {
                            int n = m.assignment().topicPartitions().size();
                            totalAssigned += n;
                            if (n == 0) {
                                allHaveSome = false;
                                break;
                            }
                        }
                        if (allHaveSome && totalAssigned == expectedTotalPartitions) {
                            // Brief settle for the first poll() on each consumer to land.
                            Thread.sleep(PRIME_MS);
                            return;
                        }
                    }
                } catch (Exception swallow) {
                    // Group not yet known to coordinator, or rebalance in progress — retry.
                }
                Thread.sleep(200);
            }
        }
        throw new AssertionError(
            "Classic consumer-group '" + groupId + "' did not reach steady-state "
            + expectedMembers + " members × " + expectedTotalPartitions + " total partitions "
            + "within " + ASSIGNMENT_TIMEOUT);
    }

    public static String createUniqueTopic(String prefix) throws Exception {
        return createUniqueTopic(prefix, 1);
    }

    public static String createUniqueTopic(String prefix, int partitions) throws Exception {
        String topic = prefix + "-" + UUID.randomUUID();
        try (AdminClient admin = AdminClient.create(adminProps())) {
            admin.createTopics(List.of(new NewTopic(topic, partitions, (short) 1)))
                .all().get(15, TimeUnit.SECONDS);
        }
        return topic;
    }

    public static void deleteTopicQuietly(String topic) {
        try (AdminClient admin = AdminClient.create(adminProps())) {
            admin.deleteTopics(List.of(topic)).all().get(5, TimeUnit.SECONDS);
        } catch (Exception ignored) {}
    }

    public static Properties adminProps() {
        Properties p = new Properties();
        p.put("bootstrap.servers", BOOTSTRAP);
        p.put("request.timeout.ms", "10000");
        return p;
    }

    public static Properties consumerProps(String groupId) {
        Properties p = new Properties();
        p.put("bootstrap.servers", BOOTSTRAP);
        p.put("group.id", groupId);
        p.put("share.acknowledgement.mode", "explicit");
        return p;
    }

    /**
     * Properties for {@code ConsumerEngine.CLASSIC_BASIC} integration tests — vanilla
     * KafkaConsumer config. NOT compatible with the SHARE engine (no share.* settings).
     */
    public static Properties classicConsumerProps(String groupId) {
        Properties p = new Properties();
        p.put("bootstrap.servers", BOOTSTRAP);
        p.put("group.id", groupId);
        p.put("auto.offset.reset", "earliest");
        return p;
    }

    /**
     * Producer with byte[] key and byte[] value.
     */
    public static KafkaProducer<byte[], byte[]> byteProducer() {
        Properties p = new Properties();
        p.put("bootstrap.servers", BOOTSTRAP);
        p.put("key.serializer", ByteArraySerializer.class.getName());
        p.put("value.serializer", ByteArraySerializer.class.getName());
        p.put("acks", "all");
        return new KafkaProducer<>(p);
    }

    /**
     * Producer with byte[] key and String value (UTF-8 serialization).
     */
    public static KafkaProducer<byte[], String> stringValueProducer() {
        Properties p = new Properties();
        p.put("bootstrap.servers", BOOTSTRAP);
        p.put("key.serializer", ByteArraySerializer.class.getName());
        p.put("value.serializer", StringSerializer.class.getName());
        p.put("acks", "all");
        return new KafkaProducer<>(p);
    }
}
