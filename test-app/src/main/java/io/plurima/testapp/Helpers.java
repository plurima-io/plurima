package io.plurima.testapp;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.ShareGroupDescription;
import org.apache.kafka.clients.admin.ShareMemberDescription;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Cluster helpers for the Plurima test app. Mirrors the test-only helpers under
 * {@code KafkaIntegrationSupport} but lives in the {@code test-app} module so the
 * main {@code :core} test sources stay clean and shippable.
 */
public final class Helpers {

    private final String bootstrap;

    public Helpers(String bootstrap) {
        this.bootstrap = bootstrap;
    }

    public Properties adminProps() {
        Properties p = new Properties();
        p.put("bootstrap.servers", bootstrap);
        p.put("request.timeout.ms", "10000");
        return p;
    }

    public Properties shareConsumerProps(String groupId) {
        Properties p = new Properties();
        p.put("bootstrap.servers", bootstrap);
        p.put("group.id", groupId);
        p.put("share.acknowledgement.mode", "explicit");
        return p;
    }

    public Properties classicConsumerProps(String groupId) {
        Properties p = new Properties();
        p.put("bootstrap.servers", bootstrap);
        p.put("group.id", groupId);
        p.put("auto.offset.reset", "earliest");
        return p;
    }

    public Producer<byte[], byte[]> byteProducer() {
        Properties p = new Properties();
        p.put("bootstrap.servers", bootstrap);
        p.put("key.serializer", ByteArraySerializer.class.getName());
        p.put("value.serializer", ByteArraySerializer.class.getName());
        p.put("acks", "all");
        return new KafkaProducer<>(p);
    }

    public String createUniqueTopic(String prefix, int partitions) throws Exception {
        String topic = prefix + "-" + UUID.randomUUID();
        try (AdminClient admin = AdminClient.create(adminProps())) {
            admin.createTopics(List.of(new NewTopic(topic, partitions, (short) 1)))
                .all().get(15, TimeUnit.SECONDS);
        }
        return topic;
    }

    public void deleteTopicQuietly(String topic) {
        try (AdminClient admin = AdminClient.create(adminProps())) {
            admin.deleteTopics(List.of(topic)).all().get(5, TimeUnit.SECONDS);
        } catch (Exception ignored) {}
    }

    /** Block until the share-group has the topic assigned to some member, or timeout. */
    public void waitForShareAssignment(String groupId, String topic) throws Exception {
        long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
        try (AdminClient admin = AdminClient.create(adminProps())) {
            while (System.nanoTime() < deadline) {
                try {
                    Map<String, ShareGroupDescription> described =
                        admin.describeShareGroups(List.of(groupId)).all().get(5, TimeUnit.SECONDS);
                    ShareGroupDescription d = described.get(groupId);
                    if (d != null) {
                        for (ShareMemberDescription m : d.members()) {
                            for (var tp : m.assignment().topicPartitions()) {
                                if (tp.topic().equals(topic)) {
                                    Thread.sleep(500);
                                    return;
                                }
                            }
                        }
                    }
                } catch (Exception swallow) {
                    // not yet known — keep polling
                }
                Thread.sleep(200);
            }
        }
        throw new AssertionError("Share-group " + groupId + " did not assign " + topic
            + " within 30s — is the broker up?");
    }

    /** Block until the classic consumer group has the expected steady-state assignment. */
    public void waitForClassicAssignment(String groupId, int expectedMembers, int expectedTotalPartitions)
        throws Exception {
        long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
        try (AdminClient admin = AdminClient.create(adminProps())) {
            while (System.nanoTime() < deadline) {
                try {
                    Map<String, ConsumerGroupDescription> described =
                        admin.describeConsumerGroups(List.of(groupId)).all().get(5, TimeUnit.SECONDS);
                    ConsumerGroupDescription d = described.get(groupId);
                    if (d != null && d.members().size() == expectedMembers) {
                        int total = 0;
                        boolean allHaveSome = true;
                        for (MemberDescription m : d.members()) {
                            int n = m.assignment().topicPartitions().size();
                            total += n;
                            if (n == 0) { allHaveSome = false; break; }
                        }
                        if (allHaveSome && total == expectedTotalPartitions) {
                            Thread.sleep(500);
                            return;
                        }
                    }
                } catch (Exception swallow) {
                    // not yet known
                }
                Thread.sleep(200);
            }
        }
        throw new AssertionError("Classic group " + groupId + " did not reach "
            + expectedMembers + " members × " + expectedTotalPartitions + " partitions within 30s");
    }
}
