package io.plurima.kafka.integration;

import io.plurima.kafka.ConsumerEngine;
import io.plurima.kafka.OrderingGuarantee;
import io.plurima.kafka.OrderingMode;
import io.plurima.kafka.PlurimaConsumer;
import io.plurima.kafka.RecordListener;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static io.plurima.kafka.integration.KafkaIntegrationSupport.byteProducer;
import static io.plurima.kafka.integration.KafkaIntegrationSupport.classicConsumerProps;
import static io.plurima.kafka.integration.KafkaIntegrationSupport.createUniqueTopic;
import static io.plurima.kafka.integration.KafkaIntegrationSupport.deleteTopicQuietly;
import static io.plurima.kafka.integration.KafkaIntegrationSupport.waitForClassicAssignment;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Live tests that prove {@link ConsumerEngine#CLASSIC_BASIC} delivers STRICT
 * cross-cluster ordering for both {@link OrderingMode#KEY} and
 * {@link OrderingMode#PARTITION} — the headline feature of the classic engine
 * vs SHARE's instance-local-only.
 *
 * <p>Both tests use TWO consumers in the same consumer group, let the group
 * rebalance settle before producing, then verify the ordering invariant from
 * BOTH consumers' perspectives. With classic consumer-group assignment, each
 * partition is owned by exactly one consumer at a time, and (with a key-aware
 * partitioner — Kafka's default) same-key records land on the same partition.
 * Therefore:
 *
 * <ul>
 *   <li><b>KEY mode</b>: all records for a given key flow through the same
 *       consumer, in producer-publish order. Steady-state assignment + no
 *       rebalance during the run = per-key FIFO across the cluster.</li>
 *   <li><b>PARTITION mode</b>: all records for a given partition flow through
 *       the same consumer, in offset order. Same guarantee, finer granularity.</li>
 * </ul>
 *
 * <p>Tests verify by collecting (key, sequence) or (partition, offset) tuples per
 * consumer and asserting each consumer's list is monotonic. Cross-consumer
 * coverage is implicit: with 4 partitions split 2-2 between consumers, both
 * consumers process some records — neither is idle.
 */
@Tag("integration")
class ClassicBasicCrossClusterOrderingIntegrationTest {

    @Test
    void perKeyFifoHoldsAcrossTwoConsumers() throws Exception {
        int partitions = 4;
        int keys = 8;
        int perKey = 25;
        int total = keys * perKey;

        String topic = createUniqueTopic("plurima-int-classic-xkey", partitions);
        String groupId = "plurima-int-classic-xkey-group-" + UUID.randomUUID();

        // Per-consumer, per-key observed sequence numbers. We assert that each
        // consumer's per-key sequence is monotonic — proving that whichever
        // consumer owns a key's partition processes records in producer order.
        Map<String, Map<String, List<Integer>>> perConsumerPerKey = new ConcurrentHashMap<>();
        perConsumerPerKey.put("A", new ConcurrentHashMap<>());
        perConsumerPerKey.put("B", new ConcurrentHashMap<>());
        CountDownLatch latch = new CountDownLatch(total);

        RecordListener<byte[], byte[]> listenerA = recordingListener("A", perConsumerPerKey, latch);
        RecordListener<byte[], byte[]> listenerB = recordingListener("B", perConsumerPerKey, latch);

        PlurimaConsumer<byte[], byte[]> consumerA = PlurimaConsumer.<byte[], byte[]>builder()
            .kafkaProperties(classicConsumerProps(groupId))
            .topic(topic)
            .engine(ConsumerEngine.CLASSIC_BASIC)
            .ordering(OrderingMode.KEY)
            .orderingGuarantee(OrderingGuarantee.STRICT)
            .pollTimeout(Duration.ofMillis(200))
            .listener(listenerA)
            .build();
        PlurimaConsumer<byte[], byte[]> consumerB = PlurimaConsumer.<byte[], byte[]>builder()
            .kafkaProperties(classicConsumerProps(groupId))
            .topic(topic)
            .engine(ConsumerEngine.CLASSIC_BASIC)
            .ordering(OrderingMode.KEY)
            .orderingGuarantee(OrderingGuarantee.STRICT)
            .pollTimeout(Duration.ofMillis(200))
            .listener(listenerB)
            .build();

        try {
            consumerA.start();
            consumerB.start();
            // Wait deterministically for the rebalance to settle into a stable 2-member +
            // 4-partition assignment BEFORE producing. A fixed Thread.sleep would flake
            // under broker load; waitForClassicAssignment polls AdminClient.describeConsumerGroups
            // until both consumers have non-empty assignment AND total assigned partitions
            // matches the topic's partition count.
            waitForClassicAssignment(groupId, 2, partitions);

            try (var producer = byteProducer()) {
                // Round-robin by sequence so the broker interleaves keys.
                for (int seq = 0; seq < perKey; seq++) {
                    for (int k = 0; k < keys; k++) {
                        String value = "key" + k + ":seq" + seq;
                        producer.send(new ProducerRecord<>(
                            topic,
                            ("key" + k).getBytes(),
                            value.getBytes())).get();
                    }
                }
            }

            assertThat(latch.await(60, TimeUnit.SECONDS))
                .as("all %d records processed within timeout (steady-state assignment)", total)
                .isTrue();
        } finally {
            consumerA.close();
            consumerB.close();
            deleteTopicQuietly(topic);
        }

        // Assertion 1: each consumer's per-key sequence is monotonic.
        for (Map.Entry<String, Map<String, List<Integer>>> consumerEntry : perConsumerPerKey.entrySet()) {
            String consumer = consumerEntry.getKey();
            consumerEntry.getValue().forEach((key, seqs) -> {
                List<Integer> sorted = new ArrayList<>(seqs);
                sorted.sort(Integer::compareTo);
                assertThat(seqs)
                    .as("per-key FIFO for key=%s on consumer=%s", key, consumer)
                    .isEqualTo(sorted);
            });
        }

        // Assertion 2: BOTH consumers received some records (rebalance settled to 2+2).
        // If one consumer is empty, we haven't actually proved cross-consumer ordering.
        assertThat(perConsumerPerKey.get("A"))
            .as("consumer A must have processed at least some keys (rebalance settled)")
            .isNotEmpty();
        assertThat(perConsumerPerKey.get("B"))
            .as("consumer B must have processed at least some keys (rebalance settled)")
            .isNotEmpty();

        // Assertion 3: every key was observed exactly perKey times (no loss, no duplicates
        // since no rebalance during the run).
        Map<String, Integer> keyTotals = new HashMap<>();
        for (Map<String, List<Integer>> byKey : perConsumerPerKey.values()) {
            byKey.forEach((key, seqs) -> keyTotals.merge(key, seqs.size(), Integer::sum));
        }
        assertThat(keyTotals)
            .as("every key must have been processed exactly %d times", perKey)
            .hasSize(keys);
        keyTotals.forEach((k, count) ->
            assertThat(count).as("count for key=%s", k).isEqualTo(perKey));

        // Assertion 4: KEY OWNER EXCLUSIVITY. Each key must have been handled by EXACTLY
        // one consumer — never split across A and B. This is the load-bearing proof of
        // per-key cross-cluster FIFO under STRICT: a same-key split would let A and B
        // process different (key, seq) records concurrently in undefined relative order.
        // Without this assertion, a split could pass assertions 1+3 silently (each side
        // is monotonic; sizes sum to perKey).
        for (int ki = 0; ki < keys; ki++) {
            String k = "key" + ki;
            int kiFinal = ki;
            long owners = perConsumerPerKey.values().stream()
                .filter(m -> m.containsKey("key" + kiFinal))
                .count();
            assertThat(owners)
                .as("key=%s must be owned by exactly one consumer (no cross-consumer split)", k)
                .isEqualTo(1L);
        }
    }

    @Test
    void perPartitionFifoHoldsAcrossTwoConsumers() throws Exception {
        int partitions = 4;
        int perPartition = 25;
        int total = partitions * perPartition;

        String topic = createUniqueTopic("plurima-int-classic-xpart", partitions);
        String groupId = "plurima-int-classic-xpart-group-" + UUID.randomUUID();

        // Per-consumer, per-partition observed offsets. Each consumer's per-partition
        // offsets must be monotonic — proves classic per-partition cross-cluster ordering.
        Map<String, Map<Integer, List<Long>>> perConsumerPerPartition = new ConcurrentHashMap<>();
        perConsumerPerPartition.put("A", new ConcurrentHashMap<>());
        perConsumerPerPartition.put("B", new ConcurrentHashMap<>());
        CountDownLatch latch = new CountDownLatch(total);

        RecordListener<byte[], byte[]> listenerA = (r, ctx) -> {
            perConsumerPerPartition.get("A")
                .computeIfAbsent(r.partition(), p -> new CopyOnWriteArrayList<>())
                .add(r.offset());
            latch.countDown();
        };
        RecordListener<byte[], byte[]> listenerB = (r, ctx) -> {
            perConsumerPerPartition.get("B")
                .computeIfAbsent(r.partition(), p -> new CopyOnWriteArrayList<>())
                .add(r.offset());
            latch.countDown();
        };

        PlurimaConsumer<byte[], byte[]> consumerA = PlurimaConsumer.<byte[], byte[]>builder()
            .kafkaProperties(classicConsumerProps(groupId))
            .topic(topic)
            .engine(ConsumerEngine.CLASSIC_BASIC)
            .ordering(OrderingMode.PARTITION)
            .orderingGuarantee(OrderingGuarantee.STRICT)
            .pollTimeout(Duration.ofMillis(200))
            .listener(listenerA)
            .build();
        PlurimaConsumer<byte[], byte[]> consumerB = PlurimaConsumer.<byte[], byte[]>builder()
            .kafkaProperties(classicConsumerProps(groupId))
            .topic(topic)
            .engine(ConsumerEngine.CLASSIC_BASIC)
            .ordering(OrderingMode.PARTITION)
            .orderingGuarantee(OrderingGuarantee.STRICT)
            .pollTimeout(Duration.ofMillis(200))
            .listener(listenerB)
            .build();

        try {
            consumerA.start();
            consumerB.start();
            // Let rebalance settle (same reason as KEY test).
            Thread.sleep(5_000);

            try (var producer = byteProducer()) {
                // Round-robin across partitions so the broker interleaves them.
                for (int n = 0; n < perPartition; n++) {
                    for (int p = 0; p < partitions; p++) {
                        byte[] value = ("p" + p + "-n" + n).getBytes();
                        producer.send(new ProducerRecord<>(topic, p, null, value)).get();
                    }
                }
            }

            assertThat(latch.await(60, TimeUnit.SECONDS))
                .as("all %d records processed within timeout (steady-state assignment)", total)
                .isTrue();
        } finally {
            consumerA.close();
            consumerB.close();
            deleteTopicQuietly(topic);
        }

        // Each consumer's per-partition offset list must be monotonically increasing.
        for (var consumerEntry : perConsumerPerPartition.entrySet()) {
            String consumer = consumerEntry.getKey();
            consumerEntry.getValue().forEach((partition, offsets) -> {
                List<Long> sorted = new ArrayList<>(offsets);
                sorted.sort(Long::compareTo);
                assertThat(offsets)
                    .as("per-partition offset FIFO for partition=%d on consumer=%s",
                        partition, consumer)
                    .isEqualTo(sorted);
            });
        }

        // Each partition was owned by exactly ONE consumer in steady state — same-partition
        // records do NOT appear in both consumers' observation maps. This is the actual
        // cross-cluster ordering invariant: a partition isn't split.
        for (int p = 0; p < partitions; p++) {
            int p_final = p;
            long owners = perConsumerPerPartition.values().stream()
                .filter(m -> m.containsKey(p_final))
                .count();
            assertThat(owners)
                .as("partition %d must be owned by exactly one consumer in steady state", p)
                .isEqualTo(1L);
        }

        // Both consumers got some work (rebalance settled to a real split).
        assertThat(perConsumerPerPartition.get("A"))
            .as("consumer A must own at least one partition").isNotEmpty();
        assertThat(perConsumerPerPartition.get("B"))
            .as("consumer B must own at least one partition").isNotEmpty();

        // Total record count = produced.
        int totalSeen = perConsumerPerPartition.values().stream()
            .flatMap(m -> m.values().stream())
            .mapToInt(List::size)
            .sum();
        assertThat(totalSeen)
            .as("total records observed across both consumers")
            .isEqualTo(total);
    }

    /** Listener factory for the KEY test: extracts key + parses sequence from value. */
    private static RecordListener<byte[], byte[]> recordingListener(
        String consumerLabel,
        Map<String, Map<String, List<Integer>>> perConsumerPerKey,
        CountDownLatch latch) {
        return (r, ctx) -> {
            String key = new String(r.key());
            String value = new String(r.value());
            // value format: "key0:seq3"
            int seq = Integer.parseInt(value.substring(value.indexOf("seq") + 3));
            perConsumerPerKey.get(consumerLabel)
                .computeIfAbsent(key, k -> new CopyOnWriteArrayList<>())
                .add(seq);
            latch.countDown();
        };
    }
}
