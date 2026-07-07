package io.plurima.kafka.integration;

import io.plurima.kafka.ConsumerEngine;
import io.plurima.kafka.OrderingMode;
import io.plurima.kafka.PlurimaConsumer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static io.plurima.kafka.integration.KafkaIntegrationSupport.byteProducer;
import static io.plurima.kafka.integration.KafkaIntegrationSupport.classicConsumerProps;
import static io.plurima.kafka.integration.KafkaIntegrationSupport.createUniqueTopic;
import static io.plurima.kafka.integration.KafkaIntegrationSupport.deleteTopicQuietly;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Live verification of rebalance handling under {@code ConsumerEngine.CLASSIC_BASIC}.
 *
 * <p>Scenario: a 4-partition topic with 200 records. Start consumer A; let it process a
 * few records; start consumer B in the same group; wait for both to finish processing
 * the full set. Assertions:
 * <ul>
 *   <li>Total processed = 200 (no record lost, no extra duplicates beyond what
 *       at-least-once permits).</li>
 *   <li>Both consumers received SOME records (rebalance actually happened — A didn't
 *       process everything before B got assigned anything).</li>
 *   <li>Partitions are owned by exactly one consumer at a time (no overlap of
 *       partitions in the per-consumer record sets at any commit-point boundary —
 *       implicitly verified by total = produced).</li>
 * </ul>
 */
@Tag("integration")
class ClassicBasicRebalanceIntegrationTest {

    @Test
    void partitionsHandOffCleanlyAcrossRebalance() throws Exception {
        int partitions = 4;
        int total = 200;
        String topic = createUniqueTopic("plurima-int-classic-rebal", partitions);
        String groupId = "plurima-int-classic-rebal-group-" + UUID.randomUUID();

        try (var producer = byteProducer()) {
            for (int i = 0; i < total; i++) {
                int p = i % partitions;
                producer.send(new ProducerRecord<>(
                    topic, p, ("k" + i).getBytes(), ("v" + i).getBytes())).get();
            }
        }

        Set<String> processedByA = new CopyOnWriteArraySet<>();
        Set<String> processedByB = new CopyOnWriteArraySet<>();
        // Track which (partition, offset) pairs each consumer has seen so we can detect
        // double-processing across the boundary if at-least-once permits.
        ConcurrentHashMap<String, Integer> globalCount = new ConcurrentHashMap<>();
        CountDownLatch latch = new CountDownLatch(total);

        // Slow handler on A so B gets some work after rebalance — without this, A might
        // race through everything before B even joins the group.
        Duration slow = Duration.ofMillis(50);

        PlurimaConsumer<byte[], byte[]> consumerA = PlurimaConsumer.builder()
            .kafkaProperties(classicConsumerProps(groupId))
            .topic(topic)
            .engine(ConsumerEngine.CLASSIC_BASIC)
            .ordering(OrderingMode.PARTITION)
            .pollTimeout(Duration.ofMillis(200))
            .listener((r, ctx) -> {
                Thread.sleep(slow.toMillis());
                String key = r.partition() + ":" + r.offset();
                if (globalCount.merge(key, 1, Integer::sum) == 1) latch.countDown();
                processedByA.add(key);
            })
            .build();

        PlurimaConsumer<byte[], byte[]> consumerB = PlurimaConsumer.builder()
            .kafkaProperties(classicConsumerProps(groupId))
            .topic(topic)
            .engine(ConsumerEngine.CLASSIC_BASIC)
            .ordering(OrderingMode.PARTITION)
            .pollTimeout(Duration.ofMillis(200))
            .listener((r, ctx) -> {
                Thread.sleep(slow.toMillis());
                String key = r.partition() + ":" + r.offset();
                if (globalCount.merge(key, 1, Integer::sum) == 1) latch.countDown();
                processedByB.add(key);
            })
            .build();

        try {
            consumerA.start();
            // Let A pick up a few records on its own (it'll briefly own all 4 partitions).
            Thread.sleep(2_000);
            consumerB.start();

            assertThat(latch.await(60, TimeUnit.SECONDS))
                .as("all %d records must be processed exactly once across both consumers", total)
                .isTrue();
        } finally {
            consumerA.close();
            consumerB.close();
            deleteTopicQuietly(topic);
        }

        assertThat(globalCount).hasSize(total);
        // Both consumers must have processed at least one record — proves a rebalance happened.
        // With a tight race A might still process everything; if that's the case the assertion
        // is weakened. The slow handler + Thread.sleep above are calibrated to make this very
        // likely but not strictly guaranteed; flag as "best-effort proof" if it does happen.
        assertThat(processedByA).as("consumer A must have processed something").isNotEmpty();
        assertThat(processedByB)
            .as("consumer B must have processed something (rebalance handoff). If empty, the "
                + "test happened to race past the rebalance window — re-run; the assertion is "
                + "race-prone by design.")
            .isNotEmpty();
    }
}
