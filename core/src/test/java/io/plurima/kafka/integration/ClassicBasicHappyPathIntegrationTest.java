package io.plurima.kafka.integration;

import io.plurima.kafka.ConsumerEngine;
import io.plurima.kafka.OrderingMode;
import io.plurima.kafka.OrderingGuarantee;
import io.plurima.kafka.PlurimaConsumer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static io.plurima.kafka.integration.KafkaIntegrationSupport.BOOTSTRAP;
import static io.plurima.kafka.integration.KafkaIntegrationSupport.byteProducer;
import static io.plurima.kafka.integration.KafkaIntegrationSupport.classicConsumerProps;
import static io.plurima.kafka.integration.KafkaIntegrationSupport.createUniqueTopic;
import static io.plurima.kafka.integration.KafkaIntegrationSupport.deleteTopicQuietly;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies the CLASSIC_BASIC engine happy path against a real broker. Produces records
 * across multiple partitions; asserts each partition's records arrive in offset order
 * at the listener, AND offsets are committed (verified indirectly: a second consumer
 * in the same group, started after the first finishes, sees no records).
 *
 * <p>Unlike the SHARE integration tests, the classic consumer joins via vanilla
 * consumer-group rebalance and uses {@code auto.offset.reset=earliest}, so we can
 * produce records BEFORE the consumer starts.
 */
@Tag("integration")
class ClassicBasicHappyPathIntegrationTest {

    @Test
    void perPartitionOrderingAndCommitsThroughRealBroker() throws Exception {
        int partitions = 3;
        int perPartition = 20;
        int total = partitions * perPartition;

        String topic = createUniqueTopic("plurima-int-classic", partitions);
        String groupId = "plurima-int-classic-group-" + UUID.randomUUID();

        // 1) Produce records first — auto.offset.reset=earliest will pick them up.
        try (var producer = byteProducer()) {
            for (int n = 0; n < perPartition; n++) {
                for (int p = 0; p < partitions; p++) {
                    byte[] value = ("p" + p + "-n" + n).getBytes();
                    producer.send(new ProducerRecord<>(topic, p, null, value)).get();
                }
            }
        }

        // 2) Start consumer; capture per-partition offset order.
        Map<Integer, List<Long>> observed = new ConcurrentHashMap<>();
        CountDownLatch latch = new CountDownLatch(total);

        try (PlurimaConsumer<byte[], byte[]> consumer = PlurimaConsumer.<byte[], byte[]>builder()
                .kafkaProperties(classicConsumerProps(groupId))
                .topic(topic)
                .engine(ConsumerEngine.CLASSIC_BASIC)
                .ordering(OrderingMode.PARTITION)
                .orderingGuarantee(OrderingGuarantee.STRICT)
                .pollTimeout(Duration.ofMillis(200))
                .listener((r, ctx) -> {
                    observed.computeIfAbsent(r.partition(), p -> new CopyOnWriteArrayList<>())
                        .add(r.offset());
                    latch.countDown();
                })
                .build()) {
            consumer.start();

            assertThat(latch.await(60, TimeUnit.SECONDS))
                .as("classic consumer must process all %d records within timeout", total)
                .isTrue();

            // Brief settle so the in-process commitAsync() round-trip lands at the broker
            // before we tear down. Without this, the next consumer (verifying commits below)
            // might race and pull the same records again.
            Thread.sleep(500);
        }

        // Per-partition ordering check.
        assertThat(observed)
            .as("must have received from all %d partitions", partitions)
            .hasSize(partitions);
        observed.forEach((partition, offsets) -> {
            List<Long> sorted = new ArrayList<>(offsets);
            sorted.sort(Long::compareTo);
            assertThat(offsets)
                .as("per-partition ordering for partition %d", partition)
                .isEqualTo(sorted);
        });

        // 3) Start a second consumer in the same group. If commits succeeded, the broker has
        // recorded the offset+1 position for every partition, so this consumer sees nothing.
        CountDownLatch secondLatch = new CountDownLatch(1);
        try (PlurimaConsumer<byte[], byte[]> verifier = PlurimaConsumer.<byte[], byte[]>builder()
                .kafkaProperties(classicConsumerProps(groupId))
                .topic(topic)
                .engine(ConsumerEngine.CLASSIC_BASIC)
                .ordering(OrderingMode.PARTITION)
                .pollTimeout(Duration.ofMillis(200))
                .listener((r, ctx) -> secondLatch.countDown())
                .build()) {
            verifier.start();
            boolean sawAny = secondLatch.await(5, TimeUnit.SECONDS);
            assertThat(sawAny)
                .as("a second consumer in the same group must see zero records after the first "
                    + "committed all offsets — if records appear, commits did not land")
                .isFalse();
        }

        deleteTopicQuietly(topic);
    }

    /** Smoke test that the basic UNORDERED + CLASSIC_BASIC path also works. UNORDERED uses
     *  ClassicUnorderedDispatcher (one worker per record); records on the same partition
     *  may complete in any order. */
    @Test
    void unorderedClassicEnginePullsAllRecords() throws Exception {
        String topic = createUniqueTopic("plurima-int-classic-unordered", 1);
        String groupId = "plurima-int-classic-unordered-group-" + UUID.randomUUID();

        int count = 30;
        try (var producer = byteProducer()) {
            for (int i = 0; i < count; i++) {
                producer.send(new ProducerRecord<>(
                    topic, ("k" + i).getBytes(), ("v" + i).getBytes())).get();
            }
        }

        CountDownLatch latch = new CountDownLatch(count);
        try (PlurimaConsumer<byte[], byte[]> consumer = PlurimaConsumer.<byte[], byte[]>builder()
                .kafkaProperties(classicConsumerProps(groupId))
                .topic(topic)
                .engine(ConsumerEngine.CLASSIC_BASIC)
                .ordering(OrderingMode.UNORDERED)
                .pollTimeout(Duration.ofMillis(200))
                .listener((r, ctx) -> latch.countDown())
                .build()) {
            consumer.start();
            assertThat(latch.await(30, TimeUnit.SECONDS))
                .as("classic UNORDERED must still process all %d records", count)
                .isTrue();
        }

        deleteTopicQuietly(topic);

        // Silence unused-import lint at the test level if BOOTSTRAP is not referenced
        // (it's only used transitively via classicConsumerProps).
        assertThat(BOOTSTRAP).isNotBlank();
    }
}
