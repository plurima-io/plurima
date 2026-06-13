package io.plurima.kafka.integration;

import io.plurima.kafka.ConsumerEngine;
import io.plurima.kafka.OrderingMode;
import io.plurima.kafka.PlurimaConsumer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static io.plurima.kafka.integration.KafkaIntegrationSupport.byteProducer;
import static io.plurima.kafka.integration.KafkaIntegrationSupport.classicConsumerProps;
import static io.plurima.kafka.integration.KafkaIntegrationSupport.createUniqueTopic;
import static io.plurima.kafka.integration.KafkaIntegrationSupport.deleteTopicQuietly;
import static io.plurima.kafka.integration.KafkaIntegrationSupport.waitForClassicAssignment;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Live proof that {@link OrderingMode#KEY} on {@link ConsumerEngine#CLASSIC_BASIC} delivers
 * actual intra-partition parallelism — the headline behavior the v0.1 G3 phase adds.
 *
 * <p>Setup: a SINGLE partition topic with N distinct keys and a per-record handler that
 * sleeps for a fixed duration. Under partition-serial dispatch (PARTITION mode or pre-v0.1
 * KEY mode) wall-clock = N × handlerSleep. Under genuine intra-partition key-shard
 * parallelism (v0.1 KEY mode) wall-clock should approach a single handlerSleep (all
 * distinct-key workers run concurrently on different shards).
 *
 * <p>The test asserts that the KEY-mode wall-clock is much smaller than the partition-serial
 * baseline — large enough margin that CI noise can't produce a false pass.
 */
@Tag("integration")
class ClassicBasicKeyParallelismIntegrationTest {

    @Test
    void distinctKeysOnSinglePartitionRunConcurrently() throws Exception {
        // 1 partition + 32 distinct keys + concurrency=32 = up to 32 workers running in parallel
        int distinctKeys = 32;
        int totalRecords = distinctKeys;
        long handlerSleepMs = 1_000;  // 1 second per record

        String topic = createUniqueTopic("plurima-int-keyparallel", 1);
        String groupId = "plurima-int-keyparallel-group-" + UUID.randomUUID();

        try (var producer = byteProducer()) {
            for (int k = 0; k < distinctKeys; k++) {
                producer.send(new ProducerRecord<>(
                    topic, ("key-" + k).getBytes(), ("v" + k).getBytes())).get();
            }
        }

        CountDownLatch done = new CountDownLatch(totalRecords);
        AtomicInteger concurrentlyRunning = new AtomicInteger();
        AtomicInteger maxConcurrent = new AtomicInteger();

        try (PlurimaConsumer<byte[], byte[]> consumer = PlurimaConsumer.<byte[], byte[]>builder()
                .kafkaProperties(classicConsumerProps(groupId))
                .topic(topic)
                .engine(ConsumerEngine.CLASSIC_BASIC)
                .ordering(OrderingMode.KEY)
                .concurrency(distinctKeys)
                .shardCount(distinctKeys * 4)  // generous shards to minimise collision
                .pollTimeout(Duration.ofMillis(200))
                .listener((r, ctx) -> {
                    int now = concurrentlyRunning.incrementAndGet();
                    maxConcurrent.updateAndGet(prev -> Math.max(prev, now));
                    try { Thread.sleep(handlerSleepMs); }
                    catch (InterruptedException ie) { Thread.currentThread().interrupt(); }
                    concurrentlyRunning.decrementAndGet();
                    done.countDown();
                })
                .build()) {
            consumer.start();
            waitForClassicAssignment(groupId, 1, 1);

            long startNanos = System.nanoTime();
            boolean completed = done.await(30, TimeUnit.SECONDS);
            long elapsedMs = (System.nanoTime() - startNanos) / 1_000_000L;
            assertThat(completed)
                .as("all %d records must process within timeout", totalRecords)
                .isTrue();

            // Partition-serial baseline would be ≥ 32 seconds (32 × 1s). KEY-mode parallel
            // should finish in ~1-2 seconds. Require < 8 seconds to leave generous margin
            // for poll latency, dispatch overhead, and CI noise. Even with some shard
            // collisions, 8s is a comfortable upper bound for the true parallelism case
            // (and an impossible lower bound for the serial case).
            assertThat(elapsedMs)
                .as("wall-clock for %d records × %dms handler with KEY-mode intra-partition "
                    + "parallelism (partition-serial baseline would be ≥ %dms)",
                    totalRecords, handlerSleepMs, totalRecords * handlerSleepMs)
                .isLessThan(8_000);

            // At least N/4 distinct workers must have been concurrently running. With
            // shardCount=128 and 32 keys, the expected collision rate is low (~12%);
            // observed concurrent peak should be ≥ 8 even with CI noise.
            assertThat(maxConcurrent.get())
                .as("KEY mode must put many distinct-key workers in flight concurrently")
                .isGreaterThanOrEqualTo(8);
        } finally {
            deleteTopicQuietly(topic);
        }
    }

    @Test
    void sameKeyRecordsStillSerialiseUnderKeyMode() throws Exception {
        // Single key + multiple records → they must process strictly in offset order
        // even though we configured KEY mode. The shard for that key serialises them.
        int totalRecords = 10;
        String topic = createUniqueTopic("plurima-int-keyserial", 1);
        String groupId = "plurima-int-keyserial-group-" + UUID.randomUUID();

        try (var producer = byteProducer()) {
            for (int i = 0; i < totalRecords; i++) {
                producer.send(new ProducerRecord<>(
                    topic, "k-single".getBytes(), ("v" + i).getBytes())).get();
            }
        }

        List<Long> observedOffsets = new java.util.concurrent.CopyOnWriteArrayList<>();
        AtomicInteger concurrentlyRunning = new AtomicInteger();
        AtomicInteger maxConcurrent = new AtomicInteger();
        CountDownLatch done = new CountDownLatch(totalRecords);

        try (PlurimaConsumer<byte[], byte[]> consumer = PlurimaConsumer.<byte[], byte[]>builder()
                .kafkaProperties(classicConsumerProps(groupId))
                .topic(topic)
                .engine(ConsumerEngine.CLASSIC_BASIC)
                .ordering(OrderingMode.KEY)
                .concurrency(16)
                .pollTimeout(Duration.ofMillis(200))
                .listener((r, ctx) -> {
                    int now = concurrentlyRunning.incrementAndGet();
                    maxConcurrent.updateAndGet(prev -> Math.max(prev, now));
                    try { Thread.sleep(100); }
                    catch (InterruptedException ie) { Thread.currentThread().interrupt(); }
                    observedOffsets.add(r.offset());
                    concurrentlyRunning.decrementAndGet();
                    done.countDown();
                })
                .build()) {
            consumer.start();
            waitForClassicAssignment(groupId, 1, 1);

            assertThat(done.await(15, TimeUnit.SECONDS)).isTrue();
        } finally {
            deleteTopicQuietly(topic);
        }

        assertThat(maxConcurrent.get())
            .as("records with the SAME key must serialise (one worker at a time)")
            .isEqualTo(1);
        List<Long> sorted = new ArrayList<>(observedOffsets);
        sorted.sort(Long::compareTo);
        assertThat(observedOffsets)
            .as("same-key records must arrive in offset order")
            .isEqualTo(sorted);
    }
}
