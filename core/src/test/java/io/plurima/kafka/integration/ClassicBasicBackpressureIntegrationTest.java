package io.plurima.kafka.integration;

import io.plurima.kafka.ConsumerEngine;
import io.plurima.kafka.OrderingMode;
import io.plurima.kafka.PlurimaConsumer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Properties;
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
 * Live verification of the v0.1 continuous-poll + backpressure design under conditions
 * that pre-v0.1 would have triggered the 80% voluntary-stop:
 *
 * <ol>
 *   <li><b>Slow handler past max.poll.interval.ms</b>: a handler sleeping LONGER than
 *       {@code max.poll.interval.ms} must NOT fence the consumer. Pre-v0.1 the 80%
 *       voluntary-stop intentionally killed the consumer at 4.8s with mpi=6s; v0.1's
 *       continuous-poll keeps the consumer heartbeating regardless of handler duration.
 *       Record must be processed and committed; no redelivery.</li>
 *   <li><b>Burst load triggers pause/resume</b>: send many more records than concurrency
 *       and a slow handler — backpressure must pause partitions when in-flight saturates
 *       and resume when handlers drain. All records process successfully without fencing.</li>
 * </ol>
 */
@Tag("integration")
class ClassicBasicBackpressureIntegrationTest {

    @Test
    void slowHandlerExceedingMaxPollIntervalDoesNotFence() throws Exception {
        // mpi=6s; handler sleeps 8s (> mpi). Pre-v0.1 the 80% voluntary-stop would fire
        // at 4.8s and the record would NOT commit. v0.1 continuous-poll heartbeats every
        // pollTimeout regardless of handler progress, so the broker never fences us.
        String topic = createUniqueTopic("plurima-int-classic-mpi", 1);
        String groupId = "plurima-int-classic-mpi-group-" + UUID.randomUUID();

        try (var producer = byteProducer()) {
            producer.send(new ProducerRecord<>(topic, "k".getBytes(), "v".getBytes())).get();
        }

        Properties props = classicConsumerProps(groupId);
        props.put("max.poll.interval.ms", "6000");
        props.put("session.timeout.ms", "10000");
        props.put("heartbeat.interval.ms", "2000");
        props.put("max.poll.records", "5");

        AtomicInteger invocations = new AtomicInteger();
        CountDownLatch handlerDone = new CountDownLatch(1);

        try (PlurimaConsumer<byte[], byte[]> consumer = PlurimaConsumer.builder()
                .kafkaProperties(props)
                .topic(topic)
                .engine(ConsumerEngine.CLASSIC_BASIC)
                .ordering(OrderingMode.PARTITION)
                .pollTimeout(Duration.ofMillis(500))
                .shutdownDrainTimeout(Duration.ofSeconds(15))
                .listener((r, ctx) -> {
                    invocations.incrementAndGet();
                    try { Thread.sleep(8_000); }
                    catch (InterruptedException ie) { Thread.currentThread().interrupt(); }
                    handlerDone.countDown();
                })
                .build()) {
            consumer.start();
            waitForClassicAssignment(groupId, 1, 1);

            assertThat(handlerDone.await(20, TimeUnit.SECONDS))
                .as("handler must run to completion under v0.1 continuous-poll model")
                .isTrue();

            // Give the commitAsync time to land before we close + spawn a verifier.
            Thread.sleep(2_000);

            assertThat(invocations.get())
                .as("handler must fire exactly once — no fencing-induced redelivery under v0.1")
                .isEqualTo(1);
        }

        // Verifier consumer in the same group should see ZERO records (offset committed).
        AtomicInteger verifierSaw = new AtomicInteger();
        CountDownLatch verifyDeadline = new CountDownLatch(1);
        try (PlurimaConsumer<byte[], byte[]> verifier = PlurimaConsumer.builder()
                .kafkaProperties(classicConsumerProps(groupId))
                .topic(topic)
                .engine(ConsumerEngine.CLASSIC_BASIC)
                .ordering(OrderingMode.PARTITION)
                .pollTimeout(Duration.ofMillis(200))
                .listener((r, ctx) -> verifierSaw.incrementAndGet())
                .build()) {
            verifier.start();
            // Wait 5s — if the original commit landed, verifier should see nothing.
            verifyDeadline.await(5, TimeUnit.SECONDS);
        }
        assertThat(verifierSaw.get())
            .as("original handler's commit must have landed; verifier in same group sees no records")
            .isZero();

        deleteTopicQuietly(topic);
    }

    @Test
    void burstLoadTriggersPauseResumeAndAllRecordsProcess() throws Exception {
        // Produce 200 records, configure concurrency=8. With 200ms-per-record handlers,
        // backpressure should pause the consumer once in-flight ≥ 8 and resume as workers
        // drain. ALL records must eventually process. The test passes if we receive all
        // 200 records (no fencing, no record loss).
        int totalRecords = 200;
        int concurrency = 8;
        String topic = createUniqueTopic("plurima-int-classic-backpressure", 1);
        String groupId = "plurima-int-classic-backpressure-group-" + UUID.randomUUID();

        try (var producer = byteProducer()) {
            for (int i = 0; i < totalRecords; i++) {
                producer.send(new ProducerRecord<>(
                    topic, ("k" + (i % 50)).getBytes(), ("v" + i).getBytes())).get();
            }
        }

        Properties props = classicConsumerProps(groupId);
        // Tight max.poll.records so multiple poll cycles are required, which exercises
        // the pause/resume code path repeatedly.
        props.put("max.poll.records", "16");

        CountDownLatch done = new CountDownLatch(totalRecords);
        AtomicInteger concurrentlyRunning = new AtomicInteger();
        AtomicInteger maxConcurrent = new AtomicInteger();

        try (PlurimaConsumer<byte[], byte[]> consumer = PlurimaConsumer.builder()
                .kafkaProperties(props)
                .topic(topic)
                .engine(ConsumerEngine.CLASSIC_BASIC)
                .ordering(OrderingMode.KEY)
                .concurrency(concurrency)
                .pollTimeout(Duration.ofMillis(200))
                .listener((r, ctx) -> {
                    int now = concurrentlyRunning.incrementAndGet();
                    maxConcurrent.updateAndGet(prev -> Math.max(prev, now));
                    try { Thread.sleep(200); }
                    catch (InterruptedException ie) { Thread.currentThread().interrupt(); }
                    concurrentlyRunning.decrementAndGet();
                    done.countDown();
                })
                .build()) {
            consumer.start();
            waitForClassicAssignment(groupId, 1, 1);

            assertThat(done.await(120, TimeUnit.SECONDS))
                .as("all %d records must process — backpressure must pause/resume without "
                    + "losing records or fencing", totalRecords)
                .isTrue();
        } finally {
            deleteTopicQuietly(topic);
        }

        // The in-flight count must respect the concurrency knob (up to a small overshoot
        // window — backpressure pauses BEFORE the next poll, so a single saturating batch
        // can briefly exceed concurrency by max.poll.records). Allow generous headroom.
        assertThat(maxConcurrent.get())
            .as("observed peak concurrency should be bounded by concurrency + max.poll.records "
                + "(backpressure pauses AFTER a saturating batch is dispatched, then the next "
                + "poll returns empty for paused partitions)")
            .isLessThanOrEqualTo(concurrency + 16);
    }
}
