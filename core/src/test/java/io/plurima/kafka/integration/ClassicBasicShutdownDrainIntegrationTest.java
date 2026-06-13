package io.plurima.kafka.integration;

import io.plurima.kafka.ConsumerEngine;
import io.plurima.kafka.OrderingMode;
import io.plurima.kafka.PlurimaConsumer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static io.plurima.kafka.integration.KafkaIntegrationSupport.byteProducer;
import static io.plurima.kafka.integration.KafkaIntegrationSupport.classicConsumerProps;
import static io.plurima.kafka.integration.KafkaIntegrationSupport.createUniqueTopic;
import static io.plurima.kafka.integration.KafkaIntegrationSupport.deleteTopicQuietly;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Live verification that {@code shutdownDrainTimeout} is honored end-to-end on
 * {@link ConsumerEngine#CLASSIC_BASIC}. Before the fix, ClassicPollLoop's dispatchBatch
 * abandoned workers within 1s of {@code close()} being called regardless of the
 * configured drain timeout. A user-configured 30s/60s drain budget was effectively
 * just {@code WorkerLauncher}'s fixed 5s.
 *
 * <p>This test configures shutdownDrainTimeout=10s and a 3s handler. The consumer is
 * closed while the handler is mid-processing. The handler MUST complete (not get
 * interrupted), and its offset MUST commit so a verifier consumer in the same group
 * sees nothing.
 */
@Tag("integration")
class ClassicBasicShutdownDrainIntegrationTest {

    @Test
    void slowHandlerCompletesWithinShutdownDrainBudget() throws Exception {
        String topic = createUniqueTopic("plurima-int-classic-drain", 1);
        String groupId = "plurima-int-classic-drain-group-" + UUID.randomUUID();

        try (var producer = byteProducer()) {
            producer.send(new ProducerRecord<>(topic, "k".getBytes(), "v".getBytes())).get();
        }

        CountDownLatch handlerEntered = new CountDownLatch(1);
        AtomicBoolean handlerInterrupted = new AtomicBoolean();
        AtomicLong handlerDurationMs = new AtomicLong(-1);

        PlurimaConsumer<byte[], byte[]> consumer = PlurimaConsumer.<byte[], byte[]>builder()
            .kafkaProperties(classicConsumerProps(groupId))
            .topic(topic)
            .engine(ConsumerEngine.CLASSIC_BASIC)
            .ordering(OrderingMode.PARTITION)
            .pollTimeout(Duration.ofMillis(200))
            // 10s drain budget — handler must finish naturally within this.
            .shutdownDrainTimeout(Duration.ofSeconds(10))
            .listener((r, ctx) -> {
                long start = System.nanoTime();
                handlerEntered.countDown();
                try {
                    // 3s sleep, well within the 10s drain budget.
                    Thread.sleep(3_000);
                } catch (InterruptedException ie) {
                    handlerInterrupted.set(true);
                    Thread.currentThread().interrupt();
                } finally {
                    handlerDurationMs.set((System.nanoTime() - start) / 1_000_000L);
                }
            })
            .build();

        consumer.start();
        assertThat(handlerEntered.await(15, TimeUnit.SECONDS))
            .as("handler must be invoked").isTrue();

        // Close while the handler is sleeping. The drain budget (10s) should be enough
        // for the 3s handler to finish naturally + commit.
        long closeStartNanos = System.nanoTime();
        consumer.close();
        long closeElapsedMs = (System.nanoTime() - closeStartNanos) / 1_000_000L;

        assertThat(handlerInterrupted.get())
            .as("handler must NOT have been interrupted — shutdownDrainTimeout must give it "
                + "enough time to finish naturally (its 3s < the 10s budget)")
            .isFalse();
        assertThat(handlerDurationMs.get())
            .as("handler should have run for ~3s (close to its configured sleep duration)")
            .isGreaterThan(2_500L);
        assertThat(closeElapsedMs)
            .as("close() should have waited for the handler (~3s) plus cleanup, "
                + "not the full 10s budget; took %dms", closeElapsedMs)
            .isLessThan(8_000L);

        // Verifier in same group: must see NOTHING — the handler ran, committed, and
        // the next consumer starts past the committed offset.
        CountDownLatch redelivered = new CountDownLatch(1);
        try (PlurimaConsumer<byte[], byte[]> verifier = PlurimaConsumer.<byte[], byte[]>builder()
                .kafkaProperties(classicConsumerProps(groupId))
                .topic(topic)
                .engine(ConsumerEngine.CLASSIC_BASIC)
                .pollTimeout(Duration.ofMillis(200))
                .listener((r, ctx) -> redelivered.countDown())
                .build()) {
            verifier.start();
            boolean saw = redelivered.await(5, TimeUnit.SECONDS);
            assertThat(saw)
                .as("verifier MUST see no record — the handler completed within drain budget "
                    + "and its offset committed; the next consumer should advance past it")
                .isFalse();
        }

        deleteTopicQuietly(topic);
    }

    @Test
    void handlerExceedingDrainBudgetIsAbandonedWithWarning() throws Exception {
        // Companion test: a handler that takes LONGER than the drain budget should be
        // abandoned with a WARN, and the record should be redelivered to the next consumer.
        String topic = createUniqueTopic("plurima-int-classic-drain-over", 1);
        String groupId = "plurima-int-classic-drain-over-group-" + UUID.randomUUID();

        try (var producer = byteProducer()) {
            producer.send(new ProducerRecord<>(topic, "k".getBytes(), "v".getBytes())).get();
        }

        CountDownLatch handlerEntered = new CountDownLatch(1);
        AtomicBoolean handlerInterrupted = new AtomicBoolean();

        PlurimaConsumer<byte[], byte[]> consumer = PlurimaConsumer.<byte[], byte[]>builder()
            .kafkaProperties(classicConsumerProps(groupId))
            .topic(topic)
            .engine(ConsumerEngine.CLASSIC_BASIC)
            .ordering(OrderingMode.PARTITION)
            .pollTimeout(Duration.ofMillis(200))
            // 2s drain budget; handler will be still running when timeout hits.
            .shutdownDrainTimeout(Duration.ofSeconds(2))
            .listener((r, ctx) -> {
                handlerEntered.countDown();
                try {
                    Thread.sleep(15_000);  // way longer than the 2s drain
                } catch (InterruptedException ie) {
                    handlerInterrupted.set(true);
                    Thread.currentThread().interrupt();
                }
            })
            .build();

        consumer.start();
        assertThat(handlerEntered.await(15, TimeUnit.SECONDS)).isTrue();

        long closeStartNanos = System.nanoTime();
        consumer.close();
        long closeElapsedMs = (System.nanoTime() - closeStartNanos) / 1_000_000L;

        // close() returned ~2s drain + ~5s WorkerLauncher grace + cleanup ≈ ~7-10s.
        // Not unbounded; bounded by the drain + launcher window.
        assertThat(closeElapsedMs)
            .as("close() must return within drain budget + cleanup, not block on the 15s "
                + "handler sleep; took %dms", closeElapsedMs)
            .isLessThan(12_000L);

        // The record should be redelivered (we abandoned before commit).
        CountDownLatch redelivered = new CountDownLatch(1);
        try (PlurimaConsumer<byte[], byte[]> verifier = PlurimaConsumer.<byte[], byte[]>builder()
                .kafkaProperties(classicConsumerProps(groupId))
                .topic(topic)
                .engine(ConsumerEngine.CLASSIC_BASIC)
                .pollTimeout(Duration.ofMillis(200))
                .listener((r, ctx) -> redelivered.countDown())
                .build()) {
            verifier.start();
            assertThat(redelivered.await(10, TimeUnit.SECONDS))
                .as("verifier must see the record — handler was abandoned past drain budget, "
                    + "no commit landed for this record")
                .isTrue();
        }

        // Sanity: the original handler was indeed interrupted by WorkerLauncher.shutdownNow.
        assertThat(handlerInterrupted.get())
            .as("the long handler should have been interrupted once drain + launcher grace expired")
            .isTrue();

        deleteTopicQuietly(topic);
    }
}
