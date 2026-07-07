package io.plurima.kafka.integration;

import io.plurima.kafka.ConsumerEngine;
import io.plurima.kafka.OrderingMode;
import io.plurima.kafka.PlurimaConsumer;
import io.plurima.kafka.dlt.DltConfig;
import io.plurima.kafka.retry.RetryPolicy;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static io.plurima.kafka.integration.KafkaIntegrationSupport.BOOTSTRAP;
import static io.plurima.kafka.integration.KafkaIntegrationSupport.adminProps;
import static io.plurima.kafka.integration.KafkaIntegrationSupport.byteProducer;
import static io.plurima.kafka.integration.KafkaIntegrationSupport.classicConsumerProps;
import static io.plurima.kafka.integration.KafkaIntegrationSupport.createUniqueTopic;
import static io.plurima.kafka.integration.KafkaIntegrationSupport.deleteTopicQuietly;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Live test of retry/DLT mapping under {@code ConsumerEngine.CLASSIC_BASIC}. Verifies:
 *
 * <ul>
 *   <li>Inline retry: listener throws twice then succeeds, broker sees the record
 *       consumed exactly once at the final attempt (committed offset = offset+1).</li>
 *   <li>DLT routing: listener always throws, retry exhausts, DLT record appears at
 *       the configured DLT topic, original commits past.</li>
 * </ul>
 */
@Tag("integration")
class ClassicBasicRetryDltIntegrationTest {

    @Test
    void inlineRetryEventuallySucceedsAndCommits() throws Exception {
        String topic = createUniqueTopic("plurima-int-classic-retry", 1);
        String groupId = "plurima-int-classic-retry-group-" + UUID.randomUUID();

        try (var producer = byteProducer()) {
            producer.send(new ProducerRecord<>(topic, "k".getBytes(), "v".getBytes())).get();
        }

        AtomicInteger attempts = new AtomicInteger();
        CountDownLatch succeeded = new CountDownLatch(1);

        RetryPolicy policy = RetryPolicy.exponential()
            .maxAttempts(3)
            .initialDelay(Duration.ofMillis(50))
            .multiplier(1.5)
            .jitter(0.0)
            .retryOn(IOException.class)
            .build();

        try (PlurimaConsumer<byte[], byte[]> consumer = PlurimaConsumer.builder()
                .kafkaProperties(classicConsumerProps(groupId))
                .topic(topic)
                .engine(ConsumerEngine.CLASSIC_BASIC)
                .ordering(OrderingMode.PARTITION)
                .pollTimeout(Duration.ofMillis(200))
                .retry(policy)
                .listener((r, ctx) -> {
                    int a = attempts.incrementAndGet();
                    if (a < 3) {
                        throw new IOException("transient #" + a);
                    }
                    succeeded.countDown();
                })
                .build()) {
            consumer.start();
            assertThat(succeeded.await(15, TimeUnit.SECONDS))
                .as("listener must succeed by the 3rd attempt under inline retry")
                .isTrue();
            assertThat(attempts.get())
                .as("exactly 3 attempts: 2 failures + 1 success")
                .isEqualTo(3);
            Thread.sleep(500);  // settle commitAsync
        }

        // Verify commit landed: a second consumer in the same group sees nothing.
        CountDownLatch secondLatch = new CountDownLatch(1);
        try (PlurimaConsumer<byte[], byte[]> verifier = PlurimaConsumer.builder()
                .kafkaProperties(classicConsumerProps(groupId))
                .topic(topic)
                .engine(ConsumerEngine.CLASSIC_BASIC)
                .pollTimeout(Duration.ofMillis(200))
                .listener((r, ctx) -> secondLatch.countDown())
                .build()) {
            verifier.start();
            assertThat(secondLatch.await(5, TimeUnit.SECONDS))
                .as("verifier must see zero records — inline retry path must have committed")
                .isFalse();
        }

        deleteTopicQuietly(topic);
    }

    @Test
    void delayedRetryAcrossOneSecondThresholdSucceeds() throws Exception {
        // Regression: an earlier version of ClassicPollLoop's delayed-retry path called
        // consumer.wakeup() while the poll thread was blocked in latch.await(); the next
        // poll() then threw WakeupException and the poll loop exited mid-record. Only the
        // inline-retry (<= 1s) path was covered by tests; the > 1s delayed path was broken.
        // This test forces the delayed-retry branch with a 2s initialDelay (RetryEngine's
        // INLINE_THRESHOLD is 1s).
        String topic = createUniqueTopic("plurima-int-classic-delayed", 1);
        String groupId = "plurima-int-classic-delayed-group-" + UUID.randomUUID();

        try (var producer = byteProducer()) {
            producer.send(new ProducerRecord<>(topic, "k".getBytes(), "v".getBytes())).get();
        }

        AtomicInteger attempts = new AtomicInteger();
        CountDownLatch succeeded = new CountDownLatch(1);

        // initialDelay=2s, multiplier=1 → every retry is a 2s delayed retry (not inline).
        RetryPolicy policy = RetryPolicy.exponential()
            .maxAttempts(3)
            .initialDelay(Duration.ofSeconds(2))
            .multiplier(1.0)
            .jitter(0.0)
            .retryOn(IOException.class)
            .build();

        try (PlurimaConsumer<byte[], byte[]> consumer = PlurimaConsumer.builder()
                .kafkaProperties(classicConsumerProps(groupId))
                .topic(topic)
                .engine(ConsumerEngine.CLASSIC_BASIC)
                .ordering(OrderingMode.PARTITION)
                .pollTimeout(Duration.ofMillis(200))
                .retry(policy)
                .listener((r, ctx) -> {
                    int a = attempts.incrementAndGet();
                    if (a < 3) {
                        throw new IOException("delayed transient #" + a);
                    }
                    succeeded.countDown();
                })
                .build()) {
            consumer.start();
            // 2 retries × 2s + handler time + commitAsync ≈ 5-7s. 20s budget is generous.
            assertThat(succeeded.await(20, TimeUnit.SECONDS))
                .as("delayed-retry path (> 1s) must reach success without the poll loop dying")
                .isTrue();
            assertThat(attempts.get())
                .as("exactly 3 attempts: 2 delayed-retry failures + 1 success")
                .isEqualTo(3);
            Thread.sleep(500);
        }

        deleteTopicQuietly(topic);
    }

    @Test
    void exhaustedRetryRoutesToDltAndCommitsOriginal() throws Exception {
        String topic = createUniqueTopic("plurima-int-classic-dlt", 1);
        String dltTopic = topic + ".DLT";
        try (AdminClient admin = AdminClient.create(adminProps())) {
            admin.createTopics(List.of(new NewTopic(dltTopic, 1, (short) 1)))
                .all().get(15, TimeUnit.SECONDS);
        }
        String groupId = "plurima-int-classic-dlt-group-" + UUID.randomUUID();

        try (var producer = byteProducer()) {
            producer.send(new ProducerRecord<>(topic, "k".getBytes(), "poison".getBytes())).get();
        }

        Properties dltProducerProps = new Properties();
        dltProducerProps.put("bootstrap.servers", BOOTSTRAP);
        dltProducerProps.put("acks", "all");
        DltConfig dlt = DltConfig.builder().producerProperties(dltProducerProps).build();

        AtomicInteger attempts = new AtomicInteger();
        CountDownLatch exhausted = new CountDownLatch(1);

        RetryPolicy policy = RetryPolicy.exponential()
            .maxAttempts(2)
            .initialDelay(Duration.ofMillis(50))
            .multiplier(1.5)
            .jitter(0.0)
            .retryOn(IOException.class)
            .build();

        try (PlurimaConsumer<byte[], byte[]> consumer = PlurimaConsumer.builder()
                .kafkaProperties(classicConsumerProps(groupId))
                .topic(topic)
                .engine(ConsumerEngine.CLASSIC_BASIC)
                .ordering(OrderingMode.PARTITION)
                .pollTimeout(Duration.ofMillis(200))
                .retry(policy)
                .deadLetter(dlt)
                .listener((r, ctx) -> {
                    // RetryEngine exhausts when COMPLETED retries reach maxAttempts.
                    // With maxAttempts=2, the listener runs 3 times total: the initial
                    // invocation plus 2 retries. Countdown fires on the third (final)
                    // invocation so the await reflects actual exhaustion, not just
                    // "we've reached the retry budget but might still attempt once more."
                    int a = attempts.incrementAndGet();
                    if (a == 3) exhausted.countDown();
                    throw new IOException("always fails");
                })
                .build()) {
            consumer.start();
            assertThat(exhausted.await(15, TimeUnit.SECONDS))
                .as("listener must run 3 times under maxAttempts=2 (initial + 2 retries) then exhaust")
                .isTrue();
            // Tiny extra wait so DLT send + commitAsync settle before we tear down.
            Thread.sleep(2_000);
            assertThat(attempts.get())
                .as("exactly 3 listener invocations under maxAttempts=2")
                .isEqualTo(3);
        }

        // Read the DLT topic — should have one record.
        Properties verifierProps = new Properties();
        verifierProps.put("bootstrap.servers", BOOTSTRAP);
        verifierProps.put("group.id", "dlt-verifier-" + UUID.randomUUID());
        verifierProps.put("auto.offset.reset", "earliest");
        verifierProps.put("key.deserializer", ByteArrayDeserializer.class.getName());
        verifierProps.put("value.deserializer", ByteArrayDeserializer.class.getName());

        boolean foundDltRecord = false;
        try (KafkaConsumer<byte[], byte[]> kc = new KafkaConsumer<>(verifierProps)) {
            kc.subscribe(List.of(dltTopic));
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            while (System.nanoTime() < deadline && !foundDltRecord) {
                var records = kc.poll(Duration.ofMillis(500));
                for (ConsumerRecord<byte[], byte[]> r : records) {
                    if (new String(r.value()).equals("poison")) {
                        foundDltRecord = true;
                        break;
                    }
                }
            }
        }
        assertThat(foundDltRecord)
            .as("DLT topic must contain the exhausted record")
            .isTrue();

        deleteTopicQuietly(topic);
        deleteTopicQuietly(dltTopic);
    }
}
