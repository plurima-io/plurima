package io.plurima.kafka.integration;

import io.plurima.kafka.ConsumerEngine;
import io.plurima.kafka.OrderingMode;
import io.plurima.kafka.PlurimaConsumer;
import io.plurima.kafka.dlt.DltConfig;
import io.plurima.kafka.metrics.PlurimaMetrics;
import io.plurima.kafka.retry.RetryPolicy;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.IntSupplier;

import static io.plurima.kafka.integration.KafkaIntegrationSupport.byteProducer;
import static io.plurima.kafka.integration.KafkaIntegrationSupport.classicConsumerProps;
import static io.plurima.kafka.integration.KafkaIntegrationSupport.createUniqueTopic;
import static io.plurima.kafka.integration.KafkaIntegrationSupport.deleteTopicQuietly;
import static io.plurima.kafka.integration.KafkaIntegrationSupport.waitForClassicAssignment;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end live verification that the CLASSIC_BASIC engine emits every documented
 * metric at runtime. Drives a real consumer through every metric-producing path
 * (poll, process, retry, DLT, backpressure, in-flight gauge) and asserts the
 * corresponding {@link PlurimaMetrics} callbacks fire with the expected values.
 *
 * <p>The existing {@code MetricsAcceptanceTest} only calls each metric method on
 * {@link io.plurima.kafka.metrics.MicrometerPlurimaMetrics} directly. That covers
 * the Micrometer adaptation but not whether production code paths actually call
 * them at the right moments. This test plugs a {@link RecordingMetrics} into a real
 * consumer and inspects the captured calls.
 *
 * <p>The test uses a simple in-test recorder rather than MicrometerPlurimaMetrics
 * to avoid a cyclic dependency between {@code :core} test and {@code :metrics}.
 */
@Tag("integration")
class ClassicMetricsIntegrationTest {

    @Test
    void happyPathEmitsCorePollProcessAndCommitMetrics() throws Exception {
        RecordingMetrics m = new RecordingMetrics();
        String topic = createUniqueTopic("plurima-int-metrics-happy", 1);
        String groupId = "plurima-int-metrics-happy-" + UUID.randomUUID();

        int total = 20;
        CountDownLatch done = new CountDownLatch(total);

        try (var producer = byteProducer()) {
            for (int i = 0; i < total; i++) {
                producer.send(new ProducerRecord<>(topic, ("k" + i).getBytes(),
                    ("v" + i).getBytes())).get();
            }
        }

        try (PlurimaConsumer<byte[], byte[]> c = PlurimaConsumer.<byte[], byte[]>builder()
                .kafkaProperties(classicConsumerProps(groupId))
                .topic(topic)
                .engine(ConsumerEngine.CLASSIC_BASIC)
                .ordering(OrderingMode.PARTITION)
                .pollTimeout(Duration.ofMillis(200))
                .metrics(m)
                .listener((r, ctx) -> done.countDown())
                .build()) {
            c.start();
            waitForClassicAssignment(groupId, 1, 1);
            assertThat(done.await(30, TimeUnit.SECONDS)).isTrue();
            Thread.sleep(1_500);  // let drainPendingCommits run + ack.committed fire
        } finally {
            deleteTopicQuietly(topic);
        }

        assertThat(m.countOfPolled(topic))
            .as("records.polled sum across batches must equal produced count")
            .isEqualTo(total);
        assertThat(m.countOfProcessed(topic, "accept"))
            .as("records.processed{result=accept} once per record")
            .isEqualTo(total);
        assertThat(m.countOfAckCommitted(topic, "accept"))
            .as("ack.committed{type=accept} fires at least once for the batch")
            .isGreaterThanOrEqualTo(1);
        assertThat(m.countOfPollDurations())
            .as("poll.duration timer recorded at least once per poll iteration")
            .isPositive();
        assertThat(m.countOfProcessDurations(topic))
            .as("process.duration recorded per listener invocation")
            .isEqualTo(total);
        assertThat(m.inFlightGaugeRegistered)
            .as("registerInFlightGauge fired during start()")
            .isTrue();
        assertThat(m.recordsFailedEmpty())
            .as("happy path: no records.failed")
            .isTrue();
        assertThat(m.dltRoutedEmpty())
            .as("happy path: no dlt.routed")
            .isTrue();
    }

    @Test
    void retryAndExceptionPathsEmitFailedAndRetryMetrics() throws Exception {
        RecordingMetrics m = new RecordingMetrics();
        String topic = createUniqueTopic("plurima-int-metrics-retry", 1);
        String groupId = "plurima-int-metrics-retry-" + UUID.randomUUID();

        try (var producer = byteProducer()) {
            producer.send(new ProducerRecord<>(topic, "k".getBytes(), "v".getBytes())).get();
        }

        AtomicInteger attempt = new AtomicInteger();
        CountDownLatch succeeded = new CountDownLatch(1);

        RetryPolicy policy = RetryPolicy.exponential()
            .maxAttempts(3)
            .initialDelay(Duration.ofMillis(50))
            .multiplier(2.0)
            .jitter(0.0)
            .retryOn(RuntimeException.class)
            .build();

        try (PlurimaConsumer<byte[], byte[]> c = PlurimaConsumer.<byte[], byte[]>builder()
                .kafkaProperties(classicConsumerProps(groupId))
                .topic(topic)
                .engine(ConsumerEngine.CLASSIC_BASIC)
                .ordering(OrderingMode.PARTITION)
                .retry(policy)
                .pollTimeout(Duration.ofMillis(200))
                .metrics(m)
                .listener((r, ctx) -> {
                    int n = attempt.incrementAndGet();
                    if (n < 3) throw new RuntimeException("transient " + n);
                    succeeded.countDown();
                })
                .build()) {
            c.start();
            waitForClassicAssignment(groupId, 1, 1);
            assertThat(succeeded.await(20, TimeUnit.SECONDS)).isTrue();
            Thread.sleep(1_500);
        } finally {
            deleteTopicQuietly(topic);
        }

        assertThat(m.countOfFailed(topic, "java.lang.RuntimeException"))
            .as("records.failed fires on each thrown attempt (2 transients before success)")
            .isEqualTo(2);
        assertThat(m.countOfRetries(topic))
            .as("retry.attempts sum across attempt-N tags = 2 retries fired")
            .isEqualTo(2);
        assertThat(m.countOfProcessed(topic, "accept"))
            .as("records.processed{result=accept} once on eventual success")
            .isEqualTo(1);
    }

    @Test
    void dltPathEmitsDltAndAcceptMetrics() throws Exception {
        RecordingMetrics m = new RecordingMetrics();
        String topic = createUniqueTopic("plurima-int-metrics-dlt-src", 1);
        String dlt = createUniqueTopic("plurima-int-metrics-dlt-dst", 1);
        String groupId = "plurima-int-metrics-dlt-" + UUID.randomUUID();

        try (var producer = byteProducer()) {
            producer.send(new ProducerRecord<>(topic, "k".getBytes(), "v".getBytes())).get();
        }

        RetryPolicy policy = RetryPolicy.exponential()
            .maxAttempts(1)
            .initialDelay(Duration.ofMillis(50))
            .multiplier(2.0)
            .jitter(0.0)
            .retryOn(RuntimeException.class)
            .build();

        Properties dltProducerProps = new Properties();
        dltProducerProps.put("bootstrap.servers",
            KafkaIntegrationSupport.adminProps().get("bootstrap.servers"));
        dltProducerProps.put("acks", "all");
        dltProducerProps.put("key.serializer",
            "org.apache.kafka.common.serialization.ByteArraySerializer");
        dltProducerProps.put("value.serializer",
            "org.apache.kafka.common.serialization.ByteArraySerializer");
        DltConfig dltConfig = DltConfig.builder()
            .producerProperties(dltProducerProps)
            .namingStrategy(t -> dlt)
            .build();

        AtomicInteger invocations = new AtomicInteger();
        try (PlurimaConsumer<byte[], byte[]> c = PlurimaConsumer.<byte[], byte[]>builder()
                .kafkaProperties(classicConsumerProps(groupId))
                .topic(topic)
                .engine(ConsumerEngine.CLASSIC_BASIC)
                .ordering(OrderingMode.PARTITION)
                .retry(policy)
                .deadLetterTopic(dltConfig)
                .pollTimeout(Duration.ofMillis(200))
                .metrics(m)
                .listener((r, ctx) -> {
                    invocations.incrementAndGet();
                    throw new RuntimeException("permanent");
                })
                .build()) {
            c.start();
            waitForClassicAssignment(groupId, 1, 1);
            long deadline = System.currentTimeMillis() + 15_000L;
            while (invocations.get() < 2 && System.currentTimeMillis() < deadline) {
                Thread.sleep(50);
            }
            Thread.sleep(3_000);
        } finally {
            deleteTopicQuietly(topic);
            deleteTopicQuietly(dlt);
        }

        assertThat(m.countOfDltRouted(topic, dlt))
            .as("dlt.routed{topic, dlt_topic} fires on successful DLT publish")
            .isEqualTo(1);
        assertThat(m.countOfProcessed(topic, "accept"))
            .as("DLT-routed records count as accept in records.processed")
            .isEqualTo(1);
        assertThat(m.dltFailedEmpty())
            .as("DLT happy path: no dlt.failures")
            .isTrue();
    }

    @Test
    void backpressureEventFiresOnPauseAndResume() throws Exception {
        RecordingMetrics m = new RecordingMetrics();
        String topic = createUniqueTopic("plurima-int-metrics-bp", 1);
        String groupId = "plurima-int-metrics-bp-" + UUID.randomUUID();

        int total = 100;
        int concurrency = 4;

        try (var producer = byteProducer()) {
            for (int i = 0; i < total; i++) {
                producer.send(new ProducerRecord<>(topic, ("k" + i).getBytes(),
                    ("v" + i).getBytes())).get();
            }
        }

        Properties props = classicConsumerProps(groupId);
        props.put("max.poll.records", "16");

        CountDownLatch done = new CountDownLatch(total);
        try (PlurimaConsumer<byte[], byte[]> c = PlurimaConsumer.<byte[], byte[]>builder()
                .kafkaProperties(props)
                .topic(topic)
                .engine(ConsumerEngine.CLASSIC_BASIC)
                .ordering(OrderingMode.UNORDERED)
                .concurrency(concurrency)
                .pollTimeout(Duration.ofMillis(200))
                .metrics(m)
                .listener((r, ctx) -> {
                    try { Thread.sleep(50); }
                    catch (InterruptedException ie) { Thread.currentThread().interrupt(); }
                    done.countDown();
                })
                .build()) {
            c.start();
            waitForClassicAssignment(groupId, 1, 1);
            assertThat(done.await(60, TimeUnit.SECONDS)).isTrue();
            // Backpressure may still be active; give applyBackpressure() one more pass
            // to fire "resumed".
            Thread.sleep(1_000);
        } finally {
            deleteTopicQuietly(topic);
        }

        assertThat(m.countOfBackpressure(topic, "paused"))
            .as("backpressure.events{event=paused} fires at least once under burst load")
            .isGreaterThanOrEqualTo(1);
        assertThat(m.countOfBackpressure(topic, "resumed"))
            .as("backpressure.events{event=resumed} fires at least once after workers drain")
            .isGreaterThanOrEqualTo(1);
        assertThat(m.countOfProcessed(topic, "accept"))
            .as("all 100 records eventually accept under backpressure")
            .isEqualTo(total);
    }

    // ---------------------------------------------------------------------------------
    // RecordingMetrics: in-test PlurimaMetrics that captures every call into ConcurrentMaps
    // keyed by tag tuples. Avoids depending on the :metrics module from :core test.
    // ---------------------------------------------------------------------------------
    private static final class RecordingMetrics implements PlurimaMetrics {
        private final ConcurrentMap<String, AtomicLong> polledMap = new ConcurrentHashMap<>();
        private final ConcurrentMap<String, AtomicLong> processedMap = new ConcurrentHashMap<>();
        private final ConcurrentMap<String, AtomicLong> failedMap = new ConcurrentHashMap<>();
        private final ConcurrentMap<String, AtomicLong> retryMap = new ConcurrentHashMap<>();
        private final ConcurrentMap<String, AtomicLong> dltRoutedMap = new ConcurrentHashMap<>();
        private final ConcurrentMap<String, AtomicLong> dltFailedMap = new ConcurrentHashMap<>();
        private final ConcurrentMap<String, AtomicLong> ackCommittedMap = new ConcurrentHashMap<>();
        private final ConcurrentMap<String, AtomicLong> ackCommitFailedMap = new ConcurrentHashMap<>();
        private final ConcurrentMap<String, AtomicLong> backpressureMap = new ConcurrentHashMap<>();
        private final AtomicLong pollDurationCalls = new AtomicLong();
        private final ConcurrentMap<String, AtomicLong> processDurationMap = new ConcurrentHashMap<>();
        volatile boolean inFlightGaugeRegistered = false;

        private static AtomicLong c(ConcurrentMap<String, AtomicLong> map, String key) {
            return map.computeIfAbsent(key, k -> new AtomicLong());
        }
        private static long get(ConcurrentMap<String, AtomicLong> map, String key) {
            AtomicLong v = map.get(key);
            return v == null ? 0L : v.get();
        }

        @Override public void recordsPolled(String topic, int count) {
            c(polledMap, topic).addAndGet(count);
        }
        @Override public void recordsProcessed(String topic, String result) {
            c(processedMap, topic + "|" + result).incrementAndGet();
        }
        @Override public void recordsFailed(String topic, String exceptionClass) {
            c(failedMap, topic + "|" + exceptionClass).incrementAndGet();
        }
        @Override public void retryAttempt(String topic, int attempt) {
            c(retryMap, topic + "|" + attempt).incrementAndGet();
        }
        @Override public void dltRouted(String topic, String dltTopic) {
            c(dltRoutedMap, topic + "|" + dltTopic).incrementAndGet();
        }
        @Override public void dltFailed(String topic, String cause) {
            c(dltFailedMap, topic + "|" + cause).incrementAndGet();
        }
        @Override public void ackCommitted(String topic, String type) {
            c(ackCommittedMap, topic + "|" + type).incrementAndGet();
        }
        @Override public void ackCommitFailed(String topic, String exceptionClass) {
            c(ackCommitFailedMap, topic + "|" + exceptionClass).incrementAndGet();
        }
        @Override public void backpressureEvent(String topic, String event) {
            c(backpressureMap, topic + "|" + event).incrementAndGet();
        }
        @Override public void recordPollDuration(Duration d) {
            pollDurationCalls.incrementAndGet();
        }
        @Override public void recordProcessDuration(String topic, Duration d) {
            c(processDurationMap, topic).incrementAndGet();
        }
        @Override public void registerInFlightGauge(String topic, String groupId, String clientId,
                                                     IntSupplier currentCount) {
            inFlightGaugeRegistered = true;
        }

        // Accessors are prefixed `countOf...` to avoid name collisions with the void
        // override methods of the same signature on PlurimaMetrics.
        long countOfPolled(String topic) { return get(polledMap, topic); }
        long countOfProcessed(String topic, String result) { return get(processedMap, topic + "|" + result); }
        long countOfFailed(String topic, String exceptionClass) {
            return get(failedMap, topic + "|" + exceptionClass);
        }
        long countOfRetries(String topic) {
            return retryMap.entrySet().stream()
                .filter(e -> e.getKey().startsWith(topic + "|"))
                .mapToLong(e -> e.getValue().get())
                .sum();
        }
        long countOfDltRouted(String topic, String dltTopic) { return get(dltRoutedMap, topic + "|" + dltTopic); }
        long countOfAckCommitted(String topic, String type) { return get(ackCommittedMap, topic + "|" + type); }
        long countOfBackpressure(String topic, String event) { return get(backpressureMap, topic + "|" + event); }
        long countOfPollDurations() { return pollDurationCalls.get(); }
        long countOfProcessDurations(String topic) { return get(processDurationMap, topic); }

        boolean recordsFailedEmpty() { return failedMap.isEmpty(); }
        boolean dltRoutedEmpty() { return dltRoutedMap.isEmpty(); }
        boolean dltFailedEmpty() { return dltFailedMap.isEmpty(); }
    }
}
