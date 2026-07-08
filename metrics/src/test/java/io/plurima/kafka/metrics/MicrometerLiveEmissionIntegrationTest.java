package io.plurima.kafka.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.plurima.kafka.ConsumerEngine;
import io.plurima.kafka.OrderingMode;
import io.plurima.kafka.PlurimaConsumer;
import io.plurima.kafka.RecordListener;
import io.plurima.kafka.dlt.DltConfig;
import io.plurima.kafka.retry.RetryPolicy;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.ShareGroupDescription;
import org.apache.kafka.clients.admin.ShareMemberDescription;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end verification that metrics flow from production code paths, through
 * {@link MicrometerPlurimaMetrics}, into a real {@link SimpleMeterRegistry} with the
 * documented names, tags, types, and non-zero values.
 *
 * <p>The pre-existing {@code ClassicMetricsIntegrationTest} in {@code :core} verifies
 * that the {@link io.plurima.kafka.metrics.PlurimaMetrics} interface is called by the
 * runtime — but it uses an in-test recording fake, so it cannot prove that the
 * Micrometer adapter actually translates those calls into {@link Counter} /
 * {@link Timer} / {@link Gauge} instances with the right metadata. Conversely, the
 * adapter's unit test {@code MicrometerPlurimaMetricsTest} verifies the translation
 * in isolation by calling each method by hand — but that can't catch the case where
 * the runtime forgets to emit a metric at all.
 *
 * <p>This test bridges the two: a live consumer running against a real broker, with
 * {@code MicrometerPlurimaMetrics(SimpleMeterRegistry)} as the metrics sink. After
 * the workload runs we walk the registry and assert each documented meter exists
 * with the expected type, tags, and a value consistent with the work that was done.
 *
 * <p>Requires a running Kafka 4.x broker at {@code localhost:9092} — same prereq as
 * the integration tests in {@code :core}. Invoke with
 * {@code ./gradlew :metrics:test -PintegrationTests=true}.
 */
@org.junit.jupiter.api.Tag("integration")
class MicrometerLiveEmissionIntegrationTest {

    private static final String BOOTSTRAP = "localhost:9092";

    @Test
    void classicEngineEmitsDocumentedMetersThroughMicrometerAdapter() throws Exception {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        MicrometerPlurimaMetrics metrics = new MicrometerPlurimaMetrics(registry);

        String topic = createTopic("plurima-mm-live");
        String groupId = "plurima-mm-live-" + UUID.randomUUID();
        String clientId = "client-" + UUID.randomUUID();
        Properties props = classicConsumerProps(groupId, clientId);

        int total = 12;
        int failingOffset = 5;          // one record always throws → exercises retry + DLT
        CountDownLatch processed = new CountDownLatch(total - 1);  // total minus the DLT'd record

        DltConfig dlt = DltConfig.builder()
            .namingStrategy(t -> t + ".DLT")
            .producerProperties(producerProps())
            .build();

        AtomicInteger failingAttempts = new AtomicInteger();
        RecordListener<byte[], byte[]> listener = (rec, ctx) -> {
            long offset = rec.offset();
            if (offset == failingOffset) {
                failingAttempts.incrementAndGet();
                throw new RuntimeException("boom");
            }
            processed.countDown();
        };

        // Produce BEFORE starting the consumer so earliest+poll picks the whole batch
        // on the first call (improves test determinism).
        try (KafkaProducer<byte[], byte[]> producer = byteProducer()) {
            for (long i = 0; i < total; i++) {
                producer.send(new ProducerRecord<>(topic, ("k" + i).getBytes(), ("v" + i).getBytes()));
            }
            producer.flush();
        }

        try (PlurimaConsumer<byte[], byte[]> consumer = PlurimaConsumer.builder()
            .kafkaProperties(props)
            .topic(topic)
            .engine(ConsumerEngine.CLASSIC_BASIC)
            .ordering(OrderingMode.UNORDERED)
            .listener(listener)
            .retry(RetryPolicy.exponential()
                .maxAttempts(2)              // 0 < 2 → retry; 1 < 2 → retry; 2 >= 2 → exhaust
                .initialDelay(Duration.ofMillis(50))
                .multiplier(2.0)
                .jitter(0.0)
                .retryOn(RuntimeException.class)
                .build())
            .deadLetter(dlt)
            .pollTimeout(Duration.ofMillis(200))
            .shutdownDrainTimeout(Duration.ofSeconds(10))
            .metrics(metrics)
            .build()) {

            consumer.start();

            // Wait for the 11 non-failing records to process AND the failing one to
            // finish its retry sequence by routing to DLT. Polling the meter directly
            // is the most reliable readiness signal — the listener-attempt counter
            // could lag behind the actual decision flow.
            assertThat(processed.await(45, TimeUnit.SECONDS))
                .as("non-failing records must process within 45s")
                .isTrue();
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
            while (registry.find("plurima.consumer.dlt.routed").counter() == null
                || registry.find("plurima.consumer.dlt.routed").counter().count() < 1.0) {
                if (System.nanoTime() > deadline) {
                    throw new AssertionError("DLT route did not happen within 30s; "
                        + "failingAttempts=" + failingAttempts.get());
                }
                Thread.sleep(100);
            }
            // failingAttempts: 1 initial + 2 retries (maxAttempts=2 means attempts 0,1,2)
            assertThat(failingAttempts.get())
                .as("failing record must attempt 3 times before exhaustion")
                .isEqualTo(3);

            Gauge inFlight = required(registry, "plurima.consumer.records.in_flight", Gauge.class,
                "topic", topic, "group_id", groupId, "client_id", clientId);
            assertThat(inFlight.value())
                .as("in_flight gauge is registered while the consumer is open and drained")
                .isEqualTo(0.0);
        }

        // ────── Assertions against the SimpleMeterRegistry ──────
        // These directly probe the Micrometer registry — what users plug into
        // Prometheus, JMX, OTel etc. The names and tag keys are the documented
        // contract surface; any drift here is a real regression in metric shape.

        Counter polled = required(registry, "plurima.consumer.records.polled", Counter.class,
            "topic", topic);
        assertThat(polled.count())
            .as("polled count >= total produced")
            .isGreaterThanOrEqualTo(total);

        Counter processedAccept = required(registry,
            "plurima.consumer.records.processed", Counter.class,
            "topic", topic, "result", "accept");
        // total-1 records succeed normally; the failing record's DLT-route counts as
        // "accept" (the wrapped record landed on DLT, original is committed).
        assertThat(processedAccept.count())
            .as("accept count includes successful records and the DLT-routed original")
            .isGreaterThanOrEqualTo(total - 1);

        Counter failed = required(registry, "plurima.consumer.records.failed", Counter.class,
            "topic", topic, "exception_class", "java.lang.RuntimeException");
        assertThat(failed.count())
            .as("failed count must equal the attempt count for the failing record")
            .isEqualTo(failingAttempts.get());

        Counter retryAttempts = required(registry, "plurima.consumer.retry.attempts", Counter.class,
            "topic", topic);
        assertThat(retryAttempts.count())
            .as("at least one retry attempt was recorded")
            .isGreaterThanOrEqualTo(1.0);

        Counter dltRouted = required(registry, "plurima.consumer.dlt.routed", Counter.class,
            "topic", topic, "dlt_topic", topic + ".DLT");
        assertThat(dltRouted.count())
            .as("exactly one DLT route for the failing record")
            .isEqualTo(1.0);

        Timer pollDuration = required(registry, "plurima.consumer.poll.duration", Timer.class);
        assertThat(pollDuration.count())
            .as("poll timer must have at least one observation")
            .isGreaterThanOrEqualTo(1);
        assertThat(pollDuration.totalTime(TimeUnit.NANOSECONDS))
            .as("poll timer totalTime > 0")
            .isGreaterThan(0);

        Timer processDuration = required(registry, "plurima.consumer.process.duration", Timer.class,
            "topic", topic);
        assertThat(processDuration.count())
            .as("process timer observation count covers every listener invocation")
            .isGreaterThanOrEqualTo(total);

        // Final smoke check: enumerate every Plurima meter and assert its name follows
        // the documented {@code plurima.consumer.*} prefix — guards against an accidental
        // misnamed meter slipping in via a future code path.
        List<String> plurimaMeterNames = registry.getMeters().stream()
            .map(Meter::getId)
            .map(Meter.Id::getName)
            .filter(n -> n.startsWith("plurima."))
            .distinct()
            .sorted()
            .toList();
        assertThat(plurimaMeterNames)
            .as("every registered Plurima meter uses the documented namespace")
            .allSatisfy(name -> assertThat(name).startsWith("plurima.consumer."));
        // Sanity: we observed AT LEAST these on the happy/retry/DLT path. Don't
        // hard-pin the full set — backpressure / poison_pill etc. may be absent
        // depending on the run.
        assertThat(plurimaMeterNames).contains(
            "plurima.consumer.dlt.routed",
            "plurima.consumer.poll.duration",
            "plurima.consumer.process.duration",
            "plurima.consumer.records.failed",
            "plurima.consumer.records.polled",
            "plurima.consumer.records.processed",
            "plurima.consumer.retry.attempts"
        );

        deleteTopicQuietly(topic);
        deleteTopicQuietly(topic + ".DLT");
    }

    @Test
    void shareEngineEmitsCorePollAndProcessMetersThroughMicrometerAdapter() throws Exception {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        MicrometerPlurimaMetrics metrics = new MicrometerPlurimaMetrics(registry);

        String topic = createTopic("plurima-mm-live-share");
        String groupId = "plurima-mm-live-share-" + UUID.randomUUID();
        String clientId = "client-" + UUID.randomUUID();

        Properties props = new Properties();
        props.put("bootstrap.servers", BOOTSTRAP);
        props.put("group.id", groupId);
        props.put("client.id", clientId);
        props.put("share.acknowledgement.mode", "explicit");

        int total = 8;
        CountDownLatch processed = new CountDownLatch(total);
        RecordListener<byte[], byte[]> listener = (rec, ctx) -> processed.countDown();

        try (PlurimaConsumer<byte[], byte[]> consumer = PlurimaConsumer.builder()
            .kafkaProperties(props)
            .topic(topic)
            .engine(ConsumerEngine.SHARE)
            .ordering(OrderingMode.UNORDERED)
            .listener(listener)
            .pollTimeout(Duration.ofMillis(200))
            .shutdownDrainTimeout(Duration.ofSeconds(10))
            .metrics(metrics)
            .build()) {

            consumer.start();
            waitForShareAssignment(groupId, topic);

            try (KafkaProducer<byte[], byte[]> producer = byteProducer()) {
                for (long i = 0; i < total; i++) {
                    producer.send(new ProducerRecord<>(topic, ("k" + i).getBytes(), ("v" + i).getBytes()));
                }
                producer.flush();
            }

            assertThat(processed.await(60, TimeUnit.SECONDS))
                .as("share-group must process all %d records within 60s", total)
                .isTrue();
            // SHARE's ack.committed fires asynchronously via the broker callback. Brief
            // settle so we can assert on it below without flakes.
            Thread.sleep(1_500);

            Gauge inFlight = required(registry, "plurima.consumer.records.in_flight", Gauge.class,
                "topic", topic, "group_id", groupId, "client_id", clientId);
            assertThat(inFlight.value())
                .as("in_flight gauge is registered while the consumer is open and drained")
                .isEqualTo(0.0);
        }

        // Cover the metrics that SHARE specifically emits (ack.queued is share-only by
        // design — classic has no per-record ack model).
        Counter polled = required(registry, "plurima.consumer.records.polled", Counter.class,
            "topic", topic);
        assertThat(polled.count()).isGreaterThanOrEqualTo(total);

        Counter processedAccept = required(registry, "plurima.consumer.records.processed",
            Counter.class, "topic", topic, "result", "accept");
        assertThat(processedAccept.count()).isGreaterThanOrEqualTo(total);

        Counter ackQueued = required(registry, "plurima.consumer.ack.queued", Counter.class,
            "type", "accept");
        assertThat(ackQueued.count())
            .as("ack.queued is SHARE-only — must fire for every processed record")
            .isGreaterThanOrEqualTo(total);

        Counter ackCommitted = required(registry, "plurima.consumer.ack.committed", Counter.class,
            "topic", topic, "type", "accept");
        assertThat(ackCommitted.count())
            .as("ack.committed fires when the broker confirms the share-ack callback")
            .isGreaterThanOrEqualTo(1.0);

        Timer pollDuration = required(registry, "plurima.consumer.poll.duration", Timer.class);
        assertThat(pollDuration.count()).isGreaterThanOrEqualTo(1);

        // createTopic provisions both `topic` and `topic + ".DLT"` for symmetry with
        // the classic test; clean up both even though SHARE didn't write to the DLT.
        deleteTopicQuietly(topic);
        deleteTopicQuietly(topic + ".DLT");
    }

    // ────── Local helpers (kept tiny so this file is self-contained inside :metrics) ──────

    private static void waitForShareAssignment(String groupId, String topic) throws Exception {
        long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        try (AdminClient admin = AdminClient.create(adminProps())) {
            while (System.nanoTime() < deadlineNanos) {
                try {
                    Map<String, ShareGroupDescription> described =
                        admin.describeShareGroups(List.of(groupId)).all().get(5, TimeUnit.SECONDS);
                    ShareGroupDescription group = described.get(groupId);
                    if (group != null && hasAssignedTopic(group, topic)) {
                        Thread.sleep(500);
                        return;
                    }
                } catch (Exception ignored) {
                    // Share group not visible yet; retry until the deadline.
                }
                Thread.sleep(200);
            }
        }
        throw new AssertionError(
            "Share-group '" + groupId + "' did not get '" + topic + "' assigned within 30s");
    }

    private static boolean hasAssignedTopic(ShareGroupDescription group, String topic) {
        for (ShareMemberDescription member : group.members()) {
            for (var assigned : member.assignment().topicPartitions()) {
                if (assigned.topic().equals(topic)) {
                    return true;
                }
            }
        }
        return false;
    }

    private static <M extends Meter> M required(
        SimpleMeterRegistry registry, String name, Class<M> type, String... tagKv) {
        var search = registry.find(name);
        for (int i = 0; i < tagKv.length; i += 2) {
            search = search.tag(tagKv[i], tagKv[i + 1]);
        }
        Meter meter = search.meter();
        assertThat(meter)
            .as("registry must contain meter '%s' with tags %s", name, java.util.Arrays.toString(tagKv))
            .isNotNull();
        assertThat(meter)
            .as("meter '%s' must be a %s", name, type.getSimpleName())
            .isInstanceOf(type);
        return type.cast(meter);
    }

    private static Properties classicConsumerProps(String groupId, String clientId) {
        Properties p = new Properties();
        p.put("bootstrap.servers", BOOTSTRAP);
        p.put("group.id", groupId);
        p.put("client.id", clientId);
        p.put("auto.offset.reset", "earliest");
        return p;
    }

    private static Properties producerProps() {
        Properties p = new Properties();
        p.put("bootstrap.servers", BOOTSTRAP);
        p.put("key.serializer", ByteArraySerializer.class.getName());
        p.put("value.serializer", ByteArraySerializer.class.getName());
        p.put("acks", "all");
        return p;
    }

    private static KafkaProducer<byte[], byte[]> byteProducer() {
        return new KafkaProducer<>(producerProps());
    }

    private static Properties adminProps() {
        Properties p = new Properties();
        p.put("bootstrap.servers", BOOTSTRAP);
        p.put("request.timeout.ms", "10000");
        return p;
    }

    private static String createTopic(String prefix) throws Exception {
        String topic = prefix + "-" + UUID.randomUUID();
        try (AdminClient admin = AdminClient.create(adminProps())) {
            admin.createTopics(List.of(
                new NewTopic(topic, 1, (short) 1),
                new NewTopic(topic + ".DLT", 1, (short) 1))).all().get(15, TimeUnit.SECONDS);
        }
        return topic;
    }

    private static void deleteTopicQuietly(String topic) {
        try (AdminClient admin = AdminClient.create(adminProps())) {
            admin.deleteTopics(List.of(topic)).all().get(5, TimeUnit.SECONDS);
        } catch (Exception ignored) {
            // best-effort cleanup
        }
    }
}
