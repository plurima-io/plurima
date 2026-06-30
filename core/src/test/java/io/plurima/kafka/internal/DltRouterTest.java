package io.plurima.kafka.internal;

import io.plurima.kafka.dlt.DltConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;


import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DltRouterTest {

    private MockProducer<byte[], byte[]> producer;
    private DltConfig config;

    @BeforeEach
    void setUp() {
        producer = new MockProducer<byte[], byte[]>(true, null, new ByteArraySerializer(), new ByteArraySerializer());
        Properties props = new Properties();
        props.put("bootstrap.servers", "ignored");
        config = DltConfig.builder().producerProperties(props).build();
    }

    private InFlightRecord<byte[], byte[]> rec() {
        ConsumerRecord<byte[], byte[]> cr = new ConsumerRecord<>(
            "orders", 2, 17L, "k".getBytes(), "v".getBytes());
        cr.headers().add("trace-id", "abc123".getBytes());
        InFlightRecord<byte[], byte[]> r = new InFlightRecord<>(cr);
        r.incrementAttempt(); r.incrementAttempt(); // attempt=2
        return r;
    }

    @Test
    void routesToDefaultDotDltTopic() throws Exception {
        try (DltRouter router = new DltRouter(producer, config)) {
            DltRouter.DltRoute route = router.route(rec(), new IOException("transient"));
            route.future().get(2, TimeUnit.SECONDS);
        }

        assertThat(producer.history()).hasSize(1);
        ProducerRecord<byte[], byte[]> sent = producer.history().get(0);
        assertThat(sent.topic()).isEqualTo("orders.DLT");
    }

    @Test
    void preservesKeyAndValueBytes() throws Exception {
        try (DltRouter router = new DltRouter(producer, config)) {
            router.route(rec(), new IOException("t")).future().get(2, TimeUnit.SECONDS);
        }

        ProducerRecord<byte[], byte[]> sent = producer.history().get(0);
        assertThat(new String(sent.key(), StandardCharsets.UTF_8)).isEqualTo("k");
        assertThat(new String(sent.value(), StandardCharsets.UTF_8)).isEqualTo("v");
    }

    @Test
    void writesDesignSection138Headers() throws Exception {
        try (DltRouter router = new DltRouter(producer, config)) {
            router.route(rec(), new IOException("transient")).future().get(2, TimeUnit.SECONDS);
        }

        Map<String, String> headers = headerMap(producer.history().get(0));
        assertThat(headers).containsKey("plurima-dlt-original-topic")
            .containsKey("plurima-dlt-original-partition")
            .containsKey("plurima-dlt-original-offset")
            .containsKey("plurima-dlt-failure-class")
            .containsKey("plurima-dlt-failure-message")
            .containsKey("plurima-dlt-attempt-count")
            .containsKey("plurima-dlt-routed-at");

        assertThat(headers.get("plurima-dlt-original-topic")).isEqualTo("orders");
        assertThat(headers.get("plurima-dlt-original-partition")).isEqualTo("2");
        assertThat(headers.get("plurima-dlt-original-offset")).isEqualTo("17");
        assertThat(headers.get("plurima-dlt-failure-class")).isEqualTo("java.io.IOException");
        assertThat(headers.get("plurima-dlt-failure-message")).isEqualTo("transient");
        assertThat(headers.get("plurima-dlt-attempt-count")).isEqualTo("2");
        assertThat(headers.get("plurima-dlt-routed-at")).startsWith("20");
    }

    @Test
    void passesThroughOriginalHeaders() throws Exception {
        try (DltRouter router = new DltRouter(producer, config)) {
            router.route(rec(), new IOException("t")).future().get(2, TimeUnit.SECONDS);
        }

        Map<String, String> headers = headerMap(producer.history().get(0));
        assertThat(headers).containsEntry("trace-id", "abc123");
    }

    @Test
    void stackTraceOmittedByDefault() throws Exception {
        try (DltRouter router = new DltRouter(producer, config)) {
            router.route(rec(), new IOException("t")).future().get(2, TimeUnit.SECONDS);
        }

        Map<String, String> headers = headerMap(producer.history().get(0));
        assertThat(headers).doesNotContainKey("plurima-dlt-stack-trace");
    }

    @Test
    void stackTraceIncludedWhenEnabled() throws Exception {
        Properties props = new Properties();
        props.put("bootstrap.servers", "ignored");
        DltConfig cfgWithStack = DltConfig.builder()
            .producerProperties(props)
            .includeStackTrace(true)
            .build();

        try (DltRouter router = new DltRouter(producer, cfgWithStack)) {
            router.route(rec(), new IOException("t")).future().get(2, TimeUnit.SECONDS);
        }

        Map<String, String> headers = headerMap(producer.history().get(0));
        assertThat(headers).containsKey("plurima-dlt-stack-trace");
        assertThat(headers.get("plurima-dlt-stack-trace")).contains("IOException");
        assertThat(headers.get("plurima-dlt-stack-trace").length()).isLessThanOrEqualTo(4096);
    }

    @Test
    void usesCustomNamingStrategy() throws Exception {
        Properties props = new Properties();
        props.put("bootstrap.servers", "ignored");
        DltConfig prefixed = DltConfig.builder()
            .producerProperties(props)
            .namingStrategy(t -> "dlt." + t)
            .build();

        try (DltRouter router = new DltRouter(producer, prefixed)) {
            router.route(rec(), new IOException("t")).future().get(2, TimeUnit.SECONDS);
        }

        assertThat(producer.history().get(0).topic()).isEqualTo("dlt.orders");
    }

    @Test
    void failedSendCompletesFutureExceptionally() {
        MockProducer<byte[], byte[]> manualProducer = new MockProducer<byte[], byte[]>(
            false, null, new ByteArraySerializer(), new ByteArraySerializer());

        Properties props = new Properties();
        props.put("bootstrap.servers", "ignored");
        DltConfig cfg = DltConfig.builder().producerProperties(props).build();

        try (DltRouter router = new DltRouter(manualProducer, cfg)) {
            DltRouter.DltRoute route = router.route(rec(), new IOException("t"));
            manualProducer.errorNext(new RuntimeException("broker down"));
            assertThatThrownBy(() -> route.future().get(2, TimeUnit.SECONDS))
                .hasRootCauseInstanceOf(RuntimeException.class)
                .hasMessageContaining("broker down");
        }
    }

    @Test
    void attemptCountUsesBrokerDeliveryCountWhenLarger() throws Exception {
        // After several delayed retries (each RELEASE → broker redeliver → fresh
        // InFlightRecord with attempt=0), the broker's deliveryCount has accumulated
        // even though the in-process counter resets. The DLT header must report the
        // broker count to give the operator the real picture.
        ConsumerRecord<byte[], byte[]> cr = new ConsumerRecord<>(
            "orders", 0, 17L, 0L,
            org.apache.kafka.common.record.TimestampType.NO_TIMESTAMP_TYPE,
            -1, -1,
            "k".getBytes(), "v".getBytes(),
            new org.apache.kafka.common.header.internals.RecordHeaders(),
            java.util.Optional.empty(),
            java.util.Optional.of((short) 5));  // 5 broker deliveries
        InFlightRecord<byte[], byte[]> redelivered = new InFlightRecord<>(cr);
        // Fresh after delayed retry → in-process attempt = 0

        try (DltRouter router = new DltRouter(producer, config)) {
            router.route(redelivered, new IOException("t")).future().get(2, TimeUnit.SECONDS);
        }

        Map<String, String> headers = headerMap(producer.history().get(0));
        assertThat(headers.get("plurima-dlt-attempt-count"))
            .as("DLT header must reflect broker deliveryCount-1=4 when in-process attempt=0 is lower")
            .isEqualTo("4");
    }

    @Test
    void attemptCountSubtractsSlownessReleases() throws Exception {
        // deliveryCount=5 (4 redeliveries) but 3 were slowness force-RELEASEs, not failures → the
        // DLT attempt header must report only the genuine attempts: max(0, (5-1) - 3) = 1.
        ConsumerRecord<byte[], byte[]> cr = new ConsumerRecord<>(
            "orders", 0, 17L, 0L,
            org.apache.kafka.common.record.TimestampType.NO_TIMESTAMP_TYPE,
            -1, -1,
            "k".getBytes(), "v".getBytes(),
            new org.apache.kafka.common.header.internals.RecordHeaders(),
            java.util.Optional.empty(),
            java.util.Optional.of((short) 5));
        InFlightRecord<byte[], byte[]> r = new InFlightRecord<>(cr);

        SlownessReleaseTracker tracker = new SlownessReleaseTracker(1024);
        RecordCoord coord = new RecordCoord("orders", 0, 17L);
        tracker.recordRelease(coord);
        tracker.recordRelease(coord);
        tracker.recordRelease(coord);

        try (DltRouter router = new DltRouter(
                producer, config, io.plurima.kafka.metrics.PlurimaMetrics.noOp(), tracker)) {
            router.route(r, new IOException("t")).future().get(2, TimeUnit.SECONDS);
        }

        Map<String, String> headers = headerMap(producer.history().get(0));
        assertThat(headers.get("plurima-dlt-attempt-count"))
            .as("slowness force-RELEASEs are subtracted from the broker deliveryCount in the DLT header")
            .isEqualTo("1");
    }

    @Test
    void synchronousProducerThrowEmitsDltFailedMetric() {
        // SerializationException, BufferExhaustedException, KafkaException on shutdown
        // etc. can be thrown synchronously by producer.send BEFORE any callback. Without
        // the synchronous catch in DltRouter, dltFailed would never fire — silently
        // missing the alert.
        org.apache.kafka.clients.producer.Producer<byte[], byte[]> throwingProducer =
            new org.apache.kafka.clients.producer.MockProducer<byte[], byte[]>(
                true, null, new ByteArraySerializer(), new ByteArraySerializer()) {
                @Override
                public java.util.concurrent.Future<org.apache.kafka.clients.producer.RecordMetadata> send(
                    ProducerRecord<byte[], byte[]> record,
                    org.apache.kafka.clients.producer.Callback callback) {
                    throw new IllegalStateException("producer already closed");
                }
            };

        java.util.concurrent.atomic.AtomicInteger dltFailedCount = new java.util.concurrent.atomic.AtomicInteger();
        io.plurima.kafka.metrics.PlurimaMetrics metrics = new io.plurima.kafka.metrics.PlurimaMetrics() {
            @Override public void dltFailed(String topic, String cause) { dltFailedCount.incrementAndGet(); }
        };

        try (DltRouter router = new DltRouter(throwingProducer, config, metrics)) {
            DltRouter.DltRoute route = router.route(rec(), new IOException("t"));
            assertThatThrownBy(() -> route.future().get(1, TimeUnit.SECONDS))
                .hasRootCauseInstanceOf(IllegalStateException.class)
                .hasMessageContaining("producer already closed");
        }

        assertThat(dltFailedCount.get())
            .as("dltFailed metric must fire for synchronous producer.send throws — operators alert on this")
            .isEqualTo(1);
    }

    @Test
    void firstCompletionWinsBetweenCallerTimeoutAndCallbackSuccess() throws Exception {
        // Slow broker scenario: the producer's callback eventually succeeds, but the
        // caller (ClassicPollLoop / WorkerProcessor) has already timed out at .get(30s)
        // and emitted dltFailed. Without first-completion-wins, the eventual callback
        // would emit dltRouted on top — operators would see both metrics for the same
        // record. The DltRoute.metricEmitted CAS guarantees only the first wins.
        MockProducer<byte[], byte[]> manualProducer = new MockProducer<byte[], byte[]>(
            false, null, new ByteArraySerializer(), new ByteArraySerializer());

        java.util.concurrent.atomic.AtomicInteger dltFailedCount = new java.util.concurrent.atomic.AtomicInteger();
        java.util.concurrent.atomic.AtomicInteger dltRoutedCount = new java.util.concurrent.atomic.AtomicInteger();
        io.plurima.kafka.metrics.PlurimaMetrics metrics = new io.plurima.kafka.metrics.PlurimaMetrics() {
            @Override public void dltFailed(String topic, String cause) { dltFailedCount.incrementAndGet(); }
            @Override public void dltRouted(String topic, String dltTopic) { dltRoutedCount.incrementAndGet(); }
        };

        try (DltRouter router = new DltRouter(manualProducer, config, metrics)) {
            DltRouter.DltRoute route = router.route(rec(), new IOException("t"));

            // Caller times out first: pre-CAS the flag (mirrors what
            // ClassicPollLoop.handleExhaustion does on TimeoutException) and emit
            // dltFailed.
            assertThat(route.metricEmitted().compareAndSet(false, true))
                .as("caller's CAS must succeed when it's the first completion")
                .isTrue();
            metrics.dltFailed("orders", "TimeoutException");

            // Now the broker finally responds successfully — the callback should
            // NOT emit dltRouted (the CAS will fail).
            manualProducer.completeNext();

            // Wait briefly for the callback to land.
            long start = System.nanoTime();
            while (!route.future().isDone()
                && (System.nanoTime() - start) < TimeUnit.SECONDS.toNanos(2)) {
                Thread.sleep(10);
            }
            assertThat(route.future().isDone()).isTrue();
        }

        assertThat(dltFailedCount.get())
            .as("exactly one metric emission — caller's dltFailed")
            .isEqualTo(1);
        assertThat(dltRoutedCount.get())
            .as("callback must NOT emit dltRouted after caller already CAS'd metricEmitted")
            .isZero();
    }

    @Test
    void nullMessageBecomesLiteralNullString() throws Exception {
        try (DltRouter router = new DltRouter(producer, config)) {
            router.route(rec(), new IOException((String) null)).future().get(2, TimeUnit.SECONDS);
        }

        Map<String, String> headers = headerMap(producer.history().get(0));
        assertThat(headers).containsEntry("plurima-dlt-failure-message", "null");
    }

    private static Map<String, String> headerMap(ProducerRecord<byte[], byte[]> r) {
        java.util.HashMap<String, String> m = new java.util.HashMap<>();
        for (Header h : r.headers()) {
            m.put(h.key(), new String(h.value(), StandardCharsets.UTF_8));
        }
        return m;
    }
}
