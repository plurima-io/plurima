package io.plurima.kafka.metrics;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class MicrometerPlurimaMetricsTest {

    private MeterRegistry registry;
    private MicrometerPlurimaMetrics metrics;

    @BeforeEach
    void setUp() {
        registry = new SimpleMeterRegistry();
        metrics = new MicrometerPlurimaMetrics(registry);
    }

    @Test
    void recordsPolledIncrementsCounter() {
        metrics.recordsPolled("orders", 5);
        metrics.recordsPolled("orders", 3);
        double count = registry.counter("plurima.consumer.records.polled", "topic", "orders").count();
        assertThat(count).isEqualTo(8.0);
    }

    @Test
    void recordsProcessedTagsByResult() {
        metrics.recordsProcessed("orders", "accept");
        metrics.recordsProcessed("orders", "reject");
        metrics.recordsProcessed("orders", "accept");

        assertThat(registry.counter("plurima.consumer.records.processed",
            "topic", "orders", "result", "accept").count()).isEqualTo(2.0);
        assertThat(registry.counter("plurima.consumer.records.processed",
            "topic", "orders", "result", "reject").count()).isEqualTo(1.0);
    }

    @Test
    void recordsFailedTagsByExceptionClass() {
        metrics.recordsFailed("orders", "java.io.IOException");
        assertThat(registry.counter("plurima.consumer.records.failed",
            "topic", "orders", "exception_class", "java.io.IOException").count()).isEqualTo(1.0);
    }

    @Test
    void retryAttemptTagsByAttempt() {
        metrics.retryAttempt("orders", 1);
        metrics.retryAttempt("orders", 2);
        metrics.retryAttempt("orders", 1);

        assertThat(registry.counter("plurima.consumer.retry.attempts",
            "topic", "orders", "attempt", "1").count()).isEqualTo(2.0);
        assertThat(registry.counter("plurima.consumer.retry.attempts",
            "topic", "orders", "attempt", "2").count()).isEqualTo(1.0);
    }

    @Test
    void dltRoutedTagsBySourceAndDest() {
        metrics.dltRouted("orders", "orders.DLT");
        assertThat(registry.counter("plurima.consumer.dlt.routed",
            "topic", "orders", "dlt_topic", "orders.DLT").count()).isEqualTo(1.0);
    }

    @Test
    void dltFailedTagsByCause() {
        metrics.dltFailed("orders", "TimeoutException");
        assertThat(registry.counter("plurima.consumer.dlt.failures",
            "topic", "orders", "cause", "TimeoutException").count()).isEqualTo(1.0);
    }

    @Test
    void ackCommitFailedTagsByExceptionClass() {
        metrics.ackCommitFailed("orders", "java.io.IOException");
        assertThat(registry.counter("plurima.consumer.ack.commit_failed",
            "topic", "orders", "exception_class", "java.io.IOException").count()).isEqualTo(1.0);
    }

    @Test
    void recordsPoisonPillTagsByCause() {
        metrics.recordsPoisonPill("orders", "deserialization");
        metrics.recordsPoisonPill("orders", "corrupt_batch");
        assertThat(registry.counter("plurima.consumer.records.poison_pill",
            "topic", "orders", "cause", "deserialization").count()).isEqualTo(1.0);
        assertThat(registry.counter("plurima.consumer.records.poison_pill",
            "topic", "orders", "cause", "corrupt_batch").count()).isEqualTo(1.0);
    }

    @Test
    void registerInFlightGaugeReadsSupplierLazily() {
        java.util.concurrent.atomic.AtomicInteger counter = new java.util.concurrent.atomic.AtomicInteger(7);
        metrics.registerInFlightGauge("orders", "group-1", "client-x", counter::get);

        io.micrometer.core.instrument.Gauge gauge = registry.find("plurima.consumer.records.in_flight")
            .tag("topic", "orders").tag("group_id", "group-1").tag("client_id", "client-x").gauge();
        assertThat(gauge).isNotNull();
        assertThat(gauge.value()).isEqualTo(7.0);

        counter.set(42);
        assertThat(gauge.value()).isEqualTo(42.0);
    }

    @Test
    void inFlightGaugesForDifferentGroupsDoNotCollide() {
        java.util.concurrent.atomic.AtomicInteger groupA = new java.util.concurrent.atomic.AtomicInteger(3);
        java.util.concurrent.atomic.AtomicInteger groupB = new java.util.concurrent.atomic.AtomicInteger(7);
        metrics.registerInFlightGauge("orders", "group-A", "client-x", groupA::get);
        metrics.registerInFlightGauge("orders", "group-B", "client-x", groupB::get);

        assertThat(registry.find("plurima.consumer.records.in_flight")
            .tag("topic", "orders").tag("group_id", "group-A").gauge().value()).isEqualTo(3.0);
        assertThat(registry.find("plurima.consumer.records.in_flight")
            .tag("topic", "orders").tag("group_id", "group-B").gauge().value()).isEqualTo(7.0);
    }

    @Test
    void inFlightGaugesForDifferentClientIdsInSameGroupDoNotCollide() {
        java.util.concurrent.atomic.AtomicInteger c1 = new java.util.concurrent.atomic.AtomicInteger(3);
        java.util.concurrent.atomic.AtomicInteger c2 = new java.util.concurrent.atomic.AtomicInteger(7);
        metrics.registerInFlightGauge("orders", "group-A", "client-1", c1::get);
        metrics.registerInFlightGauge("orders", "group-A", "client-2", c2::get);

        assertThat(registry.find("plurima.consumer.records.in_flight")
            .tag("topic", "orders").tag("group_id", "group-A").tag("client_id", "client-1").gauge().value()).isEqualTo(3.0);
        assertThat(registry.find("plurima.consumer.records.in_flight")
            .tag("topic", "orders").tag("group_id", "group-A").tag("client_id", "client-2").gauge().value()).isEqualTo(7.0);
    }

    @Test
    void barrierTimeoutGaugeReadsSupplierLazily() {
        java.util.concurrent.atomic.AtomicLong value = new java.util.concurrent.atomic.AtomicLong(30_000);
        metrics.registerBarrierTimeoutGauge("orders", "g1", "c1", value::get);

        io.micrometer.core.instrument.Gauge gauge = registry.find("plurima.consumer.barrier.timeout_ms")
            .tag("topic", "orders").tag("group_id", "g1").tag("client_id", "c1")
            .gauge();
        assertThat(gauge).isNotNull();
        assertThat(gauge.value()).isEqualTo(30_000.0);

        value.set(2_500);
        assertThat(gauge.value()).isEqualTo(2_500.0);
    }

    @Test
    void recordProcessDurationRecordsTime() {
        metrics.recordProcessDuration("orders", java.time.Duration.ofMillis(120));
        io.micrometer.core.instrument.Timer t = registry.timer("plurima.consumer.process.duration", "topic", "orders");
        assertThat(t.count()).isEqualTo(1L);
        assertThat(t.totalTime(java.util.concurrent.TimeUnit.MILLISECONDS)).isGreaterThanOrEqualTo(100.0);
    }

    @Test
    void recordPollDurationRecordsTime() {
        metrics.recordPollDuration(java.time.Duration.ofMillis(50));
        io.micrometer.core.instrument.Timer t = registry.timer("plurima.consumer.poll.duration");
        assertThat(t.count()).isEqualTo(1L);
    }

    @Test
    void ackQueuedIncrementsByType() {
        metrics.ackQueued("ACCEPT");
        metrics.ackQueued("ACCEPT");
        metrics.ackQueued("REJECT");
        assertThat(registry.counter("plurima.consumer.ack.queued", "type", "ACCEPT").count()).isEqualTo(2.0);
        assertThat(registry.counter("plurima.consumer.ack.queued", "type", "REJECT").count()).isEqualTo(1.0);
    }

    @Test
    void ackCommittedTagsByTopicAndType() {
        metrics.ackCommitted("orders", "ACCEPT");
        metrics.ackCommitted("orders", "ACCEPT");
        assertThat(registry.counter("plurima.consumer.ack.committed",
            "topic", "orders", "type", "ACCEPT").count()).isEqualTo(2.0);
    }
}
