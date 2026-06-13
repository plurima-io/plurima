package io.plurima.kafka.metrics;

import io.micrometer.core.instrument.MeterRegistry;
import io.plurima.kafka.annotation.Stable;

import java.util.Objects;

/**
 * Micrometer adapter for {@link PlurimaMetrics}. Increments counters in the supplied
 * {@link MeterRegistry} using the metric names and tag conventions committed in
 * design § 13.7.
 */
@Stable(since = "0.1.0")
public final class MicrometerPlurimaMetrics implements PlurimaMetrics {

    private final MeterRegistry registry;

    public MicrometerPlurimaMetrics(MeterRegistry registry) {
        this.registry = Objects.requireNonNull(registry, "registry");
    }

    @Override
    public void recordsPolled(String topic, int count) {
        registry.counter("plurima.consumer.records.polled", "topic", topic).increment(count);
    }

    @Override
    public void recordsProcessed(String topic, String result) {
        registry.counter("plurima.consumer.records.processed",
            "topic", topic, "result", result).increment();
    }

    @Override
    public void recordsFailed(String topic, String exceptionClass) {
        registry.counter("plurima.consumer.records.failed",
            "topic", topic, "exception_class", exceptionClass).increment();
    }

    @Override
    public void retryAttempt(String topic, int attempt) {
        registry.counter("plurima.consumer.retry.attempts",
            "topic", topic, "attempt", Integer.toString(attempt)).increment();
    }

    @Override
    public void dltRouted(String topic, String dltTopic) {
        registry.counter("plurima.consumer.dlt.routed",
            "topic", topic, "dlt_topic", dltTopic).increment();
    }

    @Override
    public void dltFailed(String topic, String cause) {
        registry.counter("plurima.consumer.dlt.failures",
            "topic", topic, "cause", cause).increment();
    }

    @Override
    public void ackCommitFailed(String topic, String exceptionClass) {
        registry.counter("plurima.consumer.ack.commit_failed",
            "topic", topic, "exception_class", exceptionClass).increment();
    }

    @Override
    public void recordsPoisonPill(String topic, String cause) {
        registry.counter("plurima.consumer.records.poison_pill",
            "topic", topic, "cause", cause).increment();
    }

    @Override
    public void registerInFlightGauge(String topic, String groupId, String clientId, java.util.function.IntSupplier currentCount) {
        io.micrometer.core.instrument.Gauge.builder(
            "plurima.consumer.records.in_flight",
            () -> (double) currentCount.getAsInt())
            .tag("topic", topic)
            .tag("group_id", groupId)
            .tag("client_id", clientId)
            .register(registry);
    }

    @Override
    public void registerBarrierTimeoutGauge(String topic, String groupId, String clientId,
                                            java.util.function.LongSupplier currentMillis) {
        io.micrometer.core.instrument.Gauge.builder(
            "plurima.consumer.barrier.timeout_ms",
            () -> (double) currentMillis.getAsLong())
            .tag("topic", topic)
            .tag("group_id", groupId)
            .tag("client_id", clientId)
            .register(registry);
    }

    @Override
    public void recordProcessDuration(String topic, java.time.Duration duration) {
        registry.timer("plurima.consumer.process.duration", "topic", topic)
            .record(duration);
    }

    @Override
    public void recordPollDuration(java.time.Duration duration) {
        registry.timer("plurima.consumer.poll.duration").record(duration);
    }

    @Override
    public void ackQueued(String type) {
        registry.counter("plurima.consumer.ack.queued", "type", type).increment();
    }

    @Override
    public void ackCommitted(String topic, String type) {
        registry.counter("plurima.consumer.ack.committed", "topic", topic, "type", type).increment();
    }

    @Override
    public void backpressureEvent(String topic, String event) {
        registry.counter("plurima.consumer.backpressure.events",
            "topic", topic, "event", event).increment();
    }
}
