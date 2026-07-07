package io.plurima.kafka.metrics;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class MetricsAcceptanceTest {

    @Test
    void counterNamesAndTagsMatchDesignSection137() {
        MeterRegistry registry = new SimpleMeterRegistry();
        MicrometerPlurimaMetrics m = new MicrometerPlurimaMetrics(registry);

        m.recordsPolled("orders", 10);
        m.recordsProcessed("orders", ProcessResult.ACCEPT);
        m.recordsProcessed("orders", ProcessResult.REJECT);
        m.recordsFailed("orders", "java.io.IOException");
        m.retryAttempt("orders", 1);
        m.dltRouted("orders", "orders.DLT");
        m.dltFailed("orders", "TimeoutException");
        m.ackCommitFailed("orders", "java.io.IOException");
        m.recordsPoisonPill("orders", "deserialization");
        m.registerInFlightGauge("orders", "group-1", "client-x", () -> 5);
        m.recordProcessDuration("orders", java.time.Duration.ofMillis(10));
        m.recordPollDuration("orders", "group-1", java.time.Duration.ofMillis(10));
        m.ackQueued(AckOutcome.ACCEPT);
        m.ackCommitted("orders", AckOutcome.ACCEPT);
        m.backpressureEvent("orders", BackpressureEvent.PAUSED);

        assertThat(registry.getMeters())
            .extracting(meter -> meter.getId().getName())
            .contains(
                "plurima.consumer.records.polled",
                "plurima.consumer.records.processed",
                "plurima.consumer.records.failed",
                "plurima.consumer.retry.attempts",
                "plurima.consumer.dlt.routed",
                "plurima.consumer.dlt.failures",
                "plurima.consumer.ack.commit_failed",
                "plurima.consumer.records.poison_pill",
                "plurima.consumer.records.in_flight",
                "plurima.consumer.process.duration",
                "plurima.consumer.poll.duration",
                "plurima.consumer.ack.queued",
                "plurima.consumer.ack.committed",
                "plurima.consumer.backpressure.events");
    }
}
