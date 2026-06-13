package io.plurima.kafka.metrics;

import io.plurima.kafka.annotation.Stable;

/**
 * Observability hooks called by Plurima on the hot path. All methods default to no-op,
 * so an unconfigured consumer pays zero overhead. The {@code metrics/} module ships a
 * Micrometer-backed implementation; users may also write their own.
 *
 * <p>Metric names and tag conventions are committed per design § 13.7 / ADR-013.
 */
@Stable(since = "0.1.0")
public interface PlurimaMetrics {

    /** plurima.consumer.records.polled — increments by the count of records returned by poll(). */
    default void recordsPolled(String topic, int count) {}

    /** plurima.consumer.records.processed — result is one of "accept", "release", "reject". */
    default void recordsProcessed(String topic, String result) {}

    /** plurima.consumer.records.failed — emitted from every listener-thrown path (pre-retry decision). */
    default void recordsFailed(String topic, String exceptionClass) {}

    /** plurima.consumer.retry.attempts — increments per retry attempt; {@code attempt} is the 1-based retry index. */
    default void retryAttempt(String topic, int attempt) {}

    /** plurima.consumer.dlt.routed — increments when a record is successfully produced to a DLT. */
    default void dltRouted(String topic, String dltTopic) {}

    /** plurima.consumer.dlt.failures — increments when DLT produce fails. */
    default void dltFailed(String topic, String cause) {}

    /** plurima.consumer.ack.commit_failed — emitted from the AcknowledgementCommitCallback for failed partitions. */
    default void ackCommitFailed(String topic, String exceptionClass) {}

    /** plurima.consumer.records.poison_pill — increments per record that fails to deserialize or is part of a corrupt batch. */
    default void recordsPoisonPill(String topic, String cause) {}

    /** plurima.consumer.records.in_flight — current count of records still being processed by workers. */
    default void registerInFlightGauge(String topic, String groupId, String clientId, java.util.function.IntSupplier currentCount) {}

    /**
     * plurima.consumer.barrier.timeout_ms — current effective adaptive drain-barrier
     * timeout in milliseconds (SHARE engine, only when adaptive barrier is enabled).
     * Default no-op.
     */
    default void registerBarrierTimeoutGauge(String topic, String groupId, String clientId,
                                             java.util.function.LongSupplier currentMillis) {}

    /**
     * plurima.consumer.backpressure.events — increments when the CLASSIC engine pauses or
     * resumes assigned partitions because in-flight crossed the concurrency threshold.
     * {@code event} is {@code "paused"} or {@code "resumed"}. SHARE engine does not emit
     * this (it uses semaphore-based backpressure inside the poll thread, not pause/resume).
     */
    default void backpressureEvent(String topic, String event) {}

    /** plurima.consumer.process.duration — listener invocation latency. */
    default void recordProcessDuration(String topic, java.time.Duration duration) {}

    /** plurima.consumer.poll.duration — duration of a single poll() call. */
    default void recordPollDuration(java.time.Duration duration) {}

    /** plurima.consumer.ack.queued — increments each time an ack of given type is queued. */
    default void ackQueued(String type) {}

    /**
     * plurima.consumer.ack.committed — increments when an ack is APPLIED to the consumer's
     * in-memory ack set (i.e., {@code consumer.acknowledge()} has been called for the record).
     * This is NOT the same as broker-side confirmation — the actual broker commit is async.
     * Broker-side commit failures are reported separately via
     * {@link #ackCommitFailed(String, String)} from the {@code AcknowledgementCommitCallback}.
     */
    default void ackCommitted(String topic, String type) {}

    /** No-op metrics. Default singleton used when the user supplies none. */
    static PlurimaMetrics noOp() {
        return NoOp.INSTANCE;
    }

    final class NoOp implements PlurimaMetrics {
        static final NoOp INSTANCE = new NoOp();
        private NoOp() {}
    }
}
