package io.plurima.kafka.metrics;

import io.plurima.kafka.annotation.Stable;

import java.time.Duration;
import java.util.function.IntSupplier;
import java.util.function.LongSupplier;

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

    /** plurima.consumer.records.processed — tagged with the terminal {@link ProcessResult}. */
    default void recordsProcessed(String topic, ProcessResult result) {}

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
    default void registerInFlightGauge(String topic, String groupId, String clientId, IntSupplier currentCount) {}

    /**
     * plurima.consumer.barrier.timeout — current effective adaptive drain-barrier
     * timeout in milliseconds (SHARE engine, only when adaptive barrier is enabled).
     * Default no-op.
     */
    default void registerBarrierTimeoutGauge(String topic, String groupId, String clientId,
                                             LongSupplier currentMillis) {}

    /**
     * plurima.consumer.backpressure.events — increments when the CLASSIC engine pauses or
     * resumes assigned partitions because in-flight crossed the concurrency threshold.
     * SHARE engine does not emit this (it uses semaphore-based backpressure inside the
     * poll thread, not pause/resume).
     */
    default void backpressureEvent(String topic, BackpressureEvent event) {}

    /** plurima.consumer.process.duration — listener invocation latency. */
    default void recordProcessDuration(String topic, Duration duration) {}

    /** plurima.consumer.poll.duration — duration of a single poll() call, tagged by topic and consumer group. */
    default void recordPollDuration(String topic, String groupId, Duration duration) {}

    /** plurima.consumer.ack.queued — increments each time an ack of the given outcome is queued. */
    default void ackQueued(AckOutcome type) {}

    /**
     * plurima.consumer.ack.committed — increments when an ack is APPLIED to the consumer's
     * in-memory ack set (i.e., {@code consumer.acknowledge()} has been called for the record).
     * This is NOT the same as broker-side confirmation — the actual broker commit is async.
     * Broker-side commit failures are reported separately via
     * {@link #ackCommitFailed(String, String)} from the {@code AcknowledgementCommitCallback}.
     */
    default void ackCommitted(String topic, AckOutcome type) {}

    /**
     * Lifecycle hook fired when a consumer that owns this instance shuts down.
     *
     * <p><b>Per-consumer contract.</b> Each {@code PlurimaConsumer} invokes {@code close()}
     * on the metrics instance it was built with exactly once over its lifetime: on
     * {@code PlurimaConsumer.close()}, on the fatal-failure self-close path (a poll-thread
     * error causes the runtime to close itself before invoking the user's
     * {@code onFatalError} callback), or on a failed {@code start()}'s rollback — never
     * from a subsequent redundant close of the same consumer.
     *
     * <p><b>Shared-instance contract.</b> The same {@code PlurimaMetrics} instance may
     * back MULTIPLE consumers (e.g. one Spring bean injected into every
     * {@code @PlurimaListener} endpoint). Such an instance may therefore see {@code close()}
     * up to once <em>per consumer</em>, at different times, while other consumers are still
     * live. Implementations shared across consumers must be idempotent and must not tear
     * down state other live consumers depend on — they may choose to make {@code close()}
     * a complete no-op and tie cleanup to their own (registry/container) lifecycle
     * instead. The Plurima Spring starter enforces this by wrapping the shared bean per
     * endpoint in a close-suppressing delegate.
     *
     * <p>A single-consumer (unshared) implementation should unregister any gauges
     * registered for that consumer via {@link #registerInFlightGauge} /
     * {@link #registerBarrierTimeoutGauge} so a closed consumer's metrics stop being
     * scraped/reported. Default no-op.
     */
    default void close() {}

    /** No-op metrics. Default singleton used when the user supplies none. */
    static PlurimaMetrics noOp() {
        return NoOp.INSTANCE;
    }

    final class NoOp implements PlurimaMetrics {
        static final NoOp INSTANCE = new NoOp();
        private NoOp() {}
    }
}
