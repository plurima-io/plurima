package io.plurima.kafka.spring.internal;

import io.plurima.kafka.annotation.Internal;
import io.plurima.kafka.metrics.AckOutcome;
import io.plurima.kafka.metrics.BackpressureEvent;
import io.plurima.kafka.metrics.PlurimaMetrics;
import io.plurima.kafka.metrics.ProcessResult;

import java.time.Duration;
import java.util.Objects;
import java.util.function.IntSupplier;
import java.util.function.LongSupplier;

/**
 * Per-endpoint wrapper around the SHARED {@link PlurimaMetrics} bean that forwards every
 * metric call but suppresses {@link #close()}.
 *
 * <p><b>Why.</b> The starter injects ONE {@code PlurimaMetrics} bean (typically the
 * Micrometer adapter) into every {@code @PlurimaListener} consumer, and each consumer
 * runtime's cleanup path invokes {@code metrics.close()} exactly once <em>per consumer</em>.
 * The Micrometer adapter's close() deregisters EVERY gauge it ever registered — across all
 * consumers — so the first consumer to close (or fail fatally and self-close) would remove
 * all other live consumers' gauges and clear the adapter's tracking lists, turning the
 * survivors' own later close into a no-op. Wrapping the shared bean per endpoint makes each
 * runtime's close() a deliberate no-op here instead: the shared bean's lifecycle belongs to
 * Spring (and the {@code MeterRegistry}'s own lifecycle), not to any single consumer.
 * Gauges for stopped endpoints simply remain registered, reading their final values, until
 * the registry itself is closed.
 *
 * <p>User-supplied {@code PlurimaMetrics} beans get the same treatment — consistent with
 * the {@link PlurimaMetrics#close()} contract, which allows shared implementations to
 * no-op close().
 */
@Internal
final class CloseSuppressingPlurimaMetrics implements PlurimaMetrics {

    private final PlurimaMetrics delegate;

    CloseSuppressingPlurimaMetrics(PlurimaMetrics delegate) {
        this.delegate = Objects.requireNonNull(delegate, "delegate");
    }

    @Override
    public void recordsPolled(String topic, int count) {
        delegate.recordsPolled(topic, count);
    }

    @Override
    public void recordsProcessed(String topic, ProcessResult result) {
        delegate.recordsProcessed(topic, result);
    }

    @Override
    public void recordsFailed(String topic, String exceptionClass) {
        delegate.recordsFailed(topic, exceptionClass);
    }

    @Override
    public void retryAttempt(String topic, int attempt) {
        delegate.retryAttempt(topic, attempt);
    }

    @Override
    public void dltRouted(String topic, String dltTopic) {
        delegate.dltRouted(topic, dltTopic);
    }

    @Override
    public void dltFailed(String topic, String cause) {
        delegate.dltFailed(topic, cause);
    }

    @Override
    public void ackCommitFailed(String topic, String exceptionClass) {
        delegate.ackCommitFailed(topic, exceptionClass);
    }

    @Override
    public void recordsPoisonPill(String topic, String cause) {
        delegate.recordsPoisonPill(topic, cause);
    }

    @Override
    public void registerInFlightGauge(String topic, String groupId, String clientId,
                                      IntSupplier currentCount) {
        delegate.registerInFlightGauge(topic, groupId, clientId, currentCount);
    }

    @Override
    public void registerBarrierTimeoutGauge(String topic, String groupId, String clientId,
                                            LongSupplier currentMillis) {
        delegate.registerBarrierTimeoutGauge(topic, groupId, clientId, currentMillis);
    }

    @Override
    public void backpressureEvent(String topic, BackpressureEvent event) {
        delegate.backpressureEvent(topic, event);
    }

    @Override
    public void recordProcessDuration(String topic, Duration duration) {
        delegate.recordProcessDuration(topic, duration);
    }

    @Override
    public void recordPollDuration(String topic, String groupId, Duration duration) {
        delegate.recordPollDuration(topic, groupId, duration);
    }

    @Override
    public void ackQueued(AckOutcome type) {
        delegate.ackQueued(type);
    }

    @Override
    public void ackCommitted(String topic, AckOutcome type) {
        delegate.ackCommitted(topic, type);
    }

    /**
     * Deliberate no-op — one consumer's close must never tear down the SHARED adapter's
     * meters (see the class javadoc). The shared bean's lifecycle belongs to Spring;
     * gauges for stopped endpoints remain registered, reading their final values.
     */
    @Override
    public void close() {
        // Intentionally empty.
    }
}
