package io.plurima.kafka.spring.internal;

import io.plurima.kafka.metrics.AckOutcome;
import io.plurima.kafka.metrics.BackpressureEvent;
import io.plurima.kafka.metrics.PlurimaMetrics;
import io.plurima.kafka.metrics.ProcessResult;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.IntSupplier;
import java.util.function.LongSupplier;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * G5 — {@link CloseSuppressingPlurimaMetrics} forwards every {@link PlurimaMetrics}
 * method to its delegate EXCEPT {@link PlurimaMetrics#close()}, which it deliberately
 * suppresses (see the class javadoc: the shared bean's lifecycle belongs to Spring, not
 * to any one consumer). Before this test, 14 of the 16 interface methods were
 * unexercised — a missed forward would silently no-op that metric for every endpoint
 * sharing the bean.
 *
 * <p>No mocking library is on this module's test classpath (spring-boot-starter has no
 * Mockito dependency), so delegation is verified with a hand-rolled recording fake
 * ({@link RecordingMetrics}) rather than {@code Mockito.verify}.
 *
 * <p>{@link #everyNonStaticInterfaceMethodIsExplicitlyCoveredByThisTest} guards against a
 * future {@code PlurimaMetrics} SPI addition slipping through unforwarded: it reflects
 * over the interface's declared instance methods and fails loudly if one isn't in the
 * hand-maintained {@link #COVERED_METHOD_NAMES} set below.
 */
class CloseSuppressingPlurimaMetricsTest {

    /** Every {@code PlurimaMetrics} instance method this test class exercises. */
    private static final Set<String> COVERED_METHOD_NAMES = Set.of(
        "recordsPolled", "recordsProcessed", "recordsFailed", "retryAttempt",
        "dltRouted", "dltFailed", "ackCommitFailed", "recordsPoisonPill",
        "registerInFlightGauge", "registerBarrierTimeoutGauge", "backpressureEvent",
        "recordProcessDuration", "recordPollDuration", "ackQueued", "ackCommitted",
        "close");

    @Test
    void everyNonStaticInterfaceMethodIsExplicitlyCoveredByThisTest() {
        Set<String> declared = new HashSet<>();
        for (Method m : PlurimaMetrics.class.getDeclaredMethods()) {
            if (!Modifier.isStatic(m.getModifiers())) {
                declared.add(m.getName());
            }
        }
        assertThat(COVERED_METHOD_NAMES)
            .as("PlurimaMetrics gained or lost a method that CloseSuppressingPlurimaMetrics "
                + "must forward (or deliberately suppress, like close()) — update this "
                + "test's COVERED_METHOD_NAMES and add/adjust the matching delegation "
                + "assertion below for the new method")
            .isEqualTo(declared);
    }

    @Test
    void recordsPolledForwards() {
        RecordingMetrics delegate = new RecordingMetrics();
        PlurimaMetrics wrapper = new CloseSuppressingPlurimaMetrics(delegate);

        wrapper.recordsPolled("t", 5);

        assertThat(delegate.calls).containsExactly("recordsPolled(t, 5)");
    }

    @Test
    void recordsProcessedForwards() {
        RecordingMetrics delegate = new RecordingMetrics();
        PlurimaMetrics wrapper = new CloseSuppressingPlurimaMetrics(delegate);

        wrapper.recordsProcessed("t", ProcessResult.ACCEPT);

        assertThat(delegate.calls).containsExactly("recordsProcessed(t, accept)");
    }

    @Test
    void recordsFailedForwards() {
        RecordingMetrics delegate = new RecordingMetrics();
        PlurimaMetrics wrapper = new CloseSuppressingPlurimaMetrics(delegate);

        wrapper.recordsFailed("t", "java.lang.RuntimeException");

        assertThat(delegate.calls).containsExactly("recordsFailed(t, java.lang.RuntimeException)");
    }

    @Test
    void retryAttemptForwards() {
        RecordingMetrics delegate = new RecordingMetrics();
        PlurimaMetrics wrapper = new CloseSuppressingPlurimaMetrics(delegate);

        wrapper.retryAttempt("t", 3);

        assertThat(delegate.calls).containsExactly("retryAttempt(t, 3)");
    }

    @Test
    void dltRoutedForwards() {
        RecordingMetrics delegate = new RecordingMetrics();
        PlurimaMetrics wrapper = new CloseSuppressingPlurimaMetrics(delegate);

        wrapper.dltRouted("t", "t-dlt");

        assertThat(delegate.calls).containsExactly("dltRouted(t, t-dlt)");
    }

    @Test
    void dltFailedForwards() {
        RecordingMetrics delegate = new RecordingMetrics();
        PlurimaMetrics wrapper = new CloseSuppressingPlurimaMetrics(delegate);

        wrapper.dltFailed("t", "boom");

        assertThat(delegate.calls).containsExactly("dltFailed(t, boom)");
    }

    @Test
    void ackCommitFailedForwards() {
        RecordingMetrics delegate = new RecordingMetrics();
        PlurimaMetrics wrapper = new CloseSuppressingPlurimaMetrics(delegate);

        wrapper.ackCommitFailed("t", "java.lang.RuntimeException");

        assertThat(delegate.calls).containsExactly("ackCommitFailed(t, java.lang.RuntimeException)");
    }

    @Test
    void recordsPoisonPillForwards() {
        RecordingMetrics delegate = new RecordingMetrics();
        PlurimaMetrics wrapper = new CloseSuppressingPlurimaMetrics(delegate);

        wrapper.recordsPoisonPill("t", "bad payload");

        assertThat(delegate.calls).containsExactly("recordsPoisonPill(t, bad payload)");
    }

    @Test
    void registerInFlightGaugeForwardsWithTheExactSupplierInstance() {
        RecordingMetrics delegate = new RecordingMetrics();
        PlurimaMetrics wrapper = new CloseSuppressingPlurimaMetrics(delegate);
        IntSupplier supplier = () -> 42;

        wrapper.registerInFlightGauge("t", "g", "c", supplier);

        assertThat(delegate.calls).containsExactly("registerInFlightGauge(t, g, c)");
        assertThat(delegate.lastIntSupplier)
            .as("the delegate must receive the SAME supplier instance, not a copy/wrapper")
            .isSameAs(supplier);
        assertThat(delegate.lastIntSupplier.getAsInt()).isEqualTo(42);
    }

    @Test
    void registerBarrierTimeoutGaugeForwardsWithTheExactSupplierInstance() {
        RecordingMetrics delegate = new RecordingMetrics();
        PlurimaMetrics wrapper = new CloseSuppressingPlurimaMetrics(delegate);
        LongSupplier supplier = () -> 99L;

        wrapper.registerBarrierTimeoutGauge("t", "g", "c", supplier);

        assertThat(delegate.calls).containsExactly("registerBarrierTimeoutGauge(t, g, c)");
        assertThat(delegate.lastLongSupplier)
            .as("the delegate must receive the SAME supplier instance, not a copy/wrapper")
            .isSameAs(supplier);
        assertThat(delegate.lastLongSupplier.getAsLong()).isEqualTo(99L);
    }

    @Test
    void backpressureEventForwards() {
        RecordingMetrics delegate = new RecordingMetrics();
        PlurimaMetrics wrapper = new CloseSuppressingPlurimaMetrics(delegate);

        wrapper.backpressureEvent("t", BackpressureEvent.PAUSED);

        assertThat(delegate.calls).containsExactly("backpressureEvent(t, paused)");
    }

    @Test
    void recordProcessDurationForwards() {
        RecordingMetrics delegate = new RecordingMetrics();
        PlurimaMetrics wrapper = new CloseSuppressingPlurimaMetrics(delegate);
        Duration d = Duration.ofMillis(123);

        wrapper.recordProcessDuration("t", d);

        assertThat(delegate.calls).containsExactly("recordProcessDuration(t, " + d + ")");
    }

    @Test
    void recordPollDurationForwards() {
        RecordingMetrics delegate = new RecordingMetrics();
        PlurimaMetrics wrapper = new CloseSuppressingPlurimaMetrics(delegate);
        Duration d = Duration.ofMillis(456);

        wrapper.recordPollDuration("t", "g", d);

        assertThat(delegate.calls).containsExactly("recordPollDuration(t, g, " + d + ")");
    }

    @Test
    void ackQueuedForwards() {
        RecordingMetrics delegate = new RecordingMetrics();
        PlurimaMetrics wrapper = new CloseSuppressingPlurimaMetrics(delegate);

        wrapper.ackQueued(AckOutcome.RELEASE);

        assertThat(delegate.calls).containsExactly("ackQueued(release)");
    }

    @Test
    void ackCommittedForwards() {
        RecordingMetrics delegate = new RecordingMetrics();
        PlurimaMetrics wrapper = new CloseSuppressingPlurimaMetrics(delegate);

        wrapper.ackCommitted("t", AckOutcome.REJECT);

        assertThat(delegate.calls).containsExactly("ackCommitted(t, reject)");
    }

    @Test
    void closeDoesNotForwardToDelegate() {
        RecordingMetrics delegate = new RecordingMetrics();
        PlurimaMetrics wrapper = new CloseSuppressingPlurimaMetrics(delegate);

        wrapper.close();

        assertThat(delegate.calls)
            .as("close() must be suppressed — the shared bean's lifecycle belongs to Spring")
            .isEmpty();
    }

    /** Records every non-close call it receives, in order, as a simple description string. */
    private static final class RecordingMetrics implements PlurimaMetrics {
        final List<String> calls = new ArrayList<>();
        IntSupplier lastIntSupplier;
        LongSupplier lastLongSupplier;

        @Override public void recordsPolled(String topic, int count) {
            calls.add("recordsPolled(" + topic + ", " + count + ")");
        }

        @Override public void recordsProcessed(String topic, ProcessResult result) {
            calls.add("recordsProcessed(" + topic + ", " + result + ")");
        }

        @Override public void recordsFailed(String topic, String exceptionClass) {
            calls.add("recordsFailed(" + topic + ", " + exceptionClass + ")");
        }

        @Override public void retryAttempt(String topic, int attempt) {
            calls.add("retryAttempt(" + topic + ", " + attempt + ")");
        }

        @Override public void dltRouted(String topic, String dltTopic) {
            calls.add("dltRouted(" + topic + ", " + dltTopic + ")");
        }

        @Override public void dltFailed(String topic, String cause) {
            calls.add("dltFailed(" + topic + ", " + cause + ")");
        }

        @Override public void ackCommitFailed(String topic, String exceptionClass) {
            calls.add("ackCommitFailed(" + topic + ", " + exceptionClass + ")");
        }

        @Override public void recordsPoisonPill(String topic, String cause) {
            calls.add("recordsPoisonPill(" + topic + ", " + cause + ")");
        }

        @Override public void registerInFlightGauge(
            String topic, String groupId, String clientId, IntSupplier currentCount) {
            calls.add("registerInFlightGauge(" + topic + ", " + groupId + ", " + clientId + ")");
            this.lastIntSupplier = currentCount;
        }

        @Override public void registerBarrierTimeoutGauge(
            String topic, String groupId, String clientId, LongSupplier currentMillis) {
            calls.add("registerBarrierTimeoutGauge(" + topic + ", " + groupId + ", " + clientId + ")");
            this.lastLongSupplier = currentMillis;
        }

        @Override public void backpressureEvent(String topic, BackpressureEvent event) {
            calls.add("backpressureEvent(" + topic + ", " + event + ")");
        }

        @Override public void recordProcessDuration(String topic, Duration duration) {
            calls.add("recordProcessDuration(" + topic + ", " + duration + ")");
        }

        @Override public void recordPollDuration(String topic, String groupId, Duration duration) {
            calls.add("recordPollDuration(" + topic + ", " + groupId + ", " + duration + ")");
        }

        @Override public void ackQueued(AckOutcome type) {
            calls.add("ackQueued(" + type + ")");
        }

        @Override public void ackCommitted(String topic, AckOutcome type) {
            calls.add("ackCommitted(" + topic + ", " + type + ")");
        }

        @Override public void close() {
            calls.add("close()");
        }
    }
}
