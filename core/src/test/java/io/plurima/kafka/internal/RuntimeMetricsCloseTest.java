package io.plurima.kafka.internal;

import io.plurima.kafka.OrderingMode;
import io.plurima.kafka.PlurimaConsumer;
import io.plurima.kafka.deserializer.RecordDeserializer;
import io.plurima.kafka.metrics.PlurimaMetrics;
import io.plurima.kafka.retry.RetryPolicy;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.time.Duration;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * Task C1 — verifies {@link PlurimaMetrics#close()}'s "called exactly once from
 * {@code PlurimaConsumer.close()}" contract at the runtime level, INCLUDING the
 * fatal-failure self-close path (B6's {@code handleFatal}) — not just the plain
 * user-initiated {@link ConsumerRuntime#close()} path.
 *
 * <p>{@code ClassicBasicRuntime}/{@code ShareConsumerRuntime}.start() construct a real
 * {@code KafkaConsumer}/{@code KafkaShareConsumer}, so (per {@code ConsumerStateTest}'s
 * own note) they aren't unit-testable end-to-end without a broker. But {@code close()}
 * and the private {@code handleFatal(Throwable)} hook are both safe to invoke on a
 * NEVER-STARTED runtime: every field {@code doClose()} touches (pollLoop, pollThread,
 * dltRouter, workerLauncher, timeoutScheduler) is null-checked, and {@code metrics} is
 * always set via the constructor — so this test exercises the exactly-once guarantee
 * directly, without a broker, by reflectively invoking the private {@code handleFatal}
 * hook the poll thread would otherwise invoke on a fatal error.
 */
class RuntimeMetricsCloseTest {

    @Test
    void classicRuntimeClosesMetricsExactlyOnceOnUserClose() {
        PlurimaMetrics metrics = mock(PlurimaMetrics.class);
        ClassicBasicRuntime<byte[], byte[]> runtime = newClassicRuntime(metrics);

        runtime.close();
        runtime.close(); // redundant close — must stay a no-op (idempotency guard)

        verify(metrics, times(1)).close();
        // G8 — the CAS guarding doClose() must make the SECOND close() a silent no-op:
        // metrics.close() fired once (above) and the state must stay CLOSED, not be
        // re-driven through the close sequence again (which could, e.g., re-invoke
        // consumer/producer shutdown on already-null fields, or flip state elsewhere).
        assertThat(runtime.state()).isEqualTo(PlurimaConsumer.State.CLOSED);
    }

    @Test
    void classicRuntimeClosesMetricsExactlyOnceOnFatalPath() throws Exception {
        PlurimaMetrics metrics = mock(PlurimaMetrics.class);
        ClassicBasicRuntime<byte[], byte[]> runtime = newClassicRuntime(metrics);

        invokeHandleFatal(runtime, new RuntimeException("boom"));
        runtime.close(); // user close racing/following the fatal self-close — must no-op

        verify(metrics, times(1)).close();
        assertThat(runtime.state()).isEqualTo(PlurimaConsumer.State.FAILED);
    }

    @Test
    void shareRuntimeClosesMetricsExactlyOnceOnUserClose() {
        PlurimaMetrics metrics = mock(PlurimaMetrics.class);
        ShareConsumerRuntime<byte[], byte[]> runtime = newShareRuntime(metrics);

        runtime.close();
        runtime.close();

        verify(metrics, times(1)).close();
        // G8 — SHARE-engine analogue of the CLASSIC assertion above.
        assertThat(runtime.state()).isEqualTo(PlurimaConsumer.State.CLOSED);
    }

    @Test
    void shareRuntimeClosesMetricsExactlyOnceOnFatalPath() throws Exception {
        PlurimaMetrics metrics = mock(PlurimaMetrics.class);
        ShareConsumerRuntime<byte[], byte[]> runtime = newShareRuntime(metrics);

        invokeHandleFatal(runtime, new RuntimeException("boom"));
        runtime.close();

        verify(metrics, times(1)).close();
        assertThat(runtime.state()).isEqualTo(PlurimaConsumer.State.FAILED);
    }

    /**
     * F6 — start() rollback must invoke the metrics close hook. {@code start()} registers
     * the {@code in_flight} gauge and can then throw (subscribe / thread start / config
     * validation); the rollback catch previously closed consumer/launcher/router but never
     * closed the metrics — leaking the gauge (bound to a dead runtime forever if the user
     * retries with a fixed {@code client.id}). Empty {@code Properties} make the underlying
     * consumer constructor throw deterministically without a broker; the metrics close must
     * fire exactly once (same {@code closed} CAS as close()/handleFatal, so a subsequent
     * user close() is a no-op) and the state must land on FAILED — not CLOSED, and not a
     * stale NEW that pretends start() never happened.
     */
    @Test
    void classicRuntimeClosesMetricsExactlyOnceWhenStartThrows() {
        PlurimaMetrics metrics = mock(PlurimaMetrics.class);
        ClassicBasicRuntime<byte[], byte[]> runtime = newClassicRuntime(metrics);

        org.assertj.core.api.Assertions.assertThatThrownBy(runtime::start)
            .isInstanceOf(RuntimeException.class);
        runtime.close(); // user close after a failed start — must not double-invoke

        verify(metrics, times(1)).close();
        assertThat(runtime.state()).isEqualTo(PlurimaConsumer.State.FAILED);
    }

    /** F6 — SHARE-engine analogue of {@link #classicRuntimeClosesMetricsExactlyOnceWhenStartThrows}. */
    @Test
    void shareRuntimeClosesMetricsExactlyOnceWhenStartThrows() {
        PlurimaMetrics metrics = mock(PlurimaMetrics.class);
        ShareConsumerRuntime<byte[], byte[]> runtime = newShareRuntime(metrics);

        org.assertj.core.api.Assertions.assertThatThrownBy(runtime::start)
            .isInstanceOf(RuntimeException.class);
        runtime.close();

        verify(metrics, times(1)).close();
        assertThat(runtime.state()).isEqualTo(PlurimaConsumer.State.FAILED);
    }

    private static ClassicBasicRuntime<byte[], byte[]> newClassicRuntime(PlurimaMetrics metrics) {
        return new ClassicBasicRuntime<>(
            new Properties(),
            "t",
            (r, ctx) -> {},
            RecordDeserializer.bytes(),
            RecordDeserializer.bytes(),
            OrderingMode.UNORDERED,
            /* concurrency */ 4,
            /* shardCount */ 4,
            RetryPolicy.noRetry(),
            /* dltConfig */ null,
            Duration.ofSeconds(1),
            Duration.ofSeconds(1),
            metrics,
            t -> {});
    }

    private static ShareConsumerRuntime<byte[], byte[]> newShareRuntime(PlurimaMetrics metrics) {
        return new ShareConsumerRuntime<>(
            new Properties(),
            "t",
            (r, ctx) -> {},
            /* manualAckListener */ null,
            RecordDeserializer.bytes(),
            RecordDeserializer.bytes(),
            OrderingMode.UNORDERED,
            /* concurrency */ 4,
            /* shardCount */ 4,
            RetryPolicy.noRetry(),
            /* dltConfig */ null,
            Duration.ofSeconds(1),
            Duration.ofSeconds(1),
            Duration.ofSeconds(1),
            metrics,
            /* adaptiveBarrierConfig */ null,
            /* lockDurationExplicitlySet */ true,
            /* handlerTimeout */ null,
            t -> {});
    }

    /** Reflectively fires the private fatal-error hook the poll thread invokes on B6's fatal path. */
    private static void invokeHandleFatal(Object runtime, Throwable t) throws Exception {
        Method m = runtime.getClass().getDeclaredMethod("handleFatal", Throwable.class);
        m.setAccessible(true);
        m.invoke(runtime, t);
    }
}
