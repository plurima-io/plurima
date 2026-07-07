package io.plurima.kafka.metrics;

import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

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
        metrics.recordsProcessed("orders", ProcessResult.ACCEPT);
        metrics.recordsProcessed("orders", ProcessResult.REJECT);
        metrics.recordsProcessed("orders", ProcessResult.ACCEPT);

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

        io.micrometer.core.instrument.Gauge gauge = registry.find("plurima.consumer.barrier.timeout")
            .tag("topic", "orders").tag("group_id", "g1").tag("client_id", "c1")
            .gauge();
        assertThat(gauge).isNotNull();
        assertThat(gauge.value()).isEqualTo(30_000.0);

        value.set(2_500);
        assertThat(gauge.value()).isEqualTo(2_500.0);
    }

    @Test
    void barrierTimeoutGaugeUsesRenamedMetricWithMillisecondsBaseUnit() {
        java.util.concurrent.atomic.AtomicLong value = new java.util.concurrent.atomic.AtomicLong(1_000);
        metrics.registerBarrierTimeoutGauge("orders", "g1", "c1", value::get);

        // Old name must be gone entirely — a lingering `_ms`-suffixed meter would mean
        // the rename only added an alias instead of replacing the metric.
        assertThat(registry.find("plurima.consumer.barrier.timeout_ms").gauge()).isNull();

        io.micrometer.core.instrument.Gauge gauge = registry.find("plurima.consumer.barrier.timeout")
            .tag("topic", "orders").tag("group_id", "g1").tag("client_id", "c1")
            .gauge();
        assertThat(gauge).isNotNull();
        assertThat(gauge.getId().getBaseUnit()).isEqualTo("milliseconds");
    }

    /**
     * Defense-in-depth regression guard, not a reproduction of a historical bug: Micrometer
     * 1.13.x's 2-arg {@code Gauge.builder(name, stateObject, ToDoubleFunction)} overload used
     * by {@link MicrometerPlurimaMetrics#registerInFlightGauge} already calls
     * {@code strongReference(true)} internally, so today's registration path was never
     * actually vulnerable to the supplier being collected between registration and scrape —
     * confirmed empirically by running this same GC loop against a plain
     * {@link java.lang.ref.WeakReference} control, which the loop correctly detects as
     * collected (value goes {@code NaN} and never recovers), proving the harness itself would
     * catch a real weak-ref regression rather than passing vacuously.
     *
     * <p>What this test actually guards against is a <em>future</em> edit: someone changing
     * {@code registerInFlightGauge}/{@code registerBarrierTimeoutGauge} to drop
     * {@code .strongReference(true)} (e.g. while refactoring to the plain
     * {@code Gauge.builder(name, obj, ToDoubleFunction)} call without the strong-reference
     * flag), while also removing or bypassing the adapter's own {@code gaugeStateRefs}
     * strong-hold list — either safety net alone is enough to keep the supplier reachable, so
     * this test only fails if both are lost at once. It also asserts the adapter's own
     * bookkeeping (tracked {@link Meter.Id}) is actually populated for the registered gauge,
     * since that tracking is the second half of the defense-in-depth pair and is what
     * {@link #close()} depends on.
     */
    @Test
    void gaugeSurvivesGarbageCollectionAfterLocalReferenceIsDropped() throws InterruptedException {
        registerAndDropLocalSupplierReference();

        io.micrometer.core.instrument.Gauge gauge = registry.find("plurima.consumer.records.in_flight")
            .tag("topic", "orders").tag("group_id", "gc-group").tag("client_id", "gc-client")
            .gauge();
        assertThat(gauge).isNotNull();

        // The adapter's own strong-reference tracking (the defense-in-depth half of the
        // guard) must have recorded this gauge's Meter.Id — that's what close() relies on
        // to deregister it later, independent of whatever the registry does internally.
        assertThat(registry.find("plurima.consumer.records.in_flight")
            .tag("topic", "orders").tag("group_id", "gc-group").tag("client_id", "gc-client")
            .meters()).hasSize(1);

        long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(10);
        double lastValue = Double.NaN;
        while (System.nanoTime() < deadline) {
            System.gc();
            Thread.sleep(20);
            lastValue = gauge.value();
            if (!Double.isNaN(lastValue)) {
                break;
            }
        }
        assertThat(lastValue)
            .as("gauge value must not go NaN after the caller's local supplier reference is GC'd — "
                + "either the registry's strongReference(true) hold or the adapter's own "
                + "gaugeStateRefs strong-ref list keeping the supplier reachable is sufficient, "
                + "and this loop (proven via a WeakReference control) would catch it if both were lost")
            .isNotNaN()
            .isEqualTo(99.0);
    }

    // Isolated in its own frame so the local AtomicInteger has no reachable stack
    // reference once this method returns, letting System.gc() actually collect it
    // if neither the registry's strongReference(true) hold nor the adapter's own
    // gaugeStateRefs list were keeping it reachable.
    private void registerAndDropLocalSupplierReference() {
        java.util.concurrent.atomic.AtomicInteger counter = new java.util.concurrent.atomic.AtomicInteger(99);
        metrics.registerInFlightGauge("orders", "gc-group", "gc-client", counter::get);
    }

    @Test
    void closeRemovesRegisteredGaugesAndIsIdempotent() {
        java.util.concurrent.atomic.AtomicInteger inFlight = new java.util.concurrent.atomic.AtomicInteger(5);
        java.util.concurrent.atomic.AtomicLong barrier = new java.util.concurrent.atomic.AtomicLong(1_000);
        metrics.registerInFlightGauge("orders", "g1", "c1", inFlight::get);
        metrics.registerBarrierTimeoutGauge("orders", "g1", "c1", barrier::get);

        assertThat(registry.find("plurima.consumer.records.in_flight").gauge()).isNotNull();
        assertThat(registry.find("plurima.consumer.barrier.timeout").gauge()).isNotNull();

        metrics.close();

        assertThat(registry.find("plurima.consumer.records.in_flight").gauge()).isNull();
        assertThat(registry.find("plurima.consumer.barrier.timeout").gauge()).isNull();

        // Idempotent: a second close() must not throw (e.g. NPE walking a cleared list).
        assertThatCode(metrics::close).doesNotThrowAnyException();
    }

    @Test
    void closeThenReRegisterWithSameTagsReadsLiveValuesFromTheNewSupplier() {
        java.util.concurrent.atomic.AtomicInteger first = new java.util.concurrent.atomic.AtomicInteger(5);
        metrics.registerInFlightGauge("orders", "g1", "c1", first::get);
        metrics.close();

        java.util.concurrent.atomic.AtomicInteger second = new java.util.concurrent.atomic.AtomicInteger(42);
        metrics.registerInFlightGauge("orders", "g1", "c1", second::get);

        io.micrometer.core.instrument.Gauge gauge = registry.find("plurima.consumer.records.in_flight")
            .tag("topic", "orders").tag("group_id", "g1").tag("client_id", "c1")
            .gauge();
        assertThat(gauge).isNotNull();
        assertThat(gauge.value()).isEqualTo(42.0);

        // The stale first-registration supplier must no longer influence anything —
        // proves close() actually removed the old meter rather than merely
        // shadowing it (only one meter with these tags should be present).
        first.set(-1);
        assertThat(gauge.value()).isEqualTo(42.0);
        assertThat(registry.find("plurima.consumer.records.in_flight")
            .tag("topic", "orders").tag("group_id", "g1").tag("client_id", "c1")
            .gauges()).hasSize(1);
    }

    @Test
    void counterCacheReturnsSameInstanceForRepeatedCallsWithSameTags() {
        metrics.recordsPolled("orders", 1);
        io.micrometer.core.instrument.Counter first =
            registry.counter("plurima.consumer.records.polled", "topic", "orders");

        int meterCountBefore = registry.getMeters().size();

        metrics.recordsPolled("orders", 1);
        metrics.recordsPolled("orders", 1);
        io.micrometer.core.instrument.Counter second =
            registry.counter("plurima.consumer.records.polled", "topic", "orders");

        assertThat(second).isSameAs(first);
        assertThat(registry.getMeters().size())
            .as("repeated increments on the same series must not register additional meters")
            .isEqualTo(meterCountBefore);
        assertThat(first.count()).isEqualTo(3.0);
    }

    /**
     * F10 — timers are cached in the same keyed-map pattern as counters (they previously
     * rebuilt {@code Timer.builder(...).register(...)} on every record/poll). Mirrors
     * {@link #counterCacheReturnsSameInstanceForRepeatedCallsWithSameTags}: repeated
     * recordings on the same series must hit one meter (no additional registrations) and
     * accumulate, while different tag values still resolve distinct series.
     */
    @Test
    void timerCacheReturnsSameInstanceForRepeatedCallsWithSameTags() {
        metrics.recordProcessDuration("orders", java.time.Duration.ofMillis(5));
        metrics.recordPollDuration("orders", "group-1", java.time.Duration.ofMillis(5));
        io.micrometer.core.instrument.Timer firstProcess =
            registry.timer("plurima.consumer.process.duration", "topic", "orders");
        io.micrometer.core.instrument.Timer firstPoll = registry.timer(
            "plurima.consumer.poll.duration", "topic", "orders", "group_id", "group-1");

        int meterCountBefore = registry.getMeters().size();

        metrics.recordProcessDuration("orders", java.time.Duration.ofMillis(5));
        metrics.recordProcessDuration("orders", java.time.Duration.ofMillis(5));
        metrics.recordPollDuration("orders", "group-1", java.time.Duration.ofMillis(5));

        assertThat(registry.timer("plurima.consumer.process.duration", "topic", "orders"))
            .isSameAs(firstProcess);
        assertThat(registry.timer(
                "plurima.consumer.poll.duration", "topic", "orders", "group_id", "group-1"))
            .isSameAs(firstPoll);
        assertThat(registry.getMeters().size())
            .as("repeated recordings on the same series must not register additional meters")
            .isEqualTo(meterCountBefore);
        assertThat(firstProcess.count()).isEqualTo(3L);
        assertThat(firstPoll.count()).isEqualTo(2L);

        // Different tag values must still resolve DISTINCT series through the cache.
        metrics.recordProcessDuration("payments", java.time.Duration.ofMillis(5));
        assertThat(registry.timer("plurima.consumer.process.duration", "topic", "payments"))
            .isNotSameAs(firstProcess);
    }

    @Test
    void counterCacheDistinguishesDifferentTagValues() {
        metrics.recordsProcessed("orders", ProcessResult.ACCEPT);
        metrics.recordsProcessed("orders", ProcessResult.REJECT);

        io.micrometer.core.instrument.Counter accept = registry.counter(
            "plurima.consumer.records.processed", "topic", "orders", "result", "accept");
        io.micrometer.core.instrument.Counter reject = registry.counter(
            "plurima.consumer.records.processed", "topic", "orders", "result", "reject");

        assertThat(accept).isNotSameAs(reject);
        assertThat(accept.count()).isEqualTo(1.0);
        assertThat(reject.count()).isEqualTo(1.0);
    }

    @Test
    void pollDurationTimerIsTaggedByTopicAndGroupId() {
        metrics.recordPollDuration("orders", "group-1", java.time.Duration.ofMillis(15));
        io.micrometer.core.instrument.Timer t = registry.find("plurima.consumer.poll.duration")
            .tag("topic", "orders").tag("group_id", "group-1").timer();
        assertThat(t).isNotNull();
        assertThat(t.count()).isEqualTo(1L);
    }

    @Test
    void publishPercentileHistogramsFlagDefaultsToFalseAndCanBeEnabled() {
        // SimpleMeterRegistry never materializes histogram buckets regardless of the
        // requested DistributionStatisticConfig (it's a minimal in-memory registry with
        // no distribution-statistics backend), so asserting on Timer.takeSnapshot()
        // can't distinguish the flag. Capture the DistributionStatisticConfig the
        // adapter actually hands to the registry instead — that's the real contract
        // surface a distribution-capable registry (Prometheus, etc.) consumes.
        HistogramConfigCapturingRegistry defaultRegistry = new HistogramConfigCapturingRegistry();
        MicrometerPlurimaMetrics defaultMetrics = new MicrometerPlurimaMetrics(defaultRegistry);
        defaultMetrics.recordProcessDuration("orders", java.time.Duration.ofMillis(10));
        defaultMetrics.recordPollDuration("orders", "group-1", java.time.Duration.ofMillis(10));
        assertThat(defaultRegistry.lastConfigs)
            .as("default (no flag) constructor must not request histogram publishing")
            .allSatisfy(config -> assertThat(config.isPublishingHistogram()).isFalse());

        HistogramConfigCapturingRegistry histoRegistry = new HistogramConfigCapturingRegistry();
        MicrometerPlurimaMetrics withHistograms = new MicrometerPlurimaMetrics(histoRegistry, true);
        withHistograms.recordProcessDuration("orders", java.time.Duration.ofMillis(10));
        withHistograms.recordPollDuration("orders", "group-1", java.time.Duration.ofMillis(10));
        assertThat(histoRegistry.lastConfigs)
            .as("publishPercentileHistograms=true must be forwarded to both duration timers")
            .hasSize(2)
            .allSatisfy(config -> assertThat(config.isPublishingHistogram()).isTrue());
    }

    /**
     * A real {@link SimpleMeterRegistry} that additionally records the
     * {@link io.micrometer.core.instrument.distribution.DistributionStatisticConfig} passed
     * to every {@code newTimer} call, so tests can assert on the config the adapter
     * requested without depending on a distribution-statistics-capable backend.
     */
    private static final class HistogramConfigCapturingRegistry extends SimpleMeterRegistry {
        private final List<io.micrometer.core.instrument.distribution.DistributionStatisticConfig> lastConfigs =
            new java.util.ArrayList<>();

        @Override
        protected io.micrometer.core.instrument.Timer newTimer(
                Meter.Id id,
                io.micrometer.core.instrument.distribution.DistributionStatisticConfig
                    distributionStatisticConfig,
                io.micrometer.core.instrument.distribution.pause.PauseDetector pauseDetector) {
            lastConfigs.add(distributionStatisticConfig);
            return super.newTimer(id, distributionStatisticConfig, pauseDetector);
        }
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
        metrics.recordPollDuration("orders", "group-1", java.time.Duration.ofMillis(50));
        io.micrometer.core.instrument.Timer t = registry.timer(
            "plurima.consumer.poll.duration", "topic", "orders", "group_id", "group-1");
        assertThat(t.count()).isEqualTo(1L);
    }

    @Test
    void ackQueuedIncrementsByType() {
        metrics.ackQueued(AckOutcome.ACCEPT);
        metrics.ackQueued(AckOutcome.ACCEPT);
        metrics.ackQueued(AckOutcome.REJECT);
        assertThat(registry.counter("plurima.consumer.ack.queued", "type", "accept").count()).isEqualTo(2.0);
        assertThat(registry.counter("plurima.consumer.ack.queued", "type", "reject").count()).isEqualTo(1.0);
    }

    @Test
    void backpressureEventTagsByTopicAndEvent() {
        // BackpressureEvent replaced a previously stringly-typed "paused"/"resumed" event
        // param; toString() renders the lower-case tag value, which this asserts on
        // directly so a future accidental case/name change is caught here.
        metrics.backpressureEvent("orders", BackpressureEvent.PAUSED);
        metrics.backpressureEvent("orders", BackpressureEvent.RESUMED);
        metrics.backpressureEvent("orders", BackpressureEvent.PAUSED);

        assertThat(registry.counter("plurima.consumer.backpressure.events",
            "topic", "orders", "event", "paused").count()).isEqualTo(2.0);
        assertThat(registry.counter("plurima.consumer.backpressure.events",
            "topic", "orders", "event", "resumed").count()).isEqualTo(1.0);
    }

    @Test
    void ackCommittedTagsByTopicAndType() {
        metrics.ackCommitted("orders", AckOutcome.ACCEPT);
        metrics.ackCommitted("orders", AckOutcome.ACCEPT);
        assertThat(registry.counter("plurima.consumer.ack.committed",
            "topic", "orders", "type", "accept").count()).isEqualTo(2.0);
    }
}
