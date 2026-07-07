package io.plurima.kafka.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.plurima.kafka.annotation.Stable;

import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.IntSupplier;
import java.util.function.LongSupplier;

/**
 * Micrometer adapter for {@link PlurimaMetrics}. Increments counters in the supplied
 * {@link MeterRegistry} using the metric names and tag conventions committed in
 * design § 13.7.
 *
 * <p><b>Counters and timers</b> are cached in local {@link ConcurrentHashMap}s keyed by
 * metric name + tag values so the hot record-processing path never repeats a
 * {@code registry.counter(...)} lookup — or a full {@code Timer.builder(...)} build +
 * registry lookup — for the same series.
 *
 * <p><b>Gauges</b> are registered with the state-object overload of
 * {@link Gauge.Builder} and {@code strongReference(true)} so the registry itself holds
 * the supplier strongly; this instance additionally keeps its own strong-reference list
 * as a defense-in-depth measure against the supplier being GC'd before the registry
 * scrapes it. Every registered {@link Meter.Id} is tracked so {@link #close()} can
 * deregister them (idempotently) when the owning consumer shuts down.
 */
@Stable(since = "0.1.0")
public final class MicrometerPlurimaMetrics implements PlurimaMetrics {

    private final MeterRegistry registry;
    private final boolean publishPercentileHistograms;

    private final ConcurrentHashMap<CounterKey, Counter> counterCache = new ConcurrentHashMap<>();

    /**
     * Timer cache — same keyed-map pattern as {@link #counterCache}, for the same reason:
     * {@code recordProcessDuration}/{@code recordPollDuration} run once per record/poll,
     * and rebuilding {@code Timer.builder(...).tag(...).register(registry)} on every call
     * repeats the builder allocation and registry lookup for a series that never changes.
     * The {@code publishPercentileHistograms} flag is constant for the lifetime of this
     * adapter (set once in the constructor), so it is deliberately NOT part of the key.
     */
    private final ConcurrentHashMap<TimerKey, Timer> timerCache = new ConcurrentHashMap<>();

    /**
     * Strong references to gauge state objects (suppliers), kept alongside the
     * registry's own {@code strongReference(true)} hold so a registered gauge's
     * backing supplier can never be collected while this adapter is alive.
     */
    private final List<Object> gaugeStateRefs = new CopyOnWriteArrayList<>();

    /** Every {@link Meter.Id} registered by this adapter's gauge methods, for {@link #close()}. */
    private final List<Meter.Id> registeredGaugeIds = new CopyOnWriteArrayList<>();

    public MicrometerPlurimaMetrics(MeterRegistry registry) {
        this(registry, false);
    }

    /**
     * @param publishPercentileHistograms when {@code true}, the {@code process.duration}
     *                                    and {@code poll.duration} timers publish
     *                                    percentile histograms (for backends like
     *                                    Prometheus that compute quantiles server-side).
     *                                    Defaults to {@code false} via the single-arg
     *                                    constructor to keep memory overhead opt-in.
     */
    public MicrometerPlurimaMetrics(MeterRegistry registry, boolean publishPercentileHistograms) {
        this.registry = Objects.requireNonNull(registry, "registry");
        this.publishPercentileHistograms = publishPercentileHistograms;
    }

    @Override
    public void recordsPolled(String topic, int count) {
        counter("plurima.consumer.records.polled", "topic", topic).increment(count);
    }

    @Override
    public void recordsProcessed(String topic, ProcessResult result) {
        counter("plurima.consumer.records.processed",
            "topic", topic, "result", result.toString()).increment();
    }

    @Override
    public void recordsFailed(String topic, String exceptionClass) {
        counter("plurima.consumer.records.failed",
            "topic", topic, "exception_class", exceptionClass).increment();
    }

    @Override
    public void retryAttempt(String topic, int attempt) {
        counter("plurima.consumer.retry.attempts",
            "topic", topic, "attempt", Integer.toString(attempt)).increment();
    }

    @Override
    public void dltRouted(String topic, String dltTopic) {
        counter("plurima.consumer.dlt.routed",
            "topic", topic, "dlt_topic", dltTopic).increment();
    }

    @Override
    public void dltFailed(String topic, String cause) {
        counter("plurima.consumer.dlt.failures",
            "topic", topic, "cause", cause).increment();
    }

    @Override
    public void ackCommitFailed(String topic, String exceptionClass) {
        counter("plurima.consumer.ack.commit_failed",
            "topic", topic, "exception_class", exceptionClass).increment();
    }

    @Override
    public void recordsPoisonPill(String topic, String cause) {
        counter("plurima.consumer.records.poison_pill",
            "topic", topic, "cause", cause).increment();
    }

    @Override
    public void registerInFlightGauge(String topic, String groupId, String clientId, IntSupplier currentCount) {
        Gauge gauge = Gauge.builder("plurima.consumer.records.in_flight", currentCount,
                supplier -> (double) supplier.getAsInt())
            .strongReference(true)
            .tag("topic", topic)
            .tag("group_id", groupId)
            .tag("client_id", clientId)
            .register(registry);
        gaugeStateRefs.add(currentCount);
        registeredGaugeIds.add(gauge.getId());
    }

    @Override
    public void registerBarrierTimeoutGauge(String topic, String groupId, String clientId,
                                            LongSupplier currentMillis) {
        Gauge gauge = Gauge.builder("plurima.consumer.barrier.timeout", currentMillis,
                supplier -> (double) supplier.getAsLong())
            .strongReference(true)
            .baseUnit("milliseconds")
            .tag("topic", topic)
            .tag("group_id", groupId)
            .tag("client_id", clientId)
            .register(registry);
        gaugeStateRefs.add(currentMillis);
        registeredGaugeIds.add(gauge.getId());
    }

    @Override
    public void recordProcessDuration(String topic, Duration duration) {
        timer("plurima.consumer.process.duration", "topic", topic).record(duration);
    }

    @Override
    public void recordPollDuration(String topic, String groupId, Duration duration) {
        timer("plurima.consumer.poll.duration", "topic", topic, "group_id", groupId)
            .record(duration);
    }

    @Override
    public void ackQueued(AckOutcome type) {
        counter("plurima.consumer.ack.queued", "type", type.toString()).increment();
    }

    @Override
    public void ackCommitted(String topic, AckOutcome type) {
        counter("plurima.consumer.ack.committed",
            "topic", topic, "type", type.toString()).increment();
    }

    @Override
    public void backpressureEvent(String topic, BackpressureEvent event) {
        counter("plurima.consumer.backpressure.events",
            "topic", topic, "event", event.toString()).increment();
    }

    /**
     * Deregisters every gauge this adapter registered ({@code registry.remove(id)} per
     * tracked {@link Meter.Id}) and releases the strong references held on their behalf.
     * Safe to call more than once: the second call iterates an already-cleared list and
     * is a no-op. Counters are intentionally left in the registry — unlike gauges they
     * hold no external supplier reference, and dropping historical counter values on
     * consumer close would lose data a scrape between close() and process exit could
     * still want.
     *
     * <p><b>Sharing caveat:</b> this implementation deregisters EVERY gauge the adapter
     * ever registered, so it assumes a single owning consumer. When one adapter instance
     * is shared across multiple consumers (e.g. as a Spring bean), a per-consumer close
     * here would remove OTHER live consumers' gauges — per the
     * {@link PlurimaMetrics#close()} shared-instance contract, such deployments must
     * suppress per-consumer close (the Plurima Spring starter wraps the shared bean in a
     * close-suppressing delegate for exactly this reason) and tie cleanup to the registry
     * lifecycle instead.
     *
     * <p><b>Concurrency invariant:</b> gauge registration happens synchronously on the
     * consumer's startup path, before the poll thread (and thus before any code that could
     * call {@code close()}) starts, and the SPI contract calls {@code close()} at most once
     * per owning consumer during shutdown. This method is therefore not written to be safe
     * against a concurrent {@code registerInFlightGauge}/{@code registerBarrierTimeoutGauge}
     * call racing with {@code close()}: a racing registration could add a {@link Meter.Id} to
     * {@code registeredGaugeIds} after the iteration below but before {@code clear()}, leaving
     * that gauge registered in {@code registry} but untracked (a leak on a subsequent close).
     * Callers outside this contract (e.g. re-registering gauges concurrently with shutdown)
     * are out of scope.
     */
    @Override
    public void close() {
        for (Meter.Id id : registeredGaugeIds) {
            registry.remove(id);
        }
        registeredGaugeIds.clear();
        gaugeStateRefs.clear();
    }

    private Counter counter(String name, String... tagKeysAndValues) {
        CounterKey key = new CounterKey(name, List.of(tagKeysAndValues));
        return counterCache.computeIfAbsent(key, k -> registry.counter(name, tagKeysAndValues));
    }

    private Timer timer(String name, String... tagKeysAndValues) {
        TimerKey key = new TimerKey(name, List.of(tagKeysAndValues));
        return timerCache.computeIfAbsent(key, k -> Timer.builder(name)
            .tags(tagKeysAndValues)
            .publishPercentileHistogram(publishPercentileHistograms)
            .register(registry));
    }

    /** Cache key for {@link #counterCache}: metric name plus the flattened tag key/value pairs. */
    private record CounterKey(String name, List<String> tagKeysAndValues) {}

    /** Cache key for {@link #timerCache}: metric name plus the flattened tag key/value pairs. */
    private record TimerKey(String name, List<String> tagKeysAndValues) {}
}
