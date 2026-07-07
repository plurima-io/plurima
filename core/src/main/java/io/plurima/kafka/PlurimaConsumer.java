package io.plurima.kafka;

import io.plurima.kafka.ack.ManualAckListener;
import io.plurima.kafka.annotation.Stable;
import io.plurima.kafka.deserializer.RecordDeserializer;
import io.plurima.kafka.dlt.DltConfig;
import io.plurima.kafka.internal.ClassicBasicRuntime;
import io.plurima.kafka.internal.ConsumerRuntime;
import io.plurima.kafka.internal.ShareConsumerRuntime;
import io.plurima.kafka.metrics.PlurimaMetrics;
import io.plurima.kafka.retry.RetryPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/**
 * Entry point for the Plurima Kafka consumer abstraction. {@code PlurimaConsumer} is a
 * thin lifecycle facade; the actual consumer pipeline is provided by an internal,
 * engine-specific runtime implementation chosen by the builder based on
 * {@link ConsumerEngine}:
 *
 * <ul>
 *   <li>{@link ConsumerEngine#SHARE SHARE} (default) — KIP-932 share-consumer pipeline.
 *       Supports {@link OrderingMode#UNORDERED} only. KEY and PARTITION are rejected
 *       at build time: share groups can hand same-key or same-partition records to any
 *       member, so an in-process shard mechanism cannot promise cross-cluster ordering.</li>
 *   <li>{@link ConsumerEngine#CLASSIC_BASIC CLASSIC_BASIC} — vanilla {@code KafkaConsumer}
 *       pipeline. Continuous-poll model with mode-tailored dispatch: one worker per
 *       record for UNORDERED, intra-partition key-shard parallelism for KEY, per-partition
 *       serial dispatch for PARTITION. Provides cross-cluster STRICT ordering for KEY and
 *       PARTITION because classic consumer-group assignment owns each partition exclusively
 *       at any moment.</li>
 * </ul>
 *
 * <p>See {@link PlurimaConsumerBuilder} for configuration, and the user guide for the
 * full feature reference and engine-choice guidance.
 *
 * @param <K> deserialized key type — defaults to {@code byte[]} when no key
 *     deserializer is supplied
 * @param <V> deserialized value type — defaults to {@code byte[]} when no value
 *     deserializer is supplied
 */
@Stable(since = "0.1.0")
public final class PlurimaConsumer<K, V> implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(PlurimaConsumer.class);

    /**
     * Lifecycle state of a {@code PlurimaConsumer}, queried via {@link #state()}.
     *
     * <p>Transitions: {@code NEW} → {@code RUNNING} on a successful {@link #start()};
     * {@code RUNNING} → {@code CLOSED} on a clean {@link #close()}; {@code RUNNING} →
     * {@code FAILED} when the underlying poll thread hits an unrecoverable error and the
     * runtime self-closes (see {@link PlurimaConsumerBuilder#onFatalError}). Both
     * {@code CLOSED} and {@code FAILED} are terminal — once either is reached the state
     * never changes again, even if the other transition is also attempted (e.g. a fatal
     * error surfacing after the runtime already cleanly closed leaves the state at
     * {@code CLOSED}, and a {@link #close()} call arriving after a fatal error leaves it
     * at {@code FAILED}).
     *
     * @since 0.3.0
     */
    @Stable(since = "0.3.0")
    public enum State {
        /** Constructed but {@link #start()} has not (yet) completed successfully. */
        NEW,
        /** {@link #start()} completed; the poll thread is running. */
        RUNNING,
        /** {@link #close()} completed with no fatal error observed first. Terminal. */
        CLOSED,
        /**
         * The poll thread hit an unrecoverable error; the runtime transitioned here and
         * initiated its own {@link #close()}. Terminal.
         */
        FAILED
    }

    private final Properties kafkaProperties;
    private final String topic;
    private final RecordListener<K, V> listener;
    private final ManualAckListener<K, V> manualAckListener; // nullable
    private final RecordDeserializer<K> keyDeserializer;
    private final RecordDeserializer<V> valueDeserializer;
    private final ConsumerEngine engine;
    @SuppressWarnings("unused") // retained for future runtime decisions; passed to builder
    private final OrderingGuarantee orderingGuarantee;
    private final OrderingMode ordering;
    private final int concurrency;
    private final boolean concurrencyExplicitlySet;
    private final int shardCount;
    private final RetryPolicy retryPolicy;
    private final DltConfig dltConfig; // nullable
    private final Duration pollTimeout;
    private final Duration lockDuration;
    private final Duration shutdownDrainTimeout;
    private final PlurimaMetrics metrics;
    private final AdaptiveBarrierConfig adaptiveBarrierConfig; // nullable; null = disabled
    private final boolean lockDurationExplicitlySet;
    private final Duration handlerTimeout; // nullable; null = no per-handler timeout
    private final Consumer<Throwable> onFatalError; // never null; builder defaults to a no-op

    private final AtomicBoolean started = new AtomicBoolean();
    private final AtomicBoolean closed = new AtomicBoolean();
    /**
     * Guards start() and close() so they cannot interleave. Without this, a concurrent
     * close() could land between start()'s closed-check and its field assignments — close
     * would observe a null runtime, return early, and the subsequent close becomes a no-op
     * (because closed.compareAndSet would already have flipped), leaking the runtime that
     * start() went on to create. Both methods take this lock for their entire body; calls
     * are rare (boot / shutdown) so the brief contention is acceptable.
     */
    private final Object lifecycleLock = new Object();

    private volatile ConsumerRuntime runtime;

    PlurimaConsumer(
        Properties kafkaProperties,
        String topic,
        RecordListener<K, V> listener,                  // nullable when manualAckListener is set
        ManualAckListener<K, V> manualAckListener,      // nullable when listener is set
        RecordDeserializer<K> keyDeserializer,
        RecordDeserializer<V> valueDeserializer,
        ConsumerEngine engine,
        OrderingGuarantee orderingGuarantee,
        OrderingMode ordering,
        int concurrency,
        boolean concurrencyExplicitlySet,
        int shardCount,
        RetryPolicy retryPolicy,
        DltConfig dltConfig,
        Duration pollTimeout,
        Duration lockDuration,
        Duration shutdownDrainTimeout,
        PlurimaMetrics metrics,
        AdaptiveBarrierConfig adaptiveBarrierConfig,
        boolean lockDurationExplicitlySet,
        Duration handlerTimeout,
        Consumer<Throwable> onFatalError) {
        this.kafkaProperties = kafkaProperties;
        this.topic = topic;
        this.listener = listener;
        this.manualAckListener = manualAckListener;
        this.keyDeserializer = keyDeserializer;
        this.valueDeserializer = valueDeserializer;
        this.engine = engine;
        this.orderingGuarantee = orderingGuarantee;
        this.ordering = ordering;
        this.concurrency = concurrency;
        this.concurrencyExplicitlySet = concurrencyExplicitlySet;
        this.shardCount = shardCount;
        this.retryPolicy = retryPolicy;
        this.dltConfig = dltConfig;
        this.pollTimeout = pollTimeout;
        this.lockDuration = lockDuration;
        this.shutdownDrainTimeout = shutdownDrainTimeout;
        this.metrics = metrics;
        this.adaptiveBarrierConfig = adaptiveBarrierConfig;
        this.lockDurationExplicitlySet = lockDurationExplicitlySet;
        this.handlerTimeout = handlerTimeout;
        this.onFatalError = onFatalError;
    }

    /**
     * Starts a builder for {@code byte[]}/{@code byte[]} records — Kafka's own wire type, no
     * deserialization performed. Call {@link PlurimaConsumerBuilder#keyDeserializer} and/or
     * {@link PlurimaConsumerBuilder#valueDeserializer} to re-type the builder to typed key/value
     * classes; those return a re-typed builder rather than mutating this one in place.
     */
    public static PlurimaConsumerBuilder<byte[], byte[]> builder() {
        return new PlurimaConsumerBuilder<>(RecordDeserializer.bytes(), RecordDeserializer.bytes());
    }

    public void start() {
        synchronized (lifecycleLock) {
            if (closed.get()) {
                throw new IllegalStateException(
                    "Consumer is closed; create a new builder/instance to start again");
            }
            if (!started.compareAndSet(false, true)) {
                throw new IllegalStateException("already started");
            }
            ConsumerRuntime r = newRuntime();
            try {
                r.start();
                this.runtime = r;
            } catch (RuntimeException e) {
                started.set(false); // allow retry
                throw e;
            }
        }
    }

    /** Constructs the engine-specific internal runtime based on the configured engine. */
    private ConsumerRuntime newRuntime() {
        return switch (engine) {
            case SHARE -> new ShareConsumerRuntime<>(
                kafkaProperties,
                topic,
                listener,
                manualAckListener,
                keyDeserializer,
                valueDeserializer,
                ordering,
                concurrency,
                shardCount,
                retryPolicy,
                dltConfig,
                pollTimeout,
                lockDuration,
                shutdownDrainTimeout,
                metrics,
                adaptiveBarrierConfig,
                lockDurationExplicitlySet,
                handlerTimeout,
                onFatalError);
            case CLASSIC_BASIC -> {
                yield new ClassicBasicRuntime<>(
                    kafkaProperties,
                    topic,
                    listener,
                    keyDeserializer,
                    valueDeserializer,
                    ordering,
                    concurrency,
                    shardCount,
                    retryPolicy,
                    dltConfig,
                    pollTimeout,
                    shutdownDrainTimeout,
                    metrics,
                    onFatalError);
            }
        };
    }

    /**
     * Current lifecycle state. Before the first successful {@link #start()} this is
     * {@code NEW} (or {@code CLOSED} if {@link #close()} was called without ever
     * starting); afterward it delegates to the underlying runtime, which is the sole
     * source of truth for {@code RUNNING} / {@code CLOSED} / {@code FAILED} — see
     * {@link State}.
     *
     * @since 0.3.0
     */
    public State state() {
        ConsumerRuntime r = runtime;
        if (r != null) return r.state();
        return closed.get() ? State.CLOSED : State.NEW;
    }

    /**
     * Convenience for {@code state() == State.RUNNING}.
     *
     * @since 0.3.0
     */
    public boolean isRunning() {
        return state() == State.RUNNING;
    }

    @Override
    public void close() {
        synchronized (lifecycleLock) {
            if (!closed.compareAndSet(false, true)) return;
            if (runtime != null) {
                try { runtime.close(); }
                catch (Exception e) { log.warn("Runtime close raised", e); }
            }
            log.info("PlurimaConsumer closed");
        }
    }
}
