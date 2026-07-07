package io.plurima.kafka.internal;

import io.plurima.kafka.OrderingMode;
import io.plurima.kafka.PlurimaConsumer;
import io.plurima.kafka.RecordListener;
import io.plurima.kafka.annotation.Internal;
import io.plurima.kafka.deserializer.RecordDeserializer;
import io.plurima.kafka.dlt.DltConfig;
import io.plurima.kafka.metrics.PlurimaMetrics;
import io.plurima.kafka.retry.RetryPolicy;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/**
 * Runtime for the CLASSIC_BASIC engine — vanilla {@code KafkaConsumer} wrapped by
 * {@link ClassicPollLoop}. Builds the consumer, subscribes (with rebalance listener),
 * spins up the worker launcher and DLT router, runs the poll loop on a dedicated
 * thread, and tears the lot down idempotently on close.
 *
 * <p>Dispatch model is mode-tailored: one worker per record for UNORDERED,
 * intra-partition key-shard parallelism for KEY, per-partition serial for PARTITION.
 * The poll loop heartbeats every iteration regardless of worker progress
 * (continuous-poll + pause/resume backpressure), so the broker's
 * {@code max.poll.interval.ms} cannot fence us — long handlers and retry sleeps
 * never block heartbeats.
 */
@Internal
public final class ClassicBasicRuntime<K, V> implements ConsumerRuntime {

    private static final Logger log = LoggerFactory.getLogger(ClassicBasicRuntime.class);

    private final Properties kafkaProperties;
    private final String topic;
    private final RecordListener<K, V> listener;
    private final RecordDeserializer<K> keyDeserializer;
    private final RecordDeserializer<V> valueDeserializer;
    private final OrderingMode ordering;
    private final int concurrency;
    private final int shardCount;
    private final RetryPolicy retryPolicy;
    private final DltConfig dltConfig;       // nullable
    private final Duration pollTimeout;
    private final Duration shutdownDrainTimeout;
    private final PlurimaMetrics metrics;
    private final Consumer<Throwable> onFatalError; // never null; PlurimaConsumerBuilder defaults to a no-op

    // Written by start() (-> RUNNING, unconditionally, before the poll thread starts) and
    // by close()/handleFatal() (-> CLOSED / FAILED respectively); the latter two only ever
    // write after winning the `closed` CAS below, so exactly one terminal write ever lands.
    private volatile PlurimaConsumer.State state = PlurimaConsumer.State.NEW;
    private volatile KafkaConsumer<byte[], byte[]> consumer;
    private volatile ClassicPollLoop<K, V> pollLoop;
    private volatile Thread pollThread;
    private volatile WorkerLauncher workerLauncher;
    private volatile DltRouter dltRouter;
    /**
     * Idempotency guard for close(). The user-initiated close() and the poll loop's
     * fatal-error exit (via the onLoopExit callback passed into ClassicPollLoop) both
     * call close(). Whichever wins the CAS does the cleanup; the other no-ops.
     */
    private final AtomicBoolean closed = new AtomicBoolean();

    public ClassicBasicRuntime(
        Properties kafkaProperties,
        String topic,
        RecordListener<K, V> listener,
        RecordDeserializer<K> keyDeserializer,
        RecordDeserializer<V> valueDeserializer,
        OrderingMode ordering,
        int concurrency,
        int shardCount,
        RetryPolicy retryPolicy,
        DltConfig dltConfig,
        Duration pollTimeout,
        Duration shutdownDrainTimeout,
        PlurimaMetrics metrics,
        Consumer<Throwable> onFatalError) {
        this.kafkaProperties = kafkaProperties;
        this.topic = topic;
        this.listener = listener;
        this.keyDeserializer = keyDeserializer;
        this.valueDeserializer = valueDeserializer;
        this.ordering = ordering;
        this.concurrency = concurrency;
        this.shardCount = shardCount;
        this.retryPolicy = retryPolicy;
        this.dltConfig = dltConfig;
        this.pollTimeout = pollTimeout;
        this.shutdownDrainTimeout = shutdownDrainTimeout;
        this.metrics = metrics;
        this.onFatalError = onFatalError;
    }

    @Override
    public void start() {
        Properties props = PropertiesCopy.copy(kafkaProperties);
        // Plurima always uses byte deserializers; user-supplied per-record deserializers
        // run inside the worker.
        props.put("key.deserializer", ByteArrayDeserializer.class.getName());
        props.put("value.deserializer", ByteArrayDeserializer.class.getName());
        // Plurima commits explicitly via the per-partition CommitFrontier — only contiguous
        // completed offsets ever land. Auto-commit would advance offsets on the broker's
        // schedule, ahead of worker completion, breaking the at-least-once guarantee.
        props.put("enable.auto.commit", "false");

        // Default max.poll.records to concurrency when the user hasn't set it. Kafka's
        // built-in default is 500; combined with Plurima's default concurrency=50 that
        // would let a single poll overshoot the backpressure cap by 10× before the
        // next applyBackpressure() pauses partitions. Aligning the two defaults caps
        // the bounded-overshoot window at concurrency itself and matches the SHARE
        // engine's auto-set behaviour (ShareConsumerRuntime).
        if (!props.containsKey("max.poll.records")) {
            props.put("max.poll.records", Integer.toString(concurrency));
            log.info("Auto-set max.poll.records={} to match concurrency on CLASSIC_BASIC", concurrency);
        }

        String clientId = props.getProperty("client.id");
        if (clientId == null || clientId.isBlank()) {
            clientId = "plurima-classic-" + UUID.randomUUID().toString().substring(0, 8);
            props.put("client.id", clientId);
            log.info("Auto-generated client.id={} for CLASSIC_BASIC engine", clientId);
        }

        // Read once, before the poll loop is constructed, so both the loop's
        // plurima.consumer.poll.duration tag and the in_flight gauge below use the
        // same value.
        String groupId = props.getProperty("group.id", "unknown");

        KafkaConsumer<byte[], byte[]> kc = null;
        WorkerLauncher launcher = null;
        ClassicPollLoop<K, V> loop = null;
        Thread t = null;
        DltRouter localDltRouter = null;
        try {
            launcher = new WorkerLauncher();
            kc = new KafkaConsumer<>(props);
            kc.subscribe(List.of(topic));

            localDltRouter = dltConfig != null ? new DltRouter(dltConfig, metrics) : null;
            loop = new ClassicPollLoop<>(
                kc, topic, groupId, listener, keyDeserializer, valueDeserializer,
                ordering, new RetryEngine(retryPolicy), localDltRouter,
                pollTimeout,
                shutdownDrainTimeout.toMillis(),
                concurrency,
                shardCount,
                metrics, launcher,
                // Loop-exit callback: closes the runtime so any unexpected loop-exit path
                // doesn't leak the KafkaConsumer / DltRouter / WorkerLauncher. The close()
                // method is idempotent via the AtomicBoolean above, so calling it again
                // from the user's close() path is a safe no-op.
                this::close,
                // Fatal-error hook (B6): fires INSTEAD OF the plain close() above when the
                // loop exits via its generic catch(Throwable) branch. handleFatal transitions
                // state to FAILED, self-closes (same cleanup as close()), then invokes the
                // user's onFatalError callback — see handleFatal for the exact ordering.
                this::handleFatal);

            // Register plurima.consumer.records.in_flight gauge backed by the poll loop's
            // own counter. SHARE registers the same metric off its InFlightRegistry; both
            // engines now emit it consistently with the UserGuide § Metrics table.
            ClassicPollLoop<K, V> finalLoop = loop;
            metrics.registerInFlightGauge(topic, groupId, clientId, finalLoop::inFlightCount);

            // Re-subscribe with the rebalance listener now that the poll loop is constructed.
            // The earlier subscribe() upgrade-path was load-bearing for the no-listener variant;
            // calling subscribe again replaces the assignment listener safely (no records have
            // been polled yet).
            kc.subscribe(List.of(topic), loop.rebalanceListener());

            t = new Thread(loop);
            t.setName("plurima-classic-poll-main");
            t.setDaemon(false);

            // Publish runtime fields BEFORE starting the poll thread. The poll loop may
            // exit instantly (e.g. some unexpected runtime condition) and invoke the
            // onLoopExit callback → this.close() while we're still inside this method;
            // if the fields aren't yet published, close() would observe all-null and
            // do nothing useful — but its AtomicBoolean would still flip to closed.
            // The later user-facing close() would then become a no-op, leaking the
            // KafkaConsumer / DltRouter / WorkerLauncher. JMM: volatile field writes
            // happen-before the t.start() that follows, so the poll thread (and any
            // callback it invokes) sees the assigned values.
            this.consumer = kc;
            this.pollLoop = loop;
            this.pollThread = t;
            this.workerLauncher = launcher;
            this.dltRouter = localDltRouter;
            this.state = PlurimaConsumer.State.RUNNING;

            t.start();

            // Log the guarantee that ACTUALLY applies for this (engine, ordering) — UNORDERED
            // has nothing to order so the guarantee is vacuous (LOCAL); KEY/PARTITION are
            // STRICT in steady state (rebalance-window overlap caveat documented in UserGuide).
            String guarantee = ordering == io.plurima.kafka.OrderingMode.UNORDERED
                ? "LOCAL (vacuous — UNORDERED)"
                : "STRICT (cross-cluster via consumer-group partition assignment)";
            log.info("PlurimaConsumer started (CLASSIC_BASIC engine): topic={} ordering={} "
                + "guarantee={}",
                topic, ordering, guarantee);
        } catch (RuntimeException e) {
            log.error("ClassicBasicRuntime.start() failed; cleaning up partial resources", e);
            if (loop != null) RuntimeCleanup.quietly(loop::shutdown);
            if (t != null) RuntimeCleanup.joinQuietly(t, 2_000);
            if (localDltRouter != null) RuntimeCleanup.quietly(localDltRouter::close);
            if (kc != null) RuntimeCleanup.quietly(kc::close);
            if (launcher != null) RuntimeCleanup.quietly(launcher::close);
            // The in_flight gauge may already be registered by the time subscribe/thread
            // start threw; without invoking the metrics close hook here the gauge stays
            // bound to this dead runtime forever — a retry with a fixed client.id can
            // then never re-register its own. Guarded by the SAME `closed` CAS as
            // close()/handleFatal so the metrics close still fires exactly once overall:
            // a subsequent (redundant) user close() on this failed runtime loses the CAS
            // and no-ops. State lands on FAILED — start() threw, so CLOSED would
            // misreport a clean shutdown that never happened, and NEW would pretend
            // start() was never attempted; the CAS also guarantees close() can no longer
            // overwrite this with CLOSED.
            if (closed.compareAndSet(false, true)) {
                state = PlurimaConsumer.State.FAILED;
                RuntimeCleanup.logIfRaised("PlurimaMetrics", metrics::close);
            }
            throw e;
        }
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) return;
        state = PlurimaConsumer.State.CLOSED;
        doClose();
    }

    /**
     * Fatal-error hook wired into {@link ClassicPollLoop}'s {@code onFatal} parameter (B6).
     * Called from the poll thread ONLY when {@code ClassicPollLoop.run()} exits via its
     * generic {@code catch (Throwable t)} branch — an unrecoverable error, never a normal
     * shutdown.
     *
     * <p>Shares the {@link #closed} CAS guard with {@link #close()} so exactly one of the
     * two "wins" the actual cleanup: if a user-initiated {@code close()} already ran (or
     * wins a concurrent race), this method's CAS fails and it is a no-op — per the
     * PlurimaConsumer.State contract, a fatal error observed after an already-completed
     * clean close leaves the state at {@code CLOSED}, and the callback is NOT invoked
     * (there is nothing new to report; the consumer was already deliberately stopped).
     * Otherwise: transition to {@code FAILED} BEFORE the self-close runs (so {@code doClose}
     * can't race a state observer into seeing a stale RUNNING), run the same cleanup
     * {@link #close()} would, then invoke the user's {@code onFatalError} callback — caught
     * and logged, never propagated, so a misbehaving callback can't wedge the poll thread's
     * shutdown.
     */
    private void handleFatal(Throwable t) {
        if (closed.compareAndSet(false, true)) {
            state = PlurimaConsumer.State.FAILED;
            doClose();
            RuntimeCleanup.logIfRaised("onFatalError callback", () -> onFatalError.accept(t));
        }
    }

    /**
     * Shared cleanup body for {@link #close()} and {@link #handleFatal}. Only the caller
     * decides the resulting {@link #state} (CLOSED vs FAILED) and only after winning the
     * {@link #closed} CAS — this method itself does not touch {@link #state}.
     */
    private void doClose() {
        if (pollLoop != null) pollLoop.shutdown();
        // onFatal/onLoopExit invoke handleFatal()/close() FROM the poll thread on a
        // fatal-error loop exit. Joining the poll thread on itself would block forever —
        // skip the self-join; the thread is already exiting its run() method below us.
        if (pollThread != null && Thread.currentThread() != pollThread) {
            RuntimeCleanup.joinQuietly(pollThread, shutdownDrainTimeout.toMillis() + 5_000);
        }
        // Consumer is closed by the poll thread itself inside ClassicPollLoop.run()'s
        // finally block. We MUST NOT call consumer.close from here — KafkaConsumer is
        // not thread-safe, and a user-initiated close racing the poll thread's
        // in-progress commitSync would corrupt the consumer. Worst case (join timed
        // out): the poll thread keeps running in the background, eventually closes
        // the consumer itself. Non-daemon thread; JVM still waits.
        if (dltRouter != null) RuntimeCleanup.logIfRaised("DltRouter", dltRouter::close);
        if (workerLauncher != null) workerLauncher.close();
        // Called exactly once here: doClose() only ever runs after winning the `closed`
        // CAS in close() or handleFatal(), and both share this method — see PlurimaMetrics
        // .close() javadoc for the "exactly once, including the fatal path" contract.
        RuntimeCleanup.logIfRaised("PlurimaMetrics", metrics::close);
    }

    @Override
    public PlurimaConsumer.State state() {
        return state;
    }

    /** Visible for tests: whether {@link #close()} has run. */
    boolean isClosed() {
        return closed.get();
    }
}
