package io.plurima.kafka.internal;

import io.plurima.kafka.AdaptiveBarrierConfig;
import io.plurima.kafka.OrderingMode;
import io.plurima.kafka.PlurimaConsumer;
import io.plurima.kafka.RecordListener;
import io.plurima.kafka.ack.ManualAckListener;
import io.plurima.kafka.annotation.Internal;
import io.plurima.kafka.deserializer.RecordDeserializer;
import io.plurima.kafka.dlt.DltConfig;
import io.plurima.kafka.metrics.PlurimaMetrics;
import io.plurima.kafka.retry.RetryPolicy;
import org.apache.kafka.clients.consumer.KafkaShareConsumer;
import org.apache.kafka.clients.consumer.ShareConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/**
 * Runtime for the KIP-932 share-consumer engine. Encapsulates the
 * {@code KafkaShareConsumer}, the per-record {@link InFlightRegistry}, the
 * {@link AckCoordinator}, the {@link PollLoop} on its own thread, the optional
 * {@link DltRouter}, and the {@link WorkerLauncher} that drives the virtual-thread
 * per-record workers.
 *
 * <p>This class is the share-engine half of the {@link ConsumerRuntime}
 * split; the classic-engine counterpart is {@code ClassicBasicRuntime}.
 */
@Internal
public final class ShareConsumerRuntime<K, V> implements ConsumerRuntime {

    private static final Logger log = LoggerFactory.getLogger(ShareConsumerRuntime.class);
    private static final int ADAPTIVE_WINDOW_SIZE = 1024;

    private final Properties kafkaProperties;
    private final String topic;
    private final RecordListener<K, V> listener;                 // nullable when manualAckListener set
    private final ManualAckListener<K, V> manualAckListener;     // nullable when listener set
    private final RecordDeserializer<K> keyDeserializer;
    private final RecordDeserializer<V> valueDeserializer;
    private final OrderingMode ordering;
    private final int concurrency;
    private final int shardCount;
    private final RetryPolicy retryPolicy;
    private final DltConfig dltConfig;                            // nullable
    private final Duration pollTimeout;
    private final Duration lockDuration;
    private final Duration shutdownDrainTimeout;
    private final PlurimaMetrics metrics;
    private final AdaptiveBarrierConfig adaptiveBarrierConfig; // nullable; null = disabled
    private final boolean lockDurationExplicitlySet;
    private final Duration handlerTimeout; // nullable; null = no per-handler timeout
    private final Consumer<Throwable> onFatalError; // never null; PlurimaConsumerBuilder defaults to a no-op

    // Written by start() (-> RUNNING, unconditionally, before the poll thread starts) and
    // by close()/handleFatal() (-> CLOSED / FAILED respectively); the latter two only ever
    // write after winning the `closed` CAS below, so exactly one terminal write ever lands.
    private volatile PlurimaConsumer.State state = PlurimaConsumer.State.NEW;
    private volatile ShareConsumer<byte[], byte[]> consumer;
    private volatile ScheduledExecutorService timeoutScheduler; // nullable; created when handlerTimeout set
    private volatile PollLoop pollLoop;
    private volatile Thread pollThread;
    private volatile WorkerLauncher workerLauncher;
    private volatile DltRouter dltRouter;
    /**
     * Idempotency guard for {@link #close()}. The PollLoop's onLoopExit callback fires
     * close() from the poll thread when the loop dies unexpectedly; the user's close()
     * path runs the same code. Whichever wins the CAS does the cleanup; the other
     * no-ops. Matches {@link ClassicBasicRuntime}'s pattern.
     */
    private final AtomicBoolean closed = new AtomicBoolean();

    public ShareConsumerRuntime(
        Properties kafkaProperties,
        String topic,
        RecordListener<K, V> listener,
        ManualAckListener<K, V> manualAckListener,
        RecordDeserializer<K> keyDeserializer,
        RecordDeserializer<V> valueDeserializer,
        OrderingMode ordering,
        int concurrency,
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
        this.ordering = ordering;
        this.concurrency = concurrency;
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

    @Override
    public void start() {
        Properties props = PropertiesCopy.copy(kafkaProperties);
        // Plurima always reads bytes from the broker and applies user-supplied
        // deserializers inside the worker thread — keeps poll-thread cost constant
        // regardless of payload size.
        props.put("key.deserializer", ByteArrayDeserializer.class.getName());
        props.put("value.deserializer", ByteArrayDeserializer.class.getName());

        String userMode = props.getProperty("share.acknowledgement.mode");
        if ("implicit".equalsIgnoreCase(userMode)) {
            log.warn("share.acknowledgement.mode=implicit is incompatible with Plurima's "
                + "retry/DLT pipeline; forcing explicit");
        }
        props.put("share.acknowledgement.mode", "explicit");

        if (!props.containsKey("max.poll.records")) {
            props.put("max.poll.records", Integer.toString(concurrency));
            log.info("Auto-set max.poll.records={} to match concurrency", concurrency);
        }

        // Per KIP-1206: share.acquire.mode=record_limit makes the broker treat max.poll.records
        // as a strict ceiling on the number of records acquired per poll, rather than advisory.
        // Without this, the broker may acquire more records than concurrency can process, and
        // the excess sit in our InFlightRegistry holding broker-side locks that tick down toward
        // expiry while waiting on the BackpressureGate.
        String userAcquireMode = props.getProperty("share.acquire.mode");
        if (userAcquireMode != null && !"record_limit".equalsIgnoreCase(userAcquireMode)) {
            log.warn("share.acquire.mode={} overridden to record_limit so max.poll.records is "
                + "honored strictly", userAcquireMode);
        }
        props.put("share.acquire.mode", "record_limit");

        InFlightRegistry registry = new InFlightRegistry();
        String clientId = props.getProperty("client.id");
        if (clientId == null || clientId.isBlank()) {
            clientId = "plurima-" + UUID.randomUUID().toString().substring(0, 8);
            props.put("client.id", clientId);
            log.info("Auto-generated client.id={}", clientId);
        }
        String groupId = props.getProperty("group.id", "unknown");
        metrics.registerInFlightGauge(topic, groupId, clientId, registry::currentInFlight);
        // Tracks drain-barrier force-RELEASEs (slowness, not failure) so RetryEngine/DltRouter
        // don't count the resulting broker redeliveries toward retry exhaustion. Bounded LRU.
        SlownessReleaseTracker slownessTracker =
            new SlownessReleaseTracker(Math.max(4096, concurrency * 16));
        AckCoordinator coordinator = new AckCoordinator(registry, metrics, slownessTracker);
        BackpressureGate gate = new BackpressureGate(concurrency);
        WorkerLauncher launcher = new WorkerLauncher();

        ShareConsumer<byte[], byte[]> sc = null;
        DltRouter localDltRouter = null;
        PollLoop loop = null;
        Thread t = null;
        try {
            sc = new KafkaShareConsumer<>(props);
            sc.setAcknowledgementCommitCallback(coordinator);
            sc.subscribe(List.of(topic));

            localDltRouter = dltConfig != null ? new DltRouter(dltConfig, metrics, slownessTracker) : null;
            ListenerInvoker invoker = listener != null
                ? ListenerInvoker.forImplicit(listener, keyDeserializer, valueDeserializer)
                : ListenerInvoker.forManual(manualAckListener, keyDeserializer, valueDeserializer);
            HandlerLatencyWindow latencyWindow =
                adaptiveBarrierConfig != null ? new HandlerLatencyWindow(ADAPTIVE_WINDOW_SIZE) : null;
            // Shared daemon watchdog scheduler for per-handler timeouts (P3); null when off.
            this.timeoutScheduler = handlerTimeout != null
                ? Executors.newSingleThreadScheduledExecutor(rr -> {
                    Thread th = new Thread(rr, "plurima-handler-timeout");
                    th.setDaemon(true);
                    return th;
                })
                : null;
            WorkerProcessor workerProcessor = new WorkerProcessor(
                invoker,
                new RetryEngine(retryPolicy, slownessTracker),
                coordinator,
                localDltRouter,
                metrics,
                latencyWindow,
                handlerTimeout,
                timeoutScheduler);
            // SHARE engine supports only UNORDERED as of v0.1 — KEY and PARTITION are
            // rejected at PlurimaConsumerBuilder.build(). The throw is unreachable in
            // practice; it exists so that future enum additions surface here.
            WorkDispatcher dispatcher = switch (ordering) {
                case UNORDERED -> new UnorderedDispatcher(workerProcessor, registry, coordinator, launcher, gate, ordering);
                case KEY, PARTITION -> throw new IllegalStateException(
                    "engine=SHARE does not support ordering=" + ordering
                    + " (should have been rejected at build time)");
            };

            loop = new PollLoop(
                sc, dispatcher, coordinator, registry, gate,
                pollTimeout, lockDuration, shutdownDrainTimeout, lockDuration, metrics, topic, groupId,
                // Loop-exit callback: closes the runtime if the loop dies unexpectedly so
                // the WorkerLauncher / DltRouter / KafkaShareConsumer can't leak. close()
                // is idempotent via the AtomicBoolean below, so a later user-initiated
                // close() is a safe no-op.
                this::close, latencyWindow, adaptiveBarrierConfig, lockDurationExplicitlySet,
                // Fatal-error hook (B6): fires INSTEAD OF the plain close() above when the
                // loop exits via its generic catch(Throwable) branch. handleFatal transitions
                // state to FAILED, self-closes (same cleanup as close()), then invokes the
                // user's onFatalError callback — see handleFatal for the exact ordering.
                this::handleFatal);
            if (adaptiveBarrierConfig != null) {
                metrics.registerBarrierTimeoutGauge(topic, groupId, clientId, loop::currentBarrierMillis);
            }

            t = new Thread(loop);
            t.setName("plurima-poll-main");
            t.setDaemon(false);

            // Publish runtime fields BEFORE thread.start() — matches ClassicBasicRuntime.
            // If the poll loop fires onLoopExit instantly (e.g. a constructor-time fault
            // surfacing on the first poll), close() needs to see non-null fields to do
            // its cleanup. JMM: volatile writes happen-before t.start(), so the loop
            // thread (and any callback it invokes) sees the assigned values.
            this.consumer = sc;
            this.pollLoop = loop;
            this.pollThread = t;
            this.workerLauncher = launcher;
            this.dltRouter = localDltRouter;
            this.state = PlurimaConsumer.State.RUNNING;

            t.start();

            log.info("PlurimaConsumer started (SHARE engine): topic={} ordering=UNORDERED concurrency={}",
                topic, concurrency);
        } catch (RuntimeException e) {
            log.error("ShareConsumerRuntime.start() failed; cleaning up partial resources", e);
            if (loop != null) RuntimeCleanup.quietly(loop::shutdown);
            if (t != null) RuntimeCleanup.joinQuietly(t, 2_000);
            if (localDltRouter != null) RuntimeCleanup.quietly(localDltRouter::close);
            if (sc != null) RuntimeCleanup.quietly(sc::close);
            RuntimeCleanup.quietly(launcher::close);
            if (timeoutScheduler != null) RuntimeCleanup.quietly(timeoutScheduler::shutdownNow);
            // The in_flight gauge is registered BEFORE this try block, so any failure
            // here has already leaked it into the registry; without invoking the metrics
            // close hook the gauge stays bound to this dead runtime forever — a retry
            // with a fixed client.id can then never re-register its own. Guarded by the
            // SAME `closed` CAS as close()/handleFatal so the metrics close still fires
            // exactly once overall: a subsequent (redundant) user close() on this failed
            // runtime loses the CAS and no-ops. State lands on FAILED — start() threw,
            // so CLOSED would misreport a clean shutdown that never happened, and NEW
            // would pretend start() was never attempted; the CAS also guarantees close()
            // can no longer overwrite this with CLOSED. Mirrors ClassicBasicRuntime.
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
     * Fatal-error hook wired into {@link PollLoop}'s {@code onFatal} parameter (B6). Called
     * from the poll thread ONLY when {@code PollLoop.run()} exits via its generic
     * {@code catch (Throwable t)} branch — an unrecoverable error, never a normal shutdown.
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
        // onFatal/onLoopExit invoke close()/handleFatal() FROM the poll thread when the
        // loop dies (fatally or otherwise). Joining the poll thread on itself would block
        // forever — skip the self-join; the thread is already exiting its run() method
        // below us.
        if (pollThread != null && Thread.currentThread() != pollThread) {
            RuntimeCleanup.joinQuietly(pollThread, shutdownDrainTimeout.toMillis() + 5_000);
        }
        if (workerLauncher != null) workerLauncher.close();
        if (dltRouter != null) RuntimeCleanup.logIfRaised("DltRouter", dltRouter::close);
        // Stop the handler-timeout watchdog scheduler (no-op if renewal/timeout was off).
        if (timeoutScheduler != null) RuntimeCleanup.quietly(timeoutScheduler::shutdownNow);
        // Called exactly once here: doClose() only ever runs after winning the `closed`
        // CAS in close() or handleFatal(), and both share this method — see PlurimaMetrics
        // .close() javadoc for the "exactly once, including the fatal path" contract.
        RuntimeCleanup.logIfRaised("PlurimaMetrics", metrics::close);
    }

    @Override
    public PlurimaConsumer.State state() {
        return state;
    }

    /** Test seam: whether close() has actually run. */
    boolean isClosed() {
        return closed.get();
    }
}
