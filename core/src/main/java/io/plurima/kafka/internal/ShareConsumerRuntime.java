package io.plurima.kafka.internal;

import io.plurima.kafka.AdaptiveBarrierConfig;
import io.plurima.kafka.OrderingMode;
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
import java.util.concurrent.atomic.AtomicBoolean;

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

    private volatile ShareConsumer<byte[], byte[]> consumer;
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
        AdaptiveBarrierConfig adaptiveBarrierConfig) {
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
        AckCoordinator coordinator = new AckCoordinator(registry, metrics);
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

            localDltRouter = dltConfig != null ? new DltRouter(dltConfig, metrics) : null;
            ListenerInvoker invoker = listener != null
                ? ListenerInvoker.forImplicit(listener, keyDeserializer, valueDeserializer)
                : ListenerInvoker.forManual(manualAckListener, keyDeserializer, valueDeserializer);
            HandlerLatencyWindow latencyWindow =
                adaptiveBarrierConfig != null ? new HandlerLatencyWindow(ADAPTIVE_WINDOW_SIZE) : null;
            WorkerProcessor workerProcessor = new WorkerProcessor(
                invoker,
                new RetryEngine(retryPolicy),
                coordinator,
                localDltRouter,
                metrics,
                latencyWindow);
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
                pollTimeout, lockDuration, shutdownDrainTimeout, lockDuration, metrics, topic,
                // Loop-exit callback: closes the runtime if the loop dies unexpectedly so
                // the WorkerLauncher / DltRouter / KafkaShareConsumer can't leak. close()
                // is idempotent via the AtomicBoolean below, so a later user-initiated
                // close() is a safe no-op.
                this::close, latencyWindow, adaptiveBarrierConfig);
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
            throw e;
        }
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) return;
        if (pollLoop != null) pollLoop.shutdown();
        // onLoopExit invokes close() FROM the poll thread when the loop dies fatally.
        // Joining the poll thread on itself would block forever — skip the self-join;
        // the thread is already exiting its run() method below us.
        if (pollThread != null && Thread.currentThread() != pollThread) {
            RuntimeCleanup.joinQuietly(pollThread, shutdownDrainTimeout.toMillis() + 5_000);
        }
        if (workerLauncher != null) workerLauncher.close();
        if (dltRouter != null) RuntimeCleanup.logIfRaised("DltRouter", dltRouter::close);
    }

    /** Test seam: whether close() has actually run. */
    boolean isClosed() {
        return closed.get();
    }
}
