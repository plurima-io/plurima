package io.plurima.kafka;

import io.plurima.kafka.ack.ManualAckListener;
import io.plurima.kafka.ack.MessageAckListener;
import io.plurima.kafka.annotation.Experimental;
import io.plurima.kafka.annotation.Stable;
import io.plurima.kafka.deserializer.RecordDeserializer;
import io.plurima.kafka.dlt.DltConfig;
import io.plurima.kafka.metrics.PlurimaMetrics;
import io.plurima.kafka.retry.RetryPolicy;

import java.time.Duration;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.function.Consumer;

@Stable(since = "0.1.0")
public final class PlurimaConsumerBuilder<K, V> {

    /**
     * Configs Plurima rejects for the SHARE engine. Share groups don't honor these
     * settings (e.g. {@code auto.offset.reset} is rejected by KafkaShareConsumer itself
     * with a cryptic ConfigException); the others Plurima manages internally.
     */
    static final Set<String> UNSUPPORTED_CONFIGS_SHARE = Set.of(
        "enable.auto.commit",
        "auto.commit.interval.ms",
        "group.instance.id",
        "isolation.level",
        "partition.assignment.strategy",
        "auto.offset.reset",
        "interceptor.classes",
        "session.timeout.ms",
        "heartbeat.interval.ms",
        "group.protocol",
        "group.remote.assignor"
    );

    /**
     * Configs Plurima rejects for the CLASSIC_BASIC engine. {@code enable.auto.commit}
     * is internally forced to {@code false} (Plurima commits after per-record success);
     * share-specific configs are meaningless on a regular KafkaConsumer; interceptors
     * remain off-limits because Plurima needs full control of the poll loop.
     *
     * <p>Configs the SHARE list rejects but classic ACCEPTS:
     * {@code auto.offset.reset}, {@code isolation.level},
     * {@code partition.assignment.strategy}, {@code session.timeout.ms},
     * {@code heartbeat.interval.ms}, {@code group.protocol},
     * {@code group.instance.id} (for static membership).
     */
    static final Set<String> UNSUPPORTED_CONFIGS_CLASSIC = Set.of(
        "enable.auto.commit",
        "auto.commit.interval.ms",
        "share.acknowledgement.mode",
        "share.acquire.mode",
        "interceptor.classes",
        "group.remote.assignor"
    );

    private Properties kafkaProperties;
    private String topic;
    private RecordListener<K, V> listener;
    private RecordDeserializer<K> keyDeserializer;
    private RecordDeserializer<V> valueDeserializer;
    private ManualAckListener<K, V> manualAckListener;
    /** Guards "exactly one handler" — set by any of listener/manualAckListener/onMessage/onMessageAck. */
    private boolean handlerConfigured = false;
    private ConsumerEngine engine = ConsumerEngine.SHARE;
    private OrderingGuarantee orderingGuarantee;  // null = infer from engine + ordering
    private OrderingMode ordering = OrderingMode.UNORDERED;
    private int concurrency = 50;
    private boolean concurrencyExplicitlySet = false;
    private Duration pollTimeout = Duration.ofSeconds(1);
    private Duration lockDuration = Duration.ofSeconds(30);
    private boolean lockDurationExplicitlySet = false;
    private Duration shutdownDrainTimeout = Duration.ofSeconds(30);
    private int shardCount = -1; // -1 means "derive from concurrency × 4 at build time"
    private RetryPolicy retryPolicy = RetryPolicy.noRetry();
    private DltConfig dltConfig; // null = no DLT (retry exhaustion → REJECT with error log)
    private AdaptiveBarrierConfig adaptiveBarrierConfig; // null = disabled
    private Duration handlerTimeout; // null = no per-handler timeout
    private PlurimaMetrics metrics = PlurimaMetrics.noOp();
    private Consumer<Throwable> onFatalError = t -> {}; // no-op default

    /**
     * Package-private: only reachable via {@link PlurimaConsumer#builder()}, which fixes
     * {@code K == V == byte[]} at this call site — so passing {@code RecordDeserializer.bytes()}
     * for both parameters type-checks with zero casts. A typed builder can only come into being
     * afterward via {@link #keyDeserializer} / {@link #valueDeserializer}, which is what makes
     * removing the old unchecked-identity-cast default field initializer safe: there is no path
     * that leaves a typed builder holding a deserializer that doesn't match its own type
     * parameter.
     */
    PlurimaConsumerBuilder(RecordDeserializer<K> keyDeserializer, RecordDeserializer<V> valueDeserializer) {
        this.keyDeserializer = keyDeserializer;
        this.valueDeserializer = valueDeserializer;
    }

    /**
     * Sets the Kafka client properties ({@code bootstrap.servers}, {@code group.id}, etc.)
     * passed through to the underlying consumer. Required — {@link #build()} throws
     * {@link IllegalStateException} if this is never called.
     *
     * <p>{@code props} is defensively copied; later mutation of the argument (or reuse of the
     * same {@link Properties} instance across builders) has no effect on this builder.
     *
     * @throws NullPointerException if {@code props} is {@code null}
     */
    public PlurimaConsumerBuilder<K, V> kafkaProperties(Properties props) {
        Objects.requireNonNull(props, "kafkaProperties");
        this.kafkaProperties = PropertiesSupport.copy(props);
        return this;
    }

    /**
     * Sets the topic to consume from. Required — {@link #build()} throws
     * {@link IllegalStateException} if this is never called.
     *
     * <p>Plurima consumes exactly one topic per consumer instance; there is no
     * multi-topic subscription. Multi-topic consumption (one consumer processing
     * several topics) is a known future direction but is not implemented in this
     * release — build one {@link PlurimaConsumer} per topic if you need more than one.
     *
     * @throws IllegalArgumentException eagerly, if {@code topic} is non-null but blank
     */
    public PlurimaConsumerBuilder<K, V> topic(String topic) {
        if (topic != null && topic.isBlank()) {
            throw new IllegalArgumentException(
                "topic must not be blank — was '" + topic + "'");
        }
        this.topic = topic; return this;
    }

    /**
     * Rejects configuring more than one handler. Exactly one of {@code listener},
     * {@code manualAckListener}, {@code onMessage}, or {@code onMessageAck} may be set.
     */
    private void markHandlerConfigured() {
        if (handlerConfigured) {
            throw new IllegalStateException(
                "a handler is already configured — set exactly one of listener(...), "
                + "manualAckListener(...), onMessage(...), or onMessageAck(...)");
        }
        handlerConfigured = true;
    }

    /**
     * Sets the raw, Kafka-typed handler invoked for each delivered record. Exactly one of
     * {@code listener}, {@link #manualAckListener}, {@link #onMessage}, or
     * {@link #onMessageAck} may be configured on a given builder; calling a second one throws
     * {@link IllegalStateException}. {@link #build()} throws {@link IllegalStateException} if
     * none of the four is ever called.
     *
     * @throws NullPointerException if {@code listener} is {@code null}
     * @throws IllegalStateException if a handler is already configured
     */
    public PlurimaConsumerBuilder<K, V> listener(RecordListener<K, V> listener) {
        Objects.requireNonNull(listener, "listener");
        markHandlerConfigured();
        this.listener = listener; return this;
    }

    /**
     * Re-types the builder's key type parameter to {@code K2}, the deserializer's output type.
     * Must be called before {@code listener}/{@code manualAckListener}/{@code onMessage}/
     * {@code onMessageAck} — those fix the handler's generic type against whatever {@code K} is
     * at the moment they're called, so retyping afterward would leave a handler field whose
     * declared type no longer matches the object it holds (a latent heap-pollution
     * {@code ClassCastException} at record-delivery time). {@link #markHandlerConfigured} tracks
     * whether a handler is already set, so we can reject that ordering here with a clear message
     * instead of letting it compile into a runtime trap.
     *
     * <p>The cast of {@code this} is safe because the only {@code K}-typed state on this builder
     * is {@link #keyDeserializer} itself, which is reassigned on the very next line — every other
     * field either doesn't mention {@code K} or is guaranteed unset (guarded by the check above).
     */
    @SuppressWarnings("unchecked")
    public <K2> PlurimaConsumerBuilder<K2, V> keyDeserializer(RecordDeserializer<K2> deser) {
        Objects.requireNonNull(deser, "keyDeserializer");
        requireDeserializerBeforeHandler("keyDeserializer");
        PlurimaConsumerBuilder<K2, V> retyped = (PlurimaConsumerBuilder<K2, V>) this;
        retyped.keyDeserializer = deser;
        return retyped;
    }

    /**
     * Re-types the builder's value type parameter to {@code V2}, the deserializer's output type.
     * See {@link #keyDeserializer} for why this must precede handler setters and why the cast of
     * {@code this} is safe.
     */
    @SuppressWarnings("unchecked")
    public <V2> PlurimaConsumerBuilder<K, V2> valueDeserializer(RecordDeserializer<V2> deser) {
        Objects.requireNonNull(deser, "valueDeserializer");
        requireDeserializerBeforeHandler("valueDeserializer");
        PlurimaConsumerBuilder<K, V2> retyped = (PlurimaConsumerBuilder<K, V2>) this;
        retyped.valueDeserializer = deser;
        return retyped;
    }

    /**
     * Rejects {@code keyDeserializer}/{@code valueDeserializer} calls made after a handler
     * (listener/manualAckListener/onMessage/onMessageAck) is already configured. Once a handler
     * is set it has fixed the current {@code K}/{@code V} into its own field type; re-typing the
     * builder afterward would silently leave that handler field mismatched against the builder's
     * new type parameters.
     */
    private void requireDeserializerBeforeHandler(String setterName) {
        if (handlerConfigured) {
            throw new IllegalStateException(
                setterName + "(...) must be called before listener(...), manualAckListener(...), "
                + "onMessage(...), or onMessageAck(...) — the handler you already configured fixed "
                + "its generic type against the builder's type at that point, so changing the "
                + "deserializer afterward would leave it mismatched. Call keyDeserializer(...) / "
                + "valueDeserializer(...) earlier in the chain, before the handler.");
        }
    }

    public PlurimaConsumerBuilder<K, V> manualAckListener(ManualAckListener<K, V> listener) {
        Objects.requireNonNull(listener, "manualAckListener");
        markHandlerConfigured();
        this.manualAckListener = listener;
        return this;
    }

    /**
     * Auto-ack listener over a {@link Message} — the recommended default for application
     * handlers. The handler receives a single Kafka-decoupled object (payload + metadata);
     * a normal return ACCEPTs the record, a thrown exception goes to the retry/DLT pipeline.
     * Adapts onto the standard {@link RecordListener}, so retry, DLT, slowness handling, and
     * the handler timeout all apply unchanged.
     */
    public PlurimaConsumerBuilder<K, V> onMessage(MessageListener<K, V> listener) {
        Objects.requireNonNull(listener, "messageListener");
        markHandlerConfigured();
        this.listener = (record, ctx) ->
            listener.onMessage(new RecordMessage<>(record, ctx.deliveryCount(), ctx.orderingMode()));
        return this;
    }

    /**
     * Explicit-ack listener over an {@link io.plurima.kafka.ack.AckMessage} — payload, metadata, and ack on one
     * object. The handler must call {@code acknowledge(...)} / {@code accept()} / {@code release()}
     * / {@code reject()} (a return without acking auto-RELEASEs). Adapts onto
     * {@link ManualAckListener}.
     */
    public PlurimaConsumerBuilder<K, V> onMessageAck(MessageAckListener<K, V> listener) {
        Objects.requireNonNull(listener, "messageAckListener");
        markHandlerConfigured();
        this.manualAckListener = (record, ack) ->
            listener.onMessage(new AckRecordMessage<>(record, ack));
        return this;
    }

    /**
     * Selects the underlying Kafka client primitive. Default {@link ConsumerEngine#SHARE}.
     * See {@link ConsumerEngine} for the trade-offs.
     *
     * @since 0.1.0
     */
    public PlurimaConsumerBuilder<K, V> engine(ConsumerEngine engine) {
        this.engine = Objects.requireNonNull(engine, "engine");
        return this;
    }

    /**
     * Asserts the ordering scope you expect. Optional — if unset, inferred from
     * {@code (engine, ordering)} per {@link OrderingGuarantee}'s matrix. If set, the
     * builder validates against the matrix and rejects mismatches at {@link #build()}.
     *
     * @since 0.1.0
     */
    public PlurimaConsumerBuilder<K, V> orderingGuarantee(OrderingGuarantee guarantee) {
        this.orderingGuarantee = Objects.requireNonNull(guarantee, "orderingGuarantee");
        return this;
    }

    /**
     * Sets the per-record ordering scope: {@link OrderingMode#UNORDERED} (default),
     * {@link OrderingMode#KEY}, or {@link OrderingMode#PARTITION}. See {@link OrderingMode}
     * and {@link OrderingGuarantee} for the guarantee each combination yields per engine;
     * {@link #build()} rejects combinations the current {@link #engine} cannot deliver (e.g.
     * {@code engine=SHARE} with {@code KEY}/{@code PARTITION}).
     *
     * @throws NullPointerException if {@code ordering} is {@code null}
     */
    public PlurimaConsumerBuilder<K, V> ordering(OrderingMode ordering) {
        this.ordering = Objects.requireNonNull(ordering, "ordering");
        return this;
    }

    /**
     * Sets the number of concurrent worker slots processing records. Default {@code 50}.
     *
     * @throws IllegalArgumentException eagerly, if {@code concurrency <= 0}
     */
    public PlurimaConsumerBuilder<K, V> concurrency(int concurrency) {
        if (concurrency <= 0) {
            throw new IllegalArgumentException("concurrency must be > 0, was " + concurrency);
        }
        this.concurrency = concurrency;
        this.concurrencyExplicitlySet = true;
        return this;
    }

    /**
     * Sets the poll loop's per-iteration broker poll timeout — how long a single
     * {@code poll(...)} call blocks waiting for records before returning (possibly empty) and
     * looping again. Default {@code 1s}.
     *
     * @throws NullPointerException if {@code t} is {@code null}
     * @throws IllegalArgumentException if {@code t} is zero or negative
     */
    public PlurimaConsumerBuilder<K, V> pollTimeout(Duration t) {
        Objects.requireNonNull(t, "pollTimeout");
        if (t.isZero() || t.isNegative()) {
            throw new IllegalArgumentException("pollTimeout must be > 0, was " + t);
        }
        this.pollTimeout = t;
        return this;
    }

    /**
     * <b>SHARE engine only.</b> Plurima's local force-RELEASE deadline for in-flight
     * records. After a record sits in Plurima's pipeline this long without completing,
     * the poll thread RELEASEs it explicitly so the broker can redeliver immediately
     * to another consumer (or back to us on the next poll) instead of waiting for its
     * own acquisition-lock expiry.
     *
     * <p>If this method is not called, Plurima auto-aligns the force-RELEASE deadline
     * to roughly 80% of the broker's {@code group.share.record.lock.duration.ms} once
     * the broker reports it. This keeps the no-force-release window as large as the
     * broker safely allows. Calling this method disables auto-alignment and uses the
     * supplied value verbatim.
     *
     * <p>When setting this explicitly, set it BELOW the broker lock — typically ~80% of
     * it. A value equal to or larger than the broker's gives no early-recovery benefit
     * and Plurima logs an error at startup once the broker reports its real value
     * (queried via {@code KafkaShareConsumer.acquisitionLockTimeoutMs}).
     *
     * <p>Has no effect on the {@code CLASSIC_BASIC} engine — classic consumer groups
     * have no per-record lease. The classic poll loop heartbeats every iteration
     * regardless of worker progress (continuous-poll + pause/resume backpressure),
     * so {@code max.poll.interval.ms} is satisfied by the poll-thread cadence and
     * does NOT bound individual handler runtime.
     */
    public PlurimaConsumerBuilder<K, V> lockDuration(Duration t) {
        Objects.requireNonNull(t, "lockDuration");
        if (t.isZero() || t.isNegative()) {
            throw new IllegalArgumentException("lockDuration must be > 0, was " + t);
        }
        this.lockDuration = t;
        this.lockDurationExplicitlySet = true;
        return this;
    }

    /**
     * Bounds how long {@link PlurimaConsumer#close()} waits for in-flight records to finish
     * processing before force-closing the underlying consumer. Default {@code 30s}.
     *
     * @throws NullPointerException if {@code t} is {@code null}
     * @throws IllegalArgumentException if {@code t} is zero or negative
     */
    public PlurimaConsumerBuilder<K, V> shutdownDrainTimeout(Duration t) {
        Objects.requireNonNull(t, "shutdownDrainTimeout");
        if (t.isZero() || t.isNegative()) {
            throw new IllegalArgumentException("shutdownDrainTimeout must be > 0, was " + t);
        }
        this.shutdownDrainTimeout = t;
        return this;
    }

    /**
     * Sets the number of internal shards used to distribute {@code KEY}/{@code PARTITION}
     * ordered work across worker slots. Optional — if never called, {@link #build()} derives
     * it as {@code concurrency * 4}.
     *
     * @throws IllegalArgumentException eagerly, if {@code shardCount <= 0}
     */
    public PlurimaConsumerBuilder<K, V> shardCount(int shardCount) {
        if (shardCount <= 0) {
            throw new IllegalArgumentException("shardCount must be > 0, was " + shardCount);
        }
        this.shardCount = shardCount; return this;
    }

    /**
     * Sets the retry policy applied when the handler throws. Default
     * {@link RetryPolicy#noRetry()} (no retries — the first failure is immediately rejected,
     * or routed to the dead-letter topic if {@link #deadLetter} is configured). See
     * {@link RetryPolicy} for building an exponential-backoff policy with a custom
     * {@link io.plurima.kafka.retry.ExceptionClassifier}.
     *
     * @throws NullPointerException if {@code policy} is {@code null}
     */
    public PlurimaConsumerBuilder<K, V> retry(RetryPolicy policy) {
        this.retryPolicy = Objects.requireNonNull(policy, "retryPolicy");
        return this;
    }

    /**
     * Sets the dead-letter-topic configuration used once {@link #retry} attempts are
     * exhausted (or immediately, on a non-retriable exception, if retry is disabled or the
     * exception is classified non-retriable). Optional — if never called, exhausted or
     * non-retriable failures are instead REJECTed with an error log and no DLT publish.
     *
     * @throws NullPointerException if {@code dltConfig} is {@code null}
     */
    public PlurimaConsumerBuilder<K, V> deadLetter(DltConfig dltConfig) {
        this.dltConfig = Objects.requireNonNull(dltConfig, "dltConfig");
        return this;
    }

    /** Enable the SHARE-engine adaptive drain barrier with default tuning (p99 × 3). */
    @Experimental
    public PlurimaConsumerBuilder<K, V> adaptiveDrainBarrier() {
        return adaptiveDrainBarrier(AdaptiveBarrierConfig.defaults());
    }

    /** Enable the SHARE-engine adaptive drain barrier with explicit tuning. */
    @Experimental
    public PlurimaConsumerBuilder<K, V> adaptiveDrainBarrier(AdaptiveBarrierConfig config) {
        this.adaptiveBarrierConfig = Objects.requireNonNull(config, "adaptiveBarrierConfig");
        return this;
    }

    /**
     * <b>SHARE engine only.</b> Bounds how long a single listener invocation may run. When a
     * handler exceeds this, its worker thread is interrupted and the failure is routed through
     * the retry/DLT pipeline as a {@link HandlerTimeoutException} (so it can be classified via
     * {@code RetryPolicy.retryOn(HandlerTimeoutException.class)}). The handler must be
     * interruptible for this to take effect. Off by default.
     */
    public PlurimaConsumerBuilder<K, V> handlerTimeout(Duration t) {
        Objects.requireNonNull(t, "handlerTimeout");
        if (t.isZero() || t.isNegative()) {
            throw new IllegalArgumentException("handlerTimeout must be > 0, was " + t);
        }
        this.handlerTimeout = t;
        return this;
    }

    /**
     * Sets the metrics sink Plurima reports consumer activity to. Default
     * {@link PlurimaMetrics#noOp()} (metrics collection disabled).
     *
     * @throws NullPointerException if {@code metrics} is {@code null}
     */
    public PlurimaConsumerBuilder<K, V> metrics(PlurimaMetrics metrics) {
        this.metrics = Objects.requireNonNull(metrics, "metrics");
        return this;
    }

    /**
     * Registers a callback for unrecoverable poll-thread errors. Optional; the default is
     * a no-op.
     *
     * <p>Invoked at most once per {@link PlurimaConsumer} instance, on the poll thread,
     * AFTER the runtime has transitioned to {@link PlurimaConsumer.State#FAILED} and
     * initiated its own {@link PlurimaConsumer#close()}. Because it runs on the poll
     * thread during the runtime's own shutdown, the callback must not block — do any
     * slow work (alerting, restart logic, etc.) asynchronously. If the callback throws,
     * the exception is caught and logged; it is never propagated and never prevents the
     * runtime from finishing its self-close.
     *
     * <p>Use {@link PlurimaConsumer#state()} / {@link PlurimaConsumer#isRunning()} to
     * poll liveness elsewhere (e.g. a health-check endpoint) without needing this
     * callback.
     *
     * @since 0.3.0
     */
    public PlurimaConsumerBuilder<K, V> onFatalError(Consumer<Throwable> callback) {
        this.onFatalError = Objects.requireNonNull(callback, "onFatalError");
        return this;
    }

    public PlurimaConsumer<K, V> build() {
        if (kafkaProperties == null) {
            throw new IllegalStateException(
                "kafkaProperties is required — call .kafkaProperties(...) before build()");
        }

        // SHARE engine supports only UNORDERED. KEY/PARTITION on a share group are
        // architecturally instance-local: the broker may deliver same-key or same-partition
        // records to different members, so any in-process shard mechanism orders only this
        // consumer's slice — never the cluster as a whole. Users hit this expectation
        // mismatch and assumed cross-cluster ordering they never had. As of v0.1 the
        // combination is rejected at build() time; users who want cross-cluster KEY or
        // PARTITION ordering must use CLASSIC_BASIC.
        if (engine == ConsumerEngine.SHARE
            && (ordering == OrderingMode.KEY || ordering == OrderingMode.PARTITION)) {
            throw new IllegalArgumentException(
                "engine=SHARE does not support ordering=" + ordering + ". Kafka 4.2 share groups "
                + "deliver any record to any consumer in the group, so an in-process shard "
                + "mechanism cannot guarantee cross-cluster per-key or per-partition FIFO. Use "
                + "engine=CLASSIC_BASIC for cross-cluster KEY/PARTITION ordering, or "
                + "ordering=UNORDERED if you only need share-group throughput.");
        }

        // Per-engine unsupported-config check. The two engines have different config
        // surfaces (e.g. classic accepts auto.offset.reset; share rejects it).
        Set<String> unsupported = switch (engine) {
            case SHARE -> UNSUPPORTED_CONFIGS_SHARE;
            case CLASSIC_BASIC -> UNSUPPORTED_CONFIGS_CLASSIC;
        };
        for (String key : unsupported) {
            if (kafkaProperties.containsKey(key)) {
                throw new IllegalArgumentException(
                    "kafkaProperties contains '" + key + "' which is not supported by Plurima with "
                    + "engine=" + engine + ". " + unsupportedConfigGuidance(key, engine));
            }
        }

        // Accept either String ("50") or numeric (Integer/Long via Properties.put(K,V)) —
        // a direct cast to String would throw ClassCastException for the numeric case before
        // our own IllegalArgumentException could report the actual config problem. String.valueOf
        // covers both shapes; Integer.parseInt then validates that the string is integer-typed.
        Object rawMaxPoll = kafkaProperties.get("max.poll.records");
        if (rawMaxPoll != null) {
            String userMaxPoll = String.valueOf(rawMaxPoll);
            int requested;
            try {
                requested = Integer.parseInt(userMaxPoll);
            } catch (NumberFormatException nfe) {
                throw new IllegalArgumentException(
                    "max.poll.records must be an integer, was '" + userMaxPoll + "'");
            }
            if (requested <= 0) {
                throw new IllegalArgumentException(
                    "max.poll.records must be > 0, was " + requested);
            }
            if (engine == ConsumerEngine.SHARE && requested > concurrency) {
                throw new IllegalArgumentException(
                    "max.poll.records=" + requested + " exceeds concurrency=" + concurrency
                    + ". The excess records would be broker-acquired but not tracked by Plurima's "
                    + "InFlightRegistry, so force-RELEASE on drain-barrier timeout could not "
                    + "release them. Either lower max.poll.records to <= " + concurrency
                    + " or increase concurrency.");
            }
        }

        if (topic == null) {
            throw new IllegalStateException(
                "topic is required — call .topic(...) before build()");
        }
        if (listener != null && manualAckListener != null) {
            throw new IllegalStateException(
                "set either listener or manualAckListener, not both");
        }
        if (listener == null && manualAckListener == null) {
            throw new IllegalStateException(
                "listener (or manualAckListener) is required — call .listener(...), "
                + ".manualAckListener(...), .onMessage(...), or .onMessageAck(...) "
                + "before build()");
        }

        // Engine-specific validation matrix.
        if (engine == ConsumerEngine.CLASSIC_BASIC) {
            if (manualAckListener != null) {
                throw new IllegalArgumentException(
                    "ManualAckListener is not supported with engine=CLASSIC_BASIC. "
                    + "AckContext.acknowledge(RELEASE) has no equivalent in classic consumers "
                    + "(would require pause+seek which blocks the entire partition). Use "
                    + "RecordListener with auto-ack on CLASSIC_BASIC, or stay on engine=SHARE.");
            }
            if (adaptiveBarrierConfig != null) {
                throw new IllegalArgumentException(
                    "adaptiveDrainBarrier is not supported with engine=CLASSIC_BASIC — the classic "
                    + "engine uses a continuous-poll model with no drain barrier. Remove "
                    + ".adaptiveDrainBarrier(...) or use engine=SHARE.");
            }
            if (handlerTimeout != null) {
                throw new IllegalArgumentException(
                    "handlerTimeout is not supported with engine=CLASSIC_BASIC in this release — "
                    + "the classic engine does not wire a per-handler watchdog. Remove "
                    + ".handlerTimeout(...) or use engine=SHARE.");
            }
            // No retry-delay vs max.poll.interval.ms check: the continuous-poll model means
            // the poll thread heartbeats every iteration regardless of how long a worker
            // sleeps for retry. Workers can sleep arbitrarily without fencing the consumer.
        }

        // Resolve ordering guarantee. If user set one, validate against the matrix; otherwise infer.
        OrderingGuarantee effectiveGuarantee = orderingGuarantee != null
            ? orderingGuarantee
            : inferGuarantee(engine, ordering);
        if (orderingGuarantee != null) {
            validateGuarantee(engine, ordering, orderingGuarantee);
        }

        int effectiveShardCount = shardCount == -1 ? concurrency * 4 : shardCount;
        Properties propsCopy = PropertiesSupport.copy(kafkaProperties);
        return new PlurimaConsumer<>(
            propsCopy, topic, listener, manualAckListener,
            keyDeserializer, valueDeserializer,
            engine,
            effectiveGuarantee,
            ordering,
            concurrency,
            concurrencyExplicitlySet,
            effectiveShardCount, retryPolicy, dltConfig,
            pollTimeout, lockDuration, shutdownDrainTimeout, this.metrics,
            adaptiveBarrierConfig, lockDurationExplicitlySet, handlerTimeout, onFatalError);
    }

    /**
     * Maps engine + ordering to the inferred guarantee. See OrderingGuarantee Javadoc.
     *
     * <ul>
     *   <li>SHARE: only UNORDERED reaches this method (KEY/PARTITION are rejected
     *       earlier). UNORDERED has nothing to order, so the inferred guarantee is
     *       LOCAL (vacuous).</li>
     *   <li>CLASSIC_BASIC + UNORDERED: same vacuous case. LOCAL. Inferring STRICT
     *       here would contradict {@link #validateGuarantee} which rejects
     *       {@code UNORDERED + STRICT}.</li>
     *   <li>CLASSIC_BASIC + KEY/PARTITION: STRICT (cross-cluster, via consumer-group
     *       exclusive partition ownership; steady-state strict — see UserGuide
     *       § CLASSIC_BASIC for the rebalance-window caveat).</li>
     * </ul>
     */
    private static OrderingGuarantee inferGuarantee(ConsumerEngine engine, OrderingMode ordering) {
        if (ordering == OrderingMode.UNORDERED) {
            return OrderingGuarantee.LOCAL;
        }
        return switch (engine) {
            case SHARE -> OrderingGuarantee.LOCAL;  // unreachable: SHARE+KEY/PARTITION rejected
            case CLASSIC_BASIC -> OrderingGuarantee.STRICT;
        };
    }

    /**
     * Rejects user-asserted guarantees that the engine cannot deliver. As of v0.1 the
     * only checkable mismatch on this path is {@code (SHARE, UNORDERED, STRICT)} —
     * SHARE+KEY/PARTITION is already rejected earlier in {@link #build()} regardless
     * of the asserted guarantee, since the combination is architecturally invalid.
     * UNORDERED cannot be STRICT on any engine (STRICT means an ordering relation,
     * which UNORDERED disclaims by definition).
     */
    private static void validateGuarantee(
        ConsumerEngine engine, OrderingMode ordering, OrderingGuarantee asserted) {
        if (ordering == OrderingMode.UNORDERED && asserted == OrderingGuarantee.STRICT) {
            throw new IllegalArgumentException(
                "ordering=UNORDERED cannot be paired with orderingGuarantee=STRICT. "
                + "STRICT requires an ordering relation; pick ordering=KEY or PARTITION "
                + "(CLASSIC_BASIC only) if you need a STRICT guarantee.");
        }
    }

    private static String unsupportedConfigGuidance(String key, ConsumerEngine engine) {
        if (engine == ConsumerEngine.SHARE && key.equals("auto.offset.reset")) {
            return "For initial offset configure share.auto.offset.reset broker-side via "
                + "kafka-share-groups.sh --alter --config share.auto.offset.reset=earliest";
        }
        if (engine == ConsumerEngine.CLASSIC_BASIC
            && (key.equals("share.acknowledgement.mode") || key.equals("share.acquire.mode"))) {
            return "This is a share-group-only config; classic consumer groups don't honor it. "
                + "Remove it or switch to engine=SHARE.";
        }
        if (key.equals("enable.auto.commit")) {
            return "Plurima manages offset commits internally after per-record success or DLT routing.";
        }
        return "Plurima manages this configuration internally.";
    }
}
