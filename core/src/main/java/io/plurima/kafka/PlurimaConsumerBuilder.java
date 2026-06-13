package io.plurima.kafka;

import io.plurima.kafka.ack.ManualAckListener;
import io.plurima.kafka.annotation.Stable;
import io.plurima.kafka.deserializer.RecordDeserializer;
import io.plurima.kafka.dlt.DltConfig;
import io.plurima.kafka.metrics.PlurimaMetrics;
import io.plurima.kafka.retry.RetryPolicy;

import java.time.Duration;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;

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
    @SuppressWarnings("unchecked")
    private RecordDeserializer<K> keyDeserializer =
        (RecordDeserializer<K>) RecordDeserializer.bytes();
    @SuppressWarnings("unchecked")
    private RecordDeserializer<V> valueDeserializer =
        (RecordDeserializer<V>) RecordDeserializer.bytes();
    private ManualAckListener<K, V> manualAckListener;
    private ConsumerEngine engine = ConsumerEngine.SHARE;
    private OrderingGuarantee orderingGuarantee;  // null = infer from engine + ordering
    private OrderingMode ordering = OrderingMode.UNORDERED;
    private int concurrency = 50;
    private boolean concurrencyExplicitlySet = false;
    private Duration pollTimeout = Duration.ofSeconds(1);
    private Duration lockDuration = Duration.ofSeconds(30);
    private Duration shutdownDrainTimeout = Duration.ofSeconds(30);
    private int shardCount = -1; // -1 means "derive from concurrency × 4 at build time"
    private RetryPolicy retryPolicy = RetryPolicy.noRetry();
    private DltConfig dltConfig; // null = no DLT (retry exhaustion → REJECT with error log)
    private AdaptiveBarrierConfig adaptiveBarrierConfig; // null = disabled
    private PlurimaMetrics metrics = PlurimaMetrics.noOp();

    PlurimaConsumerBuilder() {}

    public PlurimaConsumerBuilder<K, V> kafkaProperties(Properties props) {
        Objects.requireNonNull(props, "kafkaProperties");
        this.kafkaProperties = io.plurima.kafka.internal.PropertiesCopy.copy(props);
        return this;
    }

    public PlurimaConsumerBuilder<K, V> topic(String topic) {
        this.topic = topic; return this;
    }

    public PlurimaConsumerBuilder<K, V> listener(RecordListener<K, V> listener) {
        this.listener = listener; return this;
    }

    public PlurimaConsumerBuilder<K, V> keyDeserializer(RecordDeserializer<K> deser) {
        this.keyDeserializer = Objects.requireNonNull(deser, "keyDeserializer");
        return this;
    }

    public PlurimaConsumerBuilder<K, V> valueDeserializer(RecordDeserializer<V> deser) {
        this.valueDeserializer = Objects.requireNonNull(deser, "valueDeserializer");
        return this;
    }

    public PlurimaConsumerBuilder<K, V> manualAckListener(ManualAckListener<K, V> listener) {
        this.manualAckListener = Objects.requireNonNull(listener, "manualAckListener");
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

    public PlurimaConsumerBuilder<K, V> ordering(OrderingMode ordering) {
        this.ordering = Objects.requireNonNull(ordering, "ordering");
        return this;
    }

    public PlurimaConsumerBuilder<K, V> concurrency(int concurrency) {
        this.concurrency = concurrency;
        this.concurrencyExplicitlySet = true;
        return this;
    }

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
     * <p><b>Set this BELOW</b> the broker's {@code group.share.record.lock.duration.ms}
     * — typically ~80% of it. The point is to beat the broker's expiry, so a value
     * equal to or larger than the broker's gives no early-recovery benefit and Plurima
     * logs an error at startup once the broker reports its real value (queried via
     * {@code KafkaShareConsumer.acquisitionLockTimeoutMs}).
     *
     * <p>Default 30 s. The broker's default is 30 s in some Kafka 4.x builds and 60 s in
     * others; check your broker config before relying on the default.
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
        return this;
    }

    public PlurimaConsumerBuilder<K, V> shutdownDrainTimeout(Duration t) {
        Objects.requireNonNull(t, "shutdownDrainTimeout");
        if (t.isZero() || t.isNegative()) {
            throw new IllegalArgumentException("shutdownDrainTimeout must be > 0, was " + t);
        }
        this.shutdownDrainTimeout = t;
        return this;
    }

    public PlurimaConsumerBuilder<K, V> shardCount(int shardCount) {
        if (shardCount <= 0) {
            throw new IllegalArgumentException("shardCount must be > 0, was " + shardCount);
        }
        this.shardCount = shardCount; return this;
    }

    public PlurimaConsumerBuilder<K, V> retry(RetryPolicy policy) {
        this.retryPolicy = Objects.requireNonNull(policy, "retryPolicy");
        return this;
    }

    public PlurimaConsumerBuilder<K, V> deadLetterTopic(DltConfig dltConfig) {
        this.dltConfig = Objects.requireNonNull(dltConfig, "dltConfig");
        return this;
    }

    /** Enable the SHARE-engine adaptive drain barrier with default tuning (p99 × 3). */
    public PlurimaConsumerBuilder<K, V> adaptiveDrainBarrier() {
        return adaptiveDrainBarrier(AdaptiveBarrierConfig.defaults());
    }

    /** Enable the SHARE-engine adaptive drain barrier with explicit tuning. */
    public PlurimaConsumerBuilder<K, V> adaptiveDrainBarrier(AdaptiveBarrierConfig config) {
        this.adaptiveBarrierConfig = Objects.requireNonNull(config, "adaptiveBarrierConfig");
        return this;
    }

    public PlurimaConsumerBuilder<K, V> metrics(PlurimaMetrics metrics) {
        this.metrics = Objects.requireNonNull(metrics, "metrics");
        return this;
    }

    public PlurimaConsumer<K, V> build() {
        Objects.requireNonNull(kafkaProperties, "kafkaProperties");

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

        Objects.requireNonNull(topic, "topic");
        if (topic.isBlank()) {
            throw new IllegalArgumentException(
                "topic must not be blank — was '" + topic + "'");
        }
        if (listener != null && manualAckListener != null) {
            throw new IllegalStateException(
                "set either listener or manualAckListener, not both");
        }
        if (listener == null && manualAckListener == null) {
            throw new NullPointerException("listener (or manualAckListener) is required");
        }
        if (concurrency <= 0) {
            throw new IllegalArgumentException("concurrency must be > 0");
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
        Properties propsCopy = io.plurima.kafka.internal.PropertiesCopy.copy(kafkaProperties);
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
            adaptiveBarrierConfig);
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
