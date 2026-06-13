package io.plurima.kafka;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PlurimaConsumerBuilderTest {

    @Test
    void orderingRejectsNull() {
        assertThatThrownBy(() ->
            PlurimaConsumer.<byte[], byte[]>builder()
                .kafkaProperties(new Properties())
                .topic("t")
                .listener((r, ctx) -> {})
                .ordering(null)
                .build()
        ).isInstanceOf(NullPointerException.class)
         .hasMessageContaining("ordering");
    }

    @Test
    void startAfterCloseIsRejected() {
        PlurimaConsumer<byte[], byte[]> c = PlurimaConsumer.<byte[], byte[]>builder()
            .kafkaProperties(new Properties())
            .topic("t")
            .listener((r, ctx) -> {})
            .build();

        // close() before start() is allowed (idempotent / safe). But re-start after close
        // MUST fail, otherwise close() becomes a no-op on the second close (closed=true
        // already) and leaks the poll thread / consumer started by the re-start().
        c.close();
        assertThatThrownBy(c::start)
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("closed");
    }


    @Test
    void acceptsKeyOrderingModeOnClassicEngine() {
        // KEY ordering is CLASSIC_BASIC only as of v0.1 — the default SHARE engine
        // rejects it (see rejectsShareEngineWithKeyOrdering below).
        PlurimaConsumer<byte[], byte[]> c = PlurimaConsumer.<byte[], byte[]>builder()
            .kafkaProperties(new Properties())
            .topic("t")
            .listener((r, ctx) -> {})
            .engine(ConsumerEngine.CLASSIC_BASIC)
            .ordering(OrderingMode.KEY)
            .build();
        assertThat(c).isNotNull();
    }

    @Test
    void rejectsBlankTopic() {
        assertThatThrownBy(() ->
            PlurimaConsumer.<byte[], byte[]>builder()
                .kafkaProperties(new Properties())
                .topic("   ")
                .listener((r, ctx) -> {})
                .build()
        ).isInstanceOf(IllegalArgumentException.class)
         .hasMessageContaining("topic must not be blank");
    }

    @Test
    void engineDefaultsToShare() {
        // Back-compat: existing builders without .engine(...) keep the SHARE engine.
        // With v0.1's SHARE-is-UNORDERED-only invariant, the bare builder defaults to
        // UNORDERED so this build still succeeds — proving the default path stays valid.
        PlurimaConsumer<byte[], byte[]> c = PlurimaConsumer.<byte[], byte[]>builder()
            .kafkaProperties(new Properties())
            .topic("t")
            .listener((r, ctx) -> {})
            .build();
        assertThat(c).isNotNull();
    }

    @Test
    void rejectsShareEngineWithKeyOrdering() {
        // v0.1: SHARE engine supports only UNORDERED. KEY ordering on a share group is
        // architecturally instance-local; we reject it at build() so users don't get
        // a false cross-cluster-FIFO expectation.
        assertThatThrownBy(() ->
            PlurimaConsumer.<byte[], byte[]>builder()
                .kafkaProperties(new Properties())
                .topic("t")
                .listener((r, ctx) -> {})
                .engine(ConsumerEngine.SHARE)
                .ordering(OrderingMode.KEY)
                .build()
        ).isInstanceOf(IllegalArgumentException.class)
         .hasMessageContaining("engine=SHARE does not support ordering=KEY")
         .hasMessageContaining("CLASSIC_BASIC");
    }

    @Test
    void rejectsShareEngineWithPartitionOrdering() {
        assertThatThrownBy(() ->
            PlurimaConsumer.<byte[], byte[]>builder()
                .kafkaProperties(new Properties())
                .topic("t")
                .listener((r, ctx) -> {})
                .engine(ConsumerEngine.SHARE)
                .ordering(OrderingMode.PARTITION)
                .build()
        ).isInstanceOf(IllegalArgumentException.class)
         .hasMessageContaining("engine=SHARE does not support ordering=PARTITION")
         .hasMessageContaining("CLASSIC_BASIC");
    }

    @Test
    void shareWithKeyIsRejectedRegardlessOfGuarantee() {
        // The SHARE+KEY rejection is unconditional — the LOCAL escape hatch users had
        // pre-v0.1 (asserting LOCAL to opt into instance-local-only ordering) is gone
        // because the "ordering" was never load-bearing on the cluster anyway.
        assertThatThrownBy(() ->
            PlurimaConsumer.<byte[], byte[]>builder()
                .kafkaProperties(new Properties())
                .topic("t")
                .listener((r, ctx) -> {})
                .engine(ConsumerEngine.SHARE)
                .ordering(OrderingMode.KEY)
                .orderingGuarantee(OrderingGuarantee.LOCAL)
                .build()
        ).isInstanceOf(IllegalArgumentException.class)
         .hasMessageContaining("engine=SHARE does not support ordering=KEY");
    }

    @Test
    void allowsClassicEngineWithStrictGuarantee() {
        // CLASSIC_BASIC + STRICT is the explicit "I want cross-cluster ordering" path.
        // Build succeeds; the runtime would start a real KafkaConsumer if we called
        // start() — this test exercises the builder validation only.
        PlurimaConsumer<byte[], byte[]> c = PlurimaConsumer.<byte[], byte[]>builder()
            .kafkaProperties(new Properties())
            .topic("t")
            .listener((r, ctx) -> {})
            .engine(ConsumerEngine.CLASSIC_BASIC)
            .ordering(OrderingMode.PARTITION)
            .orderingGuarantee(OrderingGuarantee.STRICT)
            .build();
        assertThat(c).isNotNull();
    }

    @Test
    void rejectsClassicUnorderedWithStrictGuarantee() {
        // UNORDERED has nothing to order. STRICT requires an ordering relation. Asserting
        // STRICT on UNORDERED is a programming error and must be rejected — for ANY
        // engine, not just SHARE.
        assertThatThrownBy(() ->
            PlurimaConsumer.<byte[], byte[]>builder()
                .kafkaProperties(new Properties())
                .topic("t")
                .listener((r, ctx) -> {})
                .engine(ConsumerEngine.CLASSIC_BASIC)
                .ordering(OrderingMode.UNORDERED)
                .orderingGuarantee(OrderingGuarantee.STRICT)
                .build()
        ).isInstanceOf(IllegalArgumentException.class)
         .hasMessageContaining("ordering=UNORDERED cannot be paired with orderingGuarantee=STRICT");
    }

    @Test
    void allowsClassicUnorderedWithLocalGuarantee() {
        // The flip side of the above: UNORDERED + LOCAL is the inferred default. Asserting
        // it explicitly should be allowed (self-documenting; no contradiction).
        PlurimaConsumer<byte[], byte[]> c = PlurimaConsumer.<byte[], byte[]>builder()
            .kafkaProperties(new Properties())
            .topic("t")
            .listener((r, ctx) -> {})
            .engine(ConsumerEngine.CLASSIC_BASIC)
            .ordering(OrderingMode.UNORDERED)
            .orderingGuarantee(OrderingGuarantee.LOCAL)
            .build();
        assertThat(c).isNotNull();
    }

    @Test
    void rejectsManualAckOnClassicEngine() {
        // ManualAckListener.acknowledge(RELEASE) has no classic equivalent; reject at build.
        assertThatThrownBy(() ->
            PlurimaConsumer.<byte[], byte[]>builder()
                .kafkaProperties(new Properties())
                .topic("t")
                .manualAckListener((r, ack) -> {})
                .engine(ConsumerEngine.CLASSIC_BASIC)
                .build()
        ).isInstanceOf(IllegalArgumentException.class)
         .hasMessageContaining("ManualAckListener is not supported with engine=CLASSIC_BASIC");
    }

    @Test
    void rejectsShareOnlyConfigsOnClassicEngine() {
        Properties props = new Properties();
        props.put("share.acknowledgement.mode", "explicit");
        assertThatThrownBy(() ->
            PlurimaConsumer.<byte[], byte[]>builder()
                .kafkaProperties(props)
                .topic("t")
                .listener((r, ctx) -> {})
                .engine(ConsumerEngine.CLASSIC_BASIC)
                .build()
        ).isInstanceOf(IllegalArgumentException.class)
         .hasMessageContaining("share.acknowledgement.mode");
    }

    @Test
    void allowsAutoOffsetResetOnClassicEngine() {
        // auto.offset.reset is rejected on SHARE (KafkaShareConsumer's own ConfigException)
        // but ALLOWED on classic — it's a legitimate classic-consumer config.
        Properties props = new Properties();
        props.put("auto.offset.reset", "earliest");
        PlurimaConsumer<byte[], byte[]> c = PlurimaConsumer.<byte[], byte[]>builder()
            .kafkaProperties(props)
            .topic("t")
            .listener((r, ctx) -> {})
            .engine(ConsumerEngine.CLASSIC_BASIC)
            .build();
        assertThat(c).isNotNull();
    }

    @Test
    void classicEngineAcceptsArbitrarilyLongRetryDelays() {
        // Pre-v0.1 the builder rejected retry policies whose worst-case delay exceeded
        // max.poll.interval.ms / 2. With the v0.1 continuous-poll model the poll thread
        // heartbeats every iteration regardless of worker sleep duration, so this cap
        // is gone — any delay the user wants is allowed. The worker still sleeps that
        // long, but the broker doesn't fence the consumer.
        io.plurima.kafka.retry.RetryPolicy bigDelay =
            io.plurima.kafka.retry.RetryPolicy.exponential()
                .maxAttempts(3)
                .initialDelay(java.time.Duration.ofSeconds(10))
                .multiplier(10.0)
                .jitter(0.0)
                .retryOn(java.io.IOException.class)
                .build();  // worst case 1000s — would have been rejected pre-v0.1

        PlurimaConsumer<byte[], byte[]> c = PlurimaConsumer.<byte[], byte[]>builder()
            .kafkaProperties(new Properties())
            .topic("t")
            .listener((r, ctx) -> {})
            .engine(ConsumerEngine.CLASSIC_BASIC)
            .retry(bigDelay)
            .build();
        assertThat(c).isNotNull();
    }

    @Test
    void classicEngineAcceptsRetryPolicyWithinMaxPollIntervalHalf() {
        // Short retry policy still works (regression check that the relaxed validation
        // didn't accidentally introduce a new rejection on the short path).
        io.plurima.kafka.retry.RetryPolicy okPolicy =
            io.plurima.kafka.retry.RetryPolicy.exponential()
                .maxAttempts(3)
                .initialDelay(java.time.Duration.ofSeconds(1))
                .multiplier(2.0)
                .jitter(0.0)
                .retryOn(java.io.IOException.class)
                .build();

        PlurimaConsumer<byte[], byte[]> c = PlurimaConsumer.<byte[], byte[]>builder()
            .kafkaProperties(new Properties())
            .topic("t")
            .listener((r, ctx) -> {})
            .engine(ConsumerEngine.CLASSIC_BASIC)
            .retry(okPolicy)
            .build();
        assertThat(c).isNotNull();
    }

    @Test
    void classicEngineBuildSucceeds() {
        // Phase C: the runtime is implemented but starting it requires a real broker.
        // This test just verifies the build path is clean; the start path is exercised
        // by ClassicBasicHappyPathIntegrationTest under -PintegrationTests=true.
        PlurimaConsumer<byte[], byte[]> c = PlurimaConsumer.<byte[], byte[]>builder()
            .kafkaProperties(new Properties())
            .topic("t")
            .listener((r, ctx) -> {})
            .engine(ConsumerEngine.CLASSIC_BASIC)
            .build();
        assertThat(c).isNotNull();
    }

    @Test
    void requiresKafkaProperties() {
        assertThatThrownBy(() ->
            PlurimaConsumer.<byte[], byte[]>builder()
                .topic("t")
                .listener((r, ctx) -> {})
                .build()
        ).isInstanceOf(NullPointerException.class)
         .hasMessageContaining("kafkaProperties");
    }

    @Test
    void requiresTopic() {
        assertThatThrownBy(() ->
            PlurimaConsumer.<byte[], byte[]>builder()
                .kafkaProperties(new Properties())
                .listener((r, ctx) -> {})
                .build()
        ).isInstanceOf(NullPointerException.class)
         .hasMessageContaining("topic");
    }

    @Test
    void rejectsZeroConcurrency() {
        assertThatThrownBy(() ->
            PlurimaConsumer.<byte[], byte[]>builder()
                .kafkaProperties(new Properties())
                .topic("t")
                .listener((r, ctx) -> {})
                .concurrency(0)
                .build()
        ).isInstanceOf(IllegalArgumentException.class)
         .hasMessageContaining("concurrency");
    }

    @Test
    void rejectsNegativeShardCount() {
        assertThatThrownBy(() ->
            PlurimaConsumer.<byte[], byte[]>builder()
                .kafkaProperties(new Properties())
                .topic("t")
                .listener((r, ctx) -> {})
                .shardCount(-1)
                .build()
        ).isInstanceOf(IllegalArgumentException.class)
         .hasMessageContaining("shardCount");
    }

    @Test
    void rejectsZeroShardCount() {
        assertThatThrownBy(() ->
            PlurimaConsumer.<byte[], byte[]>builder()
                .kafkaProperties(new Properties())
                .topic("t")
                .listener((r, ctx) -> {})
                .shardCount(0)
                .build()
        ).isInstanceOf(IllegalArgumentException.class)
         .hasMessageContaining("shardCount");
    }

    @Test
    void retryDefaultsToNoRetry() {
        PlurimaConsumer<byte[], byte[]> c = PlurimaConsumer.<byte[], byte[]>builder()
            .kafkaProperties(new Properties())
            .topic("t")
            .listener((r, ctx) -> {})
            .build();
        assertThat(c).isNotNull();
    }

    @Test
    void retryAcceptsCustomPolicy() {
        io.plurima.kafka.retry.RetryPolicy policy = io.plurima.kafka.retry.RetryPolicy.exponential()
            .maxAttempts(3)
            .initialDelay(java.time.Duration.ofMillis(50))
            .retryOn(java.io.IOException.class)
            .build();

        PlurimaConsumer<byte[], byte[]> c = PlurimaConsumer.<byte[], byte[]>builder()
            .kafkaProperties(new Properties())
            .topic("t")
            .listener((r, ctx) -> {})
            .retry(policy)
            .build();

        assertThat(c).isNotNull();
    }

    @Test
    void deadLetterTopicAcceptsConfig() {
        Properties dltProps = new Properties();
        dltProps.put("bootstrap.servers", "localhost:9092");
        io.plurima.kafka.dlt.DltConfig dlt = io.plurima.kafka.dlt.DltConfig.builder()
            .producerProperties(dltProps)
            .build();

        PlurimaConsumer<byte[], byte[]> c = PlurimaConsumer.<byte[], byte[]>builder()
            .kafkaProperties(new Properties())
            .topic("t")
            .listener((r, ctx) -> {})
            .deadLetterTopic(dlt)
            .build();

        assertThat(c).isNotNull();
    }

    @Test
    void keyAndValueDeserializersAreOptional() {
        PlurimaConsumer<byte[], byte[]> c = PlurimaConsumer.<byte[], byte[]>builder()
            .kafkaProperties(new Properties())
            .topic("t")
            .listener((r, ctx) -> {})
            .build();
        assertThat(c).isNotNull();
    }

    @Test
    void deserializersCanBeTyped() {
        PlurimaConsumer<String, String> c = PlurimaConsumer.<String, String>builder()
            .kafkaProperties(new Properties())
            .topic("t")
            .keyDeserializer(io.plurima.kafka.deserializer.RecordDeserializer.utf8String())
            .valueDeserializer(io.plurima.kafka.deserializer.RecordDeserializer.utf8String())
            .listener((r, ctx) -> {})
            .build();
        assertThat(c).isNotNull();
    }

    @Test
    void manualAckListenerSupported() {
        PlurimaConsumer<byte[], byte[]> c = PlurimaConsumer.<byte[], byte[]>builder()
            .kafkaProperties(new Properties())
            .topic("t")
            .manualAckListener((r, ack) -> ack.acknowledge(org.apache.kafka.clients.consumer.AcknowledgeType.ACCEPT))
            .build();
        assertThat(c).isNotNull();
    }

    @Test
    void listenerAndManualAckListenerAreMutuallyExclusive() {
        assertThatThrownBy(() -> PlurimaConsumer.<byte[], byte[]>builder()
            .kafkaProperties(new Properties())
            .topic("t")
            .listener((r, ctx) -> {})
            .manualAckListener((r, ack) -> {})
            .build())
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("listener");
    }

    @Test
    void rejectsMaxPollRecordsExceedingConcurrency() {
        Properties props = new Properties();
        props.put("max.poll.records", "500");
        assertThatThrownBy(() -> PlurimaConsumer.<byte[], byte[]>builder()
                .kafkaProperties(props)
                .topic("t")
                .listener((r, ctx) -> {})
                .concurrency(50)
                .build())
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("max.poll.records=500")
            .hasMessageContaining("concurrency=50");
    }

    @Test
    void acceptsMaxPollRecordsEqualToConcurrency() {
        Properties props = new Properties();
        props.put("max.poll.records", "50");
        PlurimaConsumer<byte[], byte[]> c = PlurimaConsumer.<byte[], byte[]>builder()
            .kafkaProperties(props)
            .topic("t")
            .listener((r, ctx) -> {})
            .concurrency(50)
            .build();
        assertThat(c).isNotNull();
    }

    @Test
    void rejectsZeroOrNegativeMaxPollRecords() {
        Properties props = new Properties();
        props.put("max.poll.records", "0");
        assertThatThrownBy(() -> PlurimaConsumer.<byte[], byte[]>builder()
                .kafkaProperties(props)
                .topic("t")
                .listener((r, ctx) -> {})
                .concurrency(50)
                .build())
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("max.poll.records must be > 0");

        Properties propsNeg = new Properties();
        propsNeg.put("max.poll.records", "-5");
        assertThatThrownBy(() -> PlurimaConsumer.<byte[], byte[]>builder()
                .kafkaProperties(propsNeg)
                .topic("t")
                .listener((r, ctx) -> {})
                .concurrency(50)
                .build())
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("max.poll.records must be > 0");
    }

    @Test
    void acceptsNumericMaxPollRecords() {
        // Properties#put(K,V) accepts any Object. A user passing Integer instead of String
        // for max.poll.records used to trip a ClassCastException at builder time. Now we
        // accept either shape and validate by string-parse.
        Properties props = new Properties();
        props.put("max.poll.records", 50);  // Integer, NOT String
        PlurimaConsumer<byte[], byte[]> c = PlurimaConsumer.<byte[], byte[]>builder()
            .kafkaProperties(props)
            .topic("t")
            .listener((r, ctx) -> {})
            .concurrency(50)
            .build();
        assertThat(c).isNotNull();
    }

    @Test
    void rejectsNumericMaxPollRecordsExceedingConcurrency() {
        Properties props = new Properties();
        props.put("max.poll.records", 500);
        assertThatThrownBy(() -> PlurimaConsumer.<byte[], byte[]>builder()
                .kafkaProperties(props)
                .topic("t")
                .listener((r, ctx) -> {})
                .concurrency(50)
                .build())
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("max.poll.records=500")
            .hasMessageContaining("concurrency=50");
    }

    @Test
    void metricsAcceptsCustomImpl() {
        io.plurima.kafka.metrics.PlurimaMetrics m = new io.plurima.kafka.metrics.PlurimaMetrics() {};
        PlurimaConsumer<byte[], byte[]> c = PlurimaConsumer.<byte[], byte[]>builder()
            .kafkaProperties(new Properties())
            .topic("t")
            .listener((r, ctx) -> {})
            .metrics(m)
            .build();
        assertThat(c).isNotNull();
    }

    @Test
    void rejectsUnsupportedAutoOffsetReset() {
        Properties props = new Properties();
        props.put("auto.offset.reset", "earliest");
        assertThatThrownBy(() -> PlurimaConsumer.<byte[], byte[]>builder()
                .kafkaProperties(props)
                .topic("t")
                .listener((r, ctx) -> {})
                .build())
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("auto.offset.reset");
    }

    @Test
    void rejectsEnableAutoCommit() {
        Properties props = new Properties();
        props.put("enable.auto.commit", "true");
        assertThatThrownBy(() -> PlurimaConsumer.<byte[], byte[]>builder()
                .kafkaProperties(props)
                .topic("t")
                .listener((r, ctx) -> {})
                .build())
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("enable.auto.commit");
    }

    @Test
    void rejectsGroupInstanceId() {
        Properties props = new Properties();
        props.put("group.instance.id", "instance-1");
        assertThatThrownBy(() -> PlurimaConsumer.<byte[], byte[]>builder()
                .kafkaProperties(props)
                .topic("t")
                .listener((r, ctx) -> {})
                .build())
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("group.instance.id");
    }

    @Test
    void rejectsIsolationLevel() {
        Properties props = new Properties();
        props.put("isolation.level", "read_committed");
        assertThatThrownBy(() -> PlurimaConsumer.<byte[], byte[]>builder()
                .kafkaProperties(props)
                .topic("t")
                .listener((r, ctx) -> {})
                .build())
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("isolation.level");
    }

    @Test
    void rejectsInterceptorClasses() {
        Properties props = new Properties();
        props.put("interceptor.classes", "com.example.MyInterceptor");
        assertThatThrownBy(() -> PlurimaConsumer.<byte[], byte[]>builder()
                .kafkaProperties(props)
                .topic("t")
                .listener((r, ctx) -> {})
                .build())
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("interceptor.classes");
    }

    @Test
    void rejectsSessionTimeoutMs() {
        Properties props = new Properties();
        props.put("session.timeout.ms", "10000");
        assertThatThrownBy(() -> PlurimaConsumer.<byte[], byte[]>builder()
                .kafkaProperties(props)
                .topic("t")
                .listener((r, ctx) -> {})
                .build())
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("session.timeout.ms");
    }

    // P1.a: defensive copy of Properties
    @Test
    void propsMutationAfterBuildIsIgnored() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        PlurimaConsumer<byte[], byte[]> c = PlurimaConsumer.<byte[], byte[]>builder()
            .kafkaProperties(props)
            .topic("t")
            .listener((r, ctx) -> {})
            .build();
        // Mutate AFTER build — should NOT cause build to retroactively reject
        props.put("enable.auto.commit", "true");
        assertThat(c).isNotNull();
        // The consumer's internal Properties copy is what start() will use, not the user's mutated reference.
    }

    // P1.a: defensive copy at setter — mutation after setter but before build also isolated
    @Test
    void propsMutationAfterSetterIsIgnored() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        PlurimaConsumerBuilder<byte[], byte[]> builder = PlurimaConsumer.<byte[], byte[]>builder()
            .kafkaProperties(props)
            .topic("t")
            .listener((r, ctx) -> {});
        // Mutate AFTER setter — would normally block build() with unsupported config check
        props.put("enable.auto.commit", "true");
        // build() should succeed because the builder holds a defensive copy
        PlurimaConsumer<byte[], byte[]> c = builder.build();
        assertThat(c).isNotNull();
    }

    // P2.a: Duration validation
    @Test
    void rejectsZeroPollTimeout() {
        assertThatThrownBy(() -> PlurimaConsumer.<byte[], byte[]>builder()
                .kafkaProperties(new Properties())
                .topic("t")
                .listener((r, ctx) -> {})
                .pollTimeout(Duration.ZERO)
                .build())
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("pollTimeout");
    }

    @Test
    void rejectsNegativePollTimeout() {
        assertThatThrownBy(() -> PlurimaConsumer.<byte[], byte[]>builder()
                .kafkaProperties(new Properties())
                .topic("t")
                .listener((r, ctx) -> {})
                .pollTimeout(Duration.ofSeconds(-1))
                .build())
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("pollTimeout");
    }

    @Test
    void rejectsNegativeLockDuration() {
        assertThatThrownBy(() -> PlurimaConsumer.<byte[], byte[]>builder()
                .kafkaProperties(new Properties())
                .topic("t")
                .listener((r, ctx) -> {})
                .lockDuration(Duration.ofSeconds(-1))
                .build())
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("lockDuration");
    }

    @Test
    void rejectsZeroLockDuration() {
        assertThatThrownBy(() -> PlurimaConsumer.<byte[], byte[]>builder()
                .kafkaProperties(new Properties())
                .topic("t")
                .listener((r, ctx) -> {})
                .lockDuration(Duration.ZERO)
                .build())
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("lockDuration");
    }

    @Test
    void rejectsZeroShutdownDrainTimeout() {
        assertThatThrownBy(() -> PlurimaConsumer.<byte[], byte[]>builder()
                .kafkaProperties(new Properties())
                .topic("t")
                .listener((r, ctx) -> {})
                .shutdownDrainTimeout(Duration.ZERO)
                .build())
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("shutdownDrainTimeout");
    }

    @Test
    void rejectsNegativeShutdownDrainTimeout() {
        assertThatThrownBy(() -> PlurimaConsumer.<byte[], byte[]>builder()
                .kafkaProperties(new Properties())
                .topic("t")
                .listener((r, ctx) -> {})
                .shutdownDrainTimeout(Duration.ofSeconds(-5))
                .build())
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("shutdownDrainTimeout");
    }

    @Test
    void adaptiveDrainBarrierRejectedOnClassicEngine() {
        java.util.Properties props = new java.util.Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("auto.offset.reset", "earliest");
        assertThatThrownBy(() -> PlurimaConsumer.<byte[], byte[]>builder()
                .kafkaProperties(props)
                .topic("t")
                .listener((r, ctx) -> {})
                .engine(ConsumerEngine.CLASSIC_BASIC)
                .ordering(OrderingMode.PARTITION)
                .adaptiveDrainBarrier()
                .build())
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("adaptiveDrainBarrier")
            .hasMessageContaining("CLASSIC_BASIC");
    }

    @Test
    void adaptiveDrainBarrierAcceptedOnShareEngine() {
        PlurimaConsumer<byte[], byte[]> c = PlurimaConsumer.<byte[], byte[]>builder()
            .kafkaProperties(new java.util.Properties())
            .topic("t")
            .listener((r, ctx) -> {})
            .engine(ConsumerEngine.SHARE)
            .adaptiveDrainBarrier(AdaptiveBarrierConfig.builder().percentile(0.95).multiplier(2.0).build())
            .build();
        assertThat(c).isNotNull();
    }
}
