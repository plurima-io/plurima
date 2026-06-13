package io.plurima.kafka.integration;

import io.plurima.kafka.AdaptiveBarrierConfig;
import io.plurima.kafka.ConsumerEngine;
import io.plurima.kafka.OrderingMode;
import io.plurima.kafka.PlurimaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static io.plurima.kafka.integration.KafkaIntegrationSupport.byteProducer;
import static io.plurima.kafka.integration.KafkaIntegrationSupport.consumerProps;
import static io.plurima.kafka.integration.KafkaIntegrationSupport.createUniqueTopic;
import static io.plurima.kafka.integration.KafkaIntegrationSupport.deleteTopicQuietly;
import static io.plurima.kafka.integration.KafkaIntegrationSupport.waitForAssignment;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Live check that the adaptive drain barrier (a) actually fires — a straggler that
 * exceeds the adaptive give-up point is force-RELEASEd and redelivered — and (b) loses
 * nothing: every produced record still reaches a terminal state.
 *
 * <h3>Why this is a TWO-PHASE test</h3>
 * The adaptive barrier only engages after {@code ADAPTIVE_MIN_SAMPLES} (100) handler
 * completions have filled the latency window; before that it returns the flat
 * {@code lockDuration}. It also tracks a percentile, so if stragglers make up a
 * meaningful fraction of the window the p99 itself rises to include them and the
 * barrier stops force-releasing them. Both effects mean a naive "send N records, some
 * slow" test never actually exercises the adaptive path. So this test is deliberately
 * staged:
 *
 * <ol>
 *   <li><b>Warm-up:</b> produce {@value #WARMUP} fast (~0ms) records and wait until all
 *       are processed. The window now holds 100+ near-zero samples → adaptive is active
 *       and p99×3 floors at {@code max(1s, pollTimeout)} = 1s.</li>
 *   <li><b>Stragglers:</b> produce {@value #STRAGGLERS} records that each sleep 2s —
 *       above the ~1s adaptive barrier (so the FIRST delivery of each is force-RELEASEd
 *       and redelivered: the adaptive path firing) but below the 30s lockDuration (so it
 *       is the adaptive barrier doing the work, not the flat fallback). The window is
 *       dominated by the 100+ fast samples, so p99 stays ~0 when the stragglers first
 *       arrive — guaranteeing the force-release.</li>
 * </ol>
 *
 * <p>Assertions: no-loss (every produced record processed — the force-released orphan
 * worker still completes and records it) AND at least one redelivery occurred
 * ({@code calls > total}, proving the adaptive barrier fired). We wait for BOTH
 * conditions before asserting, because a force-released original can finish and land in
 * {@code processed} before its broker redelivery is observed in {@code calls}.
 */
@Tag("integration")
class ShareAdaptiveBarrierIntegrationTest {

    private static final int WARMUP = 150;       // > ADAPTIVE_MIN_SAMPLES (100) so adaptive engages
    private static final int STRAGGLERS = 5;     // tiny fraction of the window → p99 stays ~0 on arrival
    private static final int TOTAL = WARMUP + STRAGGLERS;

    @Test
    void adaptiveBarrierForceReleasesStragglerAndLosesNothing() throws Exception {
        String topic = createUniqueTopic("plurima-int-adaptive", 1);
        String groupId = "plurima-int-adaptive-" + UUID.randomUUID();
        java.util.Set<String> processed = ConcurrentHashMap.newKeySet();
        AtomicInteger calls = new AtomicInteger();

        try (PlurimaConsumer<byte[], byte[]> consumer = PlurimaConsumer.<byte[], byte[]>builder()
                .kafkaProperties(consumerProps(groupId))
                .topic(topic)
                .engine(ConsumerEngine.SHARE)
                .ordering(OrderingMode.UNORDERED)
                .concurrency(16)
                .pollTimeout(Duration.ofMillis(200))
                .lockDuration(Duration.ofSeconds(30))   // ceiling well above the ~1s adaptive barrier
                .adaptiveDrainBarrier(AdaptiveBarrierConfig.builder().percentile(0.99).multiplier(3.0).build())
                .listener((rec, ctx) -> {
                    calls.incrementAndGet();
                    String v = new String(rec.value());
                    // Straggler records ("s…") sleep 2s — above the ~1s warmed adaptive
                    // barrier (force-released) but below the 30s lockDuration. Warm-up
                    // records ("w…") return immediately.
                    if (v.startsWith("s")) {
                        try { Thread.sleep(2_000); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
                    }
                    processed.add(v);
                })
                .build()) {

            consumer.start();
            waitForAssignment(groupId, topic);

            try (KafkaProducer<byte[], byte[]> producer = byteProducer()) {
                // Phase 1: warm the latency window past the activation threshold.
                for (int i = 0; i < WARMUP; i++) {
                    producer.send(new ProducerRecord<>(topic, ("kw" + i).getBytes(), ("w" + i).getBytes()));
                }
                producer.flush();
                awaitUntil(() -> processed.size() >= WARMUP, Duration.ofSeconds(60),
                    "warm-up: all " + WARMUP + " fast records processed (window now active)");

                // Phase 2: now that adaptive is active and p99 ~0, send the stragglers.
                for (int i = 0; i < STRAGGLERS; i++) {
                    producer.send(new ProducerRecord<>(topic, ("ks" + i).getBytes(), ("s" + i).getBytes()));
                }
                producer.flush();
            }

            // Wait for BOTH no-loss (all processed) AND a redelivery (calls > total) before
            // asserting — the two become true at different moments.
            awaitUntil(() -> processed.size() == TOTAL && calls.get() > TOTAL, Duration.ofSeconds(90),
                "all " + TOTAL + " processed AND at least one straggler redelivered");

            assertThat(processed)
                .as("no loss: every produced record (warm-up + stragglers) processed at least once")
                .hasSize(TOTAL);
            assertThat(calls.get())
                .as("adaptive barrier must have force-RELEASEd at least one straggler, "
                    + "causing a redelivery (total invocations > distinct records)")
                .isGreaterThan(TOTAL);
        } finally {
            deleteTopicQuietly(topic);
        }
    }

    /** Poll {@code condition} until true or the timeout elapses; fail with {@code what} on timeout. */
    private static void awaitUntil(java.util.function.BooleanSupplier condition, Duration timeout, String what)
            throws InterruptedException {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) return;
            Thread.sleep(200);
        }
        throw new AssertionError("Timed out after " + timeout + " waiting for: " + what);
    }
}
