package io.plurima.kafka.internal;

import io.plurima.kafka.retry.RetryDecision;
import io.plurima.kafka.retry.RetryPolicy;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

class RetryEngineTest {

    private static InFlightRecord<byte[], byte[]> recAt(int attempt) {
        InFlightRecord<byte[], byte[]> r = new InFlightRecord<>(new ConsumerRecord<>("t", 0, 1L, new byte[0], new byte[0]));
        for (int i = 0; i < attempt; i++) r.incrementAttempt();
        return r;
    }

    @Test
    void rejectsWhenClassifierSaysNo() {
        RetryEngine engine = new RetryEngine(RetryPolicy.exponential()
            .maxAttempts(3)
            .initialDelay(Duration.ofMillis(10))
            .retryOn(IOException.class)
            .build());

        RetryDecision d = engine.evaluate(recAt(0), new IllegalArgumentException("nope"));

        assertThat(d).isInstanceOf(RetryDecision.Reject.class);
    }

    @Test
    void exhaustedAtMaxAttempts() {
        RetryEngine engine = new RetryEngine(RetryPolicy.exponential()
            .maxAttempts(3)
            .initialDelay(Duration.ofMillis(10))
            .retryOn(IOException.class)
            .build());

        RetryDecision d = engine.evaluate(recAt(3), new IOException("still failing"));

        assertThat(d).isInstanceOf(RetryDecision.Exhausted.class);
    }

    @Test
    void shortDelayBecomesInlineRetry() {
        RetryEngine engine = new RetryEngine(RetryPolicy.exponential()
            .maxAttempts(5)
            .initialDelay(Duration.ofMillis(50))
            .multiplier(2.0)
            .jitter(0.0)
            .retryOn(IOException.class)
            .build());

        RetryDecision d = engine.evaluate(recAt(0), new IOException("transient"));

        assertThat(d).isInstanceOf(RetryDecision.RetryInline.class);
        assertThat(((RetryDecision.RetryInline) d).delay())
            .isBetween(Duration.ofMillis(40), Duration.ofMillis(60));
    }

    @Test
    void longDelayBecomesDelayedRetry() {
        RetryEngine engine = new RetryEngine(RetryPolicy.exponential()
            .maxAttempts(10)
            .initialDelay(Duration.ofMillis(500))
            .multiplier(2.0)
            .jitter(0.0)
            .retryOn(IOException.class)
            .build());

        // attempt 2 → 500ms × 2² = 2000ms (delayed, > 1s threshold)
        RetryDecision d = engine.evaluate(recAt(2), new IOException("transient"));

        assertThat(d).isInstanceOf(RetryDecision.RetryDelayed.class);
        assertThat(((RetryDecision.RetryDelayed) d).delay())
            .isGreaterThan(Duration.ofSeconds(1));
    }

    @Test
    void jitterStaysWithinBand() {
        RetryEngine engine = new RetryEngine(RetryPolicy.exponential()
            .maxAttempts(5)
            .initialDelay(Duration.ofMillis(200))
            .multiplier(1.0)
            .jitter(0.5)
            .retryOn(IOException.class)
            .build());

        for (int i = 0; i < 50; i++) {
            RetryDecision d = engine.evaluate(recAt(0), new IOException("t"));
            Duration delay = d instanceof RetryDecision.RetryInline ri
                ? ri.delay()
                : ((RetryDecision.RetryDelayed) d).delay();
            // jitter=0.5 → factor ∈ [0.5, 1.5], so delay ∈ [100ms, 300ms]
            assertThat(delay).isBetween(Duration.ofMillis(90), Duration.ofMillis(310));
        }
    }

    @Test
    void zeroMaxAttemptsAlwaysExhaustedForFirstFailure() {
        RetryEngine engine = new RetryEngine(RetryPolicy.noRetry());

        RetryDecision d = engine.evaluate(recAt(0), new IOException("t"));

        // noRetry()'s classifier returns false → Reject, not Exhausted.
        assertThat(d).isInstanceOf(RetryDecision.Reject.class);
    }

    @Test
    void delayedRetryIsExhaustedWhenBrokerDeliveryCountReachesMaxAttempts() {
        RetryEngine engine = new RetryEngine(RetryPolicy.exponential()
            .maxAttempts(3)
            .initialDelay(Duration.ofMillis(10))
            .retryOn(IOException.class)
            .build());

        // Simulate a record on its 4th broker delivery (deliveryCount=4 means: 1 initial + 3 retries).
        // In-memory attempt is still 0 because each redelivery creates a fresh InFlightRecord.
        // brokerAttempts = deliveryCount - 1 = 3, which satisfies 3 >= maxAttempts(3) → Exhausted.
        InFlightRecord<byte[], byte[]> r = recWithDeliveryCount(0, (short) 4);

        RetryDecision d = engine.evaluate(r, new IOException("still failing"));

        assertThat(d).isInstanceOf(RetryDecision.Exhausted.class);
    }

    private static InFlightRecord<byte[], byte[]> recWithDeliveryCount(int inMemoryAttempt, short deliveryCount) {
        ConsumerRecord<byte[], byte[]> cr = new ConsumerRecord<>(
            "t", 0, 1L,
            ConsumerRecord.NO_TIMESTAMP,
            TimestampType.NO_TIMESTAMP_TYPE,
            0, 0,
            new byte[0], new byte[0],
            new RecordHeaders(),
            Optional.empty(),
            Optional.of(deliveryCount));
        InFlightRecord<byte[], byte[]> r = new InFlightRecord<>(cr);
        for (int i = 0; i < inMemoryAttempt; i++) r.incrementAttempt();
        return r;
    }
}
