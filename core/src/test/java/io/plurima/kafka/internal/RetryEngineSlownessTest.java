package io.plurima.kafka.internal;

import io.plurima.kafka.retry.RetryPolicy;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies Pillar 2: slowness-driven force-RELEASE redeliveries must NOT count toward retry
 * exhaustion (otherwise a slow-but-healthy record is wrongly routed to the DLT), while genuine
 * failure-driven retries still do.
 */
class RetryEngineSlownessTest {

    private static final RecordCoord COORD = new RecordCoord("t", 0, 1L);

    private static InFlightRecord<byte[], byte[]> recWithDeliveryCount(short deliveryCount) {
        ConsumerRecord<byte[], byte[]> cr = new ConsumerRecord<>(
            "t", 0, 1L,
            ConsumerRecord.NO_TIMESTAMP, TimestampType.NO_TIMESTAMP_TYPE,
            0, 0, new byte[0], new byte[0],
            new RecordHeaders(), Optional.empty(), Optional.of(deliveryCount));
        return new InFlightRecord<>(cr);
    }

    private static RetryPolicy policy() {
        return RetryPolicy.exponential()
            .maxAttempts(3).initialDelay(Duration.ofMillis(10)).retryOn(IOException.class).build();
    }

    @Test
    void slownessRedeliveriesDoNotExhaustBudget() {
        SlownessReleaseTracker tracker = new SlownessReleaseTracker(1024);
        // The record has been redelivered to deliveryCount=4, but ALL 3 redeliveries were
        // slowness-driven force-RELEASEs (not failures).
        tracker.recordRelease(COORD);
        tracker.recordRelease(COORD);
        tracker.recordRelease(COORD);
        RetryEngine engine = new RetryEngine(policy(), tracker);

        // brokerAttempts = (4-1) - 3 = 0 → effective attempt 0 < maxAttempts(3) → still retriable.
        RetryDecision d = engine.evaluate(recWithDeliveryCount((short) 4), new IOException("slow, not failing"));

        assertThat(d).isNotInstanceOf(RetryDecision.Exhausted.class);
    }

    @Test
    void withoutTrackerSameRecordWouldExhaust() {
        // Regression guard / baseline: no tracker (or zero slowness) → deliveryCount drives
        // exhaustion exactly as before Pillar 2.
        RetryEngine engine = new RetryEngine(policy());
        RetryDecision d = engine.evaluate(recWithDeliveryCount((short) 4), new IOException("failing"));
        assertThat(d).isInstanceOf(RetryDecision.Exhausted.class);
    }

    @Test
    void genuineFailuresStillCountWithSomeSlowness() {
        SlownessReleaseTracker tracker = new SlownessReleaseTracker(1024);
        // deliveryCount=4 = 1 initial + 3 redeliveries, of which only 1 was slowness; the other
        // 2 were genuine failure retries. brokerAttempts = 3 - 1 = 2 → still < 3 → retriable.
        tracker.recordRelease(COORD);
        RetryEngine engine = new RetryEngine(policy(), tracker);

        RetryDecision two = engine.evaluate(recWithDeliveryCount((short) 4), new IOException("x"));
        assertThat(two).isNotInstanceOf(RetryDecision.Exhausted.class);

        // One more genuine failure (deliveryCount=5, slowness still 1) → brokerAttempts = 4-1 = 3
        // → 3 >= maxAttempts(3) → exhausted. Genuine failures are NOT masked by the slowness fix.
        RetryDecision three = engine.evaluate(recWithDeliveryCount((short) 5), new IOException("x"));
        assertThat(three).isInstanceOf(RetryDecision.Exhausted.class);
    }
}
