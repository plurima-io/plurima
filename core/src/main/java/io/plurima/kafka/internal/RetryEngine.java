package io.plurima.kafka.internal;

import io.plurima.kafka.annotation.Internal;
import io.plurima.kafka.retry.RetryDecision;
import io.plurima.kafka.retry.RetryPolicy;

import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;

@Internal
public final class RetryEngine {

    /** Per ADR-007: inline up to 1 second, delayed beyond that. */
    static final Duration INLINE_THRESHOLD = Duration.ofSeconds(1);

    private final RetryPolicy policy;
    private final SlownessReleaseTracker slownessTracker; // nullable — null = subtract 0

    public RetryEngine(RetryPolicy policy) {
        this(policy, null);
    }

    public RetryEngine(RetryPolicy policy, SlownessReleaseTracker slownessTracker) {
        this.policy = policy;
        this.slownessTracker = slownessTracker;
    }

    public RetryDecision evaluate(InFlightRecord<?, ?> r, Throwable error) {
        if (!policy.classifier().isRetriable(error)) {
            return new RetryDecision.Reject(error);
        }
        // Effective attempt count: max of our in-memory inline-retry counter and the broker's
        // delivery count (minus 1 to skip the initial delivery). This bounds BOTH inline retries
        // AND delayed retries (where each broker redelivery is itself a new attempt).
        //
        // Subtract slowness-driven force-RELEASEs: those redeliveries were caused by Plurima
        // abandoning a slow-but-healthy record at the drain barrier, NOT by a failure, so they
        // must not consume the retry budget (otherwise a merely-slow record is wrongly DLT'd).
        // Clamp at 0 — slowness count can briefly exceed deliveryCount-1 across the redelivery race.
        int slownessReleases = slownessTracker == null ? 0 : slownessTracker.releaseCount(r.coord());
        int brokerAttempts = Math.max(0,
            (r.consumerRecord().deliveryCount().orElse((short) 1) - 1) - slownessReleases);
        int attempt = Math.max(r.attempt(), brokerAttempts);
        if (attempt >= policy.maxAttempts()) {
            return new RetryDecision.Exhausted(error);
        }
        Duration base = policy.delayFor(attempt);
        Duration jittered = applyJitter(base, policy.jitter());

        if (jittered.compareTo(INLINE_THRESHOLD) <= 0) {
            return new RetryDecision.RetryInline(jittered);
        }
        return new RetryDecision.RetryDelayed(jittered);
    }

    private Duration applyJitter(Duration delay, double jitter) {
        if (jitter == 0.0) return delay;
        double factor = 1.0 - jitter + (2.0 * jitter * ThreadLocalRandom.current().nextDouble());
        long ms = Math.max(1, (long) (delay.toMillis() * factor));
        return Duration.ofMillis(ms);
    }
}
