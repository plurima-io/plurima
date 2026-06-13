package io.plurima.kafka.retry;

import io.plurima.kafka.annotation.Stable;

import java.time.Duration;
import java.util.List;
import java.util.Objects;

@Stable(since = "0.1.0")
public final class RetryPolicy {

    private final int maxAttempts;
    private final ExceptionClassifier classifier;
    private final Duration initialDelay;
    private final double multiplier;
    private final double jitter;

    private RetryPolicy(int maxAttempts, ExceptionClassifier classifier,
                        Duration initialDelay, double multiplier, double jitter) {
        this.maxAttempts = maxAttempts;
        this.classifier = classifier;
        this.initialDelay = initialDelay;
        this.multiplier = multiplier;
        this.jitter = jitter;
    }

    public int maxAttempts() { return maxAttempts; }
    public ExceptionClassifier classifier() { return classifier; }
    public double jitter() { return jitter; }

    /**
     * Base delay before the next retry given the number of already-completed attempts.
     * {@code attempt} is zero-indexed: 0 means "no prior retry has happened, next is the first".
     */
    public Duration delayFor(int attempt) {
        double factor = Math.pow(multiplier, attempt);
        long ms = Math.max(1, (long) (initialDelay.toMillis() * factor));
        return Duration.ofMillis(ms);
    }

    public static RetryPolicy noRetry() {
        return new RetryPolicy(0, t -> false, Duration.ZERO, 1.0, 0.0);
    }

    public static ExponentialBuilder exponential() {
        return new ExponentialBuilder();
    }

    public static final class ExponentialBuilder {
        private int maxAttempts = 3;
        private Duration initialDelay = Duration.ofMillis(100);
        private double multiplier = 2.0;
        private double jitter = 0.2;
        private ExceptionClassifier classifier;
        private List<Class<? extends Throwable>> retriable = List.of();

        ExponentialBuilder() {}

        public ExponentialBuilder maxAttempts(int maxAttempts) {
            this.maxAttempts = maxAttempts; return this;
        }

        public ExponentialBuilder initialDelay(Duration d) {
            this.initialDelay = Objects.requireNonNull(d, "initialDelay"); return this;
        }

        public ExponentialBuilder multiplier(double m) {
            this.multiplier = m; return this;
        }

        public ExponentialBuilder jitter(double j) {
            this.jitter = j; return this;
        }

        public ExponentialBuilder classifier(ExceptionClassifier c) {
            this.classifier = Objects.requireNonNull(c, "classifier"); return this;
        }

        @SafeVarargs
        @SuppressWarnings("varargs")
        public final ExponentialBuilder retryOn(Class<? extends Throwable>... classes) {
            this.retriable = List.of(classes);
            return this;
        }

        public RetryPolicy build() {
            if (maxAttempts < 0) {
                throw new IllegalArgumentException("maxAttempts must be >= 0, was " + maxAttempts);
            }
            // Reject non-positive initialDelay at build time. Previously delayFor() silently
            // clamped to 1ms; a user setting initialDelay(Duration.ZERO) by mistake would get
            // surprise behavior. Be loud at construction instead.
            if (initialDelay.isNegative() || initialDelay.isZero()) {
                throw new IllegalArgumentException(
                    "initialDelay must be positive, was " + initialDelay);
            }
            // Reject NaN/Infinity explicitly. Plain range comparisons (multiplier < 1.0,
            // jitter < 0.0 || jitter > 1.0) silently pass NaN since every comparison with
            // NaN is false; Double.POSITIVE_INFINITY also satisfies multiplier >= 1.0 and
            // then overflows downstream in RetryEngine.delayFor's exponential computation.
            // Check finiteness BEFORE the range checks so the error message is clear.
            if (!Double.isFinite(multiplier)) {
                throw new IllegalArgumentException(
                    "multiplier must be a finite number, was " + multiplier);
            }
            if (multiplier < 1.0) {
                throw new IllegalArgumentException("multiplier must be >= 1.0, was " + multiplier);
            }
            if (!Double.isFinite(jitter)) {
                throw new IllegalArgumentException(
                    "jitter must be a finite number, was " + jitter);
            }
            if (jitter < 0.0 || jitter > 1.0) {
                throw new IllegalArgumentException("jitter must be in [0.0, 1.0], was " + jitter);
            }
            ExceptionClassifier effective = classifier;
            if (effective == null) {
                List<Class<? extends Throwable>> classes = retriable;
                effective = t -> {
                    for (Class<? extends Throwable> c : classes) {
                        if (c.isInstance(t)) return true;
                    }
                    return false;
                };
            }
            return new RetryPolicy(maxAttempts, effective, initialDelay, multiplier, jitter);
        }
    }
}
