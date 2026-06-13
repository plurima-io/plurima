package io.plurima.kafka;

import io.plurima.kafka.annotation.Stable;

/**
 * Tuning for the SHARE engine's adaptive drain barrier. When enabled via
 * {@link PlurimaConsumerBuilder#adaptiveDrainBarrier(AdaptiveBarrierConfig)}, the
 * drain barrier force-RELEASEs straggler records at
 * {@code clamp(p<percentile> × multiplier, max(floor, pollTimeout), lockDuration)}
 * instead of waiting a flat {@code lockDuration}. SHARE engine only — rejected at
 * build time on {@code CLASSIC_BASIC} (no drain barrier there).
 *
 * @since 0.4.0
 */
@Stable(since = "0.4.0")
public final class AdaptiveBarrierConfig {

    private final double percentile;
    private final double multiplier;

    private AdaptiveBarrierConfig(double percentile, double multiplier) {
        this.percentile = percentile;
        this.multiplier = multiplier;
    }

    /** Handler-latency quantile the barrier tracks (e.g. 0.99). */
    public double percentile() { return percentile; }

    /** Safety factor applied to the tracked quantile to set the give-up point. */
    public double multiplier() { return multiplier; }

    /** Config with the recommended defaults: p99 × 3. */
    public static AdaptiveBarrierConfig defaults() {
        return builder().build();
    }

    public static Builder builder() { return new Builder(); }

    public static final class Builder {
        private double percentile = 0.99;
        private double multiplier = 3.0;

        Builder() {}

        /** @param p quantile in (0.0, 1.0) */
        public Builder percentile(double p) { this.percentile = p; return this; }

        /** @param m safety factor, must be a finite value >= 1.0 */
        public Builder multiplier(double m) { this.multiplier = m; return this; }

        public AdaptiveBarrierConfig build() {
            if (!(percentile > 0.0 && percentile < 1.0)) {
                throw new IllegalArgumentException(
                    "percentile must be in (0.0, 1.0), was " + percentile);
            }
            if (!Double.isFinite(multiplier) || multiplier < 1.0) {
                throw new IllegalArgumentException(
                    "multiplier must be a finite number >= 1.0, was " + multiplier);
            }
            return new AdaptiveBarrierConfig(percentile, multiplier);
        }
    }
}
