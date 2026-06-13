package io.plurima.kafka;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class AdaptiveBarrierConfigTest {

    @Test
    void defaultsAreP99TimesThree() {
        AdaptiveBarrierConfig c = AdaptiveBarrierConfig.defaults();
        assertThat(c.percentile()).isEqualTo(0.99);
        assertThat(c.multiplier()).isEqualTo(3.0);
    }

    @Test
    void customValuesRetained() {
        AdaptiveBarrierConfig c = AdaptiveBarrierConfig.builder()
            .percentile(0.95).multiplier(2.0).build();
        assertThat(c.percentile()).isEqualTo(0.95);
        assertThat(c.multiplier()).isEqualTo(2.0);
    }

    @Test
    void rejectsPercentileOutsideOpenUnitInterval() {
        assertThatThrownBy(() -> AdaptiveBarrierConfig.builder().percentile(0.0).build())
            .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("percentile");
        assertThatThrownBy(() -> AdaptiveBarrierConfig.builder().percentile(1.0).build())
            .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("percentile");
        assertThatThrownBy(() -> AdaptiveBarrierConfig.builder().percentile(1.5).build())
            .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsMultiplierBelowOneOrNonFinite() {
        assertThatThrownBy(() -> AdaptiveBarrierConfig.builder().multiplier(0.5).build())
            .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("multiplier");
        assertThatThrownBy(() -> AdaptiveBarrierConfig.builder().multiplier(Double.NaN).build())
            .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> AdaptiveBarrierConfig.builder().multiplier(Double.POSITIVE_INFINITY).build())
            .isInstanceOf(IllegalArgumentException.class);
    }
}
