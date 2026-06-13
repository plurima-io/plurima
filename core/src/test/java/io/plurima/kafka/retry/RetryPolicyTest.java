package io.plurima.kafka.retry;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RetryPolicyTest {

    @Test
    void noRetryIsTheDefault() {
        RetryPolicy p = RetryPolicy.noRetry();
        assertThat(p.maxAttempts()).isEqualTo(0);
        assertThat(p.classifier().isRetriable(new RuntimeException())).isFalse();
    }

    @Test
    void exponentialBuilderProducesExpectedDelays() {
        RetryPolicy p = RetryPolicy.exponential()
            .maxAttempts(5)
            .initialDelay(Duration.ofMillis(100))
            .multiplier(2.0)
            .jitter(0.0)
            .retryOn(IOException.class)
            .build();

        assertThat(p.maxAttempts()).isEqualTo(5);
        assertThat(p.jitter()).isZero();
        assertThat(p.delayFor(0)).isEqualTo(Duration.ofMillis(100));
        assertThat(p.delayFor(1)).isEqualTo(Duration.ofMillis(200));
        assertThat(p.delayFor(2)).isEqualTo(Duration.ofMillis(400));
        assertThat(p.delayFor(3)).isEqualTo(Duration.ofMillis(800));

        assertThat(p.classifier().isRetriable(new IOException("transient"))).isTrue();
        assertThat(p.classifier().isRetriable(new IllegalArgumentException("bug"))).isFalse();
    }

    @Test
    void retryOnAcceptsMultipleClassesAndSubclasses() {
        RetryPolicy p = RetryPolicy.exponential()
            .maxAttempts(3)
            .initialDelay(Duration.ofMillis(10))
            .retryOn(IOException.class, IllegalStateException.class)
            .build();

        assertThat(p.classifier().isRetriable(new IOException())).isTrue();
        assertThat(p.classifier().isRetriable(new java.net.SocketTimeoutException("subclass"))).isTrue();
        assertThat(p.classifier().isRetriable(new IllegalStateException("state"))).isTrue();
        assertThat(p.classifier().isRetriable(new RuntimeException("other"))).isFalse();
    }

    @Test
    void customClassifierOverridesRetryOn() {
        RetryPolicy p = RetryPolicy.exponential()
            .maxAttempts(3)
            .initialDelay(Duration.ofMillis(10))
            .classifier(t -> t.getMessage() != null && t.getMessage().startsWith("retry-"))
            .build();

        assertThat(p.classifier().isRetriable(new RuntimeException("retry-this"))).isTrue();
        assertThat(p.classifier().isRetriable(new IOException("retry-network"))).isTrue();
        assertThat(p.classifier().isRetriable(new RuntimeException("nope"))).isFalse();
    }

    @Test
    void rejectsNonPositiveMultiplier() {
        assertThatThrownBy(() -> RetryPolicy.exponential()
            .maxAttempts(3)
            .initialDelay(Duration.ofMillis(10))
            .multiplier(0.5)
            .build())
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("multiplier");
    }

    @Test
    void rejectsNegativeMaxAttempts() {
        assertThatThrownBy(() -> RetryPolicy.exponential()
            .maxAttempts(-1)
            .initialDelay(Duration.ofMillis(10))
            .build())
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("maxAttempts");
    }

    @Test
    void rejectsZeroOrNegativeInitialDelay() {
        // Previously delayFor() silently clamped non-positive delays to 1ms; build() now
        // refuses the configuration so a user typo (initialDelay(Duration.ZERO)) is loud.
        assertThatThrownBy(() -> RetryPolicy.exponential()
            .maxAttempts(3)
            .initialDelay(Duration.ZERO)
            .build())
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("initialDelay");

        assertThatThrownBy(() -> RetryPolicy.exponential()
            .maxAttempts(3)
            .initialDelay(Duration.ofMillis(-5))
            .build())
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("initialDelay");
    }

    @Test
    void rejectsNonFiniteMultiplierAndJitter() {
        // NaN slips through plain range comparisons (every NaN comparison is false), and
        // Infinity passes multiplier >= 1.0 before overflowing in delayFor. Explicit
        // Double.isFinite checks catch both at build().
        assertThatThrownBy(() -> RetryPolicy.exponential()
            .maxAttempts(3)
            .initialDelay(Duration.ofMillis(10))
            .multiplier(Double.NaN)
            .build())
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("multiplier must be a finite number");

        assertThatThrownBy(() -> RetryPolicy.exponential()
            .maxAttempts(3)
            .initialDelay(Duration.ofMillis(10))
            .multiplier(Double.POSITIVE_INFINITY)
            .build())
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("multiplier must be a finite number");

        assertThatThrownBy(() -> RetryPolicy.exponential()
            .maxAttempts(3)
            .initialDelay(Duration.ofMillis(10))
            .jitter(Double.NaN)
            .build())
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("jitter must be a finite number");

        assertThatThrownBy(() -> RetryPolicy.exponential()
            .maxAttempts(3)
            .initialDelay(Duration.ofMillis(10))
            .jitter(Double.POSITIVE_INFINITY)
            .build())
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("jitter must be a finite number");
    }

    @Test
    void rejectsJitterOutsideUnitInterval() {
        assertThatThrownBy(() -> RetryPolicy.exponential()
            .maxAttempts(3)
            .initialDelay(Duration.ofMillis(10))
            .jitter(1.5)
            .build())
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("jitter");

        assertThatThrownBy(() -> RetryPolicy.exponential()
            .maxAttempts(3)
            .initialDelay(Duration.ofMillis(10))
            .jitter(-0.1)
            .build())
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("jitter");
    }
}
