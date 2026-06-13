package io.plurima.kafka.internal;

import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class HandlerLatencyWindowTest {

    private static long ms(long millis) { return millis * 1_000_000L; }

    @Test
    void emptyWindowReportsZeroSamplesAndZeroPercentile() {
        HandlerLatencyWindow w = new HandlerLatencyWindow(1024);
        assertThat(w.sampleCount()).isZero();
        assertThat(w.percentileMillis(0.99)).isZero();
    }

    @Test
    void percentileOverKnownSet() {
        HandlerLatencyWindow w = new HandlerLatencyWindow(1024);
        for (int i = 1; i <= 100; i++) w.record(ms(i));
        assertThat(w.sampleCount()).isEqualTo(100);
        assertThat(w.percentileMillis(0.99)).isEqualTo(99.0);
        assertThat(w.percentileMillis(1.0)).isEqualTo(100.0);
        assertThat(w.percentileMillis(0.50)).isEqualTo(50.0);
    }

    @Test
    void sampleCountSaturatesAtWindowSize() {
        HandlerLatencyWindow w = new HandlerLatencyWindow(4);
        for (int i = 0; i < 10; i++) w.record(ms(1));
        assertThat(w.sampleCount()).isEqualTo(4);
    }

    @Test
    void wraparoundKeepsMostRecentSamples() {
        HandlerLatencyWindow w = new HandlerLatencyWindow(4);
        for (int i = 1; i <= 8; i++) w.record(ms(i));
        assertThat(w.percentileMillis(1.0)).isEqualTo(8.0);
        assertThat(w.percentileMillis(0.01)).isEqualTo(5.0);
    }

    @Test
    void rejectsNonPositiveWindowSize() {
        assertThatThrownBy(() -> new HandlerLatencyWindow(0))
            .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void concurrentWritersAndReaderDoNotThrow() throws Exception {
        HandlerLatencyWindow w = new HandlerLatencyWindow(1024);
        int threads = 8;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        try {
            for (int t = 0; t < threads; t++) {
                pool.submit(() -> {
                    try { start.await(); } catch (InterruptedException e) { return; }
                    for (int i = 0; i < 5000; i++) w.record(ms(1 + (i % 50)));
                });
            }
            start.countDown();
            for (int i = 0; i < 200; i++) {
                double p = w.percentileMillis(0.99);
                assertThat(p).isBetween(0.0, 50.0);
            }
        } finally {
            pool.shutdown();
            assertThat(pool.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
        }
        assertThat(w.sampleCount()).isEqualTo(1024);
    }
}
