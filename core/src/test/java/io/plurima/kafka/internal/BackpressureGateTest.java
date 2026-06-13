package io.plurima.kafka.internal;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;

import static org.assertj.core.api.Assertions.assertThat;

class BackpressureGateTest {

    @Test
    void permitsBoundedByCapacity() throws Exception {
        BackpressureGate gate = new BackpressureGate(2);
        gate.acquire(1);
        gate.acquire(1);

        // 3rd acquire should block until a release happens
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch finished = new CountDownLatch(1);
        Thread t = new Thread(() -> {
            started.countDown();
            try {
                gate.acquire(1);
                finished.countDown();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        t.start();
        started.await();
        Thread.sleep(50);
        assertThat(finished.getCount()).isEqualTo(1);

        gate.release(1);
        assertThat(finished.await(1, java.util.concurrent.TimeUnit.SECONDS)).isTrue();
        t.join();
    }

    @Test
    void acquireBatchBlocksUntilEnoughPermits() throws Exception {
        BackpressureGate gate = new BackpressureGate(5);
        gate.acquire(3);
        long start = System.nanoTime();

        Thread releaser = new Thread(() -> {
            try { Thread.sleep(30); } catch (InterruptedException ignored) {}
            gate.release(2);
        });
        releaser.start();

        gate.acquire(4); // needs 4 more, but only 2 free; blocks until releaser releases 2
        releaser.join();
        long elapsedMs = (System.nanoTime() - start) / 1_000_000;
        assertThat(elapsedMs).isGreaterThanOrEqualTo(25);
    }

    @Test
    void availablePermitsReportsRemainingCapacity() {
        BackpressureGate gate = new BackpressureGate(5);
        assertThat(gate.availablePermits()).isEqualTo(5);
    }
}
