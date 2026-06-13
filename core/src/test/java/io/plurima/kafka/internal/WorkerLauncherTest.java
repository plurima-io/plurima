package io.plurima.kafka.internal;

import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

class WorkerLauncherTest {

    @Test
    void launchExecutesOnVirtualThread() throws Exception {
        WorkerLauncher launcher = new WorkerLauncher();
        try {
            CountDownLatch latch = new CountDownLatch(1);
            AtomicReference<Boolean> isVirtual = new AtomicReference<>();
            AtomicReference<String> threadName = new AtomicReference<>();

            launcher.launch(() -> {
                isVirtual.set(Thread.currentThread().isVirtual());
                threadName.set(Thread.currentThread().getName());
                latch.countDown();
            });

            assertThat(latch.await(1, java.util.concurrent.TimeUnit.SECONDS)).isTrue();
            assertThat(isVirtual.get()).isTrue();
            assertThat(threadName.get()).startsWith("plurima-worker-");
        } finally {
            launcher.close();
        }
    }
}
