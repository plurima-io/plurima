package io.plurima.kafka.internal;

import io.plurima.kafka.annotation.Internal;

import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

@Internal
public final class WorkerLauncher implements AutoCloseable {

    private final ExecutorService executor;

    public WorkerLauncher() {
        AtomicLong counter = new AtomicLong();
        ThreadFactory factory = Thread.ofVirtual()
            .name("plurima-worker-", 0)
            .factory();
        this.executor = Executors.newThreadPerTaskExecutor(r -> {
            Thread t = factory.newThread(r);
            t.setName("plurima-worker-" + counter.incrementAndGet());
            return t;
        });
    }

    public void launch(Runnable task) {
        executor.execute(task);
    }

    @Override
    public void close() {
        // Default 5s grace. Callers that want a configurable drain budget should call
        // close(Duration) — the classic runtime passes its shutdownDrainTimeout.
        close(Duration.ofSeconds(5));
    }

    /**
     * Closes the worker pool, allowing in-flight tasks up to {@code gracePeriod} to
     * finish naturally before forcing {@code shutdownNow()} (which interrupts running
     * tasks).
     *
     * <p>Currently both engines invoke the no-arg {@link #close()} which uses the 5s
     * default. The classic engine deliberately doesn't pass its
     * {@code shutdownDrainTimeout} here because {@code ClassicPollLoop.dispatchBatch}
     * already spent that budget waiting for workers; by the time the launcher's close
     * runs, the remaining workers are stragglers — 5s extra grace is the right
     * cleanup window, not the full user budget all over again. This overload exists
     * for future callers that might want a different drain budget (e.g. a Spring
     * lifecycle wrapper) and to make the per-caller behavior explicit.
     */
    public void close(Duration gracePeriod) {
        executor.shutdown();
        try {
            if (!executor.awaitTermination(gracePeriod.toMillis(), TimeUnit.MILLISECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            executor.shutdownNow();
        }
    }
}
