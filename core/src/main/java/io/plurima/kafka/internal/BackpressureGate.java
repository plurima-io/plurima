package io.plurima.kafka.internal;

import io.plurima.kafka.annotation.Internal;

import java.util.concurrent.Semaphore;

@Internal
public final class BackpressureGate {

    private final Semaphore semaphore;
    private final int capacity;

    public BackpressureGate(int capacity) {
        if (capacity <= 0) {
            throw new IllegalArgumentException("capacity must be > 0, was " + capacity);
        }
        this.capacity = capacity;
        // Unfair semaphore: under contention, allows queue-jumping which gives ~2-5× higher
        // throughput vs fair mode (no per-acquire FIFO bookkeeping). Acceptable here because
        // fairness across worker threads isn't observable to users — the only callers are the
        // poll thread (acquire) and worker threads (release).
        this.semaphore = new Semaphore(capacity);
    }

    public void acquire(int permits) throws InterruptedException {
        semaphore.acquire(permits);
    }

    public void release(int permits) {
        semaphore.release(permits);
    }

    public int availablePermits() {
        return semaphore.availablePermits();
    }

    public int capacity() {
        return capacity;
    }
}
