package io.plurima.kafka.internal;

import io.plurima.kafka.annotation.Internal;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

@Internal
public final class InFlightRegistry {

    private final ConcurrentMap<RecordCoord, InFlightRecord<?, ?>> records =
        new ConcurrentHashMap<>();
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition empty = lock.newCondition();

    /**
     * Register a new in-flight record. Returns {@code true} if it was added,
     * {@code false} if the coord was already registered (a different InFlightRecord
     * instance is still in flight for the same topic-partition-offset).
     *
     * <p><b>Why the boolean return matters.</b> Under broker redelivery /
     * lock-expiry / force-RELEASE timing edges, the broker can hand us a record
     * whose coord still has a live in-flight entry from a prior delivery. The
     * caller (PollLoop.dispatchBatch) has already consumed a backpressure permit
     * for this record; if register silently dropped the duplicate and dispatch
     * went ahead, the dispatcher's worker would complete the dispatched
     * InFlightRecord via identity-aware complete() — which returns {@code false}
     * because the registered entry is the OLDER instance — and never release the
     * permit. That's a slow permit leak.
     *
     * <p>By returning {@code false} here we tell the caller "do NOT dispatch and
     * DO release the permit you already took"; the in-flight worker for the
     * older coord remains the rightful owner of any state for this coord.
     */
    public boolean register(InFlightRecord<?, ?> record) {
        return records.putIfAbsent(record.coord(), record) == null;
    }

    /**
     * Identity-aware removal: removes the entry ONLY if the registered InFlightRecord
     * is the same instance as {@code record}. Returns {@code true} if this call
     * actually removed the entry (i.e. the caller "won" the race to clean it up),
     * {@code false} if it was already gone OR if a NEWER InFlightRecord is now
     * registered for the same coord (e.g. broker redelivered after force-RELEASE).
     *
     * <p>Used by both workers (in their {@code finally} block) and by
     * {@link PollLoop#forceReleaseStuckRecords()} to abandon stuck records. Whoever
     * gets the {@code true} return value is responsible for releasing the matching
     * backpressure permit, so that permits are released exactly once even when
     * force-RELEASE races with worker completion.
     *
     * <p>The identity check is what prevents the "abandoned worker accidentally
     * completes the redelivered record" bug. Without it, an orphan worker calling
     * {@code complete(coord)} after force-RELEASE could remove a freshly-registered
     * redelivery and release its permit too early.
     */
    public boolean complete(InFlightRecord<?, ?> record) {
        if (!records.remove(record.coord(), record)) return false;
        if (records.isEmpty()) {
            lock.lock();
            try {
                empty.signalAll();
            } finally {
                lock.unlock();
            }
        }
        return true;
    }

    /**
     * Returns true if the given InFlightRecord is currently the registered value for
     * its coord. Used by AckCoordinator to drop stale acks (where an abandoned worker
     * tries to ACCEPT/REJECT/RELEASE a record whose coord has since been re-registered
     * for a redelivery).
     */
    public boolean isCurrent(InFlightRecord<?, ?> record) {
        return records.get(record.coord()) == record;
    }

    public int currentInFlight() {
        return records.size();
    }

    public Collection<InFlightRecord<?, ?>> activeRecords() {
        return List.copyOf(records.values());
    }

    public boolean awaitDrain(Duration timeout) {
        long deadlineNanos = System.nanoTime() + timeout.toNanos();
        lock.lock();
        try {
            while (!records.isEmpty()) {
                long remaining = deadlineNanos - System.nanoTime();
                if (remaining <= 0) return false;
                try {
                    empty.awaitNanos(remaining);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return false;
                }
            }
            return true;
        } finally {
            lock.unlock();
        }
    }
}
