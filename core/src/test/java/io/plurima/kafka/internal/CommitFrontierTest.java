package io.plurima.kafka.internal;

import org.junit.jupiter.api.Test;

import java.util.OptionalLong;

import static org.assertj.core.api.Assertions.assertThat;

class CommitFrontierTest {

    @Test
    void inOrderCompletionAdvancesLinearly() {
        CommitFrontier f = new CommitFrontier();
        f.observe(10);
        f.observe(11);
        f.observe(12);
        // Before any completion the frontier sits at the starting offset; drainCommittable
        // returns "next-to-consume = 10" once, then nothing until it advances.
        assertThat(f.drainCommittable()).hasValue(10L);
        assertThat(f.drainCommittable()).isEmpty();

        f.complete(10);
        assertThat(f.drainCommittable()).hasValue(11L);

        f.complete(11);
        f.complete(12);
        assertThat(f.drainCommittable()).hasValue(13L);
    }

    @Test
    void outOfOrderCompletionParksAheadAndAdvancesOnGapClose() {
        CommitFrontier f = new CommitFrontier();
        for (long o = 0; o <= 2; o++) f.observe(o);
        // 1, 2 finish before 0 — they get parked in completedAhead. Frontier stays at 0.
        f.complete(1);
        f.complete(2);
        assertThat(f.drainCommittable()).hasValue(0L);
        assertThat(f.gapSize()).isEqualTo(2);

        // Closing the gap with 0 should advance through 0, 1, 2 in one sweep → next = 3.
        f.complete(0);
        assertThat(f.drainCommittable()).hasValue(3L);
        assertThat(f.gapSize()).isZero();
    }

    @Test
    void sparseHoleHoldsFrontierUntilFilled() {
        CommitFrontier f = new CommitFrontier();
        for (long o = 0; o <= 5; o++) f.observe(o);
        // Complete 0, 1, then skip 2 and complete 3, 4, 5.
        f.complete(0);
        f.complete(1);
        f.complete(3);
        f.complete(4);
        f.complete(5);
        // Frontier should be at 2 (smallest unfinished offset).
        assertThat(f.drainCommittable()).hasValue(2L);
        assertThat(f.gapSize()).isEqualTo(3);  // 3, 4, 5 parked

        // Fill the hole. Frontier should jump to 6.
        f.complete(2);
        assertThat(f.drainCommittable()).hasValue(6L);
        assertThat(f.gapSize()).isZero();
    }

    @Test
    void drainOnlyEmitsWhenFrontierMoved() {
        CommitFrontier f = new CommitFrontier();
        f.observe(100);
        f.observe(101);
        assertThat(f.drainCommittable()).hasValue(100L);
        assertThat(f.drainCommittable()).isEmpty();

        // A completion AHEAD of the frontier does not advance it; drain stays empty.
        f.complete(101);
        assertThat(f.drainCommittable()).isEmpty();

        // Closing the gap advances and the next drain emits.
        f.complete(100);
        assertThat(f.drainCommittable()).hasValue(102L);
        assertThat(f.drainCommittable()).isEmpty();
    }

    @Test
    void duplicateCompletionBelowFrontierIsIgnored() {
        CommitFrontier f = new CommitFrontier();
        f.observe(0);
        f.observe(1);
        f.complete(0);
        f.complete(1);
        assertThat(f.drainCommittable()).hasValue(2L);

        // Replaying 0 (e.g. a buggy retry path) must NOT decrement the frontier.
        f.complete(0);
        assertThat(f.drainCommittable()).isEmpty();
        assertThat(f.nextExpected()).isEqualTo(2L);
    }

    @Test
    void observeOnlyHonoredOnFirstCall() {
        // "First call only" applies to the COMMIT FLOOR: only the first observe pins it.
        // Later observes register additional delivered offsets (60, 100 below) but never
        // move the floor.
        CommitFrontier f = new CommitFrontier();
        f.observe(50);
        f.observe(60);  // queued as a delivered offset; floor stays 50
        assertThat(f.nextExpected()).isEqualTo(50L);

        // Completing 50 advances the frontier to 51 (60 still outstanding); a later
        // observe(100) queues another delivered offset without touching the floor.
        f.complete(50);
        f.observe(100);
        assertThat(f.nextExpected()).isEqualTo(51L);
    }

    @Test
    void completeWithoutObserveDefaultsFloorToFirstOffset() {
        // Defensive: complete() without observe() should still produce a sensible frontier
        // starting from the first observed completion.
        CommitFrontier f = new CommitFrontier();
        f.complete(42);
        assertThat(f.drainCommittable()).hasValue(43L);
    }

    @Test
    void gapInDeliveredOffsetsDoesNotStallCommits() {
        // Compacted topic / read_committed: offset 6 is never delivered (compacted away, or a
        // transaction-marker slot). The frontier must NOT wait for a completion of 6 — 6 was
        // never delivered, so it can never complete. Only DELIVERED offsets (5 and 7) gate
        // commits; the missing 6 must not stall the frontier forever.
        CommitFrontier f = new CommitFrontier();
        f.observe(5);
        f.observe(7);   // 6 skipped — never delivered
        assertThat(f.drainCommittable()).hasValue(5L);   // initial floor
        assertThat(f.drainCommittable()).isEmpty();

        // 7 completes first; 5 is still outstanding → no advance.
        f.complete(7);
        assertThat(f.drainCommittable()).isEmpty();

        // 5 completes → sweep across the delivered offsets 5 then 7 (6 doesn't exist) → next = 8.
        f.complete(5);
        assertThat(f.drainCommittable()).hasValue(8L);
        assertThat(f.gapSize()).isZero();
    }

    @Test
    void gapWithOutOfOrderCompletion() {
        // Delivered offsets 10, 12, 15 (11, 13, 14 never delivered — gaps). Completions arrive
        // out of order; the frontier advances only across delivered offsets whose completions
        // have arrived contiguously.
        CommitFrontier f = new CommitFrontier();
        f.observe(10);
        f.observe(12);
        f.observe(15);
        assertThat(f.drainCommittable()).hasValue(10L);  // initial floor

        f.complete(15);
        f.complete(10);
        // 10 done → next-to-consume = 11. (11 was never delivered, but committing 11 is valid:
        // the broker redelivers from >= 11, i.e. 12.) 12 is not yet complete, so we stop there.
        assertThat(f.drainCommittable()).hasValue(11L);

        f.complete(12);
        // 12 done → advance across 12 then 15 (13, 14 don't exist) → next = 16.
        assertThat(f.drainCommittable()).hasValue(16L);
        assertThat(f.gapSize()).isZero();
    }

    @Test
    void denseOffsetsStillWork() {
        // Regression: dense (gap-free) delivery must behave exactly as before — out-of-order
        // completions absorbed in one sweep once the front closes.
        CommitFrontier f = new CommitFrontier();
        for (long o = 0; o <= 4; o++) f.observe(o);
        f.complete(2);
        f.complete(0);
        f.complete(4);
        f.complete(1);
        f.complete(3);
        assertThat(f.drainCommittable()).hasValue(5L);
        assertThat(f.gapSize()).isZero();
    }

    @Test
    void parallelCompletionsAreThreadSafe() throws Exception {
        // Stress test: 1000 records, 16 threads completing in arbitrary order. Final
        // frontier must be 1000 with no parked entries — proves the synchronized methods
        // produce the correct linearization regardless of completion order.
        CommitFrontier f = new CommitFrontier();
        int total = 1000;
        for (long o = 0; o < total; o++) f.observe(o);

        java.util.List<Long> offsets = new java.util.ArrayList<>();
        for (long i = 0; i < total; i++) offsets.add(i);
        java.util.Collections.shuffle(offsets, new java.util.Random(7));

        int threads = 16;
        var pool = java.util.concurrent.Executors.newFixedThreadPool(threads);
        try {
            var futures = new java.util.ArrayList<java.util.concurrent.Future<?>>();
            int chunk = total / threads;
            for (int t = 0; t < threads; t++) {
                int from = t * chunk;
                int to = (t == threads - 1) ? total : (t + 1) * chunk;
                var slice = offsets.subList(from, to);
                futures.add(pool.submit(() -> {
                    for (long o : slice) f.complete(o);
                }));
            }
            for (var fut : futures) fut.get();
        } finally {
            pool.shutdownNow();
        }
        OptionalLong drained = f.drainCommittable();
        assertThat(drained).hasValue((long) total);
        assertThat(f.gapSize()).isZero();
    }

    @Test
    void concurrentCompletesAndDrainsConvergeToCorrectFrontier() throws Exception {
        // Race: a poll thread keeps calling drainCommittable() while many worker threads
        // call complete() in arbitrary order. The frontier must end at exactly `total`
        // with no parked entries and no lost completions.
        //
        // This catches a class of bug where a drain that's partway through absorbing
        // contiguous offsets races with a complete() that adds an offset just past the
        // current advancing-front. With the synchronized absorbContiguous() and the
        // ConcurrentSkipListSet for parked offsets, the linearization is well-defined
        // and the test must reach `total` deterministically.
        CommitFrontier f = new CommitFrontier();
        int total = 5_000;
        for (long o = 0; o < total; o++) f.observe(o);

        java.util.List<Long> offsets = new java.util.ArrayList<>();
        for (long i = 0; i < total; i++) offsets.add(i);
        java.util.Collections.shuffle(offsets, new java.util.Random(13));

        int completerThreads = 8;
        var completerPool = java.util.concurrent.Executors.newFixedThreadPool(completerThreads);
        var stopDraining = new java.util.concurrent.atomic.AtomicBoolean();
        var drainCount = new java.util.concurrent.atomic.AtomicInteger();

        // A separate "poll" thread that keeps draining as completions roll in.
        Thread drainer = new Thread(() -> {
            while (!stopDraining.get()) {
                f.drainCommittable();
                drainCount.incrementAndGet();
                // Tight loop — exercise the race window aggressively.
                Thread.yield();
            }
            // Final drain after stop to absorb any last completions.
            f.drainCommittable();
        }, "drainer");
        drainer.start();

        try {
            int chunk = total / completerThreads;
            var futures = new java.util.ArrayList<java.util.concurrent.Future<?>>();
            for (int t = 0; t < completerThreads; t++) {
                int from = t * chunk;
                int to = (t == completerThreads - 1) ? total : (t + 1) * chunk;
                var slice = offsets.subList(from, to);
                futures.add(completerPool.submit(() -> {
                    for (long o : slice) f.complete(o);
                }));
            }
            for (var fut : futures) fut.get();
        } finally {
            completerPool.shutdownNow();
            stopDraining.set(true);
            drainer.join(5_000);
        }

        // After everything is done, the frontier must be at `total` (next-to-consume)
        // with zero parked entries — proves no completion was lost in a drain race.
        // These are the load-bearing correctness assertions.
        assertThat(f.nextExpected()).isEqualTo((long) total);
        assertThat(f.gapSize()).isZero();
        // Sanity: the drainer thread ran at all. We deliberately don't assert a
        // specific iteration count — that's timing-sensitive (CI noise can drop it
        // to a few dozen on a busy host). The "convergence to correct frontier"
        // assertions above are what proves the concurrent race was handled; this
        // is just a smoke check that the drainer wasn't dead.
        assertThat(drainCount.get()).isPositive();
    }
}
