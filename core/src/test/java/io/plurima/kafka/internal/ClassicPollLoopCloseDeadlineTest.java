package io.plurima.kafka.internal;

import io.plurima.kafka.OrderingMode;
import io.plurima.kafka.RecordListener;
import io.plurima.kafka.deserializer.RecordDeserializer;
import io.plurima.kafka.metrics.PlurimaMetrics;
import io.plurima.kafka.retry.RetryPolicy;
import org.apache.kafka.clients.consumer.CloseOptions;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * L4: {@code ClassicPollLoop.run()}'s shutdown finally block — drain, final commit, and
 * consumer close — must share ONE deadline (mirroring {@link PollLoop#drainAndClose()}
 * for the SHARE engine), not each get their own fresh {@code shutdownDrainTimeoutMs}
 * budget. Previously the drain used its own full timer, then {@code commitSync} used the
 * client's unbounded default, then {@code close()} used the client's unbounded default —
 * worst case that's the drain budget PLUS two independent unbounded blocking calls,
 * which can blow past {@code ClassicBasicRuntime.close()}'s join budget
 * ({@code shutdownDrainTimeout + 5s}) and leave the non-daemon poll thread running in
 * the background after the runtime claims to be closed.
 */
class ClassicPollLoopCloseDeadlineTest {

    private WorkerLauncher launcher;
    private KafkaConsumer<byte[], byte[]> consumer;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        launcher = new WorkerLauncher();
        consumer = mock(KafkaConsumer.class);
    }

    @AfterEach
    void tearDown() {
        launcher.close();
    }

    private ClassicPollLoop<byte[], byte[]> newLoop(
        RecordListener<byte[], byte[]> listener, long shutdownDrainTimeoutMs) {
        return new ClassicPollLoop<>(
            consumer, "t", "g1", listener,
            RecordDeserializer.bytes(), RecordDeserializer.bytes(),
            OrderingMode.UNORDERED,
            new RetryEngine(RetryPolicy.noRetry()),
            /* dltRouter */ null,
            Duration.ofMillis(20),
            shutdownDrainTimeoutMs,
            /* concurrency */ 8,
            /* shardCount */ 16,
            PlurimaMetrics.noOp(), launcher,
            /* onLoopExit */ () -> {});
    }

    @Test
    @SuppressWarnings({"unchecked", "deprecation"})
    void finalCommitGetsTheReservedBudgetWhenDrainConsumesItsEntireSlice() throws Exception {
        // Strategy (mirrors PollLoopTest.consumerCloseFallsToFloorWhenDrainConsumesEntireBudget
        // for the SHARE engine, extended to the CLASSIC engine's extra synchronous final
        // commit): offset 0 completes immediately so the frontier has a committable
        // offset at shutdown time; offset 1 blocks well past the drain window, so
        // shutdownDrain spins for its FULL slice before giving up. The tail phases are
        // reserved UP FRONT (F7): the drain slice is the overall budget minus
        // COMMIT_RESERVE (5s) minus CLOSE_FLOOR (2s), so even a fully-exhausted drain
        // leaves the final commitSync a meaningful ~COMMIT_RESERVE budget instead of
        // starving it to the 2s floor (routine slow-drain shutdowns previously risked
        // failing the final commit → duplicates on restart).
        TopicPartition tp = new TopicPartition("t", 0);
        CountDownLatch offset1Started = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        RecordListener<byte[], byte[]> listener = (r, ctx) -> {
            if ("stuck".equals(new String(r.value()))) {
                offset1Started.countDown();
                try { release.await(5, TimeUnit.SECONDS); }
                catch (InterruptedException ie) { Thread.currentThread().interrupt(); }
            }
            // offset 0's record ("ok") returns immediately — success.
        };
        // 7.5s overall − 5s COMMIT_RESERVE − 2s CLOSE_FLOOR → a 500ms drain slice that
        // the stuck offset-1 worker fully exhausts.
        long shutdownDrainTimeoutMs = 7_500L;
        ClassicPollLoop<byte[], byte[]> loop = newLoop(listener, shutdownDrainTimeoutMs);

        when(consumer.poll(any(Duration.class))).thenReturn(new ConsumerRecords<>(Map.of(), Map.of()));
        when(consumer.assignment()).thenReturn(Set.of(tp));

        Thread pollThread = new Thread(loop, "close-deadline-floor-loop");
        pollThread.start();
        try {
            ConsumerRecord<byte[], byte[]> r0 = new ConsumerRecord<>(
                tp.topic(), tp.partition(), 0L, "k0".getBytes(), "ok".getBytes());
            ConsumerRecord<byte[], byte[]> r1 = new ConsumerRecord<>(
                tp.topic(), tp.partition(), 1L, "k1".getBytes(), "stuck".getBytes());
            loop.dispatchBatchAsync(new ConsumerRecords<>(Map.of(tp, List.of(r0, r1)), Map.of()));

            assertThat(offset1Started.await(5, TimeUnit.SECONDS))
                .as("offset 1's worker must be blocked before we trigger shutdown")
                .isTrue();

            loop.shutdown();
        } finally {
            pollThread.join(shutdownDrainTimeoutMs + 5_000);
            release.countDown();
        }
        assertThat(pollThread.isAlive())
            .as("poll thread must finish within the join budget")
            .isFalse();

        ArgumentCaptor<Duration> commitTimeout = ArgumentCaptor.forClass(Duration.class);
        verify(consumer).commitSync(any(Map.class), commitTimeout.capture());
        verify(consumer, never()).commitSync(any(Map.class));

        ArgumentCaptor<CloseOptions> closeOptions = ArgumentCaptor.forClass(CloseOptions.class);
        verify(consumer).close(closeOptions.capture());
        verify(consumer, never()).close();
        verify(consumer, never()).close(any(Duration.class));
        Duration closeTimeout = closeOptions.getValue().timeout()
            .orElseThrow(() -> new AssertionError("close(CloseOptions) must carry an explicit timeout"));

        assertThat(commitTimeout.getValue())
            .as("final commitSync must get AT LEAST the up-front COMMIT_RESERVE (5s) even "
                + "when the drain phase exhausted its entire slice — never starved to the "
                + "2s close floor")
            .isGreaterThanOrEqualTo(Duration.ofSeconds(5))
            .isLessThan(Duration.ofSeconds(6));
        assertThat(closeTimeout)
            .as("consumer.close gets what's left of the ONE shared overall deadline — "
                + "bounded by the configured budget, never below its own floor")
            .isGreaterThanOrEqualTo(Duration.ofSeconds(2))
            .isLessThanOrEqualTo(Duration.ofMillis(shutdownDrainTimeoutMs));
    }

    @Test
    @SuppressWarnings("unchecked")
    void finalCommitAndCloseStayBoundedByConfiguredBudgetWhenDrainFinishesQuickly() throws Exception {
        // Happy path: nothing is stuck, so shutdownDrain returns almost instantly. Both
        // the final commitSync and consumer.close must still receive an EXPLICIT bounded
        // Duration (never the client's own unbounded default) and that duration must
        // never exceed the configured shutdownDrainTimeoutMs — the two phases are
        // sharing what's left of ONE budget, not each drawing a fresh one.
        TopicPartition tp = new TopicPartition("t", 0);
        RecordListener<byte[], byte[]> listener = (r, ctx) -> { };
        // Comfortably larger than COMMIT_RESERVE + CLOSE_FLOOR so neither tail phase's
        // floor dominates and the "never exceeds the configured budget" bound is exact.
        long shutdownDrainTimeoutMs = 10_000L;
        ClassicPollLoop<byte[], byte[]> loop = newLoop(listener, shutdownDrainTimeoutMs);

        when(consumer.poll(any(Duration.class))).thenReturn(new ConsumerRecords<>(Map.of(), Map.of()));
        when(consumer.assignment()).thenReturn(Set.of(tp));

        Thread pollThread = new Thread(loop, "close-deadline-bounded-loop");
        pollThread.start();
        try {
            ConsumerRecord<byte[], byte[]> r0 = new ConsumerRecord<>(
                tp.topic(), tp.partition(), 0L, "k0".getBytes(), "ok".getBytes());
            loop.dispatchBatchAsync(new ConsumerRecords<>(Map.of(tp, List.of(r0)), Map.of()));

            // Let the fast (no-op) listener complete before shutting down.
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
            while (loop.inFlightCount() != 0 && System.nanoTime() < deadline) {
                Thread.sleep(5);
            }
            assertThat(loop.inFlightCount()).isZero();

            loop.shutdown();
        } finally {
            pollThread.join(shutdownDrainTimeoutMs + 5_000);
        }
        assertThat(pollThread.isAlive()).isFalse();

        ArgumentCaptor<Duration> commitTimeout = ArgumentCaptor.forClass(Duration.class);
        verify(consumer).commitSync(any(Map.class), commitTimeout.capture());
        ArgumentCaptor<CloseOptions> closeOptions = ArgumentCaptor.forClass(CloseOptions.class);
        verify(consumer).close(closeOptions.capture());
        Duration closeTimeout = closeOptions.getValue().timeout()
            .orElseThrow(() -> new AssertionError("close(CloseOptions) must carry an explicit timeout"));

        assertThat(commitTimeout.getValue())
            .as("commitSync must not exceed the configured shutdownDrainTimeoutMs budget")
            .isLessThanOrEqualTo(Duration.ofMillis(shutdownDrainTimeoutMs));
        assertThat(closeTimeout)
            .as("close must not exceed the configured shutdownDrainTimeoutMs budget either "
                + "— it shares what's left after the drain + commit phases, not a fresh one")
            .isLessThanOrEqualTo(Duration.ofMillis(shutdownDrainTimeoutMs));
    }
}
