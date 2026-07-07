package io.plurima.kafka.internal;

import io.plurima.kafka.OrderingMode;
import io.plurima.kafka.RecordListener;
import io.plurima.kafka.deserializer.RecordDeserializer;
import io.plurima.kafka.metrics.AckOutcome;
import io.plurima.kafka.metrics.PlurimaMetrics;
import io.plurima.kafka.retry.RetryPolicy;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.time.Duration;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * Verifies the {@code pendingFailedCommits} retry buffer: when {@link KafkaConsumer#commitAsync(Map, OffsetCommitCallback)}
 * reports a failure via its callback, the next drain MUST re-issue the failed offset
 * so a subsequent shutdown / rebalance picks it up instead of leaving the partition
 * committed below its real frontier.
 */
class ClassicPollLoopCommitRetryTest {

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

    private ClassicPollLoop<byte[], byte[]> newLoop() {
        return newLoop(PlurimaMetrics.noOp());
    }

    private ClassicPollLoop<byte[], byte[]> newLoop(PlurimaMetrics metrics) {
        RecordListener<byte[], byte[]> listener = (r, ctx) -> {};
        return new ClassicPollLoop<>(
            consumer, "t", "g1", listener,
            RecordDeserializer.bytes(), RecordDeserializer.bytes(),
            OrderingMode.PARTITION,
            new RetryEngine(RetryPolicy.noRetry()),
            /* dltRouter */ null,
            Duration.ofMillis(50),
            /* shutdownDrainTimeoutMs */ 1_000L,
            /* concurrency */ 8,
            /* shardCount */ 16,
            metrics, launcher,
            /* onLoopExit */ () -> {});
    }

    @Test
    @SuppressWarnings("unchecked")
    void failedCommitAsyncIsRetriedOnNextDrain() throws Exception {
        TopicPartition tp = new TopicPartition("t", 0);
        ClassicPollLoop<byte[], byte[]> loop = newLoop();

        // Install a frontier and complete a few offsets so drainCommittable produces
        // a real offset.
        CommitFrontier f = loop.installFrontier(tp, 0L);
        loop.markComplete(f, tp, 0L);
        loop.markComplete(f, tp, 1L);
        loop.markComplete(f, tp, 2L);

        // First drain: capture the commitAsync callback so we can drive a failure.
        ArgumentCaptor<OffsetCommitCallback> cb1 = ArgumentCaptor.forClass(OffsetCommitCallback.class);
        loop.drainPendingCommits();

        verify(consumer).commitAsync(
            eq(Map.of(tp, new OffsetAndMetadata(3L))),
            cb1.capture());

        // Simulate the broker reporting an exception via the async callback.
        cb1.getValue().onComplete(
            Map.of(tp, new OffsetAndMetadata(3L)),
            new org.apache.kafka.common.errors.DisconnectException("transient broker hiccup"));

        // No new completions: frontier won't emit anything new. Second drain MUST
        // still issue commitAsync for the failed offset — that's the retry buffer
        // doing its job.
        ArgumentCaptor<OffsetCommitCallback> cb2 = ArgumentCaptor.forClass(OffsetCommitCallback.class);
        ArgumentCaptor<Map<TopicPartition, OffsetAndMetadata>> snapshot2 = ArgumentCaptor.forClass(Map.class);
        loop.drainPendingCommits();

        verify(consumer, times(2)).commitAsync(snapshot2.capture(), cb2.capture());
        assertThat(snapshot2.getValue())
            .as("buffered failed offset must be re-issued by the next drain")
            .containsEntry(tp, new OffsetAndMetadata(3L));

        // Now have the broker succeed. After success the buffer must be cleared so
        // a third drain does NOT re-issue a stale lower offset.
        cb2.getValue().onComplete(Map.of(tp, new OffsetAndMetadata(3L)), null);

        loop.drainPendingCommits();
        // Only the two earlier calls — no third commit because frontier has nothing
        // new AND the buffer was cleared by the successful callback.
        verify(consumer, times(2)).commitAsync(any(Map.class), any(OffsetCommitCallback.class));
    }

    @Test
    @SuppressWarnings("unchecked")
    void newerFrontierOffsetSubsumesBufferedFailure() {
        TopicPartition tp = new TopicPartition("t", 0);
        ClassicPollLoop<byte[], byte[]> loop = newLoop();
        CommitFrontier f = loop.installFrontier(tp, 0L);
        loop.markComplete(f, tp, 0L);

        // First drain → commit 1, fail.
        ArgumentCaptor<OffsetCommitCallback> cb1 = ArgumentCaptor.forClass(OffsetCommitCallback.class);
        loop.drainPendingCommits();
        verify(consumer).commitAsync(eq(Map.of(tp, new OffsetAndMetadata(1L))), cb1.capture());
        cb1.getValue().onComplete(
            Map.of(tp, new OffsetAndMetadata(1L)),
            new org.apache.kafka.common.errors.DisconnectException("hiccup"));

        // More records complete — frontier advances to offset 5.
        loop.markComplete(f, tp, 1L);
        loop.markComplete(f, tp, 2L);
        loop.markComplete(f, tp, 3L);
        loop.markComplete(f, tp, 4L);

        ArgumentCaptor<Map<TopicPartition, OffsetAndMetadata>> snapshot2 = ArgumentCaptor.forClass(Map.class);
        loop.drainPendingCommits();
        verify(consumer, times(2)).commitAsync(snapshot2.capture(), any(OffsetCommitCallback.class));
        // Snapshot should be the NEWER offset (5), not the buffered lower one (1).
        // Newer subsumes older.
        assertThat(snapshot2.getValue())
            .as("newer frontier offset must subsume the buffered failed lower offset")
            .containsEntry(tp, new OffsetAndMetadata(5L));
    }

    @Test
    @SuppressWarnings("unchecked")
    void noCommitWhenNothingNewAndNoBufferedFailures() {
        ClassicPollLoop<byte[], byte[]> loop = newLoop();
        // No installFrontier, no completions, no buffered failures.
        loop.drainPendingCommits();
        verify(consumer, never()).commitAsync(any(Map.class), any(OffsetCommitCallback.class));
    }

    @Test
    @SuppressWarnings("unchecked")
    void newerSuccessThenOlderFailureDoesNotRegressTheCommittedOffset() {
        // Regression scenario the high-water guard exists to prevent:
        //   1. commitAsync(P→20) fires — callback delivers SUCCESS first → highWater(P)=20.
        //   2. commitAsync(P→10) had been issued earlier OR its callback arrives late
        //      with EXCEPTION. Without the guard we would buffer 10; the next drain
        //      would re-commit 10 and regress the broker's committed offset, opening
        //      a duplicate window covering offsets 11..19 on restart/rebalance.
        // The high-water filter rejects the stale failure callback so 10 is NEVER
        // re-emitted by a later drain.
        TopicPartition tp = new TopicPartition("t", 0);
        ClassicPollLoop<byte[], byte[]> loop = newLoop();

        // First drain — install frontier, complete offsets 0..19, drain to commit 20.
        CommitFrontier f = loop.installFrontier(tp, 0L);
        for (long o = 0L; o < 20L; o++) loop.markComplete(f, tp, o);

        ArgumentCaptor<OffsetCommitCallback> cb1 = ArgumentCaptor.forClass(OffsetCommitCallback.class);
        loop.drainPendingCommits();
        verify(consumer).commitAsync(eq(Map.of(tp, new OffsetAndMetadata(20L))), cb1.capture());

        // Newer commit succeeds first — high-water now 20.
        cb1.getValue().onComplete(Map.of(tp, new OffsetAndMetadata(20L)), null);

        // Now simulate an OLDER (stale) failure callback arriving for offset 10. The
        // guard must reject it so nothing is buffered.
        cb1.getValue().onComplete(
            Map.of(tp, new OffsetAndMetadata(10L)),
            new org.apache.kafka.common.errors.DisconnectException("stale"));

        // Next drain MUST NOT re-commit the stale 10 — neither from the buffer (we
        // didn't store it) nor from any other source (frontier already drained to 20).
        loop.drainPendingCommits();
        verify(consumer, times(1)).commitAsync(any(Map.class), any(OffsetCommitCallback.class));
    }

    @Test
    @SuppressWarnings("unchecked")
    void steadyStateDrainDoesNotReEmitInFlightAsyncCommits() {
        // A slow async callback used to cause the same offset to be sent again on
        // every drainPendingCommits — wasteful broker traffic that scaled with poll
        // frequency under coordinator slowness. Steady-state drains now skip
        // inFlightAsyncCommits; only shutdown/revoke sync paths include them.
        TopicPartition tp = new TopicPartition("t", 0);
        ClassicPollLoop<byte[], byte[]> loop = newLoop();
        CommitFrontier f = loop.installFrontier(tp, 0L);
        for (long o = 0L; o < 3L; o++) loop.markComplete(f, tp, o);

        // First drain → commitAsync(3) with no callback fired.
        ArgumentCaptor<OffsetCommitCallback> cb = ArgumentCaptor.forClass(OffsetCommitCallback.class);
        loop.drainPendingCommits();
        verify(consumer, times(1)).commitAsync(eq(Map.of(tp, new OffsetAndMetadata(3L))), cb.capture());

        // Repeated drains with the callback still pending. None of these should
        // re-emit commitAsync — frontier has no new completions, no buffered
        // failures, and in-flight is not included in the steady-state snapshot.
        loop.drainPendingCommits();
        loop.drainPendingCommits();
        loop.drainPendingCommits();
        verify(consumer, times(1)).commitAsync(any(Map.class), any(OffsetCommitCallback.class));

        // Once the callback fires (success or failure), the lifecycle moves on:
        // success → highWater advances, in-flight clears; failure → moves into
        // pendingFailedCommits which DOES feed back into steady drains.
        cb.getValue().onComplete(Map.of(tp, new OffsetAndMetadata(3L)), null);
        loop.drainPendingCommits();
        // Still just one commitAsync — high-water covers offset 3 now.
        verify(consumer, times(1)).commitAsync(any(Map.class), any(OffsetCommitCallback.class));
    }

    @Test
    @SuppressWarnings("unchecked")
    void inFlightAsyncCommitIsPickedUpByRevokeSync() throws Exception {
        // The data-loss scenario this guards against:
        //   1. drainPendingCommits issues commitAsync(P→5). CommitFrontier advances
        //      lastDrained immediately; the async callback hasn't fired yet.
        //   2. Revoke fires BEFORE the callback. collectPendingCommits would normally
        //      see frontier.drainCommittable() = empty and pendingFailedCommits empty,
        //      skip the partition, and hand off without confirming offset 5.
        //   3. The async callback later reports failure during consumer close — too
        //      late to retry. Restart redelivers records 0..4 the consumer had
        //      already processed.
        // The inFlightAsyncCommits map plugs this: every commitAsync writes it,
        // the rebalance listener's snapshot includes it, the sync commit confirms
        // it before handoff.
        TopicPartition tp = new TopicPartition("t", 0);
        ClassicPollLoop<byte[], byte[]> loop = newLoop();
        CommitFrontier f = loop.installFrontier(tp, 0L);
        for (long o = 0L; o < 5L; o++) loop.markComplete(f, tp, o);

        // Issue the async commit but DO NOT fire the callback yet.
        ArgumentCaptor<OffsetCommitCallback> cb = ArgumentCaptor.forClass(OffsetCommitCallback.class);
        loop.drainPendingCommits();
        verify(consumer).commitAsync(eq(Map.of(tp, new OffsetAndMetadata(5L))), cb.capture());

        // Revoke fires while the async commit is still in flight. The rebalance
        // listener must include offset 5 in its commitSync snapshot — otherwise
        // the partition hands off without confirmation.
        ArgumentCaptor<Map<TopicPartition, OffsetAndMetadata>> syncSnapshot = ArgumentCaptor.forClass(Map.class);
        loop.rebalanceListener().onPartitionsRevoked(java.util.List.of(tp));

        verify(consumer).commitSync(syncSnapshot.capture());
        assertThat(syncSnapshot.getValue())
            .as("revoke commitSync must include in-flight async commit offset 5")
            .containsEntry(tp, new OffsetAndMetadata(5L));

        // The async callback later fires with failure — too late to retry, but the
        // buffer cleanup in the revoke finally block already cleared the entry, so
        // a subsequent drain on this consumer (if it kept running) wouldn't try to
        // commit on the revoked partition.
        cb.getValue().onComplete(
            Map.of(tp, new OffsetAndMetadata(5L)),
            new org.apache.kafka.common.errors.DisconnectException("late failure"));

        org.mockito.Mockito.reset(consumer);
        loop.drainPendingCommits();
        verify(consumer, never()).commitAsync(any(Map.class), any(OffsetCommitCallback.class));
    }

    @Test
    @SuppressWarnings("unchecked")
    void revokeCommitFailureStillClearsBufferedEntriesForRevokedPartitions() throws Exception {
        // Buffer contains a failed commit for tp. Revoke fires; the listener's
        // commitSync fails. The buffer MUST still be cleared for the revoked
        // partition — otherwise a later drainPendingCommits on the old owner would
        // commit offsets for a partition the new owner now controls, which is
        // exactly the offset-corruption scenario the at-least-once contract bans.
        TopicPartition tp = new TopicPartition("t", 0);
        ClassicPollLoop<byte[], byte[]> loop = newLoop();
        CommitFrontier f = loop.installFrontier(tp, 0L);
        loop.markComplete(f, tp, 0L);

        // Drain → buffer the failure for offset 1.
        ArgumentCaptor<OffsetCommitCallback> cb = ArgumentCaptor.forClass(OffsetCommitCallback.class);
        loop.drainPendingCommits();
        verify(consumer).commitAsync(eq(Map.of(tp, new OffsetAndMetadata(1L))), cb.capture());
        cb.getValue().onComplete(
            Map.of(tp, new OffsetAndMetadata(1L)),
            new org.apache.kafka.common.errors.DisconnectException("buffered"));

        // Sanity: buffer holds the failed offset.
        assertThat(loop.hasFrontier(tp)).isTrue();

        // Force commitSync (the revoke-time call) to fail.
        org.mockito.Mockito.doThrow(new org.apache.kafka.common.errors.TimeoutException("revoke timed out"))
            .when(consumer).commitSync(org.mockito.ArgumentMatchers.any(Map.class));

        // Drive the rebalance listener.
        loop.rebalanceListener().onPartitionsRevoked(java.util.List.of(tp));

        // Frontier dropped (revoke removed it). Now a later drain MUST NOT re-issue
        // a commit for tp via the buffer — that would target a partition we no
        // longer own. Verify by triggering another drain and asserting no new
        // commitAsync calls for tp.
        org.mockito.Mockito.reset(consumer);
        loop.drainPendingCommits();
        verify(consumer, never()).commitAsync(any(Map.class), any(OffsetCommitCallback.class));
    }

    @Test
    @SuppressWarnings("unchecked")
    void finalShutdownCommitSyncRetriesOnceWhenArmedWakeupFires() {
        // shutdown() sets running=false then calls consumer.wakeup(). If the poll
        // thread is between polls when this happens, the armed wakeup doesn't fire
        // at poll() (nothing is polling) — it fires at the very next blocking call
        // into the consumer, which is the final commitSync in drainPendingCommitsSync.
        // Without a retry, that WakeupException would be swallowed by the generic
        // failure handler and the last batch of offsets would silently go
        // uncommitted. Since KafkaConsumer.wakeup() only arms ONE interrupt, a
        // single retry is guaranteed to get a clean commitSync.
        TopicPartition tp = new TopicPartition("t", 0);
        PlurimaMetrics metrics = mock(PlurimaMetrics.class);
        ClassicPollLoop<byte[], byte[]> loop = newLoop(metrics);
        CommitFrontier f = loop.installFrontier(tp, 0L);
        loop.markComplete(f, tp, 0L);
        loop.markComplete(f, tp, 1L);
        loop.markComplete(f, tp, 2L);

        Map<TopicPartition, OffsetAndMetadata> expected = Map.of(tp, new OffsetAndMetadata(3L));
        doAnswer(invocation -> {
            throw new org.apache.kafka.common.errors.WakeupException();
        }).doAnswer(invocation -> null)
            .when(consumer).commitSync(eq(expected));

        loop.drainPendingCommitsSync();

        // The wakeup-interrupted attempt was retried and the offsets WERE committed.
        verify(consumer, times(2)).commitSync(eq(expected));

        // Success bookkeeping ran: the commit was recorded as accepted...
        verify(metrics).ackCommitted("t", AckOutcome.ACCEPT);
        // ...and the error path was NEVER taken — the WakeupException must not be
        // treated as a commit failure.
        verify(metrics, never()).ackCommitFailed(
            org.mockito.ArgumentMatchers.anyString(), org.mockito.ArgumentMatchers.anyString());

        // Post-commit state is fully resolved: committedHighWater now covers offset 3
        // and no failure/in-flight buffers remain, so a subsequent drain has nothing
        // to (re-)commit.
        loop.drainPendingCommits();
        verify(consumer, never()).commitAsync(any(Map.class), any(OffsetCommitCallback.class));
    }
}
