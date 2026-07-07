package io.plurima.kafka.internal;

import io.plurima.kafka.metrics.PlurimaMetrics;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

/**
 * Unit tests for {@link ClassicRebalanceListener}, exercised directly (no
 * {@code ClassicPollLoop}, no broker) with fake/mock collaborators so the revoke vs.
 * lost distinction — specifically, that {@code onPartitionsLost} must NOT commit — is
 * pinned down precisely.
 *
 * <p>Task A7: {@code onPartitionsLost}'s default implementation (inherited from
 * {@link org.apache.kafka.clients.consumer.ConsumerRebalanceListener}) simply delegates
 * to {@code onPartitionsRevoked}, which commits. That default is wrong for a listener
 * whose revoke path calls {@code commitSync} for partitions we no longer own without an
 * orderly handoff — see {@link ClassicRebalanceListener#onPartitionsLost} javadoc for the
 * race this override avoids. These tests confirm the override breaks that delegation.
 */
class ClassicRebalanceListenerTest {

    private KafkaConsumer<byte[], byte[]> consumer;
    private ConcurrentMap<TopicPartition, CommitFrontier> frontiers;
    private ClassicKeyShardDispatcher keyShardDispatcher;
    private Set<TopicPartition> pausedByBackpressure;
    private Set<TopicPartition> pausedByDltFailure;
    private AtomicBoolean clearBackpressureCalled;
    private ConcurrentMap<TopicPartition, OffsetAndMetadata> pendingFailedCommits;
    private ConcurrentMap<TopicPartition, Long> committedHighWater;
    private ConcurrentMap<TopicPartition, OffsetAndMetadata> inFlightAsyncCommits;

    private static final TopicPartition TP = new TopicPartition("t", 0);

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        consumer = mock(KafkaConsumer.class);
        frontiers = new ConcurrentHashMap<>();
        keyShardDispatcher = mock(ClassicKeyShardDispatcher.class);
        pausedByBackpressure = ConcurrentHashMap.newKeySet();
        pausedByDltFailure = ConcurrentHashMap.newKeySet();
        clearBackpressureCalled = new AtomicBoolean();
        pendingFailedCommits = new ConcurrentHashMap<>();
        committedHighWater = new ConcurrentHashMap<>();
        inFlightAsyncCommits = new ConcurrentHashMap<>();
    }

    private ClassicRebalanceListener newListener(ClassicKeyShardDispatcher dispatcher) {
        return new ClassicRebalanceListener(
            consumer, frontiers, dispatcher,
            pausedByBackpressure, pausedByDltFailure,
            () -> clearBackpressureCalled.set(true),
            PlurimaMetrics.noOp(),
            pendingFailedCommits, committedHighWater, inFlightAsyncCommits);
    }

    private ClassicRebalanceListener newListener() {
        return newListener(keyShardDispatcher);
    }

    /** Populates every piece of per-partition state the listener is responsible for. */
    private void seedFullState(TopicPartition tp) {
        CommitFrontier frontier = new CommitFrontier();
        frontier.observe(5L);
        frontier.complete(5L);  // a real pending commit sits here — revoke would commit 6L
        frontiers.put(tp, frontier);
        pausedByBackpressure.add(tp);
        pausedByDltFailure.add(tp);
        pendingFailedCommits.put(tp, new OffsetAndMetadata(3L));
        inFlightAsyncCommits.put(tp, new OffsetAndMetadata(4L));
    }

    // ---- onPartitionsLost: the core A7 behaviour ---------------------------------

    @Test
    void onPartitionsLostCleansAllStateButNeverCommits() {
        seedFullState(TP);
        ClassicRebalanceListener listener = newListener();

        listener.onPartitionsLost(List.of(TP));

        // State cleanup: identical to what onPartitionsRevoked would do.
        assertThat(frontiers).as("frontier removed").doesNotContainKey(TP);
        assertThat(pendingFailedCommits).as("pending failed commit dropped").doesNotContainKey(TP);
        assertThat(inFlightAsyncCommits).as("in-flight async commit dropped").doesNotContainKey(TP);
        assertThat(pausedByBackpressure).as("backpressure pause cleared").doesNotContain(TP);
        assertThat(pausedByDltFailure).as("DLT-failure pause cleared").doesNotContain(TP);
        assertThat(clearBackpressureCalled.get()).as("backpressure-active flag reconciled").isTrue();
        verify(keyShardDispatcher).purgePartitions(List.of(TP));

        // The one thing that must NOT happen: no commit to the broker at all. This is
        // the assertion that fails if onPartitionsLost falls through to the JDK
        // default (delegate to onPartitionsRevoked).
        verifyNoInteractions(consumer);
    }

    @Test
    void onPartitionsLostWithEmptyCollectionIsNoOp() {
        ClassicRebalanceListener listener = newListener();

        listener.onPartitionsLost(List.of());

        verifyNoInteractions(consumer);
        verifyNoInteractions(keyShardDispatcher);
        assertThat(clearBackpressureCalled.get()).isFalse();
    }

    @Test
    void onPartitionsLostWithNullKeyShardDispatcherDoesNotThrow() {
        seedFullState(TP);
        ClassicRebalanceListener listener = newListener(null);

        listener.onPartitionsLost(List.of(TP));

        assertThat(frontiers).doesNotContainKey(TP);
        verifyNoInteractions(consumer);
    }

    @Test
    void onPartitionsLostWhileOnlyDltPausedClearsThatSetAndStillDoesNotCommit() {
        // Edge case from self-review: lost while a DLT-publish-failure pause is the
        // ONLY active pause (no backpressure pause). Must still be cleared, and the
        // backpressure-empty callback still fires (there was nothing there to begin
        // with, so the set is trivially empty).
        pausedByDltFailure.add(TP);
        CommitFrontier frontier = new CommitFrontier();
        frontiers.put(TP, frontier);
        ClassicRebalanceListener listener = newListener();

        listener.onPartitionsLost(List.of(TP));

        assertThat(pausedByDltFailure).doesNotContain(TP);
        assertThat(pausedByBackpressure).isEmpty();
        assertThat(clearBackpressureCalled.get()).isTrue();
        verifyNoInteractions(consumer);
    }

    @Test
    void onPartitionsLostWhileOnlyBackpressurePausedClearsThatSetAndStillDoesNotCommit() {
        pausedByBackpressure.add(TP);
        ClassicRebalanceListener listener = newListener();

        listener.onPartitionsLost(List.of(TP));

        assertThat(pausedByBackpressure).doesNotContain(TP);
        assertThat(clearBackpressureCalled.get()).isTrue();
        verifyNoInteractions(consumer);
    }

    // ---- onPartitionsRevoked: regression guard for the shared-method refactor ----

    @Test
    void onPartitionsRevokedStillCommitsAndCleansState() {
        seedFullState(TP);
        ClassicRebalanceListener listener = newListener();

        listener.onPartitionsRevoked(List.of(TP));

        verify(consumer).commitSync(Map.of(TP, new OffsetAndMetadata(6L)));
        assertThat(frontiers).doesNotContainKey(TP);
        assertThat(pendingFailedCommits).doesNotContainKey(TP);
        assertThat(inFlightAsyncCommits).doesNotContainKey(TP);
        assertThat(pausedByBackpressure).doesNotContain(TP);
        assertThat(pausedByDltFailure).doesNotContain(TP);
        assertThat(clearBackpressureCalled.get()).isTrue();
        verify(keyShardDispatcher).purgePartitions(List.of(TP));
    }

    @Test
    void onPartitionsRevokedWithNoPendingOffsetsSkipsCommitSync() {
        pausedByBackpressure.add(TP);
        ClassicRebalanceListener listener = newListener();

        listener.onPartitionsRevoked(List.of(TP));

        verify(consumer, never()).commitSync(anyMap());
        assertThat(pausedByBackpressure).doesNotContain(TP);
    }
}
