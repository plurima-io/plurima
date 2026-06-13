package io.plurima.kafka.internal;

import io.plurima.kafka.annotation.Internal;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

@Internal
public final class InFlightRecord<K, V> {

    private final ConsumerRecord<K, V> record;
    private final RecordCoord coord;
    private int attempt;
    private final AtomicBoolean terminalAckQueued = new AtomicBoolean();

    public InFlightRecord(ConsumerRecord<K, V> record) {
        this.record = Objects.requireNonNull(record, "record");
        this.coord = new RecordCoord(record.topic(), record.partition(), record.offset());
    }

    public ConsumerRecord<K, V> consumerRecord() { return record; }
    public RecordCoord coord() { return coord; }
    public int attempt() { return attempt; }

    public void incrementAttempt() {
        this.attempt++;
    }

    /**
     * First-wins terminal-ack marker. Returns {@code true} only the first time it is called
     * for this record instance; subsequent calls return {@code false}. The
     * {@link AckCoordinator} uses this to prevent force-RELEASE from overwriting a
     * just-queued successful ACCEPT/REJECT/RELEASE on the same record — without the marker,
     * commitPendingAcks could drain {@code [ACCEPT, RELEASE]} for the same offset and the
     * broker (which stores one ack type per offset before the next commitAsync) silently
     * keeps the later RELEASE, causing redelivery of an already-processed record.
     */
    public boolean markTerminalAckQueued() {
        return terminalAckQueued.compareAndSet(false, true);
    }

    /** Returns {@code true} if a terminal ACCEPT/REJECT/RELEASE has already been queued. */
    public boolean isTerminalAckQueued() {
        return terminalAckQueued.get();
    }
}
