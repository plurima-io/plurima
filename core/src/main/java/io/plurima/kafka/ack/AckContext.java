package io.plurima.kafka.ack;

import io.plurima.kafka.ConsumerContext;
import io.plurima.kafka.annotation.Stable;

/**
 * Extends {@link ConsumerContext} with explicit acknowledgment. Available to
 * {@link ManualAckListener} implementations only — the standard
 * {@link io.plurima.kafka.RecordListener} receives a plain {@link ConsumerContext}
 * and is auto-acknowledged on normal return.
 */
@Stable(since = "0.1.0")
public interface AckContext extends ConsumerContext {
    /**
     * Acknowledge the current record; the actual ack is sent on the next poll cycle by
     * the {@code AckCoordinator}.
     *
     * <p>returning without acknowledging auto-RELEASEs; the first acknowledgement wins and
     * subsequent calls are no-ops.
     *
     * @param type terminal ack type — {@code ACCEPT}, {@code REJECT}, or {@code RELEASE}.
     *     Lock renewal is not exposed by Plurima — rely on a sufficiently long broker-side
     *     {@code group.share.record.lock.duration.ms} for long handlers.
     */
    void acknowledge(AckType type);
}
