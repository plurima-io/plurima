package io.plurima.kafka;

import io.plurima.kafka.annotation.Stable;

/**
 * Selects the underlying Kafka client primitive the consumer runs on.
 *
 * <p>Plurima offers two engines and the choice is fundamental — not just a knob.
 * It determines the entire delivery model:
 *
 * <ul>
 *   <li>{@link #SHARE} — uses {@code KafkaShareConsumer} (KIP-932). Records are
 *       individually acquired by any consumer in the share group; the broker
 *       tracks per-record acquisition. Best for unordered, high-concurrency
 *       workloads where multiple consumers share the work. Per-key/per-partition
 *       FIFO is instance-local (see {@link OrderingMode}).</li>
 *
 *   <li>{@link #CLASSIC_BASIC} — uses vanilla {@code KafkaConsumer} with
 *       consumer-group partition assignment. Each assigned partition is owned by
 *       exactly one consumer in the group; dispatch within a partition is
 *       {@link OrderingMode}-dependent (offset-serial for {@code PARTITION},
 *       key-sharded for {@code KEY}, fully parallel for {@code UNORDERED}). Gives
 *       real cross-cluster {@link OrderingGuarantee#STRICT STRICT} ordering on
 *       KEY/PARTITION at the cost of share-group features (no per-record
 *       acquisition, no broker-side delivery counter, no per-record RELEASE).</li>
 * </ul>
 *
 * <p>The two engines have different lifecycle, retry, and rebalance semantics.
 * They are NOT interchangeable mid-deployment — switching engines on a running
 * application means the broker-side consumer state (share-group vs
 * consumer-group offsets) does NOT carry over. See the user guide § Engines.
 *
 * <p>Default: {@link #SHARE}. Set {@link PlurimaConsumerBuilder#engine(ConsumerEngine)}
 * to opt into classic.
 *
 * @since 0.1.0
 */
@Stable(since = "0.1.0")
public enum ConsumerEngine {

    /**
     * KIP-932 share-consumer engine. The original Plurima primitive.
     * Suitable for: unordered high-concurrency processing, fine-grained per-record
     * retry/DLT semantics. Only {@link OrderingMode#UNORDERED} is supported; KEY and
     * PARTITION are rejected at build time (share groups deliver records to any
     * member of the group, so in-process ordering cannot extend to the cluster).
     */
    SHARE,

    /**
     * Vanilla {@code KafkaConsumer} engine. Continuous-poll model with dispatch
     * tailored to the chosen {@link OrderingMode}:
     * <ul>
     *   <li>{@link OrderingMode#UNORDERED}: one virtual-thread worker per record;
     *       no in-partition ordering. Maximum intra-partition throughput.</li>
     *   <li>{@link OrderingMode#KEY}: records sharded by hash of key within each
     *       partition; same-key records serial, distinct keys parallel.</li>
     *   <li>{@link OrderingMode#PARTITION}: one worker per partition processing
     *       records in offset order.</li>
     * </ul>
     * Suitable for: workloads requiring cross-cluster per-partition or per-key FIFO
     * ordering with multiple consumer instances horizontally scaled.
     *
     * <p>Restrictions:
     * <ul>
     *   <li>No {@link io.plurima.kafka.ack.ManualAckListener} support — manual ack
     *       with {@code RELEASE} has no equivalent in classic consumers; rejected
     *       at builder time.</li>
     *   <li>No per-record lock leases — classic consumer groups don't have them.
     *       Plurima's poll thread polls on every iteration regardless of worker
     *       progress (continuous-poll + pause/resume backpressure), so the broker's
     *       {@code max.poll.interval.ms} is satisfied by the poll-thread cadence,
     *       not by handler completion. Long handlers and long retry delays do not
     *       fence the consumer.</li>
     * </ul>
     */
    CLASSIC_BASIC
}
