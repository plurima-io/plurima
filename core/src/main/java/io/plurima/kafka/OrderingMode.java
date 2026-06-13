package io.plurima.kafka;

import io.plurima.kafka.annotation.Stable;

/**
 * Ordering guarantees the consumer pipeline provides for delivered records.
 *
 * <p>The semantics are tied to {@link ConsumerEngine}:
 *
 * <ul>
 *   <li>{@link ConsumerEngine#SHARE SHARE} supports only {@link #UNORDERED} —
 *       Kafka 4.2 share groups (KIP-932) deliver any record to any consumer in
 *       the group, so no instance-local mechanism can promise cross-cluster
 *       per-key or per-partition FIFO. The builder rejects SHARE + KEY and
 *       SHARE + PARTITION at construction time.</li>
 *   <li>{@link ConsumerEngine#CLASSIC_BASIC CLASSIC_BASIC} supports all three.
 *       Classic consumer groups pin each partition to exactly one member, so
 *       per-partition records flow through one consumer in offset order, and
 *       (with the default key-aware partitioner) same-key records always land
 *       on the same partition — yielding genuine cross-cluster per-key FIFO.
 *       {@link #KEY} additionally parallelises within a partition by hashing
 *       keys into in-process shards.</li>
 * </ul>
 *
 * <p><b>Prior to v0.1</b>, {@code OrderingMode.KEY} and {@code OrderingMode.PARTITION}
 * were also available with {@link ConsumerEngine#SHARE} but offered only
 * instance-local ordering — a guarantee that did not extend across multiple share
 * consumers in the same group. The labels misled users into expecting cluster-wide
 * ordering, so v0.1 removes the combination at builder time. Users who need
 * cross-cluster ordering should use {@link ConsumerEngine#CLASSIC_BASIC}; users
 * who were running a single share consumer with KEY/PARTITION for in-process
 * parallelism should switch to UNORDERED (which gives the same throughput) or to
 * the classic engine if they want explicit ordering semantics.
 */
@Stable(since = "0.1.0")
public enum OrderingMode {
    /**
     * No ordering guarantees. Records dispatch to workers as polled.
     * Highest throughput. Default. The only mode supported by SHARE engine.
     */
    UNORDERED,

    /**
     * Per-key FIFO ordering with intra-partition parallelism. <b>CLASSIC_BASIC only.</b>
     *
     * <p>Records are hashed into a fixed {@code shardCount} of in-memory shards per
     * partition (default {@code concurrency × 4}); same key → same shard → serial
     * processing inside the shard. Different shards run concurrently, so a partition
     * can have N records in flight when there are N distinct key-shards in the batch.
     *
     * <p>Cross-cluster guarantee: with the default Kafka key-aware partitioner, same-key
     * records land on the same partition, and classic consumer-group assignment owns
     * each partition exclusively. The per-key FIFO therefore holds across the whole
     * consumer group, not just within one Plurima instance.
     *
     * <p>Collision tradeoff: different keys may hash to the same shard and be serialised
     * unnecessarily. With {@code shardCount = concurrency × 4} (default), the rate is
     * &lt; 2% for typical key cardinalities.
     */
    KEY,

    /**
     * Strict per-partition FIFO ordering. <b>CLASSIC_BASIC only.</b>
     *
     * <p>Records on the same {@code (topic, partition)} are processed in offset order
     * by one worker; partitions run concurrently up to the number of assigned
     * partitions. No intra-partition parallelism — if you want concurrent processing
     * of distinct keys within a partition, use {@link #KEY} instead.
     *
     * <p>Cross-cluster guarantee: classic consumer-group assignment pins each
     * partition to exactly one member at a time, so per-partition records flow
     * through one consumer in offset order across the whole group.
     */
    PARTITION
}
