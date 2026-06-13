package io.plurima.kafka.internal;

import io.plurima.kafka.annotation.Internal;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;

import java.util.List;

/**
 * Strategy for dispatching a partition's records to worker threads on the classic engine.
 *
 * <p>Three implementations exist:
 * <ul>
 *   <li>{@link ClassicUnorderedDispatcher} — one worker per record; same-partition
 *       records run concurrently. Used by UNORDERED mode.</li>
 *   <li>{@link ClassicPartitionSerialDispatcher} — one worker per partition, records
 *       processed strictly in offset order. Used by PARTITION mode.</li>
 *   <li>{@link ClassicKeyShardDispatcher} — multiple workers per partition, keys hashed
 *       into a fixed shard count; records with the same key run serially in offset order
 *       inside their shard but distinct shards run concurrently. Used by KEY mode.</li>
 * </ul>
 *
 * <p>The dispatcher does not own the per-record processing logic — that lives in
 * {@code ClassicPollLoop.processOne} and is passed in via constructor injection. The
 * dispatcher's job is purely the topology of which worker thread runs which record.
 *
 * <p><b>Generation guard.</b> Each dispatch is parameterised by the {@link CommitFrontier}
 * the records belong to. The frontier reference is the per-partition generation token —
 * if a partition is revoked and later reassigned to this consumer, the new dispatch
 * receives a different frontier instance. Workers pass the captured frontier through to
 * {@code processOne} so {@code markComplete} can identity-check before applying the
 * completion. PARTITION-mode workers also use the frontier reference to short-circuit
 * mid-batch iteration when ownership changes (see
 * {@link ClassicPartitionSerialDispatcher}).
 */
@Internal
interface ClassicDispatcher {

    /**
     * Submit a partition's records for processing. The dispatcher launches workers via
     * its injected {@code WorkerLauncher}; {@code onRecordDone} is invoked exactly once
     * per record after processing completes (success, reject, DLT — any terminal state,
     * INCLUDING skipped records when this dispatch's frontier is no longer current).
     *
     * <p>This method must NOT block on worker completion — it returns as soon as the
     * records have been enqueued / launched.
     *
     * @param frontier the commit frontier this batch belongs to. Workers pass it
     *                 through to {@code processOne} so {@code markComplete} can
     *                 identity-check against {@code frontiers.get(tp)} and drop
     *                 completions from stale generations.
     */
    void dispatch(
        TopicPartition tp,
        CommitFrontier frontier,
        List<ConsumerRecord<byte[], byte[]>> records,
        Runnable onRecordDone);
}
