package io.plurima.kafka.internal;

import io.plurima.kafka.annotation.Internal;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.function.BiPredicate;
import java.util.function.BooleanSupplier;

/**
 * Partition-serial dispatch: one worker per partition processes that partition's records
 * in offset order. Used by the {@code PARTITION} ordering mode.
 *
 * <p>Same-partition records flow through one worker, so cross-cluster per-partition
 * FIFO holds when combined with classic consumer-group exclusive partition ownership.
 * For UNORDERED workloads that don't need any FIFO contract, see
 * {@link ClassicUnorderedDispatcher} which launches each record into its own worker
 * for higher intra-partition throughput.
 *
 * <p><b>Generation guard.</b> Each dispatch carries a {@link CommitFrontier} reference;
 * before processing every record the worker checks {@code stillOwned.test(tp, frontier)}.
 * If the partition was revoked (or revoked + reassigned, which installs a NEW frontier
 * for the same TopicPartition), the worker skips the remaining records — they would
 * be re-processed by the new owner anyway, and continuing to invoke the listener for
 * them is wasted work AND risks double-side-effects. We still fire {@code onRecordDone}
 * for each skipped record to keep the backpressure counter balanced.
 */
@Internal
final class ClassicPartitionSerialDispatcher implements ClassicDispatcher {

    private static final Logger log = LoggerFactory.getLogger(ClassicPartitionSerialDispatcher.class);

    private final WorkerLauncher launcher;
    private final ClassicRecordProcessor processor;
    private final BooleanSupplier running;
    private final BiPredicate<TopicPartition, CommitFrontier> stillOwned;

    ClassicPartitionSerialDispatcher(
        WorkerLauncher launcher,
        ClassicRecordProcessor processor,
        BooleanSupplier running,
        BiPredicate<TopicPartition, CommitFrontier> stillOwned) {
        this.launcher = launcher;
        this.processor = processor;
        this.running = running;
        this.stillOwned = stillOwned;
    }

    @Override
    public void dispatch(
        TopicPartition tp,
        CommitFrontier frontier,
        List<ConsumerRecord<byte[], byte[]>> records,
        Runnable onRecordDone) {
        try {
            launcher.launch(() -> {
                for (ConsumerRecord<byte[], byte[]> raw : records) {
                    if (!running.getAsBoolean()) {
                        // Loop is shutting down — call onRecordDone for the remaining records so
                        // the batch-completion accounting (inFlight counter) doesn't deadlock.
                        onRecordDone.run();
                        continue;
                    }
                    if (!stillOwned.test(tp, frontier)) {
                        // Partition was revoked (or revoked + reassigned, which installs a NEW
                        // frontier instance) since dispatch. Stop processing this batch — the
                        // new owner redelivers from the last committed offset. Per-record check
                        // means we stop AS SOON AS revoke is detected, not at end of batch.
                        onRecordDone.run();
                        continue;
                    }
                    try {
                        processor.process(frontier, tp, raw);
                    } catch (Throwable t) {
                        log.error("processOne threw for {}@{}; skipping record and continuing batch",
                            tp, raw.offset(), t);
                    } finally {
                        onRecordDone.run();
                    }
                }
            });
        } catch (Throwable t) {
            // Launcher rejected the task (RejectedExecutionException on shutdown / pool
            // saturation). Nothing will fire onRecordDone for any record in this batch;
            // we must compensate so inFlight stays accurate (caller already incremented
            // by records.size() before this call).
            log.error("Worker launch rejected for {} batch of {} records; dropping",
                tp, records.size(), t);
            for (int i = 0; i < records.size(); i++) {
                onRecordDone.run();
            }
        }
    }
}
