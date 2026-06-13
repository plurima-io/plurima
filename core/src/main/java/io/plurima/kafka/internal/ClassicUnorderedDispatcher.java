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
 * Unordered dispatch for the CLASSIC_BASIC engine: every record gets its own worker
 * thread, regardless of partition. Used by {@link io.plurima.kafka.OrderingMode#UNORDERED}.
 *
 * <p>Pre-v0.1 UNORDERED routed through {@link ClassicPartitionSerialDispatcher}, which
 * processes one record at a time per partition. That's correct for PARTITION mode but
 * wastes throughput when the user explicitly opted out of ordering — distinct records
 * on the same partition could run concurrently with no semantic loss. This dispatcher
 * launches each record into its own virtual thread; {@link CommitFrontier} handles the
 * out-of-order completion arithmetic safely.
 *
 * <p><b>Revoke safety.</b> Each launched worker carries the captured {@code frontier}
 * reference. {@code processor.process} does an identity check via
 * {@code frontiers.get(tp) == frontier} before invoking the listener and again at every
 * {@code markComplete} call — so a record dispatched on generation N never lands
 * side-effects on generation N+1's frontier after revoke + reassign.
 *
 * <p><b>Launcher failure.</b> If {@code launcher.launch(...)} is rejected (typically
 * during shutdown), we fire {@code onRecordDone} for the affected record so the
 * caller's {@code inFlight} counter stays balanced. We do NOT throw — see the matching
 * recovery path in {@link ClassicKeyShardDispatcher#launchNext} for the reasoning.
 */
@Internal
final class ClassicUnorderedDispatcher implements ClassicDispatcher {

    private static final Logger log = LoggerFactory.getLogger(ClassicUnorderedDispatcher.class);

    private final WorkerLauncher launcher;
    private final ClassicRecordProcessor processor;
    private final BooleanSupplier running;
    private final BiPredicate<TopicPartition, CommitFrontier> stillOwned;

    ClassicUnorderedDispatcher(
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
        for (ConsumerRecord<byte[], byte[]> raw : records) {
            try {
                launcher.launch(() -> {
                    try {
                        if (!running.getAsBoolean() || !stillOwned.test(tp, frontier)) {
                            return;
                        }
                        try {
                            processor.process(frontier, tp, raw);
                        } catch (Throwable t) {
                            log.error("processOne threw for {}@{}; record will not commit",
                                tp, raw.offset(), t);
                        }
                    } finally {
                        onRecordDone.run();
                    }
                });
            } catch (Throwable t) {
                log.error("Worker launch rejected for {}@{}; dropping record",
                    tp, raw.offset(), t);
                onRecordDone.run();
            }
        }
    }
}
