package io.plurima.kafka.internal;

import io.plurima.kafka.ConsumerContext;
import io.plurima.kafka.OrderingMode;
import io.plurima.kafka.annotation.Internal;
import org.apache.kafka.clients.consumer.AcknowledgeType;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Internal
public final class UnorderedDispatcher implements WorkDispatcher {

    private static final Logger log = LoggerFactory.getLogger(UnorderedDispatcher.class);

    private final WorkerProcessor processor;
    private final InFlightRegistry registry;
    private final AckCoordinator coordinator;
    private final WorkerLauncher launcher;
    private final BackpressureGate gate;
    private final OrderingMode orderingMode;

    public UnorderedDispatcher(
        WorkerProcessor processor,
        InFlightRegistry registry,
        AckCoordinator coordinator,
        WorkerLauncher launcher,
        BackpressureGate gate,
        OrderingMode orderingMode) {
        this.processor = processor;
        this.registry = registry;
        this.coordinator = coordinator;
        this.launcher = launcher;
        this.gate = gate;
        this.orderingMode = orderingMode;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void dispatch(InFlightRecord<?, ?> r) {
        InFlightRecord<byte[], byte[]> typed = (InFlightRecord<byte[], byte[]>) r;
        try {
            launcher.launch(() -> {
                try {
                    processor.process(typed, new Ctx(typed, orderingMode));
                } finally {
                    // Release the permit only if WE removed the entry. Identity-aware complete()
                    // returns false when forceReleaseStuckRecords has abandoned this record OR
                    // when a newer redelivery has taken over our coord; either way, the permit
                    // has already been (or will be) released by the rightful owner.
                    if (registry.complete(typed)) {
                        gate.release(1);
                    }
                }
            });
        } catch (Throwable t) {
            // Launcher rejected the task (typically RejectedExecutionException once the
            // worker pool is shut down). PollLoop has already called registry.register
            // and consumed a backpressure permit for this record; if we returned here
            // without compensating:
            //
            //   1. The registry would carry a never-completed entry — the next poll's
            //      drain barrier would block until it times out.
            //   2. The gate would leak the permit — backpressure deadlock.
            //   3. KIP-932 explicit mode requires EVERY polled record be acked before the
            //      next poll(); the absent ack would make the next consumer.poll() throw
            //      IllegalStateException and stall the loop entirely.
            //
            // Compensate by queueing a RELEASE (broker redelivers to any consumer in the
            // share group on the next poll) and then completing the registry + releasing
            // the gate. The RELEASE must happen BEFORE registry.complete because the
            // coordinator's commit batch is keyed off active registry entries; completing
            // first would race the eventual commitPendingAcks call.
            log.error("Worker launch rejected for {} (likely shutdown or pool saturation); "
                + "queueing RELEASE and dropping record so the drain barrier, gate, and "
                + "KIP-932 ack contract stay consistent", typed.coord(), t);
            coordinator.queueAck(typed, AcknowledgeType.RELEASE);
            if (registry.complete(typed)) {
                gate.release(1);
            }
        }
    }

    private record Ctx(InFlightRecord<byte[], byte[]> r, OrderingMode mode)
        implements ConsumerContext {

        @Override
        public int deliveryCount() {
            ConsumerRecord<byte[], byte[]> cr = r.consumerRecord();
            return cr.deliveryCount().orElse((short) 1);
        }

        @Override
        public OrderingMode orderingMode() {
            return mode;
        }
    }
}
