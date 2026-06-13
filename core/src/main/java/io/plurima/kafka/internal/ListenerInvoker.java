package io.plurima.kafka.internal;

import io.plurima.kafka.ConsumerContext;
import io.plurima.kafka.OrderingMode;
import io.plurima.kafka.RecordListener;
import io.plurima.kafka.ack.AckContext;
import io.plurima.kafka.ack.ManualAckListener;
import io.plurima.kafka.annotation.Internal;
import io.plurima.kafka.deserializer.RecordDeserializer;
import org.apache.kafka.clients.consumer.AcknowledgeType;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Encapsulates deserialization and listener dispatch for a single in-flight record.
 * <p>
 * <ul>
 *   <li><em>Implicit mode</em>: deserializes then calls a {@link RecordListener};
 *       the caller (WorkerProcessor) is responsible for emitting the ACCEPT ack on
 *       normal return.</li>
 *   <li><em>Manual mode</em>: deserializes then calls a {@link ManualAckListener},
 *       providing an idempotent {@link AckContext} that delegates to
 *       {@link AckCoordinator#queueAck}.</li>
 * </ul>
 */
@Internal
public final class ListenerInvoker {

    private static final Logger log = LoggerFactory.getLogger(ListenerInvoker.class);

    private final Invoker invoker;
    private final boolean manualAck;

    private ListenerInvoker(Invoker invoker, boolean manualAck) {
        this.invoker = invoker;
        this.manualAck = manualAck;
    }

    /** Returns {@code true} if this invoker was created in manual-ack mode. */
    public boolean isManualAck() {
        return manualAck;
    }

    /**
     * Deserializes the raw record, calls the listener, and (in manual mode) wires
     * the provided {@link AckContext} to the coordinator.
     *
     * @throws Throwable any exception thrown by the listener is propagated as-is.
     */
    public void invoke(InFlightRecord<byte[], byte[]> r,
                       ConsumerContext ctx,
                       AckCoordinator coordinator) throws Throwable {
        invoker.run(r, ctx, coordinator);
    }

    // ── Factory methods ──────────────────────────────────────────────────────

    /** Creates an invoker backed by an auto-ack {@link RecordListener}. */
    public static <K, V> ListenerInvoker forImplicit(
            RecordListener<K, V> listener,
            RecordDeserializer<K> keyDeser,
            RecordDeserializer<V> valueDeser) {
        return new ListenerInvoker((r, ctx, coord) -> {
            ConsumerRecord<K, V> typed = deserialize(r, keyDeser, valueDeser);
            listener.onRecord(typed, ctx);
        }, false);
    }

    /** Creates an invoker backed by a manual-ack {@link ManualAckListener}. */
    public static <K, V> ListenerInvoker forManual(
            ManualAckListener<K, V> listener,
            RecordDeserializer<K> keyDeser,
            RecordDeserializer<V> valueDeser) {
        return new ListenerInvoker((r, ctx, coord) -> {
            ConsumerRecord<K, V> typed = deserialize(r, keyDeser, valueDeser);
            IdempotentAckContext ackCtx = new IdempotentAckContext(ctx, r, coord);
            listener.onRecord(typed, ackCtx);
            if (!ackCtx.wasAcked()) {
                // User listener returned without calling acknowledge() — auto-RELEASE so the
                // broker re-delivers (preserving at-least-once). This is a programmer error
                // worth a warn-log.
                log.warn("ManualAckListener returned without calling AckContext.acknowledge() for {}; "
                    + "auto-RELEASE issued. Either call acknowledge() explicitly or use RecordListener.",
                    r.coord());
                coord.queueAck(r, AcknowledgeType.RELEASE);
                ackCtx.markAckedExternally();  // close the context — late async acks become no-ops
            }
        }, true);
    }

    // ── Internal helpers ─────────────────────────────────────────────────────

    @SuppressWarnings({"OptionalUsedAsFieldOrParameterType", "unchecked"})
    private static <K, V> ConsumerRecord<K, V> deserialize(
            InFlightRecord<byte[], byte[]> r,
            RecordDeserializer<K> keyDeser,
            RecordDeserializer<V> valueDeser) {
        ConsumerRecord<byte[], byte[]> raw = r.consumerRecord();
        // Fast path: identity bytes-in/bytes-out. K == V == byte[]; the new
        // ConsumerRecord would just wrap the same byte[] references — pure waste on
        // the worker hot path. The cast is safe because both type parameters
        // resolve to byte[].
        if (keyDeser == RecordDeserializer.IDENTITY_BYTES
            && valueDeser == RecordDeserializer.IDENTITY_BYTES) {
            return (ConsumerRecord<K, V>) raw;
        }
        K key   = keyDeser.deserialize(raw.topic(), raw.key());
        V value = valueDeser.deserialize(raw.topic(), raw.value());
        // Use the 12-arg constructor available in kafka-clients 4.2.0:
        // (topic, partition, offset, timestamp, timestampType,
        //  serializedKeySize, serializedValueSize, key, value,
        //  headers, leaderEpoch, deliveryCount)
        return new ConsumerRecord<>(
            raw.topic(),
            raw.partition(),
            raw.offset(),
            raw.timestamp(),
            raw.timestampType(),
            raw.serializedKeySize(),
            raw.serializedValueSize(),
            key,
            value,
            raw.headers(),
            raw.leaderEpoch(),
            raw.deliveryCount());
    }

    @FunctionalInterface
    private interface Invoker {
        void run(InFlightRecord<byte[], byte[]> r,
                 ConsumerContext ctx,
                 AckCoordinator coord) throws Throwable;
    }

    private static final class IdempotentAckContext implements AckContext {

        private final ConsumerContext delegate;
        private final InFlightRecord<byte[], byte[]> record;
        private final AckCoordinator coordinator;
        private final AtomicBoolean acked = new AtomicBoolean();

        IdempotentAckContext(ConsumerContext delegate,
                             InFlightRecord<byte[], byte[]> record,
                             AckCoordinator coordinator) {
            this.delegate   = delegate;
            this.record     = record;
            this.coordinator = coordinator;
        }

        @Override
        public void acknowledge(AcknowledgeType type) {
            if (type == AcknowledgeType.RENEW) {
                // RENEW is non-terminal: returning from the handler would complete the
                // registry entry while the broker still considered the record acquired.
                // Plurima does not expose lock renewal — long handlers must rely on a
                // sufficiently large broker-side group.share.record.lock.duration.ms.
                log.warn("AckContext.acknowledge(RENEW) is not supported; ignored. Configure a "
                    + "longer broker-side group.share.record.lock.duration.ms instead.");
                return;
            }
            if (acked.compareAndSet(false, true)) {
                coordinator.queueAck(record, type);
            }
        }

        boolean wasAcked() { return acked.get(); }

        /** Mark as already acked to silence any late async user-acknowledge() calls. */
        void markAckedExternally() {
            acked.set(true);
        }

        @Override public short deliveryCount()                    { return delegate.deliveryCount(); }
        @Override public Optional<Short> deliveryCountOptional()  { return delegate.deliveryCountOptional(); }
        @Override public OrderingMode orderingMode()              { return delegate.orderingMode(); }
    }
}
