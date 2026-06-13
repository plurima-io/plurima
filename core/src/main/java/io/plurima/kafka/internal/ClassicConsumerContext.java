package io.plurima.kafka.internal;

import io.plurima.kafka.ConsumerContext;
import io.plurima.kafka.OrderingMode;
import io.plurima.kafka.annotation.Internal;

import java.util.Optional;

/**
 * {@link ConsumerContext} implementation for the CLASSIC_BASIC engine. Classic
 * consumer groups have no broker-side delivery counter, so {@code deliveryCount}
 * reports Plurima's in-process attempt counter (1 on the first invocation,
 * incremented on each retry).
 */
@Internal
record ClassicConsumerContext(InFlightRecord<byte[], byte[]> in, OrderingMode ordering)
    implements ConsumerContext {

    @Override
    public short deliveryCount() {
        return (short) (in.attempt() + 1);
    }

    @Override
    public Optional<Short> deliveryCountOptional() {
        return Optional.of((short) (in.attempt() + 1));
    }

    @Override
    public OrderingMode orderingMode() {
        return ordering;
    }
}
