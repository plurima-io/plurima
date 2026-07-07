package io.plurima.kafka.internal;

import io.plurima.kafka.ConsumerContext;
import io.plurima.kafka.OrderingMode;
import io.plurima.kafka.annotation.Internal;

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
    public int deliveryCount() {
        return in.attempt() + 1;
    }

    @Override
    public OrderingMode orderingMode() {
        return ordering;
    }
}
