package io.plurima.kafka.internal;

import io.plurima.kafka.annotation.Internal;

@Internal
public interface WorkDispatcher {
    void dispatch(InFlightRecord<?, ?> record);
}
