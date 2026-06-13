package io.plurima.kafka.internal;

import io.plurima.kafka.annotation.Internal;

import java.util.Objects;

@Internal
public record RecordCoord(String topic, int partition, long offset) {

    public RecordCoord {
        Objects.requireNonNull(topic, "topic");
        if (partition < 0) {
            throw new IllegalArgumentException("partition must be >= 0, was " + partition);
        }
        if (offset < 0) {
            throw new IllegalArgumentException("offset must be >= 0, was " + offset);
        }
    }
}
