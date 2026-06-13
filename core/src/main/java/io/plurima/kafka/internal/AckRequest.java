package io.plurima.kafka.internal;

import io.plurima.kafka.annotation.Internal;
import org.apache.kafka.clients.consumer.AcknowledgeType;

import java.util.Objects;

@Internal
public record AckRequest(InFlightRecord<?, ?> record, AcknowledgeType type) {
    public AckRequest {
        Objects.requireNonNull(record, "record");
        Objects.requireNonNull(type, "type");
    }

    /** Convenience: same as {@code record().coord()}. */
    public RecordCoord coord() {
        return record.coord();
    }
}
