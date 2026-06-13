package io.plurima.kafka.internal;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RecordCoordTest {

    @Test
    void equalRecordsHaveEqualCoords() {
        RecordCoord a = new RecordCoord("orders", 3, 42L);
        RecordCoord b = new RecordCoord("orders", 3, 42L);
        assertThat(a).isEqualTo(b).hasSameHashCodeAs(b);
    }

    @Test
    void rejectsNullTopic() {
        assertThatThrownBy(() -> new RecordCoord(null, 0, 0))
            .isInstanceOf(NullPointerException.class);
    }

    @Test
    void rejectsNegativePartition() {
        assertThatThrownBy(() -> new RecordCoord("t", -1, 0))
            .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsNegativeOffset() {
        assertThatThrownBy(() -> new RecordCoord("t", 0, -1))
            .isInstanceOf(IllegalArgumentException.class);
    }
}
