package io.plurima.kafka.internal;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;


import static org.assertj.core.api.Assertions.assertThat;

class InFlightRecordTest {

    @Test
    void coordDerivedFromConsumerRecord() {
        ConsumerRecord<byte[], byte[]> cr =
            new ConsumerRecord<>("orders", 2, 17L, "k".getBytes(), "v".getBytes());

        InFlightRecord<byte[], byte[]> r = new InFlightRecord<>(cr);

        assertThat(r.coord()).isEqualTo(new RecordCoord("orders", 2, 17L));
        assertThat(r.attempt()).isZero();
        assertThat(r.consumerRecord()).isSameAs(cr);
    }

    @Test
    void incrementAttemptIsMonotonic() {
        ConsumerRecord<byte[], byte[]> cr =
            new ConsumerRecord<>("t", 0, 1L, new byte[0], new byte[0]);
        InFlightRecord<byte[], byte[]> r = new InFlightRecord<>(cr);

        r.incrementAttempt();
        r.incrementAttempt();

        assertThat(r.attempt()).isEqualTo(2);
    }
}
