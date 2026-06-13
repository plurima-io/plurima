package io.plurima.kafka;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class OrderingModeTest {

    @Test
    void exposesUnorderedKeyAndPartitionValues() {
        assertThat(OrderingMode.values())
            .containsExactlyInAnyOrder(
                OrderingMode.UNORDERED,
                OrderingMode.KEY,
                OrderingMode.PARTITION);
    }
}
