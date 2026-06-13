package io.plurima.kafka.internal;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

class InFlightRegistryTest {

    private static InFlightRecord<byte[], byte[]> rec(String topic, int p, long o) {
        return new InFlightRecord<>(new ConsumerRecord<>(topic, p, o, new byte[0], new byte[0]));
    }

    @Test
    void registerIncrementsCount() {
        InFlightRegistry reg = new InFlightRegistry();
        assertThat(reg.register(rec("t", 0, 1))).isTrue();
        assertThat(reg.register(rec("t", 0, 2))).isTrue();
        assertThat(reg.currentInFlight()).isEqualTo(2);
    }

    @Test
    void duplicateCoordRegistrationReturnsFalseAndDoesNotReplace() {
        // PollLoop relies on this contract to skip dispatch + release the permit
        // when a broker redelivery arrives while the previous in-flight record for
        // the same coord is still active. Replacing the registered instance would
        // race the older worker's identity-aware complete() and leak a permit.
        InFlightRegistry reg = new InFlightRegistry();
        InFlightRecord<byte[], byte[]> original = rec("t", 0, 1);
        InFlightRecord<byte[], byte[]> duplicate = rec("t", 0, 1);

        assertThat(reg.register(original)).isTrue();
        assertThat(reg.register(duplicate))
            .as("duplicate coord while original is in-flight must return false")
            .isFalse();
        assertThat(reg.currentInFlight())
            .as("registry must NOT double-count")
            .isEqualTo(1);
        assertThat(reg.isCurrent(original))
            .as("original (the registered instance) is still current")
            .isTrue();
        assertThat(reg.isCurrent(duplicate))
            .as("duplicate instance is NOT current — older worker retains ownership")
            .isFalse();
    }

    @Test
    void completeDecrementsCountOnce() {
        InFlightRegistry reg = new InFlightRegistry();
        InFlightRecord<byte[], byte[]> r = rec("t", 0, 1);
        reg.register(r);
        assertThat(reg.complete(r)).isTrue();
        assertThat(reg.complete(r)).isFalse(); // idempotent
        assertThat(reg.currentInFlight()).isZero();
    }

    @Test
    void completeIsIdentityAwareSoStaleCallerCannotEvictRedelivery() {
        InFlightRegistry reg = new InFlightRegistry();
        InFlightRecord<byte[], byte[]> original = rec("t", 0, 1);
        InFlightRecord<byte[], byte[]> redelivery = rec("t", 0, 1);  // same coord, different instance

        reg.register(original);
        // force-RELEASE path: abandons the original by completing on the original instance.
        assertThat(reg.complete(original)).isTrue();
        assertThat(reg.currentInFlight()).isZero();

        // Broker redelivers the same coord; we register the new instance.
        reg.register(redelivery);
        assertThat(reg.currentInFlight()).isEqualTo(1);

        // The orphan worker from the original now finishes and calls complete(original).
        // It MUST NOT remove the redelivery, because the redelivery is a distinct instance.
        assertThat(reg.complete(original)).isFalse();
        assertThat(reg.currentInFlight())
            .as("orphan worker's stale complete() must not evict the redelivery")
            .isEqualTo(1);

        // isCurrent reports identity correctly for both.
        assertThat(reg.isCurrent(redelivery)).isTrue();
        assertThat(reg.isCurrent(original)).isFalse();
    }

    @Test
    void activeRecordsReturnsSnapshot() {
        InFlightRegistry reg = new InFlightRegistry();
        InFlightRecord<byte[], byte[]> a = rec("t", 0, 1);
        InFlightRecord<byte[], byte[]> b = rec("t", 0, 2);
        reg.register(a);
        reg.register(b);

        assertThat(reg.activeRecords()).extracting(InFlightRecord::coord)
            .containsExactlyInAnyOrder(a.coord(), b.coord());
    }

    @Test
    void awaitDrainReturnsTrueWhenEmpty() {
        InFlightRegistry reg = new InFlightRegistry();
        assertThat(reg.awaitDrain(Duration.ofMillis(50))).isTrue();
    }

    @Test
    void awaitDrainReturnsFalseWhenStillInFlight() {
        InFlightRegistry reg = new InFlightRegistry();
        reg.register(rec("t", 0, 1));
        assertThat(reg.awaitDrain(Duration.ofMillis(50))).isFalse();
    }

    @Test
    void awaitDrainWakesOnComplete() throws Exception {
        InFlightRegistry reg = new InFlightRegistry();
        InFlightRecord<byte[], byte[]> r = rec("t", 0, 1);
        reg.register(r);

        Thread completer = new Thread(() -> {
            try { Thread.sleep(20); } catch (InterruptedException ignored) {}
            reg.complete(r);
        });
        completer.start();

        long start = System.nanoTime();
        boolean drained = reg.awaitDrain(Duration.ofSeconds(2));
        long elapsedMs = (System.nanoTime() - start) / 1_000_000;

        completer.join();
        assertThat(drained).isTrue();
        assertThat(elapsedMs).isLessThan(1_500);
    }
}
