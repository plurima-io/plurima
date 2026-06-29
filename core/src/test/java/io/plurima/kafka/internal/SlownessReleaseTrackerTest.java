package io.plurima.kafka.internal;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SlownessReleaseTrackerTest {

    private static RecordCoord coord(long offset) {
        return new RecordCoord("t", 0, offset);
    }

    @Test
    void countsAccumulatePerCoord() {
        SlownessReleaseTracker t = new SlownessReleaseTracker(1024);
        assertThat(t.releaseCount(coord(1))).isZero();
        t.recordRelease(coord(1));
        t.recordRelease(coord(1));
        t.recordRelease(coord(2));
        assertThat(t.releaseCount(coord(1))).isEqualTo(2);
        assertThat(t.releaseCount(coord(2))).isEqualTo(1);
        assertThat(t.releaseCount(coord(3))).isZero();
    }

    @Test
    void evictsOldestPastCapacity() {
        SlownessReleaseTracker t = new SlownessReleaseTracker(2);
        t.recordRelease(coord(1));
        t.recordRelease(coord(2));
        t.recordRelease(coord(3));   // exceeds cap → oldest (coord 1) evicted
        // coord 1 evicted → count 0 (safe direction: a slowness release counts toward budget,
        // never the inverse). coords 2 and 3 retained.
        assertThat(t.releaseCount(coord(1))).isZero();
        assertThat(t.releaseCount(coord(2))).isEqualTo(1);
        assertThat(t.releaseCount(coord(3))).isEqualTo(1);
    }

    @Test
    void accessKeepsHotEntryFromEviction() {
        SlownessReleaseTracker t = new SlownessReleaseTracker(2);
        t.recordRelease(coord(1));
        t.recordRelease(coord(2));
        // Touch coord 1 (access-ordered LRU moves it to most-recent), then insert coord 3.
        // The least-recently-used (coord 2) is evicted, not the freshly-accessed coord 1.
        t.releaseCount(coord(1));
        t.recordRelease(coord(3));
        assertThat(t.releaseCount(coord(1))).isEqualTo(1);
        assertThat(t.releaseCount(coord(2))).isZero();
        assertThat(t.releaseCount(coord(3))).isEqualTo(1);
    }

    @Test
    void clearForgetsCount() {
        SlownessReleaseTracker t = new SlownessReleaseTracker(1024);
        t.recordRelease(coord(1));
        t.recordRelease(coord(1));
        assertThat(t.releaseCount(coord(1))).isEqualTo(2);
        t.clear(coord(1));
        assertThat(t.releaseCount(coord(1))).isZero();
        t.clear(coord(2));   // no-op for an absent coord
    }

    @Test
    void rejectsNonPositiveCapacity() {
        assertThatThrownBy(() -> new SlownessReleaseTracker(0))
            .isInstanceOf(IllegalArgumentException.class);
    }
}
