package io.plurima.testapp.bench;

/**
 * One row of the final benchmark summary table. {@code vanillaMs} or {@code plurimaMs} can
 * be -1 to denote "not applicable" — useful for Plurima-only features (KEY-shard
 * parallelism, DLT routing) where the vanilla side has no equivalent.
 */
record BenchResult(
    String name,
    String setup,
    long vanillaMs,
    long plurimaMs,
    String notes
) {

    String formatSpeedup() {
        if (vanillaMs < 0 || plurimaMs < 0) return "n/a";
        if (plurimaMs == 0) return "inf";
        double s = (double) vanillaMs / plurimaMs;
        return String.format("%.1fx", s);
    }

    static String dash(long ms) {
        return ms < 0 ? "n/a" : ms + " ms";
    }
}
