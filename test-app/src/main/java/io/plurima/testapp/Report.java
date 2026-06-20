package io.plurima.testapp;

import java.util.ArrayList;
import java.util.List;

/** Collects per-scenario PASS/FAIL outcomes and prints a final summary. */
public final class Report {

    private final List<Entry> entries = new ArrayList<>();

    public Report() {}

    /** Run a scenario, swallowing exceptions so one failure does not abort the suite. */
    public void runScenario(String name, String description, Scenario body) {
        long startNanos = System.nanoTime();
        System.out.println();
        System.out.println("=== " + name + " ===");
        System.out.println("    " + description);
        try {
            body.run();
            long ms = (System.nanoTime() - startNanos) / 1_000_000L;
            System.out.println("    PASS (" + ms + " ms)");
            entries.add(new Entry(name, true, ms, null));
        } catch (Throwable t) {
            long ms = (System.nanoTime() - startNanos) / 1_000_000L;
            System.out.println("    FAIL (" + ms + " ms): " + t.getMessage());
            t.printStackTrace(System.out);
            entries.add(new Entry(name, false, ms, t.getMessage()));
        }
    }

    /** Print the final summary table and exit with code = number of failures. */
    public int printSummaryAndExitCode() {
        System.out.println();
        System.out.println("============================================================");
        System.out.println("  Plurima test-app — final summary");
        System.out.println("============================================================");
        int passed = 0, failed = 0;
        for (Entry e : entries) {
            String status = e.passed ? "PASS" : "FAIL";
            System.out.printf("  %s  %-60s  %6d ms%n", status, e.name, e.ms);
            if (!e.passed && e.error != null) {
                System.out.printf("        %s%n", e.error);
            }
            if (e.passed) passed++; else failed++;
        }
        System.out.println("------------------------------------------------------------");
        System.out.printf("  %d passed, %d failed (%d total)%n",
            passed, failed, entries.size());
        System.out.println("============================================================");
        return failed;
    }

    @FunctionalInterface
    public interface Scenario {
        void run() throws Exception;
    }

    private record Entry(String name, boolean passed, long ms, String error) {}
}
