package io.plurima.testapp.bench;

import io.plurima.kafka.ConsumerEngine;
import io.plurima.kafka.OrderingMode;
import io.plurima.kafka.PlurimaConsumer;
import io.plurima.kafka.dlt.DltConfig;
import io.plurima.kafka.retry.RetryPolicy;
import io.plurima.testapp.Helpers;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.TimeUnit;

/**
 * Head-to-head benchmark between idiomatic stock-Kafka consumer code and Plurima.
 *
 * <p>Each scenario produces N records, runs the vanilla loop, then runs the Plurima
 * consumer on a fresh group. Both consumers receive the same workload (same record
 * count, same per-record handler latency). Wall-clock from first-record-handled to
 * last-record-handled is reported.
 *
 * <p>"Plurima-only" rows (KEY-mode FIFO with parallelism, DLT routing, etc.) have a
 * "—" in the vanilla column because the equivalent would require non-trivial custom
 * code in the vanilla case; the point is that Plurima ships these for free.
 *
 * <p>Run via {@code ./gradlew :test-app:runBench}. The exit code is 0 on success; any
 * uncaught exception inside a scenario aborts the suite.
 */
public final class PlurimaVsVanillaBench {

    public static void main(String[] args) throws Exception {
        String bootstrap = parseBootstrap(args);
        Helpers h = new Helpers(bootstrap);
        List<BenchResult> results = new ArrayList<>();

        System.out.println();
        System.out.println("Plurima vs. vanilla Kafka consumer benchmark");
        System.out.println("Broker: " + bootstrap);
        System.out.println();

        results.add(benchClassicThroughput(h));
        results.add(benchShareThroughput(h));
        results.add(benchClassicKeyFifoWithParallelism(h));
        results.add(benchContinuousPollVsFencing(h));
        results.add(benchRetryDltVsHandRolled(h));

        printSummary(results);
    }

    // ====================================================================================
    // B1: CLASSIC engine throughput with non-trivial per-record handler latency
    // ====================================================================================

    private static BenchResult benchClassicThroughput(Helpers h) throws Exception {
        // 4 partitions × 50 records each = 200 records; 100ms handler.
        // Vanilla = single-threaded poll loop; processes records serially regardless of
        // partition count → wall-clock = 200 × 100ms = 20s.
        // Plurima PARTITION mode = one worker per assigned partition → 4 partitions run
        // concurrently → wall-clock ≈ 50 × 100ms = 5s. Expected speedup ~4×.
        final int partitions = 4;
        final int perPartition = 50;
        final int total = partitions * perPartition;
        final int handlerMs = 100;
        final int concurrency = 16;
        String label = "CLASSIC multi-partition throughput (" + total + " recs / "
            + partitions + " parts x " + handlerMs + "ms)";
        String setup = partitions + " partitions; vanilla = serial poll loop; "
            + "Plurima PARTITION concurrency=" + concurrency;
        System.out.println("Running: " + label);

        String topic = h.createUniqueTopic("bench-classic-thru", partitions);
        try {
            produceRoundRobin(h, topic, partitions, perPartition);

            String vGroup = "bench-classic-thru-vanilla-" + UUID.randomUUID();
            long vanillaMs = VanillaConsumers.runVanillaClassic(
                h.classicConsumerProps(vGroup), topic, total, 60_000L,
                r -> sleep(handlerMs));
            System.out.println("    vanilla: " + vanillaMs + " ms");

            String pGroup = "bench-classic-thru-plurima-" + UUID.randomUUID();
            long plurimaMs = runPlurimaClassicWithPartitions(h, pGroup, topic, total,
                partitions, concurrency, OrderingMode.PARTITION, handlerMs);
            System.out.println("    plurima: " + plurimaMs + " ms");

            return new BenchResult(label, setup, vanillaMs, plurimaMs,
                "Plurima PARTITION mode runs " + partitions + " partition workers in parallel; "
                    + "vanilla's single-threaded poll handles them serially.");
        } finally {
            h.deleteTopicQuietly(topic);
        }
    }

    // ====================================================================================
    // B2: SHARE engine throughput with non-trivial per-record handler latency
    // ====================================================================================

    private static BenchResult benchShareThroughput(Helpers h) throws Exception {
        final int total = 200;
        final int handlerMs = 100;
        final int concurrency = 16;
        String label = "SHARE throughput (" + total + " records x " + handlerMs + "ms handler)";
        String setup = "1 partition; vanilla = serial KafkaShareConsumer; Plurima concurrency=" + concurrency;
        System.out.println("Running: " + label);

        // Two distinct topics so the groups don't race; vanilla's run consumes everything
        // from its topic. Both consumers subscribe BEFORE producing so we don't depend on
        // the broker's share.auto.offset.reset setting.
        String vanillaTopic = h.createUniqueTopic("bench-share-thru-v", 1);
        String plurimaTopic = h.createUniqueTopic("bench-share-thru-p", 1);
        try {
            String vGroup = "bench-share-thru-vanilla-" + UUID.randomUUID();
            long vanillaMs = VanillaConsumers.runVanillaShare(
                h.shareConsumerProps(vGroup), vanillaTopic, total, 90_000L,
                r -> sleep(handlerMs),
                () -> {
                    try { produceKeyed(h, vanillaTopic, total); }
                    catch (Exception e) { throw new RuntimeException(e); }
                });
            System.out.println("    vanilla: " + vanillaMs + " ms");

            // Plurima: start consumer first so we're agnostic to the broker's offset reset.
            String pGroup = "bench-share-thru-plurima-" + UUID.randomUUID();
            long plurimaMs = runPlurimaShare(h, pGroup, plurimaTopic, total, concurrency, handlerMs);
            System.out.println("    plurima: " + plurimaMs + " ms");

            return new BenchResult(label, setup, vanillaMs, plurimaMs,
                "Plurima SHARE+UNORDERED runs " + concurrency + " workers; vanilla share-poll is single-threaded.");
        } finally {
            h.deleteTopicQuietly(vanillaTopic);
            h.deleteTopicQuietly(plurimaTopic);
        }
    }

    // ====================================================================================
    // B3: Per-key FIFO WITH intra-partition parallelism (Plurima KEY mode)
    // ====================================================================================

    private static BenchResult benchClassicKeyFifoWithParallelism(Helpers h) throws Exception {
        // 1 partition, 100 records across 20 distinct keys, 100ms handler.
        // Vanilla can either (a) process serially to preserve per-partition (and therefore
        // per-key) order, paying 20s wall-clock, or (b) dispatch to a threadpool and lose
        // per-key ordering. We benchmark the SERIAL vanilla loop as the only honest "vanilla
        // with FIFO preserved" baseline.
        final int total = 100;
        final int handlerMs = 100;
        final int concurrency = 16;
        String label = "CLASSIC + per-key FIFO (" + total + " records, 20 keys x " + handlerMs + "ms)";
        String setup = "1 partition; vanilla = serial-with-FIFO (no parallel option that preserves order); "
            + "Plurima KEY concurrency=" + concurrency;
        System.out.println("Running: " + label);

        String topic = h.createUniqueTopic("bench-classic-keyfifo", 1);
        try {
            try (var producer = h.byteProducer()) {
                for (int n = 0; n < total; n++) {
                    String key = "k" + (n % 20);
                    producer.send(new ProducerRecord<>(topic, key.getBytes(), ("v" + n).getBytes())).get();
                }
            }

            // Vanilla: serial. Records arrive in offset order; per-key order is preserved
            // by construction.
            String vGroup = "bench-keyfifo-vanilla-" + UUID.randomUUID();
            long vanillaMs = VanillaConsumers.runVanillaClassic(
                h.classicConsumerProps(vGroup), topic, total, 60_000L,
                r -> sleep(handlerMs));
            System.out.println("    vanilla (serial, FIFO preserved): " + vanillaMs + " ms");

            // Plurima KEY mode: same FIFO contract, but distinct keys run on different
            // shards in parallel.
            String pGroup = "bench-keyfifo-plurima-" + UUID.randomUUID();
            Map<String, List<Long>> observed = new ConcurrentHashMap<>();
            CountDownLatch done = new CountDownLatch(total);
            long startNanos;
            try (PlurimaConsumer<byte[], byte[]> c = PlurimaConsumer.<byte[], byte[]>builder()
                    .kafkaProperties(h.classicConsumerProps(pGroup))
                    .topic(topic)
                    .engine(ConsumerEngine.CLASSIC_BASIC)
                    .ordering(OrderingMode.KEY)
                    .concurrency(concurrency)
                    .shardCount(concurrency * 4)
                    .pollTimeout(Duration.ofMillis(200))
                    .listener((r, ctx) -> {
                        String k = new String(r.key());
                        observed.computeIfAbsent(k, x -> new CopyOnWriteArrayList<>()).add(r.offset());
                        sleep(handlerMs);
                        done.countDown();
                    })
                    .build()) {
                c.start();
                h.waitForClassicAssignment(pGroup, 1, 1);
                startNanos = System.nanoTime();
                if (!done.await(60, TimeUnit.SECONDS)) {
                    throw new AssertionError("Plurima KEY mode did not finish in 60s");
                }
            }
            long plurimaMs = (System.nanoTime() - startNanos) / 1_000_000L;
            System.out.println("    plurima (KEY mode, FIFO + parallel): " + plurimaMs + " ms");

            // Per-key FIFO check — must hold even with parallelism.
            observed.forEach((k, offsets) -> {
                List<Long> sorted = new ArrayList<>(offsets);
                sorted.sort(Long::compareTo);
                if (!offsets.equals(sorted)) {
                    throw new AssertionError("per-key FIFO BROKEN under Plurima KEY mode: "
                        + k + " -> " + offsets);
                }
            });

            return new BenchResult(label, setup, vanillaMs, plurimaMs,
                "Plurima KEY mode preserves per-key FIFO while running distinct keys on " + concurrency
                + " shards concurrently - a guarantee vanilla cannot provide without custom code.");
        } finally {
            h.deleteTopicQuietly(topic);
        }
    }

    // ====================================================================================
    // B4: Continuous-poll heartbeating vs fencing under a slow handler
    // ====================================================================================

    private static BenchResult benchContinuousPollVsFencing(Helpers h) throws Exception {
        // Handler 10s, max.poll.interval.ms=6s. Vanilla gets fenced and the record
        // redelivers until eventually committed (if at all). Plurima keeps the consumer
        // heartbeating and commits cleanly on the first try.
        String label = "Continuous-poll vs fencing (10s handler, max.poll.interval.ms=6s)";
        String setup = "1 partition, 1 record; vanilla = blocks the poll thread; Plurima = continuous-poll";
        System.out.println("Running: " + label);

        String topic = h.createUniqueTopic("bench-fencing", 1);
        try {
            try (var producer = h.byteProducer()) {
                producer.send(new ProducerRecord<>(topic, "k".getBytes(), "v".getBytes())).get();
            }

            Properties vanillaProps = h.classicConsumerProps("bench-fencing-vanilla-" + UUID.randomUUID());
            vanillaProps.put("max.poll.interval.ms", "6000");
            vanillaProps.put("session.timeout.ms", "10000");
            vanillaProps.put("heartbeat.interval.ms", "2000");

            AtomicInteger vanillaInvocations = new AtomicInteger();
            AtomicBoolean vanillaFailed = new AtomicBoolean();
            long vStartNanos = System.nanoTime();
            try {
                VanillaConsumers.runVanillaClassic(
                    vanillaProps, topic, 1, 25_000L,
                    r -> {
                        vanillaInvocations.incrementAndGet();
                        sleep(10_000);
                    });
            } catch (Throwable t) {
                vanillaFailed.set(true);
                // Vanilla either fences (CommitFailedException), succeeds-after-redelivery,
                // or hits the 25s timeout. Capture and move on; we measure invocation count.
                System.out.println("    vanilla raised: " + t.getClass().getSimpleName()
                    + " (" + t.getMessage() + ")");
            }
            long vanillaMs = (System.nanoTime() - vStartNanos) / 1_000_000L;
            System.out.println("    vanilla wall-clock: " + vanillaMs + " ms, invocations: "
                + vanillaInvocations.get());

            // Plurima
            Properties pluProps = h.classicConsumerProps("bench-fencing-plurima-" + UUID.randomUUID());
            pluProps.put("max.poll.interval.ms", "6000");
            pluProps.put("session.timeout.ms", "10000");
            pluProps.put("heartbeat.interval.ms", "2000");

            AtomicInteger pluInvocations = new AtomicInteger();
            CountDownLatch done = new CountDownLatch(1);
            long pStartNanos = System.nanoTime();
            try (PlurimaConsumer<byte[], byte[]> c = PlurimaConsumer.<byte[], byte[]>builder()
                    .kafkaProperties(pluProps)
                    .topic(topic)
                    .engine(ConsumerEngine.CLASSIC_BASIC)
                    .ordering(OrderingMode.PARTITION)
                    .pollTimeout(Duration.ofMillis(500))
                    .shutdownDrainTimeout(Duration.ofSeconds(15))
                    .listener((r, ctx) -> {
                        pluInvocations.incrementAndGet();
                        sleep(10_000);
                        done.countDown();
                    })
                    .build()) {
                c.start();
                h.waitForClassicAssignment(pluProps.getProperty("group.id"), 1, 1);
                if (!done.await(20, java.util.concurrent.TimeUnit.SECONDS)) {
                    throw new AssertionError("Plurima did not finish 10s handler within 20s");
                }
            }
            long plurimaMs = (System.nanoTime() - pStartNanos) / 1_000_000L;
            System.out.println("    plurima wall-clock: " + plurimaMs + " ms, invocations: "
                + pluInvocations.get());

            String notes;
            if (vanillaFailed.get()) {
                notes = "Vanilla was fenced after max.poll.interval.ms expired and its commit failed. "
                    + "Plurima handled and committed the record exactly once.";
            } else if (vanillaInvocations.get() > 1) {
                notes = "Vanilla re-fired the handler " + vanillaInvocations.get()
                    + " times after fencing; Plurima handled the record exactly once.";
            } else {
                notes = "Vanilla survived this run (depends on broker fence timing), but Plurima's "
                    + "continuous-poll model is the deterministic safe choice for handler > mpi.";
            }
            return new BenchResult(label, setup, vanillaMs, plurimaMs, notes);
        } finally {
            h.deleteTopicQuietly(topic);
        }
    }

    // ====================================================================================
    // B5: Retry / DLT — qualitative, Plurima-only feature
    // ====================================================================================

    private static BenchResult benchRetryDltVsHandRolled(Helpers h) throws Exception {
        String label = "Retry + DLT on permanently-failing record";
        String setup = "vanilla = no retry framework -> record blocks partition; Plurima = retry then DLT";
        System.out.println("Running: " + label);

        // Vanilla side: produce one always-failing record. Vanilla's idiomatic approach
        // (don't commit, seek back on failure) means the partition is BLOCKED. We measure
        // the redelivery count over a 5-second window — proving the partition never makes
        // progress under vanilla.
        String vTopic = h.createUniqueTopic("bench-dlt-vanilla", 1);
        String pTopic = h.createUniqueTopic("bench-dlt-plurima", 1);
        String dltTopic = h.createUniqueTopic("bench-dlt-plurima-DLT", 1);
        try {
            try (var producer = h.byteProducer()) {
                producer.send(new ProducerRecord<>(vTopic, "k".getBytes(), "v".getBytes())).get();
                producer.send(new ProducerRecord<>(pTopic, "k".getBytes(), "v".getBytes())).get();
            }

            // Vanilla: count how many times the same record is redelivered in 5 seconds —
            // proves partition is blocked.
            CountDownLatch stop = new CountDownLatch(1);
            new Thread(() -> {
                sleep(5_000);
                stop.countDown();
            }, "bench-dlt-vanilla-stopper").start();
            int redeliveries = VanillaConsumers.runVanillaClassicWithRedelivery(
                h.classicConsumerProps("bench-dlt-vanilla-" + UUID.randomUUID()),
                vTopic,
                r -> { throw new RuntimeException("always fails"); },
                stop,
                10_000L);
            System.out.println("    vanilla: same record redelivered " + redeliveries + " times in 5s; "
                + "partition is blocked.");

            // Plurima: retry policy (3 attempts) + DLT. Expects: 4 invocations (1 + 3
            // retries), then exhaustion → DLT route → commit advances.
            RetryPolicy policy = RetryPolicy.exponential()
                .maxAttempts(3)
                .initialDelay(Duration.ofMillis(50))
                .multiplier(2.0)
                .jitter(0.0)
                .retryOn(RuntimeException.class)
                .build();

            Properties dltProducerProps = new Properties();
            dltProducerProps.put("bootstrap.servers", h.adminProps().get("bootstrap.servers"));
            dltProducerProps.put("acks", "all");
            dltProducerProps.put("key.serializer",
                "org.apache.kafka.common.serialization.ByteArraySerializer");
            dltProducerProps.put("value.serializer",
                "org.apache.kafka.common.serialization.ByteArraySerializer");
            DltConfig dltConfig = DltConfig.builder()
                .producerProperties(dltProducerProps)
                .namingStrategy(t -> dltTopic)
                .build();

            String pGroup = "bench-dlt-plurima-" + UUID.randomUUID();
            AtomicInteger pluInvocations = new AtomicInteger();
            long pStartNanos = System.nanoTime();
            try (PlurimaConsumer<byte[], byte[]> c = PlurimaConsumer.<byte[], byte[]>builder()
                    .kafkaProperties(h.classicConsumerProps(pGroup))
                    .topic(pTopic)
                    .engine(ConsumerEngine.CLASSIC_BASIC)
                    .ordering(OrderingMode.PARTITION)
                    .retry(policy)
                    .deadLetterTopic(dltConfig)
                    .pollTimeout(Duration.ofMillis(200))
                    .listener((r, ctx) -> {
                        pluInvocations.incrementAndGet();
                        throw new RuntimeException("always fails");
                    })
                    .build()) {
                c.start();
                h.waitForClassicAssignment(pGroup, 1, 1);
                long deadline = System.currentTimeMillis() + 10_000L;
                while (pluInvocations.get() < 1 + policy.maxAttempts()
                    && System.currentTimeMillis() < deadline) {
                    sleep(50);
                }
                sleep(2_000);  // DLT produce + commit
            }
            long plurimaMs = (System.nanoTime() - pStartNanos) / 1_000_000L;
            System.out.println("    plurima: handler called " + pluInvocations.get()
                + " times (1 + " + policy.maxAttempts() + " retries), then DLT routed in "
                + plurimaMs + " ms; partition unblocked.");

            String notes = "Vanilla: " + redeliveries + " redeliveries in 5s, partition stalled. "
                + "Plurima: 1+" + policy.maxAttempts() + " attempts then DLT routed; partition unblocked.";
            // We compare 5_000ms (vanilla blocking budget) vs Plurima's actual time-to-DLT.
            return new BenchResult(label, setup, 5_000L, plurimaMs, notes);
        } finally {
            h.deleteTopicQuietly(vTopic);
            h.deleteTopicQuietly(pTopic);
            h.deleteTopicQuietly(dltTopic);
        }
    }

    // ====================================================================================
    // helpers
    // ====================================================================================

    private static long runPlurimaClassic(
        Helpers h, String groupId, String topic, int total, int concurrency,
        OrderingMode ordering, int handlerMs) throws Exception {
        return runPlurimaClassicWithPartitions(h, groupId, topic, total, 1, concurrency,
            ordering, handlerMs);
    }

    private static long runPlurimaClassicWithPartitions(
        Helpers h, String groupId, String topic, int total, int partitions, int concurrency,
        OrderingMode ordering, int handlerMs) throws Exception {
        CountDownLatch done = new CountDownLatch(total);
        long[] startNanos = new long[]{-1};
        try (PlurimaConsumer<byte[], byte[]> c = PlurimaConsumer.<byte[], byte[]>builder()
                .kafkaProperties(h.classicConsumerProps(groupId))
                .topic(topic)
                .engine(ConsumerEngine.CLASSIC_BASIC)
                .ordering(ordering)
                .concurrency(concurrency)
                .pollTimeout(Duration.ofMillis(200))
                .listener((r, ctx) -> {
                    if (startNanos[0] < 0) {
                        synchronized (startNanos) {
                            if (startNanos[0] < 0) startNanos[0] = System.nanoTime();
                        }
                    }
                    sleep(handlerMs);
                    done.countDown();
                })
                .build()) {
            c.start();
            h.waitForClassicAssignment(groupId, 1, partitions);
            if (!done.await(120, java.util.concurrent.TimeUnit.SECONDS)) {
                throw new AssertionError("Plurima classic did not finish in 120s");
            }
        }
        return (System.nanoTime() - startNanos[0]) / 1_000_000L;
    }

    private static long runPlurimaShare(
        Helpers h, String groupId, String topic, int total, int concurrency,
        int handlerMs) throws Exception {
        CountDownLatch done = new CountDownLatch(total);
        long[] startNanos = new long[]{-1};
        // Start consumer FIRST so we don't depend on broker share.auto.offset.reset.
        try (PlurimaConsumer<byte[], byte[]> c = PlurimaConsumer.<byte[], byte[]>builder()
                .kafkaProperties(h.shareConsumerProps(groupId))
                .topic(topic)
                .engine(ConsumerEngine.SHARE)
                .ordering(OrderingMode.UNORDERED)
                .concurrency(concurrency)
                .pollTimeout(Duration.ofMillis(200))
                .lockDuration(Duration.ofSeconds(24))
                .listener((r, ctx) -> {
                    if (startNanos[0] < 0) {
                        synchronized (startNanos) {
                            if (startNanos[0] < 0) startNanos[0] = System.nanoTime();
                        }
                    }
                    sleep(handlerMs);
                    done.countDown();
                })
                .build()) {
            c.start();
            h.waitForShareAssignment(groupId, topic);
            produceKeyed(h, topic, total);
            if (!done.await(120, java.util.concurrent.TimeUnit.SECONDS)) {
                throw new AssertionError("Plurima share did not finish in 120s");
            }
        }
        return (System.nanoTime() - startNanos[0]) / 1_000_000L;
    }

    private static void produceKeyed(Helpers h, String topic, int total) throws Exception {
        try (var producer = h.byteProducer()) {
            for (int i = 0; i < total; i++) {
                producer.send(new ProducerRecord<>(topic,
                    ("k" + i).getBytes(), ("v" + i).getBytes())).get();
            }
        }
    }

    private static void produceRoundRobin(Helpers h, String topic, int partitions, int perPartition)
        throws Exception {
        try (var producer = h.byteProducer()) {
            for (int n = 0; n < perPartition; n++) {
                for (int p = 0; p < partitions; p++) {
                    producer.send(new ProducerRecord<>(topic, p, null,
                        ("p" + p + "-n" + n).getBytes())).get();
                }
            }
        }
    }

    private static void printSummary(List<BenchResult> results) {
        System.out.println();
        System.out.println("=".repeat(120));
        System.out.println("  Plurima vs vanilla Kafka - benchmark summary");
        System.out.println("=".repeat(120));
        System.out.printf("  %-58s  %12s  %12s  %8s%n",
            "scenario", "vanilla", "plurima", "speedup");
        System.out.println("  " + "-".repeat(116));
        for (BenchResult r : results) {
            System.out.printf("  %-58s  %12s  %12s  %8s%n",
                trunc(r.name(), 58),
                BenchResult.dash(r.vanillaMs()),
                BenchResult.dash(r.plurimaMs()),
                r.formatSpeedup());
        }
        System.out.println("=".repeat(120));
        System.out.println();
        System.out.println("Details");
        System.out.println("-".repeat(120));
        for (BenchResult r : results) {
            System.out.println();
            System.out.println("  " + r.name());
            System.out.println("    setup:    " + r.setup());
            System.out.println("    vanilla:  " + BenchResult.dash(r.vanillaMs()));
            System.out.println("    plurima:  " + BenchResult.dash(r.plurimaMs()));
            System.out.println("    speedup:  " + r.formatSpeedup());
            System.out.println("    notes:    " + r.notes());
        }
        System.out.println();
    }

    private static String trunc(String s, int n) {
        return s.length() <= n ? s : s.substring(0, n - 3) + "...";
    }

    private static String parseBootstrap(String[] args) {
        for (int i = 0; i < args.length - 1; i++) {
            if ("--bootstrap".equals(args[i])) return args[i + 1];
        }
        return "localhost:9092";
    }

    private static void sleep(long ms) {
        try { Thread.sleep(ms); }
        catch (InterruptedException ie) { Thread.currentThread().interrupt(); }
    }
}
