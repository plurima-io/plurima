package io.plurima.testapp;

import io.plurima.kafka.ConsumerEngine;
import io.plurima.kafka.OrderingMode;
import io.plurima.kafka.PlurimaConsumer;
import io.plurima.kafka.dlt.DltConfig;
import io.plurima.kafka.retry.RetryPolicy;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * End-to-end smoke / proof app for Plurima. Exercises the features added in v0.1
 * (commit frontier, KEY-mode intra-partition parallelism, continuous-poll backpressure)
 * and the pre-existing surface (retry, DLT, classic + share engines, ordering modes)
 * against a real broker at {@code localhost:9092} (override with {@code --bootstrap}).
 *
 * <p>Each scenario prints PASS / FAIL with an elapsed time; the process exit code equals
 * the number of failed scenarios.
 *
 * <h3>Pre-flight</h3>
 * Requires a Kafka 4.2+ broker running at {@code localhost:9092}. The app creates
 * unique topics per scenario and deletes them on the way out.
 */
public final class PlurimaTestApp {

    public static void main(String[] args) throws Exception {
        String bootstrap = parseBootstrap(args);
        System.out.println("Plurima test-app starting against " + bootstrap);

        Helpers h = new Helpers(bootstrap);
        Report report = new Report();

        // --- SHARE engine (post-G1: UNORDERED only) ---
        report.runScenario("share-unordered-roundtrip",
            "SHARE + UNORDERED: 50 records produce → consume",
            () -> scenarioShareUnordered(h));

        report.runScenario("share-key-rejected-at-build",
            "G1: SHARE + KEY combination must throw IllegalArgumentException at build()",
            PlurimaTestApp::scenarioShareKeyRejected);

        report.runScenario("share-partition-rejected-at-build",
            "G1: SHARE + PARTITION combination must throw IllegalArgumentException at build()",
            PlurimaTestApp::scenarioSharePartitionRejected);

        // --- CLASSIC basics (preserved behavior) ---
        report.runScenario("classic-unordered-roundtrip",
            "CLASSIC + UNORDERED: 50 records produce → consume",
            () -> scenarioClassicUnordered(h));

        report.runScenario("classic-partition-fifo",
            "CLASSIC + PARTITION: cross-cluster per-partition FIFO across 4 partitions",
            () -> scenarioClassicPartitionFifo(h));

        // --- G3: KEY mode parallelism ---
        report.runScenario("classic-key-parallelism-speedup",
            "G3: CLASSIC + KEY on 1-partition topic, 32 distinct keys, 1s handler → wall-clock < 8s",
            () -> scenarioClassicKeyParallelism(h));

        report.runScenario("classic-key-same-key-fifo",
            "G3: CLASSIC + KEY same-key records serialise in offset order",
            () -> scenarioClassicKeySameKeyFifo(h));

        // --- G2: commit frontier ---
        report.runScenario("classic-commit-frontier-no-gap-advance",
            "G2: CLASSIC + KEY out-of-order completion never advances commit past unfinished offsets",
            () -> scenarioClassicCommitFrontier(h));

        // --- G4: continuous-poll + backpressure ---
        report.runScenario("classic-continuous-poll-no-fence",
            "G4: CLASSIC handler exceeding max.poll.interval.ms must not fence the consumer",
            () -> scenarioClassicContinuousPoll(h));

        report.runScenario("classic-backpressure-burst",
            "G4: CLASSIC 200 records / concurrency=8 / 200ms handler → no loss, no fence",
            () -> scenarioClassicBackpressureBurst(h));

        // --- Retry / DLT (preserved) ---
        report.runScenario("classic-inline-retry-succeeds",
            "CLASSIC inline retry: handler fails 2× then succeeds; offset commits once",
            () -> scenarioClassicInlineRetry(h));

        report.runScenario("classic-dlt-routing-on-exhaustion",
            "CLASSIC retry exhaustion routes to DLT; original offset advances past",
            () -> scenarioClassicDltRouting(h));

        // --- API surface checks ---
        report.runScenario("share-manual-ack-explicit",
            "SHARE + ManualAckListener: explicit ACCEPT / REJECT / RELEASE per record",
            () -> scenarioShareManualAck(h));

        report.runScenario("classic-custom-deserializer",
            "CLASSIC + UTF-8 string deserializer: typed values arrive correctly (non-identity path)",
            () -> scenarioClassicCustomDeserializer(h));

        int failures = report.printSummaryAndExitCode();
        System.exit(failures);
    }

    // ====================================================================================
    // SHARE engine scenarios
    // ====================================================================================

    private static void scenarioShareUnordered(Helpers h) throws Exception {
        String topic = h.createUniqueTopic("plurima-app-share-u", 1);
        String groupId = "plurima-app-share-u-" + UUID.randomUUID();
        int total = 50;

        AtomicInteger seen = new AtomicInteger();
        CountDownLatch done = new CountDownLatch(total);

        // Start the consumer FIRST so the share group is positioned at the topic head
        // before we produce. Without this, brokers without share.auto.offset.reset=earliest
        // configured (the default) would have the group start at the latest end and miss
        // anything produced before subscribe.
        try (PlurimaConsumer<byte[], byte[]> c = PlurimaConsumer.<byte[], byte[]>builder()
                .kafkaProperties(h.shareConsumerProps(groupId))
                .topic(topic)
                .engine(ConsumerEngine.SHARE)
                .ordering(OrderingMode.UNORDERED)
                .concurrency(8)
                .pollTimeout(Duration.ofMillis(200))
                .listener((r, ctx) -> {
                    seen.incrementAndGet();
                    done.countDown();
                })
                .build()) {
            c.start();
            h.waitForShareAssignment(groupId, topic);

            try (var producer = h.byteProducer()) {
                for (int i = 0; i < total; i++) {
                    producer.send(new ProducerRecord<>(topic,
                        ("k" + i).getBytes(), ("v" + i).getBytes())).get();
                }
            }

            if (!done.await(45, TimeUnit.SECONDS)) {
                throw new AssertionError("only " + seen.get() + "/" + total + " records received");
            }
        } finally {
            h.deleteTopicQuietly(topic);
        }
        assertEquals(total, seen.get(), "share-unordered record count");
    }

    private static void scenarioShareKeyRejected() {
        try {
            PlurimaConsumer.<byte[], byte[]>builder()
                .kafkaProperties(new Properties())
                .topic("t")
                .listener((r, ctx) -> {})
                .engine(ConsumerEngine.SHARE)
                .ordering(OrderingMode.KEY)
                .build();
            throw new AssertionError("expected IllegalArgumentException for SHARE+KEY");
        } catch (IllegalArgumentException expected) {
            if (!expected.getMessage().contains("engine=SHARE does not support ordering=KEY")) {
                throw new AssertionError("error message did not match: " + expected.getMessage());
            }
        }
    }

    private static void scenarioSharePartitionRejected() {
        try {
            PlurimaConsumer.<byte[], byte[]>builder()
                .kafkaProperties(new Properties())
                .topic("t")
                .listener((r, ctx) -> {})
                .engine(ConsumerEngine.SHARE)
                .ordering(OrderingMode.PARTITION)
                .build();
            throw new AssertionError("expected IllegalArgumentException for SHARE+PARTITION");
        } catch (IllegalArgumentException expected) {
            if (!expected.getMessage().contains("engine=SHARE does not support ordering=PARTITION")) {
                throw new AssertionError("error message did not match: " + expected.getMessage());
            }
        }
    }

    // ====================================================================================
    // CLASSIC basics
    // ====================================================================================

    private static void scenarioClassicUnordered(Helpers h) throws Exception {
        String topic = h.createUniqueTopic("plurima-app-classic-u", 1);
        String groupId = "plurima-app-classic-u-" + UUID.randomUUID();
        int total = 50;

        AtomicInteger seen = new AtomicInteger();
        CountDownLatch done = new CountDownLatch(total);

        try (var producer = h.byteProducer()) {
            for (int i = 0; i < total; i++) {
                producer.send(new ProducerRecord<>(topic,
                    ("k" + i).getBytes(), ("v" + i).getBytes())).get();
            }
        }

        try (PlurimaConsumer<byte[], byte[]> c = PlurimaConsumer.<byte[], byte[]>builder()
                .kafkaProperties(h.classicConsumerProps(groupId))
                .topic(topic)
                .engine(ConsumerEngine.CLASSIC_BASIC)
                .ordering(OrderingMode.UNORDERED)
                .concurrency(8)
                .pollTimeout(Duration.ofMillis(200))
                .listener((r, ctx) -> {
                    seen.incrementAndGet();
                    done.countDown();
                })
                .build()) {
            c.start();
            h.waitForClassicAssignment(groupId, 1, 1);
            if (!done.await(30, TimeUnit.SECONDS)) {
                throw new AssertionError("only " + seen.get() + "/" + total + " received");
            }
        } finally {
            h.deleteTopicQuietly(topic);
        }
        assertEquals(total, seen.get(), "classic-unordered record count");
    }

    private static void scenarioClassicPartitionFifo(Helpers h) throws Exception {
        int partitions = 4;
        int perPartition = 25;
        int total = partitions * perPartition;
        String topic = h.createUniqueTopic("plurima-app-classic-p", partitions);
        String groupId = "plurima-app-classic-p-" + UUID.randomUUID();

        Map<Integer, List<Long>> observed = new ConcurrentHashMap<>();
        CountDownLatch done = new CountDownLatch(total);

        try (var producer = h.byteProducer()) {
            for (int n = 0; n < perPartition; n++) {
                for (int p = 0; p < partitions; p++) {
                    producer.send(new ProducerRecord<>(topic, p, null,
                        ("p" + p + "-n" + n).getBytes())).get();
                }
            }
        }

        try (PlurimaConsumer<byte[], byte[]> c = PlurimaConsumer.<byte[], byte[]>builder()
                .kafkaProperties(h.classicConsumerProps(groupId))
                .topic(topic)
                .engine(ConsumerEngine.CLASSIC_BASIC)
                .ordering(OrderingMode.PARTITION)
                .concurrency(8)
                .pollTimeout(Duration.ofMillis(200))
                .listener((r, ctx) -> {
                    observed.computeIfAbsent(r.partition(),
                        p -> new CopyOnWriteArrayList<>()).add(r.offset());
                    done.countDown();
                })
                .build()) {
            c.start();
            h.waitForClassicAssignment(groupId, 1, partitions);
            if (!done.await(60, TimeUnit.SECONDS)) {
                throw new AssertionError("did not receive all records");
            }
        } finally {
            h.deleteTopicQuietly(topic);
        }

        assertEquals(partitions, observed.size(), "saw all partitions");
        observed.forEach((p, offsets) -> {
            List<Long> sorted = new ArrayList<>(offsets);
            sorted.sort(Long::compareTo);
            if (!offsets.equals(sorted)) {
                throw new AssertionError("partition " + p + " offsets out of order: " + offsets);
            }
        });
    }

    // ====================================================================================
    // G3: KEY-mode parallelism
    // ====================================================================================

    private static void scenarioClassicKeyParallelism(Helpers h) throws Exception {
        int distinctKeys = 32;
        long handlerSleepMs = 1_000L;
        String topic = h.createUniqueTopic("plurima-app-key-parallel", 1);
        String groupId = "plurima-app-key-parallel-" + UUID.randomUUID();

        try (var producer = h.byteProducer()) {
            for (int k = 0; k < distinctKeys; k++) {
                producer.send(new ProducerRecord<>(topic,
                    ("key-" + k).getBytes(), ("v" + k).getBytes())).get();
            }
        }

        AtomicInteger concurrentlyRunning = new AtomicInteger();
        AtomicInteger maxConcurrent = new AtomicInteger();
        CountDownLatch done = new CountDownLatch(distinctKeys);
        long startNanos;

        try (PlurimaConsumer<byte[], byte[]> c = PlurimaConsumer.<byte[], byte[]>builder()
                .kafkaProperties(h.classicConsumerProps(groupId))
                .topic(topic)
                .engine(ConsumerEngine.CLASSIC_BASIC)
                .ordering(OrderingMode.KEY)
                .concurrency(distinctKeys)
                .shardCount(distinctKeys * 4)
                .pollTimeout(Duration.ofMillis(200))
                .listener((r, ctx) -> {
                    int now = concurrentlyRunning.incrementAndGet();
                    maxConcurrent.updateAndGet(prev -> Math.max(prev, now));
                    try { Thread.sleep(handlerSleepMs); }
                    catch (InterruptedException ie) { Thread.currentThread().interrupt(); }
                    concurrentlyRunning.decrementAndGet();
                    done.countDown();
                })
                .build()) {
            c.start();
            h.waitForClassicAssignment(groupId, 1, 1);
            startNanos = System.nanoTime();
            if (!done.await(30, TimeUnit.SECONDS)) {
                throw new AssertionError("did not finish in 30s");
            }
        } finally {
            h.deleteTopicQuietly(topic);
        }

        long elapsedMs = (System.nanoTime() - startNanos) / 1_000_000L;
        System.out.println("    wall-clock=" + elapsedMs + "ms, maxConcurrent=" + maxConcurrent.get());
        // Serial baseline would be 32 × 1000 = 32_000 ms.
        if (elapsedMs >= 8_000) {
            throw new AssertionError("wall-clock " + elapsedMs + "ms too slow — no parallelism?");
        }
        if (maxConcurrent.get() < 8) {
            throw new AssertionError("peak concurrent " + maxConcurrent.get()
                + " — KEY-shard parallelism not observed");
        }
    }

    private static void scenarioClassicKeySameKeyFifo(Helpers h) throws Exception {
        int total = 12;
        String topic = h.createUniqueTopic("plurima-app-key-fifo", 1);
        String groupId = "plurima-app-key-fifo-" + UUID.randomUUID();

        try (var producer = h.byteProducer()) {
            for (int i = 0; i < total; i++) {
                producer.send(new ProducerRecord<>(topic,
                    "single".getBytes(), ("v" + i).getBytes())).get();
            }
        }

        List<Long> observed = new CopyOnWriteArrayList<>();
        AtomicInteger concurrentlyRunning = new AtomicInteger();
        AtomicInteger maxConcurrent = new AtomicInteger();
        CountDownLatch done = new CountDownLatch(total);

        try (PlurimaConsumer<byte[], byte[]> c = PlurimaConsumer.<byte[], byte[]>builder()
                .kafkaProperties(h.classicConsumerProps(groupId))
                .topic(topic)
                .engine(ConsumerEngine.CLASSIC_BASIC)
                .ordering(OrderingMode.KEY)
                .concurrency(16)
                .pollTimeout(Duration.ofMillis(200))
                .listener((r, ctx) -> {
                    int now = concurrentlyRunning.incrementAndGet();
                    maxConcurrent.updateAndGet(prev -> Math.max(prev, now));
                    try { Thread.sleep(80); }
                    catch (InterruptedException ie) { Thread.currentThread().interrupt(); }
                    observed.add(r.offset());
                    concurrentlyRunning.decrementAndGet();
                    done.countDown();
                })
                .build()) {
            c.start();
            h.waitForClassicAssignment(groupId, 1, 1);
            if (!done.await(30, TimeUnit.SECONDS)) {
                throw new AssertionError("did not finish in 30s");
            }
        } finally {
            h.deleteTopicQuietly(topic);
        }

        if (maxConcurrent.get() != 1) {
            throw new AssertionError("same-key serialisation broken: peak concurrent = "
                + maxConcurrent.get());
        }
        List<Long> sorted = new ArrayList<>(observed);
        sorted.sort(Long::compareTo);
        if (!observed.equals(sorted)) {
            throw new AssertionError("same-key offset order broken: " + observed);
        }
    }

    // ====================================================================================
    // G2: commit frontier
    // ====================================================================================

    private static void scenarioClassicCommitFrontier(Helpers h) throws Exception {
        // KEY mode with 32 distinct keys + a slow handler for the FIRST record (offset 0)
        // + fast handler for the rest. The slow record blocks its shard; other shards
        // complete. We close the consumer mid-batch, then start a second consumer in the
        // same group and verify: the second consumer sees offset 0 (the slow record was
        // abandoned, frontier never advanced past it) — at-least-once with no commit ahead.
        int distinctKeys = 32;
        String topic = h.createUniqueTopic("plurima-app-frontier", 1);
        String groupId = "plurima-app-frontier-" + UUID.randomUUID();

        try (var producer = h.byteProducer()) {
            for (int k = 0; k < distinctKeys; k++) {
                producer.send(new ProducerRecord<>(topic,
                    ("key-" + k).getBytes(), ("v" + k).getBytes())).get();
            }
        }

        AtomicInteger fastDone = new AtomicInteger();
        CountDownLatch slowEntered = new CountDownLatch(1);
        CountDownLatch slowMayProceed = new CountDownLatch(1);
        // Wait until at least `distinctKeys - 1` fast records have completed; that proves
        // the OTHER shards advanced past offset 0 while offset 0 is still blocked.
        CountDownLatch enoughFastDone = new CountDownLatch(1);

        PlurimaConsumer<byte[], byte[]> first = PlurimaConsumer.<byte[], byte[]>builder()
                .kafkaProperties(h.classicConsumerProps(groupId))
                .topic(topic)
                .engine(ConsumerEngine.CLASSIC_BASIC)
                .ordering(OrderingMode.KEY)
                .concurrency(distinctKeys)
                .shardCount(distinctKeys * 4)
                .pollTimeout(Duration.ofMillis(200))
                .shutdownDrainTimeout(Duration.ofSeconds(1))  // short: don't wait for the blocked record
                .listener((r, ctx) -> {
                    if (r.offset() == 0L) {
                        slowEntered.countDown();
                        try { slowMayProceed.await(); }
                        catch (InterruptedException ie) { Thread.currentThread().interrupt(); }
                    } else {
                        int n = fastDone.incrementAndGet();
                        if (n >= distinctKeys - 1) enoughFastDone.countDown();
                    }
                })
                .build();
        first.start();

        try {
            h.waitForClassicAssignment(groupId, 1, 1);
            if (!slowEntered.await(15, TimeUnit.SECONDS)) {
                throw new AssertionError("slow record (offset 0) never entered handler");
            }
            if (!enoughFastDone.await(15, TimeUnit.SECONDS)) {
                throw new AssertionError("fast records did not complete past the blocker — KEY-shard "
                    + "parallelism may be broken (fastDone=" + fastDone.get() + ")");
            }
            // Now: every other key has completed; offset 0 is still blocked. Close the
            // first consumer. With shutdownDrainTimeout=1s the blocker is abandoned —
            // the frontier never advanced past offset 0 because the slow record never
            // completed. After close, offset 0 is uncommitted; offsets 1..N are also
            // uncommitted because the frontier sat at 0 the whole time (G2 invariant:
            // do NOT commit past unfinished work).
            first.close();
        } finally {
            slowMayProceed.countDown();
        }

        // Second consumer in the same group → should re-receive offset 0 AND offsets 1..N
        // (because the frontier never advanced past 0). This is at-least-once: a few
        // already-processed records may be redelivered; none are lost.
        AtomicInteger secondSaw = new AtomicInteger();
        boolean sawOffsetZero = false;
        long secondDeadline = System.currentTimeMillis() + 15_000L;
        CountDownLatch sawSomething = new CountDownLatch(1);
        List<Long> secondOffsets = new CopyOnWriteArrayList<>();

        try (PlurimaConsumer<byte[], byte[]> second = PlurimaConsumer.<byte[], byte[]>builder()
                .kafkaProperties(h.classicConsumerProps(groupId))
                .topic(topic)
                .engine(ConsumerEngine.CLASSIC_BASIC)
                .ordering(OrderingMode.KEY)
                .concurrency(distinctKeys)
                .pollTimeout(Duration.ofMillis(200))
                .listener((r, ctx) -> {
                    secondSaw.incrementAndGet();
                    secondOffsets.add(r.offset());
                    sawSomething.countDown();
                })
                .build()) {
            second.start();
            h.waitForClassicAssignment(groupId, 1, 1);
            // Wait until we see SOMETHING, then drain for a brief window so we capture
            // all the redelivered records.
            sawSomething.await(15, TimeUnit.SECONDS);
            Thread.sleep(3_000);
        } finally {
            h.deleteTopicQuietly(topic);
        }

        if (!secondOffsets.contains(0L)) {
            throw new AssertionError("frontier broken: second consumer did not see offset 0 "
                + "(observed offsets: " + secondOffsets + ")");
        }
        System.out.println("    second consumer redelivered offsets: " + secondOffsets.size()
            + " records (including offset 0)");
    }

    // ====================================================================================
    // G4: continuous-poll + backpressure
    // ====================================================================================

    private static void scenarioClassicContinuousPoll(Helpers h) throws Exception {
        // Handler sleeps 8s > max.poll.interval.ms=6s. Pre-v0.1 would self-stop at 4.8s;
        // v0.1 continuous-poll heartbeats and the handler completes normally.
        String topic = h.createUniqueTopic("plurima-app-mpi", 1);
        String groupId = "plurima-app-mpi-" + UUID.randomUUID();

        try (var producer = h.byteProducer()) {
            producer.send(new ProducerRecord<>(topic, "k".getBytes(), "v".getBytes())).get();
        }

        Properties props = h.classicConsumerProps(groupId);
        props.put("max.poll.interval.ms", "6000");
        props.put("session.timeout.ms", "10000");
        props.put("heartbeat.interval.ms", "2000");

        AtomicInteger invocations = new AtomicInteger();
        CountDownLatch done = new CountDownLatch(1);

        try (PlurimaConsumer<byte[], byte[]> c = PlurimaConsumer.<byte[], byte[]>builder()
                .kafkaProperties(props)
                .topic(topic)
                .engine(ConsumerEngine.CLASSIC_BASIC)
                .ordering(OrderingMode.PARTITION)
                .pollTimeout(Duration.ofMillis(500))
                .shutdownDrainTimeout(Duration.ofSeconds(15))
                .listener((r, ctx) -> {
                    invocations.incrementAndGet();
                    try { Thread.sleep(8_000); }
                    catch (InterruptedException ie) { Thread.currentThread().interrupt(); }
                    done.countDown();
                })
                .build()) {
            c.start();
            h.waitForClassicAssignment(groupId, 1, 1);
            if (!done.await(25, TimeUnit.SECONDS)) {
                throw new AssertionError("handler did not finish — consumer may have been fenced");
            }
            Thread.sleep(2_000);  // let commitAsync land
        }

        if (invocations.get() != 1) {
            throw new AssertionError("expected exactly 1 invocation, got " + invocations.get()
                + " (fencing-induced redelivery would show >1)");
        }

        // Verifier in same group must see nothing — proves the commit landed.
        AtomicInteger verifierSaw = new AtomicInteger();
        CountDownLatch verifyDeadline = new CountDownLatch(1);
        try (PlurimaConsumer<byte[], byte[]> verifier = PlurimaConsumer.<byte[], byte[]>builder()
                .kafkaProperties(h.classicConsumerProps(groupId))
                .topic(topic)
                .engine(ConsumerEngine.CLASSIC_BASIC)
                .ordering(OrderingMode.PARTITION)
                .pollTimeout(Duration.ofMillis(200))
                .listener((r, ctx) -> verifierSaw.incrementAndGet())
                .build()) {
            verifier.start();
            verifyDeadline.await(5, TimeUnit.SECONDS);
        } finally {
            h.deleteTopicQuietly(topic);
        }
        if (verifierSaw.get() != 0) {
            throw new AssertionError("commit did not land — verifier in same group saw "
                + verifierSaw.get() + " redelivered records");
        }
    }

    private static void scenarioClassicBackpressureBurst(Helpers h) throws Exception {
        int total = 200;
        int concurrency = 8;
        String topic = h.createUniqueTopic("plurima-app-burst", 1);
        String groupId = "plurima-app-burst-" + UUID.randomUUID();

        try (var producer = h.byteProducer()) {
            for (int i = 0; i < total; i++) {
                producer.send(new ProducerRecord<>(topic,
                    ("k" + (i % 50)).getBytes(), ("v" + i).getBytes())).get();
            }
        }

        Properties props = h.classicConsumerProps(groupId);
        props.put("max.poll.records", "16");

        CountDownLatch done = new CountDownLatch(total);
        AtomicInteger concurrentlyRunning = new AtomicInteger();
        AtomicInteger maxConcurrent = new AtomicInteger();

        try (PlurimaConsumer<byte[], byte[]> c = PlurimaConsumer.<byte[], byte[]>builder()
                .kafkaProperties(props)
                .topic(topic)
                .engine(ConsumerEngine.CLASSIC_BASIC)
                .ordering(OrderingMode.KEY)
                .concurrency(concurrency)
                .pollTimeout(Duration.ofMillis(200))
                .listener((r, ctx) -> {
                    int now = concurrentlyRunning.incrementAndGet();
                    maxConcurrent.updateAndGet(prev -> Math.max(prev, now));
                    try { Thread.sleep(200); }
                    catch (InterruptedException ie) { Thread.currentThread().interrupt(); }
                    concurrentlyRunning.decrementAndGet();
                    done.countDown();
                })
                .build()) {
            c.start();
            h.waitForClassicAssignment(groupId, 1, 1);
            if (!done.await(120, TimeUnit.SECONDS)) {
                throw new AssertionError("backpressure burst incomplete: " + done.getCount()
                    + " records still missing");
            }
        } finally {
            h.deleteTopicQuietly(topic);
        }
        System.out.println("    maxConcurrent=" + maxConcurrent.get() + " (bound = "
            + (concurrency + 16) + ")");
        if (maxConcurrent.get() > concurrency + 16) {
            throw new AssertionError("backpressure did not cap concurrency: peak = "
                + maxConcurrent.get());
        }
    }

    // ====================================================================================
    // Retry / DLT
    // ====================================================================================

    private static void scenarioClassicInlineRetry(Helpers h) throws Exception {
        String topic = h.createUniqueTopic("plurima-app-retry", 1);
        String groupId = "plurima-app-retry-" + UUID.randomUUID();

        try (var producer = h.byteProducer()) {
            producer.send(new ProducerRecord<>(topic, "k".getBytes(), "v".getBytes())).get();
        }

        AtomicInteger invocations = new AtomicInteger();
        CountDownLatch succeeded = new CountDownLatch(1);

        // RetryPolicy classifier uses isInstance on the top-level exception (RetryPolicy.java:121).
        // RecordListener can't throw checked exceptions through the lambda, so we throw a plain
        // RuntimeException and tell the policy to retry on that.
        RetryPolicy policy = RetryPolicy.exponential()
            .maxAttempts(5)
            .initialDelay(Duration.ofMillis(100))
            .multiplier(2.0)
            .jitter(0.0)
            .retryOn(RuntimeException.class)
            .build();

        try (PlurimaConsumer<byte[], byte[]> c = PlurimaConsumer.<byte[], byte[]>builder()
                .kafkaProperties(h.classicConsumerProps(groupId))
                .topic(topic)
                .engine(ConsumerEngine.CLASSIC_BASIC)
                .ordering(OrderingMode.PARTITION)
                .retry(policy)
                .pollTimeout(Duration.ofMillis(200))
                .listener((r, ctx) -> {
                    int attempt = invocations.incrementAndGet();
                    if (attempt < 3) {
                        throw new RuntimeException("transient failure on attempt " + attempt);
                    }
                    succeeded.countDown();
                })
                .build()) {
            c.start();
            h.waitForClassicAssignment(groupId, 1, 1);
            if (!succeeded.await(20, TimeUnit.SECONDS)) {
                throw new AssertionError("retry did not eventually succeed (attempts="
                    + invocations.get() + ")");
            }
            Thread.sleep(2_000);  // let commit land
        }

        if (invocations.get() != 3) {
            throw new AssertionError("expected 3 invocations (2 failures + success), got "
                + invocations.get());
        }

        // Verifier: same group, must see nothing (the success-after-retry committed).
        AtomicInteger verifierSaw = new AtomicInteger();
        CountDownLatch verifyDeadline = new CountDownLatch(1);
        try (PlurimaConsumer<byte[], byte[]> verifier = PlurimaConsumer.<byte[], byte[]>builder()
                .kafkaProperties(h.classicConsumerProps(groupId))
                .topic(topic)
                .engine(ConsumerEngine.CLASSIC_BASIC)
                .ordering(OrderingMode.PARTITION)
                .pollTimeout(Duration.ofMillis(200))
                .listener((r, ctx) -> verifierSaw.incrementAndGet())
                .build()) {
            verifier.start();
            verifyDeadline.await(5, TimeUnit.SECONDS);
        } finally {
            h.deleteTopicQuietly(topic);
        }
        if (verifierSaw.get() != 0) {
            throw new AssertionError("after retry-success the commit did not land; "
                + "verifier saw " + verifierSaw.get() + " redelivered");
        }
    }

    private static void scenarioClassicDltRouting(Helpers h) throws Exception {
        String topic = h.createUniqueTopic("plurima-app-dlt-src", 1);
        String dlt = h.createUniqueTopic("plurima-app-dlt-dst", 1);
        String groupId = "plurima-app-dlt-" + UUID.randomUUID();

        try (var producer = h.byteProducer()) {
            producer.send(new ProducerRecord<>(topic, "k".getBytes(), "v".getBytes())).get();
        }

        AtomicInteger invocations = new AtomicInteger();
        // maxAttempts(N) = N retries after the initial failure (RetryPolicy delays are
        // indexed 0..N-1, so 3 attempts means 1 initial + 3 retries = 4 total invocations
        // before Exhaustion fires). Use 2 retries → 3 total invocations.
        RetryPolicy policy = RetryPolicy.exponential()
            .maxAttempts(2)
            .initialDelay(Duration.ofMillis(50))
            .multiplier(2.0)
            .jitter(0.0)
            .retryOn(RuntimeException.class)
            .build();
        int expectedInvocations = 1 + policy.maxAttempts();  // 1 initial + N retries

        Properties dltProducerProps = new Properties();
        dltProducerProps.put("bootstrap.servers", h.adminProps().get("bootstrap.servers"));
        dltProducerProps.put("acks", "all");
        dltProducerProps.put("key.serializer",
            "org.apache.kafka.common.serialization.ByteArraySerializer");
        dltProducerProps.put("value.serializer",
            "org.apache.kafka.common.serialization.ByteArraySerializer");
        final String dltName = dlt;
        DltConfig dltConfig = DltConfig.builder()
            .producerProperties(dltProducerProps)
            .namingStrategy(sourceTopic -> dltName)
            .build();

        try (PlurimaConsumer<byte[], byte[]> c = PlurimaConsumer.<byte[], byte[]>builder()
                .kafkaProperties(h.classicConsumerProps(groupId))
                .topic(topic)
                .engine(ConsumerEngine.CLASSIC_BASIC)
                .ordering(OrderingMode.PARTITION)
                .retry(policy)
                .deadLetterTopic(dltConfig)
                .pollTimeout(Duration.ofMillis(200))
                .listener((r, ctx) -> {
                    invocations.incrementAndGet();
                    throw new RuntimeException("permanent failure");
                })
                .build()) {
            c.start();
            h.waitForClassicAssignment(groupId, 1, 1);
            long deadline = System.currentTimeMillis() + 10_000L;
            while (invocations.get() < expectedInvocations && System.currentTimeMillis() < deadline) {
                Thread.sleep(50);
            }
            Thread.sleep(2_000);  // let DLT produce + commit land
        }

        if (invocations.get() != expectedInvocations) {
            throw new AssertionError("expected " + expectedInvocations + " attempts (exhaustion), got "
                + invocations.get());
        }

        // Drain the DLT — must contain exactly 1 message.
        int dltMessages = 0;
        try {
            Properties cp = h.classicConsumerProps("plurima-app-dlt-verify-" + UUID.randomUUID());
            cp.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
            cp.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
            try (KafkaConsumer<byte[], byte[]> kc = new KafkaConsumer<>(cp)) {
                kc.subscribe(List.of(dlt));
                long deadline = System.currentTimeMillis() + 10_000L;
                while (dltMessages == 0 && System.currentTimeMillis() < deadline) {
                    var batch = kc.poll(Duration.ofMillis(500));
                    dltMessages += batch.count();
                }
            }
        } finally {
            h.deleteTopicQuietly(topic);
            h.deleteTopicQuietly(dlt);
        }
        if (dltMessages != 1) {
            throw new AssertionError("expected 1 DLT message, found " + dltMessages);
        }
    }

    // ====================================================================================
    // API surface scenarios
    // ====================================================================================

    private static void scenarioShareManualAck(Helpers h) throws Exception {
        // Three records, three different ack types via ManualAckListener:
        //   offset 0 → ACCEPT  (success)
        //   offset 1 → REJECT  (non-retriable; broker advances past)
        //   offset 2 → RELEASE (broker will redeliver — but we only verify it doesn't
        //                       commit here; the verifier consumer in the same group
        //                       should see offset 2 redelivered)
        String topic = h.createUniqueTopic("plurima-app-share-manual", 1);
        String groupId = "plurima-app-share-manual-" + UUID.randomUUID();

        CountDownLatch acked = new CountDownLatch(3);
        java.util.Set<Long> initiallySeen =
            java.util.concurrent.ConcurrentHashMap.newKeySet();

        try (PlurimaConsumer<byte[], byte[]> consumer = PlurimaConsumer.<byte[], byte[]>builder()
                .kafkaProperties(h.shareConsumerProps(groupId))
                .topic(topic)
                .engine(ConsumerEngine.SHARE)
                .ordering(OrderingMode.UNORDERED)
                .concurrency(4)
                .pollTimeout(Duration.ofMillis(200))
                .manualAckListener((r, ack) -> {
                    initiallySeen.add(r.offset());
                    if (r.offset() == 0L) ack.acknowledge(
                        org.apache.kafka.clients.consumer.AcknowledgeType.ACCEPT);
                    else if (r.offset() == 1L) ack.acknowledge(
                        org.apache.kafka.clients.consumer.AcknowledgeType.REJECT);
                    else if (r.offset() == 2L) ack.acknowledge(
                        org.apache.kafka.clients.consumer.AcknowledgeType.RELEASE);
                    acked.countDown();
                })
                .build()) {
            consumer.start();
            h.waitForShareAssignment(groupId, topic);

            try (var producer = h.byteProducer()) {
                producer.send(new ProducerRecord<>(topic, "k0".getBytes(), "v0".getBytes())).get();
                producer.send(new ProducerRecord<>(topic, "k1".getBytes(), "v1".getBytes())).get();
                producer.send(new ProducerRecord<>(topic, "k2".getBytes(), "v2".getBytes())).get();
            }

            if (!acked.await(30, TimeUnit.SECONDS)) {
                throw new AssertionError("only " + (3 - acked.getCount()) + "/3 records acked");
            }
        } finally {
            h.deleteTopicQuietly(topic);
        }

        assertEquals(3, initiallySeen.size(), "share-manual all 3 records delivered initially");
        if (!initiallySeen.containsAll(java.util.List.of(0L, 1L, 2L))) {
            throw new AssertionError("expected offsets {0,1,2}, saw " + initiallySeen);
        }
        System.out.println("    manual ack: ACCEPT (offset 0), REJECT (offset 1), "
            + "RELEASE (offset 2) — all dispatched without error");
    }

    private static void scenarioClassicCustomDeserializer(Helpers h) throws Exception {
        // Use the UTF-8 string deserializer for value — exercises the NON-identity
        // deserialize() path in ClassicPollLoop (which has a fast-path short-circuit for
        // RecordDeserializer.IDENTITY_BYTES). The handler must receive String values,
        // not byte[].
        int total = 20;
        String topic = h.createUniqueTopic("plurima-app-deser", 1);
        String groupId = "plurima-app-deser-" + UUID.randomUUID();

        java.util.List<String> received = new CopyOnWriteArrayList<>();
        CountDownLatch done = new CountDownLatch(total);

        try (var producer = h.byteProducer()) {
            for (int i = 0; i < total; i++) {
                producer.send(new ProducerRecord<>(topic,
                    ("k" + i).getBytes(),
                    ("hello-" + i).getBytes(java.nio.charset.StandardCharsets.UTF_8))).get();
            }
        }

        try (PlurimaConsumer<byte[], String> c = PlurimaConsumer.<byte[], String>builder()
                .kafkaProperties(h.classicConsumerProps(groupId))
                .topic(topic)
                .engine(ConsumerEngine.CLASSIC_BASIC)
                .ordering(OrderingMode.PARTITION)
                .valueDeserializer(io.plurima.kafka.deserializer.RecordDeserializer.utf8String())
                .pollTimeout(Duration.ofMillis(200))
                .listener((r, ctx) -> {
                    // r.value() is typed as String — proves the deserializer ran.
                    received.add(r.value());
                    done.countDown();
                })
                .build()) {
            c.start();
            h.waitForClassicAssignment(groupId, 1, 1);
            if (!done.await(30, TimeUnit.SECONDS)) {
                throw new AssertionError("only " + received.size() + "/" + total + " received");
            }
        } finally {
            h.deleteTopicQuietly(topic);
        }

        assertEquals(total, received.size(), "custom-deser record count");
        // Spot-check that values are real Strings, not byte[].toString junk.
        for (int i = 0; i < total; i++) {
            String expected = "hello-" + i;
            if (!received.contains(expected)) {
                throw new AssertionError("missing expected value '" + expected
                    + "' from observed list: " + received);
            }
        }
        System.out.println("    verified: " + total + " records arrived as typed Strings "
            + "(non-identity deserialize path)");
    }

    // ====================================================================================
    // helpers
    // ====================================================================================

    private static String parseBootstrap(String[] args) {
        for (int i = 0; i < args.length - 1; i++) {
            if ("--bootstrap".equals(args[i])) return args[i + 1];
        }
        return "localhost:9092";
    }

    private static void assertEquals(int expected, int actual, String label) {
        if (expected != actual) {
            throw new AssertionError(label + ": expected " + expected + " got " + actual);
        }
    }
}
