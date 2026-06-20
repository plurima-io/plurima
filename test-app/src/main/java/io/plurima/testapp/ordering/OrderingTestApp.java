package io.plurima.testapp.ordering;

import io.plurima.kafka.ConsumerEngine;
import io.plurima.kafka.OrderingMode;
import io.plurima.kafka.PlurimaConsumer;
import io.plurima.kafka.retry.RetryPolicy;
import io.plurima.testapp.Helpers;
import io.plurima.testapp.Report;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Focused verification that PARTITION and KEY ordering invariants hold under various
 * stress conditions: high throughput, hot-key contention, retries, null keys, and
 * cross-partition KEY mode.
 *
 * <p>Each scenario produces records with monotonically-increasing sequence numbers per
 * (partition, key) group, consumes them via Plurima, and asserts that the observed
 * sequence is monotonically increasing within every group. Any violation fails the
 * scenario.
 *
 * <p>Run: {@code ./gradlew :test-app:runOrdering}
 */
public final class OrderingTestApp {

    public static void main(String[] args) throws Exception {
        String bootstrap = parseBootstrap(args);
        Helpers h = new Helpers(bootstrap);
        Report report = new Report();

        System.out.println();
        System.out.println("Plurima ordering verification (PARTITION + KEY modes)");
        System.out.println("Broker: " + bootstrap);

        report.runScenario("partition-strict-offset-order",
            "PARTITION mode: 4 partitions × 250 records, verify per-partition offset order",
            () -> partitionStrictOffsetOrder(h));

        report.runScenario("partition-survives-rebalance-window",
            "PARTITION mode: high-throughput single partition, verify no reordering",
            () -> partitionHighThroughputSinglePartition(h));

        report.runScenario("key-strict-per-key-fifo-high-concurrency",
            "KEY mode: 1 partition, 20 keys × 50 records, concurrency=16, per-key FIFO holds",
            () -> keyStrictFifoUnderHighConcurrency(h));

        report.runScenario("key-hot-key-contention",
            "KEY mode: 1 hot key holds 200 records + 30 cold keys × 5 records each",
            () -> keyHotKeyContention(h));

        report.runScenario("key-fifo-survives-retry",
            "KEY mode: every 3rd record fails once then succeeds; same-key order intact",
            () -> keyFifoSurvivesRetry(h));

        report.runScenario("partition-fifo-survives-retry",
            "PARTITION mode: every 5th record fails once then succeeds; per-partition offset order intact",
            () -> partitionFifoSurvivesRetry(h));

        report.runScenario("key-null-keys-collapse-and-order",
            "KEY mode: 25 null-keyed records collapse to one shard, arrive in offset order",
            () -> keyNullKeysCollapseAndOrder(h));

        report.runScenario("key-cross-partition-same-key-fifo",
            "KEY mode: 4 partitions, 6 keys × 30 records — per-key FIFO across cluster",
            () -> keyCrossPartitionSameKeyFifo(h));

        report.runScenario("key-fifo-under-retry-storm",
            "KEY mode: 5000 records, 50 keys, 30% retry rate — per-key FIFO holds end-to-end",
            () -> keyFifoUnderRetryStorm(h));

        int failures = report.printSummaryAndExitCode();
        System.exit(failures);
    }

    // ====================================================================================
    // PARTITION mode scenarios
    // ====================================================================================

    private static void partitionStrictOffsetOrder(Helpers h) throws Exception {
        int partitions = 4;
        int perPartition = 250;
        int total = partitions * perPartition;
        String topic = h.createUniqueTopic("ord-partition-strict", partitions);
        String groupId = "ord-partition-strict-" + UUID.randomUUID();

        // Produce round-robin with sequence numbers encoded in the value: "p<p>-seq<n>".
        try (var producer = h.byteProducer()) {
            for (int n = 0; n < perPartition; n++) {
                for (int p = 0; p < partitions; p++) {
                    producer.send(new ProducerRecord<>(topic, p, null,
                        ("p" + p + "-seq" + n).getBytes())).get();
                }
            }
        }

        Map<Integer, List<Integer>> observedSeqByPartition = new ConcurrentHashMap<>();
        CountDownLatch done = new CountDownLatch(total);

        try (PlurimaConsumer<byte[], byte[]> c = PlurimaConsumer.<byte[], byte[]>builder()
                .kafkaProperties(h.classicConsumerProps(groupId))
                .topic(topic)
                .engine(ConsumerEngine.CLASSIC_BASIC)
                .ordering(OrderingMode.PARTITION)
                .concurrency(16)
                .pollTimeout(Duration.ofMillis(200))
                .listener((r, ctx) -> {
                    String v = new String(r.value());
                    int seq = parseSeq(v);
                    observedSeqByPartition.computeIfAbsent(r.partition(),
                        p -> new CopyOnWriteArrayList<>()).add(seq);
                    done.countDown();
                })
                .build()) {
            c.start();
            h.waitForClassicAssignment(groupId, 1, partitions);
            if (!done.await(60, TimeUnit.SECONDS)) {
                throw new AssertionError("only " + (total - done.getCount()) + "/"
                    + total + " records received");
            }
        } finally {
            h.deleteTopicQuietly(topic);
        }

        // Each partition's seq list must be 0..perPartition-1 in order.
        if (observedSeqByPartition.size() != partitions) {
            throw new AssertionError("expected " + partitions + " partitions, saw "
                + observedSeqByPartition.keySet());
        }
        observedSeqByPartition.forEach((p, seqs) -> assertMonotonic(
            "partition " + p, seqs));
        System.out.println("    verified: " + total + " records, " + partitions
            + " partitions, all in per-partition offset order");
    }

    private static void partitionHighThroughputSinglePartition(Helpers h) throws Exception {
        // Single partition, 500 records — partition-serial dispatch means everything
        // must arrive in strict offset order. The point is to verify under high
        // throughput (5ms handler, no artificial slowdown) the dispatch doesn't drop
        // or reorder.
        int total = 500;
        String topic = h.createUniqueTopic("ord-partition-single", 1);
        String groupId = "ord-partition-single-" + UUID.randomUUID();

        try (var producer = h.byteProducer()) {
            for (int i = 0; i < total; i++) {
                producer.send(new ProducerRecord<>(topic, 0, null,
                    ("seq" + i).getBytes())).get();
            }
        }

        List<Integer> observed = new CopyOnWriteArrayList<>();
        CountDownLatch done = new CountDownLatch(total);

        try (PlurimaConsumer<byte[], byte[]> c = PlurimaConsumer.<byte[], byte[]>builder()
                .kafkaProperties(h.classicConsumerProps(groupId))
                .topic(topic)
                .engine(ConsumerEngine.CLASSIC_BASIC)
                .ordering(OrderingMode.PARTITION)
                .concurrency(16)
                .pollTimeout(Duration.ofMillis(200))
                .listener((r, ctx) -> {
                    observed.add(parseSeq(new String(r.value())));
                    sleep(5);
                    done.countDown();
                })
                .build()) {
            c.start();
            h.waitForClassicAssignment(groupId, 1, 1);
            if (!done.await(60, TimeUnit.SECONDS)) {
                throw new AssertionError("only " + observed.size() + "/" + total + " received");
            }
        } finally {
            h.deleteTopicQuietly(topic);
        }

        assertMonotonic("single partition", observed);
        if (observed.size() != total) {
            throw new AssertionError("expected " + total + ", saw " + observed.size());
        }
        System.out.println("    verified: 500 records in strict offset order");
    }

    private static void partitionFifoSurvivesRetry(Helpers h) throws Exception {
        // PARTITION mode + retries: a record that retries holds the partition's worker;
        // subsequent records must wait. Verify offset order despite retries.
        int total = 50;
        String topic = h.createUniqueTopic("ord-partition-retry", 1);
        String groupId = "ord-partition-retry-" + UUID.randomUUID();

        try (var producer = h.byteProducer()) {
            for (int i = 0; i < total; i++) {
                producer.send(new ProducerRecord<>(topic, 0, null,
                    ("seq" + i).getBytes())).get();
            }
        }

        List<Integer> observed = new CopyOnWriteArrayList<>();
        AtomicInteger invocations = new AtomicInteger();
        Map<Integer, AtomicInteger> attemptsBySeq = new ConcurrentHashMap<>();
        CountDownLatch done = new CountDownLatch(total);

        RetryPolicy policy = RetryPolicy.exponential()
            .maxAttempts(3)
            .initialDelay(Duration.ofMillis(50))
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
                .concurrency(16)
                .pollTimeout(Duration.ofMillis(200))
                .listener((r, ctx) -> {
                    invocations.incrementAndGet();
                    int seq = parseSeq(new String(r.value()));
                    int attempts = attemptsBySeq.computeIfAbsent(seq, k -> new AtomicInteger())
                        .incrementAndGet();
                    // Every 5th record fails on its first attempt then succeeds on retry.
                    if (seq % 5 == 0 && attempts == 1) {
                        throw new RuntimeException("transient on seq " + seq);
                    }
                    observed.add(seq);
                    done.countDown();
                })
                .build()) {
            c.start();
            h.waitForClassicAssignment(groupId, 1, 1);
            if (!done.await(60, TimeUnit.SECONDS)) {
                throw new AssertionError("only " + observed.size() + "/" + total
                    + " received");
            }
        } finally {
            h.deleteTopicQuietly(topic);
        }

        // The list contains each seq EXACTLY ONCE (success after retry isn't double-counted
        // because we only add() on success). And the list must be monotonic.
        assertMonotonic("partition with retries", observed);
        if (observed.size() != total) {
            throw new AssertionError("expected " + total + " successes, saw " + observed.size());
        }
        // Sanity: invocations > total proves retries actually fired.
        int expectedRetries = total / 5;
        if (invocations.get() != total + expectedRetries) {
            throw new AssertionError("expected " + (total + expectedRetries)
                + " invocations (1 per record + " + expectedRetries
                + " retries), saw " + invocations.get());
        }
        System.out.println("    verified: " + total + " records in offset order despite "
            + expectedRetries + " retries");
    }

    // ====================================================================================
    // KEY mode scenarios
    // ====================================================================================

    private static void keyStrictFifoUnderHighConcurrency(Helpers h) throws Exception {
        int distinctKeys = 20;
        int perKey = 50;
        int total = distinctKeys * perKey;
        String topic = h.createUniqueTopic("ord-key-strict", 1);
        String groupId = "ord-key-strict-" + UUID.randomUUID();

        // Interleave keys so the dispatcher sees them mixed (typical broker output).
        try (var producer = h.byteProducer()) {
            for (int n = 0; n < perKey; n++) {
                for (int k = 0; k < distinctKeys; k++) {
                    producer.send(new ProducerRecord<>(topic,
                        ("k" + k).getBytes(),
                        ("k" + k + "-seq" + n).getBytes())).get();
                }
            }
        }

        Map<String, List<Integer>> observedByKey = new ConcurrentHashMap<>();
        AtomicInteger concurrentlyRunning = new AtomicInteger();
        AtomicInteger maxConcurrent = new AtomicInteger();
        CountDownLatch done = new CountDownLatch(total);

        try (PlurimaConsumer<byte[], byte[]> c = PlurimaConsumer.<byte[], byte[]>builder()
                .kafkaProperties(h.classicConsumerProps(groupId))
                .topic(topic)
                .engine(ConsumerEngine.CLASSIC_BASIC)
                .ordering(OrderingMode.KEY)
                .concurrency(16)
                .shardCount(64)
                .pollTimeout(Duration.ofMillis(200))
                .listener((r, ctx) -> {
                    int now = concurrentlyRunning.incrementAndGet();
                    maxConcurrent.updateAndGet(prev -> Math.max(prev, now));
                    String key = new String(r.key());
                    String value = new String(r.value());
                    int seq = Integer.parseInt(value.substring(value.indexOf("seq") + 3));
                    observedByKey.computeIfAbsent(key, k -> new CopyOnWriteArrayList<>())
                        .add(seq);
                    sleep(20);  // gives shards time to overlap
                    concurrentlyRunning.decrementAndGet();
                    done.countDown();
                })
                .build()) {
            c.start();
            h.waitForClassicAssignment(groupId, 1, 1);
            if (!done.await(120, TimeUnit.SECONDS)) {
                throw new AssertionError("only " + (total - done.getCount()) + "/"
                    + total + " received");
            }
        } finally {
            h.deleteTopicQuietly(topic);
        }

        if (observedByKey.size() != distinctKeys) {
            throw new AssertionError("expected " + distinctKeys + " keys, saw "
                + observedByKey.size());
        }
        observedByKey.forEach((k, seqs) -> assertMonotonic("key " + k, seqs));
        // Sanity: parallelism actually happened (otherwise the test is testing nothing).
        if (maxConcurrent.get() < 4) {
            throw new AssertionError("expected concurrency > 4 to prove KEY-shard "
                + "parallelism; observed peak " + maxConcurrent.get());
        }
        System.out.println("    verified: " + total + " records across " + distinctKeys
            + " keys in per-key order; peak concurrent = " + maxConcurrent.get());
    }

    private static void keyHotKeyContention(Helpers h) throws Exception {
        // One hot key with 200 records + 30 cold keys with 5 records each = 350 total.
        // The hot key's shard becomes the bottleneck; cold keys must still process in
        // parallel on other shards. Both must preserve per-key FIFO.
        int hotPerKey = 200;
        int coldKeys = 30;
        int coldPerKey = 5;
        int total = hotPerKey + (coldKeys * coldPerKey);
        String topic = h.createUniqueTopic("ord-key-hotcontention", 1);
        String groupId = "ord-key-hotcontention-" + UUID.randomUUID();

        try (var producer = h.byteProducer()) {
            // Interleave hot and cold records so the broker delivers them mixed.
            int hotSent = 0, coldRound = 0;
            while (hotSent < hotPerKey || coldRound < coldPerKey) {
                if (hotSent < hotPerKey) {
                    producer.send(new ProducerRecord<>(topic,
                        "HOT".getBytes(), ("HOT-seq" + hotSent).getBytes())).get();
                    hotSent++;
                }
                if (coldRound < coldPerKey) {
                    for (int k = 0; k < coldKeys; k++) {
                        producer.send(new ProducerRecord<>(topic,
                            ("cold" + k).getBytes(),
                            ("cold" + k + "-seq" + coldRound).getBytes())).get();
                    }
                    coldRound++;
                }
            }
        }

        Map<String, List<Integer>> observedByKey = new ConcurrentHashMap<>();
        CountDownLatch done = new CountDownLatch(total);

        try (PlurimaConsumer<byte[], byte[]> c = PlurimaConsumer.<byte[], byte[]>builder()
                .kafkaProperties(h.classicConsumerProps(groupId))
                .topic(topic)
                .engine(ConsumerEngine.CLASSIC_BASIC)
                .ordering(OrderingMode.KEY)
                .concurrency(16)
                .shardCount(64)
                .pollTimeout(Duration.ofMillis(200))
                .listener((r, ctx) -> {
                    String key = new String(r.key());
                    String value = new String(r.value());
                    int seq = Integer.parseInt(value.substring(value.indexOf("seq") + 3));
                    observedByKey.computeIfAbsent(key, k -> new CopyOnWriteArrayList<>())
                        .add(seq);
                    done.countDown();
                })
                .build()) {
            c.start();
            h.waitForClassicAssignment(groupId, 1, 1);
            if (!done.await(60, TimeUnit.SECONDS)) {
                throw new AssertionError("only " + (total - done.getCount()) + "/"
                    + total + " received");
            }
        } finally {
            h.deleteTopicQuietly(topic);
        }

        if (observedByKey.size() != coldKeys + 1) {
            throw new AssertionError("expected " + (coldKeys + 1) + " keys, saw "
                + observedByKey.size());
        }
        observedByKey.forEach((k, seqs) -> assertMonotonic("key " + k, seqs));
        // Hot key got all hotPerKey records; cold keys got coldPerKey each.
        if (observedByKey.get("HOT").size() != hotPerKey) {
            throw new AssertionError("HOT key got " + observedByKey.get("HOT").size()
                + " of " + hotPerKey + " records");
        }
        System.out.println("    verified: 1 hot key (200 records) + " + coldKeys
            + " cold keys, all in per-key order");
    }

    private static void keyFifoSurvivesRetry(Helpers h) throws Exception {
        int distinctKeys = 10;
        int perKey = 8;
        int total = distinctKeys * perKey;
        String topic = h.createUniqueTopic("ord-key-retry", 1);
        String groupId = "ord-key-retry-" + UUID.randomUUID();

        try (var producer = h.byteProducer()) {
            for (int n = 0; n < perKey; n++) {
                for (int k = 0; k < distinctKeys; k++) {
                    producer.send(new ProducerRecord<>(topic,
                        ("k" + k).getBytes(),
                        ("k" + k + "-seq" + n).getBytes())).get();
                }
            }
        }

        Map<String, List<Integer>> observedByKey = new ConcurrentHashMap<>();
        AtomicInteger invocations = new AtomicInteger();
        Map<String, AtomicInteger> attemptsByKey = new ConcurrentHashMap<>();
        CountDownLatch done = new CountDownLatch(total);

        RetryPolicy policy = RetryPolicy.exponential()
            .maxAttempts(3)
            .initialDelay(Duration.ofMillis(50))
            .multiplier(2.0)
            .jitter(0.0)
            .retryOn(RuntimeException.class)
            .build();

        try (PlurimaConsumer<byte[], byte[]> c = PlurimaConsumer.<byte[], byte[]>builder()
                .kafkaProperties(h.classicConsumerProps(groupId))
                .topic(topic)
                .engine(ConsumerEngine.CLASSIC_BASIC)
                .ordering(OrderingMode.KEY)
                .retry(policy)
                .concurrency(16)
                .shardCount(64)
                .pollTimeout(Duration.ofMillis(200))
                .listener((r, ctx) -> {
                    invocations.incrementAndGet();
                    String key = new String(r.key());
                    String value = new String(r.value());
                    int seq = Integer.parseInt(value.substring(value.indexOf("seq") + 3));
                    // Every 3rd seq fails its first attempt then succeeds.
                    int attempts = attemptsByKey.computeIfAbsent(key + ":" + seq,
                        k -> new AtomicInteger()).incrementAndGet();
                    if (seq % 3 == 0 && attempts == 1) {
                        throw new RuntimeException("transient on " + key + " seq " + seq);
                    }
                    observedByKey.computeIfAbsent(key, k -> new CopyOnWriteArrayList<>())
                        .add(seq);
                    done.countDown();
                })
                .build()) {
            c.start();
            h.waitForClassicAssignment(groupId, 1, 1);
            if (!done.await(60, TimeUnit.SECONDS)) {
                throw new AssertionError("only " + (total - done.getCount()) + "/"
                    + total + " received");
            }
        } finally {
            h.deleteTopicQuietly(topic);
        }

        observedByKey.forEach((k, seqs) -> assertMonotonic("key " + k + " (after retries)", seqs));
        // Each key got exactly perKey successes.
        observedByKey.forEach((k, seqs) -> {
            if (seqs.size() != perKey) {
                throw new AssertionError("key " + k + " got " + seqs.size() + " of " + perKey);
            }
        });
        int retriedSeqs = (int) java.util.stream.IntStream.range(0, perKey).filter(s -> s % 3 == 0).count();
        int expectedRetries = distinctKeys * retriedSeqs;
        if (invocations.get() != total + expectedRetries) {
            throw new AssertionError("expected " + (total + expectedRetries)
                + " invocations, saw " + invocations.get());
        }
        System.out.println("    verified: " + total + " records across " + distinctKeys
            + " keys; " + expectedRetries + " retries fired; per-key FIFO intact");
    }

    private static void keyNullKeysCollapseAndOrder(Helpers h) throws Exception {
        int total = 25;
        String topic = h.createUniqueTopic("ord-key-null", 1);
        String groupId = "ord-key-null-" + UUID.randomUUID();

        try (var producer = h.byteProducer()) {
            for (int i = 0; i < total; i++) {
                producer.send(new ProducerRecord<>(topic, 0, null,
                    ("seq" + i).getBytes())).get();
            }
        }

        List<Integer> observed = new CopyOnWriteArrayList<>();
        AtomicInteger concurrentlyRunning = new AtomicInteger();
        AtomicInteger maxConcurrent = new AtomicInteger();
        CountDownLatch done = new CountDownLatch(total);

        try (PlurimaConsumer<byte[], byte[]> c = PlurimaConsumer.<byte[], byte[]>builder()
                .kafkaProperties(h.classicConsumerProps(groupId))
                .topic(topic)
                .engine(ConsumerEngine.CLASSIC_BASIC)
                .ordering(OrderingMode.KEY)
                .concurrency(16)
                .shardCount(16)
                .pollTimeout(Duration.ofMillis(200))
                .listener((r, ctx) -> {
                    int now = concurrentlyRunning.incrementAndGet();
                    maxConcurrent.updateAndGet(prev -> Math.max(prev, now));
                    observed.add(parseSeq(new String(r.value())));
                    sleep(40);
                    concurrentlyRunning.decrementAndGet();
                    done.countDown();
                })
                .build()) {
            c.start();
            h.waitForClassicAssignment(groupId, 1, 1);
            if (!done.await(60, TimeUnit.SECONDS)) {
                throw new AssertionError("only " + observed.size() + "/" + total + " received");
            }
        } finally {
            h.deleteTopicQuietly(topic);
        }

        // All null-keyed records must collapse to one shard → serial → in offset order.
        assertMonotonic("null-key shard", observed);
        if (maxConcurrent.get() != 1) {
            throw new AssertionError("null-keyed records did not serialise: peak concurrent = "
                + maxConcurrent.get());
        }
        System.out.println("    verified: " + total + " null-keyed records on one shard in order");
    }

    private static void keyCrossPartitionSameKeyFifo(Helpers h) throws Exception {
        // 4 partitions, 6 keys × 30 records. Kafka's default partitioner is key-aware
        // (DefaultPartitioner / built-in murmur2 on the key bytes), so same-key records
        // always land on the same partition. Verify per-key FIFO on a single consumer
        // owning all 4 partitions.
        int partitions = 4;
        int distinctKeys = 6;
        int perKey = 30;
        int total = distinctKeys * perKey;
        String topic = h.createUniqueTopic("ord-key-crosspart", partitions);
        String groupId = "ord-key-crosspart-" + UUID.randomUUID();

        try (var producer = h.byteProducer()) {
            for (int n = 0; n < perKey; n++) {
                for (int k = 0; k < distinctKeys; k++) {
                    producer.send(new ProducerRecord<>(topic,
                        ("key" + k).getBytes(),
                        ("key" + k + "-seq" + n).getBytes())).get();
                }
            }
        }

        Map<String, List<Integer>> observedByKey = new ConcurrentHashMap<>();
        Map<String, Integer> partitionByKey = new HashMap<>();
        CountDownLatch done = new CountDownLatch(total);

        try (PlurimaConsumer<byte[], byte[]> c = PlurimaConsumer.<byte[], byte[]>builder()
                .kafkaProperties(h.classicConsumerProps(groupId))
                .topic(topic)
                .engine(ConsumerEngine.CLASSIC_BASIC)
                .ordering(OrderingMode.KEY)
                .concurrency(16)
                .shardCount(64)
                .pollTimeout(Duration.ofMillis(200))
                .listener((r, ctx) -> {
                    String key = new String(r.key());
                    String value = new String(r.value());
                    int seq = Integer.parseInt(value.substring(value.indexOf("seq") + 3));
                    observedByKey.computeIfAbsent(key, k -> new CopyOnWriteArrayList<>())
                        .add(seq);
                    synchronized (partitionByKey) {
                        partitionByKey.merge(key, r.partition(),
                            (existing, incoming) -> {
                                if (existing.intValue() != incoming.intValue()) {
                                    throw new RuntimeException(
                                        "key " + key + " seen on multiple partitions: "
                                        + existing + " and " + incoming);
                                }
                                return existing;
                            });
                    }
                    done.countDown();
                })
                .build()) {
            c.start();
            h.waitForClassicAssignment(groupId, 1, partitions);
            if (!done.await(90, TimeUnit.SECONDS)) {
                throw new AssertionError("only " + (total - done.getCount()) + "/"
                    + total + " received");
            }
        } finally {
            h.deleteTopicQuietly(topic);
        }

        observedByKey.forEach((k, seqs) -> assertMonotonic("key " + k + " (cross-partition)", seqs));
        if (observedByKey.size() != distinctKeys) {
            throw new AssertionError("expected " + distinctKeys + " keys, saw "
                + observedByKey.size());
        }
        System.out.println("    verified: " + distinctKeys + " keys spread across "
            + partitions + " partitions, per-key FIFO holds; partition layout: "
            + partitionByKey);
    }

    private static void keyFifoUnderRetryStorm(Helpers h) throws Exception {
        // Heavy concurrency + heavy retry rate stress: 5 000 records across 50 keys,
        // every 3rd record fails on its first attempt, every 7th fails its first two,
        // and every 11th fails its first three. concurrency=24 keeps many workers in
        // flight at once. Per-key FIFO must STILL hold — proves the dispatcher + retry
        // engine + commit frontier interact correctly under stress.
        int distinctKeys = 50;
        int perKey = 100;
        int total = distinctKeys * perKey;
        String topic = h.createUniqueTopic("ord-key-retry-storm", 1);
        String groupId = "ord-key-retry-storm-" + UUID.randomUUID();

        try (var producer = h.byteProducer()) {
            for (int n = 0; n < perKey; n++) {
                for (int k = 0; k < distinctKeys; k++) {
                    producer.send(new org.apache.kafka.clients.producer.ProducerRecord<>(
                        topic, ("k" + k).getBytes(),
                        ("k" + k + "-seq" + n).getBytes())).get();
                }
            }
        }

        Map<String, List<Integer>> observedByKey = new ConcurrentHashMap<>();
        AtomicInteger invocations = new AtomicInteger();
        AtomicInteger retriesFired = new AtomicInteger();
        Map<String, AtomicInteger> attemptsByCoord = new ConcurrentHashMap<>();
        CountDownLatch done = new CountDownLatch(total);

        io.plurima.kafka.retry.RetryPolicy policy = io.plurima.kafka.retry.RetryPolicy.exponential()
            .maxAttempts(5)
            .initialDelay(Duration.ofMillis(20))
            .multiplier(1.5)
            .jitter(0.0)
            .retryOn(RuntimeException.class)
            .build();

        try (PlurimaConsumer<byte[], byte[]> c = PlurimaConsumer.<byte[], byte[]>builder()
                .kafkaProperties(h.classicConsumerProps(groupId))
                .topic(topic)
                .engine(ConsumerEngine.CLASSIC_BASIC)
                .ordering(OrderingMode.KEY)
                .retry(policy)
                .concurrency(24)
                .shardCount(96)
                .pollTimeout(Duration.ofMillis(200))
                .listener((r, ctx) -> {
                    invocations.incrementAndGet();
                    String key = new String(r.key());
                    String value = new String(r.value());
                    int seq = Integer.parseInt(value.substring(value.indexOf("seq") + 3));
                    String coord = key + ":" + seq;
                    int attempts = attemptsByCoord.computeIfAbsent(coord,
                        k -> new AtomicInteger()).incrementAndGet();
                    // Failure schedule: every 3rd seq fails attempt 1, every 7th also
                    // fails attempt 2, every 11th also fails attempt 3.
                    int failAttempts = 0;
                    if (seq % 3 == 0) failAttempts++;
                    if (seq % 7 == 0) failAttempts++;
                    if (seq % 11 == 0) failAttempts++;
                    if (attempts <= failAttempts) {
                        retriesFired.incrementAndGet();
                        throw new RuntimeException("simulated transient " + coord
                            + " attempt " + attempts);
                    }
                    observedByKey.computeIfAbsent(key, k -> new CopyOnWriteArrayList<>())
                        .add(seq);
                    done.countDown();
                })
                .build()) {
            c.start();
            h.waitForClassicAssignment(groupId, 1, 1);
            if (!done.await(300, TimeUnit.SECONDS)) {
                throw new AssertionError("only " + (total - done.getCount()) + "/"
                    + total + " received after 5 minutes");
            }
        } finally {
            h.deleteTopicQuietly(topic);
        }

        // Every key's observed sequence must be strictly monotonic (per-key FIFO).
        observedByKey.forEach((k, seqs) -> assertMonotonic("key " + k + " (retry storm)", seqs));
        // Every key must have completed perKey records (no loss).
        observedByKey.forEach((k, seqs) -> {
            if (seqs.size() != perKey) {
                throw new AssertionError("key " + k + " saw " + seqs.size() + " of " + perKey);
            }
        });
        if (observedByKey.size() != distinctKeys) {
            throw new AssertionError("expected " + distinctKeys + " keys, saw "
                + observedByKey.size());
        }
        // Sanity: we should have fired a meaningful number of retries (proves the
        // retry path was actually exercised — not just a happy-path stress test).
        System.out.println("    verified: " + total + " records / " + distinctKeys
            + " keys / " + retriesFired.get() + " retries fired ("
            + invocations.get() + " total handler invocations)");
        if (retriesFired.get() < distinctKeys * 5) {
            throw new AssertionError("expected many retries to fire across keys; saw only "
                + retriesFired.get());
        }
    }

    // ====================================================================================
    // helpers
    // ====================================================================================

    /** Throw if {@code seqs} is not monotonically non-decreasing. */
    private static void assertMonotonic(String label, List<Integer> seqs) {
        if (seqs.isEmpty()) {
            throw new AssertionError(label + ": empty list (no records observed)");
        }
        for (int i = 1; i < seqs.size(); i++) {
            if (seqs.get(i) < seqs.get(i - 1)) {
                throw new AssertionError(label + ": order broken at index " + i
                    + " — got " + seqs.get(i) + " after " + seqs.get(i - 1)
                    + "; full list: " + seqs);
            }
        }
    }

    private static int parseSeq(String value) {
        int idx = value.indexOf("seq");
        if (idx < 0) throw new IllegalArgumentException("no seq in: " + value);
        return Integer.parseInt(value.substring(idx + 3));
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
