package io.plurima.testapp.bench;

import org.apache.kafka.clients.consumer.AcknowledgeType;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.KafkaShareConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.function.Consumer;

/**
 * Thin "no-framework" reference implementations that consume records using only the
 * stock Kafka clients API. These mirror what a typical application would write before
 * adopting Plurima — single-threaded loops, manual commit, no retry/DLT, no backpressure.
 *
 * <p>They exist so the benchmark can quote wall-clock numbers for "vanilla" and compare
 * to Plurima fairly. The vanilla loops do NOT try to be slow or buggy; they're the
 * straightforward, idiomatic implementation an engineer would land on starting from
 * the Kafka docs.
 */
final class VanillaConsumers {

    /**
     * Single-threaded vanilla {@link KafkaConsumer} loop: poll → for each record run the
     * handler synchronously → manual commit. Stops once {@code total} records have been
     * handled or the {@code timeoutMs} expires. Returns wall-clock ms from first record
     * to last.
     */
    static long runVanillaClassic(
        Properties props,
        String topic,
        int total,
        long timeoutMs,
        Consumer<ConsumerRecord<byte[], byte[]>> handler) {

        Properties p = new Properties();
        p.putAll(props);
        p.put("key.deserializer", ByteArrayDeserializer.class.getName());
        p.put("value.deserializer", ByteArrayDeserializer.class.getName());
        p.put("enable.auto.commit", "false");

        try (KafkaConsumer<byte[], byte[]> kc = new KafkaConsumer<>(p)) {
            kc.subscribe(List.of(topic));
            int seen = 0;
            long startNanos = -1;
            long deadline = System.currentTimeMillis() + timeoutMs;
            while (seen < total && System.currentTimeMillis() < deadline) {
                ConsumerRecords<byte[], byte[]> batch = kc.poll(Duration.ofMillis(500));
                if (batch.isEmpty()) continue;
                if (startNanos < 0) startNanos = System.nanoTime();
                Map<TopicPartition, OffsetAndMetadata> commits = new HashMap<>();
                for (ConsumerRecord<byte[], byte[]> r : batch) {
                    handler.accept(r);
                    seen++;
                    commits.put(new TopicPartition(r.topic(), r.partition()),
                        new OffsetAndMetadata(r.offset() + 1));
                }
                kc.commitSync(commits);
            }
            if (seen < total) {
                throw new AssertionError("vanilla-classic: only " + seen + "/" + total
                    + " records consumed within " + timeoutMs + "ms");
            }
            return (System.nanoTime() - startNanos) / 1_000_000L;
        }
    }

    /**
     * Single-threaded vanilla {@link KafkaShareConsumer} loop. Idiomatic usage: poll,
     * handle each record, accept. Implicit-mode acknowledgement (the default) is used so
     * the loop is as simple as possible.
     *
     * <p>{@code beforeProduce} is invoked once the consumer has assigned the topic but
     * before the main poll loop starts processing — typically a producer call. Without
     * this hook the broker's default {@code share.auto.offset.reset} (often {@code latest})
     * would mean records produced BEFORE subscribe never get delivered to a fresh group.
     */
    static long runVanillaShare(
        Properties props,
        String topic,
        int total,
        long timeoutMs,
        Consumer<ConsumerRecord<byte[], byte[]>> handler,
        Runnable beforeProduce) {

        Properties p = new Properties();
        p.putAll(props);
        p.put("key.deserializer", ByteArrayDeserializer.class.getName());
        p.put("value.deserializer", ByteArrayDeserializer.class.getName());
        // The vanilla "simplest possible share consumer" example uses IMPLICIT
        // acknowledgement — explicit mode would require an acknowledge() per record before
        // each poll, making the example non-trivial. Plurima users get explicit mode for
        // free; for the apples-to-apples vanilla baseline we use the simpler implicit mode.
        p.put("share.acknowledgement.mode", "implicit");

        try (KafkaShareConsumer<byte[], byte[]> sc = new KafkaShareConsumer<>(p)) {
            sc.subscribe(List.of(topic));
            // Prime the broker-side group registration with a few empty polls; share groups
            // don't expose .assignment() the same way classic groups do, so we just give
            // the coordinator a fixed window to register us before producing.
            long primeDeadline = System.currentTimeMillis() + 3_000L;
            while (System.currentTimeMillis() < primeDeadline) {
                sc.poll(Duration.ofMillis(200));
            }
            if (beforeProduce != null) beforeProduce.run();

            int seen = 0;
            long startNanos = -1;
            long deadline = System.currentTimeMillis() + timeoutMs;
            while (seen < total && System.currentTimeMillis() < deadline) {
                ConsumerRecords<byte[], byte[]> batch = sc.poll(Duration.ofMillis(500));
                if (batch.isEmpty()) continue;
                if (startNanos < 0) startNanos = System.nanoTime();
                for (ConsumerRecord<byte[], byte[]> r : batch) {
                    handler.accept(r);
                    seen++;
                }
                sc.commitSync();
            }
            if (seen < total) {
                throw new AssertionError("vanilla-share: only " + seen + "/" + total
                    + " records consumed within " + timeoutMs + "ms");
            }
            return (System.nanoTime() - startNanos) / 1_000_000L;
        }
    }

    /**
     * Vanilla classic consumer running in "naive at-least-once with no progress on a
     * failing record" mode: poll → if handler throws, do NOT commit, loop back. Used to
     * demonstrate that without a retry/DLT framework a single bad record blocks its
     * partition until it succeeds (or the operator manually skips it).
     *
     * <p>Returns the number of times the same offset was redelivered before the
     * caller-provided {@code shouldStop} flips true.
     */
    static int runVanillaClassicWithRedelivery(
        Properties props,
        String topic,
        Consumer<ConsumerRecord<byte[], byte[]>> handler,
        CountDownLatch shouldStop,
        long timeoutMs) {

        Properties p = new Properties();
        p.putAll(props);
        p.put("key.deserializer", ByteArrayDeserializer.class.getName());
        p.put("value.deserializer", ByteArrayDeserializer.class.getName());
        p.put("enable.auto.commit", "false");

        int redeliveries = 0;
        List<Long> seenOffsets = new ArrayList<>();
        try (KafkaConsumer<byte[], byte[]> kc = new KafkaConsumer<>(p)) {
            kc.subscribe(List.of(topic));
            long deadline = System.currentTimeMillis() + timeoutMs;
            while (shouldStop.getCount() > 0 && System.currentTimeMillis() < deadline) {
                ConsumerRecords<byte[], byte[]> batch = kc.poll(Duration.ofMillis(500));
                for (ConsumerRecord<byte[], byte[]> r : batch) {
                    seenOffsets.add(r.offset());
                    try {
                        handler.accept(r);
                        kc.commitSync(Map.of(
                            new TopicPartition(r.topic(), r.partition()),
                            new OffsetAndMetadata(r.offset() + 1)));
                    } catch (RuntimeException ignore) {
                        // No retry framework — just don't commit. Next poll will redeliver.
                        // Without a seek-back the broker still has us as owner; record will
                        // be polled again from the last committed offset.
                        redeliveries++;
                        // Force a re-poll of the same offset by seeking back. (Without this
                        // the in-memory consumer's position advances even though commit
                        // didn't land.)
                        kc.seek(new TopicPartition(r.topic(), r.partition()), r.offset());
                        break;  // restart batch loop
                    }
                }
            }
        }
        return redeliveries;
    }

    private VanillaConsumers() {}
}
