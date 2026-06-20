package io.plurima.testapp;

import io.plurima.kafka.ConsumerEngine;
import io.plurima.kafka.OrderingMode;
import io.plurima.kafka.PlurimaConsumer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Minimal executable proof that a Plurima share consumer can process records from
 * the Docker quick-start broker.
 */
public final class QuickstartApp {

    private static final int RECORD_COUNT = 10;

    private QuickstartApp() {}

    public static void main(String[] args) throws Exception {
        String bootstrap = parseBootstrap(args);
        Helpers helpers = new Helpers(bootstrap);
        String topic = helpers.createUniqueTopic("plurima-quickstart", 1);
        String groupId = "plurima-quickstart-" + UUID.randomUUID();
        CountDownLatch processed = new CountDownLatch(RECORD_COUNT);
        Set<String> values = ConcurrentHashMap.newKeySet();

        try (PlurimaConsumer<byte[], byte[]> consumer = PlurimaConsumer.<byte[], byte[]>builder()
            .kafkaProperties(helpers.shareConsumerProps(groupId))
            .topic(topic)
            .engine(ConsumerEngine.SHARE)
            .ordering(OrderingMode.UNORDERED)
            .concurrency(4)
            .pollTimeout(Duration.ofMillis(200))
            .lockDuration(Duration.ofSeconds(24))
            .listener((record, context) -> {
                values.add(new String(record.value(), StandardCharsets.UTF_8));
                processed.countDown();
            })
            .build()) {

            consumer.start();
            helpers.waitForShareAssignment(groupId, topic);

            try (var producer = helpers.byteProducer()) {
                for (int i = 0; i < RECORD_COUNT; i++) {
                    producer.send(new ProducerRecord<>(
                        topic,
                        ("key-" + i).getBytes(StandardCharsets.UTF_8),
                        ("value-" + i).getBytes(StandardCharsets.UTF_8))).get();
                }
            }

            if (!processed.await(30, TimeUnit.SECONDS)) {
                throw new IllegalStateException(
                    "Timed out waiting for records: processed=" + values.size()
                    + "/" + RECORD_COUNT);
            }
        } finally {
            helpers.deleteTopicQuietly(topic);
        }

        if (values.size() != RECORD_COUNT) {
            throw new IllegalStateException(
                "Expected " + RECORD_COUNT + " unique values, got " + values.size());
        }

        System.out.printf(
            "QUICKSTART_OK bootstrap=%s produced=%d processed=%d%n",
            bootstrap, RECORD_COUNT, values.size());
    }

    private static String parseBootstrap(String[] args) {
        for (int i = 0; i < args.length - 1; i++) {
            if ("--bootstrap".equals(args[i])) {
                return args[i + 1];
            }
        }
        return "localhost:9092";
    }
}
