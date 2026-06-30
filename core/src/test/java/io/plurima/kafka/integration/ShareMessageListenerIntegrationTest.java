package io.plurima.kafka.integration;

import io.plurima.kafka.Message;
import io.plurima.kafka.MessageListener;
import io.plurima.kafka.PlurimaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end verification of the Tier-2 builder adapters through the real engine + broker:
 * {@code onMessage} (auto-ack) and {@code onMessageAck} (explicit). Confirms the record→Message
 * translation (value, header, deliveryCount) and that auto/explicit acknowledgement complete
 * each record exactly once.
 *
 * <p>Tagged {@code integration} (needs a real 4.2 broker at localhost:9092).
 */
@Tag("integration")
class ShareMessageListenerIntegrationTest {

    @Test
    void onMessageAutoAckProcessesEachRecordOnceWithCorrectMetadata() throws Exception {
        String topic = KafkaIntegrationSupport.createUniqueTopic("plurima-onmessage");
        String groupId = "plurima-onmessage-group-" + java.util.UUID.randomUUID();
        int n = 8;

        Set<String> processed = ConcurrentHashMap.newKeySet();
        AtomicInteger invocations = new AtomicInteger();
        AtomicInteger maxDeliveryCount = new AtomicInteger();
        AtomicReference<String> seenHeader = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(n);
        Properties props = KafkaIntegrationSupport.consumerProps(groupId);

        MessageListener<byte[], byte[]> handler = (Message<byte[], byte[]> msg) -> {
            invocations.incrementAndGet();
            processed.add(new String(msg.value(), StandardCharsets.UTF_8));
            maxDeliveryCount.accumulateAndGet(msg.deliveryCount(), Math::max);
            msg.header("h").ifPresent(b -> seenHeader.set(new String(b, StandardCharsets.UTF_8)));
            latch.countDown();
        };

        PlurimaConsumer<byte[], byte[]> consumer = PlurimaConsumer.<byte[], byte[]>builder()
            .kafkaProperties(props).topic(topic).concurrency(8)
            .pollTimeout(Duration.ofMillis(500))
            .onMessage(handler)
            .build();

        try {
            consumer.start();
            KafkaIntegrationSupport.waitForAssignment(groupId, topic);
            try (KafkaProducer<byte[], byte[]> producer = KafkaIntegrationSupport.byteProducer()) {
                for (int i = 0; i < n; i++) {
                    ProducerRecord<byte[], byte[]> pr = new ProducerRecord<>(
                        topic, null, new byte[]{(byte) i}, ("msg-" + i).getBytes(StandardCharsets.UTF_8));
                    pr.headers().add("h", "hval".getBytes(StandardCharsets.UTF_8));
                    producer.send(pr).get();
                }
                producer.flush();
            }

            assertThat(latch.await(30, TimeUnit.SECONDS)).as("all %d processed", n).isTrue();
            Thread.sleep(2_000);   // let any erroneous redelivery surface

            assertThat(processed).hasSize(n);
            assertThat(invocations.get()).as("each record processed exactly once").isEqualTo(n);
            assertThat(maxDeliveryCount.get()).as("first delivery").isEqualTo(1);
            assertThat(seenHeader.get()).as("header surfaced through Message").isEqualTo("hval");
        } finally {
            consumer.close();
            KafkaIntegrationSupport.deleteTopicQuietly(topic);
        }
    }

    @Test
    void onMessageAckExplicitAcceptCompletesEachRecordOnce() throws Exception {
        String topic = KafkaIntegrationSupport.createUniqueTopic("plurima-onmessageack");
        String groupId = "plurima-onmessageack-group-" + java.util.UUID.randomUUID();
        int n = 6;

        Set<String> processed = ConcurrentHashMap.newKeySet();
        AtomicInteger invocations = new AtomicInteger();
        CountDownLatch latch = new CountDownLatch(n);
        Properties props = KafkaIntegrationSupport.consumerProps(groupId);

        PlurimaConsumer<byte[], byte[]> consumer = PlurimaConsumer.<byte[], byte[]>builder()
            .kafkaProperties(props).topic(topic).concurrency(6)
            .pollTimeout(Duration.ofMillis(500))
            .onMessageAck(msg -> {
                invocations.incrementAndGet();
                processed.add(new String(msg.value(), StandardCharsets.UTF_8));
                msg.accept();                     // explicit terminal ack
                latch.countDown();
            })
            .build();

        try {
            consumer.start();
            KafkaIntegrationSupport.waitForAssignment(groupId, topic);
            try (KafkaProducer<byte[], byte[]> producer = KafkaIntegrationSupport.byteProducer()) {
                for (int i = 0; i < n; i++) {
                    producer.send(new ProducerRecord<>(
                        topic, new byte[]{(byte) i}, ("m-" + i).getBytes(StandardCharsets.UTF_8))).get();
                }
                producer.flush();
            }

            assertThat(latch.await(30, TimeUnit.SECONDS)).as("all %d processed", n).isTrue();
            Thread.sleep(2_000);

            assertThat(processed).hasSize(n);
            assertThat(invocations.get())
                .as("accept() is terminal — no reprocessing").isEqualTo(n);
        } finally {
            consumer.close();
            KafkaIntegrationSupport.deleteTopicQuietly(topic);
        }
    }
}
