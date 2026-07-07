package io.plurima.kafka.integration;

import io.plurima.kafka.PlurimaConsumer;
import io.plurima.kafka.retry.RetryPolicy;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end proof of Pillar 2: a record that is force-RELEASEd because it was <em>slow</em>
 * (drain-barrier timeout) and then <em>fails once</em> must NOT be treated as having exhausted
 * its retry budget — the slowness redelivery is subtracted, so the genuine failure counts as the
 * first attempt and the record is retried.
 *
 * <p>Setup: {@code maxAttempts=1}, delayed retry. The handler, keyed on broker deliveryCount:
 * <ol>
 *   <li>delivery 1 → blocks past the 1s drain barrier → force-RELEASEd (slowness) → redelivered</li>
 *   <li>delivery 2 → throws once (genuine failure)</li>
 *   <li>delivery ≥ 3 → succeeds</li>
 * </ol>
 * With Pillar 2 the delivery-2 failure has effective attempt 0 ({@code (2-1) - 1 slowness}), so it
 * is retried and delivery 3 succeeds. WITHOUT Pillar 2 the delivery-2 failure would have effective
 * attempt 1 ≥ maxAttempts(1) → exhausted → the record is terminally rejected and delivery 3 never
 * happens. So reaching deliveryCount ≥ 3 (and succeeding) is the discriminating evidence.
 *
 * <p>Tagged {@code integration} (needs a real 4.2 broker at localhost:9092).
 */
@Tag("integration")
class SlownessRetryBudgetIntegrationTest {

    @Test
    void slowThenFailingRecordIsRetriedNotPrematurelyExhausted() throws Exception {
        String topic = KafkaIntegrationSupport.createUniqueTopic("plurima-slowbudget");
        String groupId = "plurima-slowbudget-group-" + java.util.UUID.randomUUID();

        AtomicInteger maxDeliveryCount = new AtomicInteger(0);
        CountDownLatch succeeded = new CountDownLatch(1);
        Properties props = KafkaIntegrationSupport.consumerProps(groupId);

        PlurimaConsumer<byte[], byte[]> consumer = PlurimaConsumer.builder()
            .kafkaProperties(props)
            .topic(topic)
            .concurrency(4)
            .pollTimeout(Duration.ofMillis(300))
            .lockDuration(Duration.ofSeconds(1))   // barrier fires at 1s → forces RELEASE of the slow delivery
            .retry(RetryPolicy.exponential()
                .maxAttempts(1)                    // a single genuine failure would normally exhaust
                .initialDelay(Duration.ofSeconds(2))   // > 1s → DELAYED retry (RELEASE → broker redelivers, dc++)
                .multiplier(1.0).jitter(0.0)
                .retryOn(IOException.class)
                .build())
            .listener((record, ctx) -> {
                int dc = ctx.deliveryCount();
                maxDeliveryCount.accumulateAndGet(dc, Math::max);
                if (dc == 1) {
                    Thread.sleep(3_000);            // slow → force-RELEASEd at the 1s barrier (slowness)
                } else if (dc == 2) {
                    throw new IOException("one genuine failure");
                } else {
                    succeeded.countDown();          // dc >= 3 → success (only reachable if P2 retried it)
                }
            })
            .build();

        try {
            consumer.start();
            KafkaIntegrationSupport.waitForAssignment(groupId, topic);
            try (KafkaProducer<byte[], byte[]> producer = KafkaIntegrationSupport.byteProducer()) {
                producer.send(new ProducerRecord<>(topic, new byte[]{0}, "x".getBytes())).get();
                producer.flush();
            }

            boolean ok = succeeded.await(45, TimeUnit.SECONDS);
            assertThat(ok)
                .as("record reached delivery >= 3 and succeeded — proving the slowness redelivery "
                    + "was NOT counted toward the retry budget (saw maxDeliveryCount=%d)",
                    maxDeliveryCount.get())
                .isTrue();
            assertThat(maxDeliveryCount.get()).isGreaterThanOrEqualTo(3);
        } finally {
            consumer.close();
            KafkaIntegrationSupport.deleteTopicQuietly(topic);
        }
    }
}
