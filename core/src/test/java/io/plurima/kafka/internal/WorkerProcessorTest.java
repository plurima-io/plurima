package io.plurima.kafka.internal;

import io.plurima.kafka.ConsumerContext;
import io.plurima.kafka.OrderingMode;
import io.plurima.kafka.RecordListener;
import io.plurima.kafka.deserializer.RecordDeserializer;
import io.plurima.kafka.retry.RetryPolicy;
import org.apache.kafka.clients.consumer.AcknowledgeType;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ShareConsumer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;

class WorkerProcessorTest {

    private InFlightRegistry registry;
    private AckCoordinator coordinator;
    private ShareConsumer<byte[], byte[]> consumer;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        registry = new InFlightRegistry();
        coordinator = new AckCoordinator(registry);
        consumer = mock(ShareConsumer.class);
    }

    private InFlightRecord<byte[], byte[]> rec(long offset) {
        InFlightRecord<byte[], byte[]> r = new InFlightRecord<>(new ConsumerRecord<>("t", 0, offset, new byte[0], new byte[0]));
        registry.register(r);
        return r;
    }

    private ConsumerContext ctxFor(InFlightRecord<byte[], byte[]> r) {
        return new ConsumerContext() {
            @Override public short deliveryCount() { return 1; }
            @Override public Optional<Short> deliveryCountOptional() { return Optional.of((short) 1); }
            @Override public OrderingMode orderingMode() { return OrderingMode.UNORDERED; }
        };
    }

    @Test
    void successfulListenerQueuesAccept() {
        RecordListener<byte[], byte[]> listener = (r, ctx) -> { /* ok */ };
        ListenerInvoker invoker = ListenerInvoker.forImplicit(listener, RecordDeserializer.bytes(), RecordDeserializer.bytes());
        WorkerProcessor p = new WorkerProcessor(
            invoker, new RetryEngine(RetryPolicy.noRetry()), coordinator);

        InFlightRecord<byte[], byte[]> r = rec(1);
        p.process(r, ctxFor(r));

        coordinator.commitPendingAcks(consumer);
        org.mockito.Mockito.verify(consumer).acknowledge(
            argThat(cr -> cr.topic().equals("t") && cr.partition() == 0 && cr.offset() == 1L),
            eq(AcknowledgeType.ACCEPT));
    }

    @Test
    void nonRetriableExceptionQueuesReject() {
        RecordListener<byte[], byte[]> listener = (r, ctx) -> { throw new RuntimeException("perm"); };
        ListenerInvoker invoker = ListenerInvoker.forImplicit(listener, RecordDeserializer.bytes(), RecordDeserializer.bytes());
        WorkerProcessor p = new WorkerProcessor(
            invoker,
            new RetryEngine(RetryPolicy.exponential()
                .maxAttempts(3)
                .initialDelay(Duration.ofMillis(10))
                .retryOn(IOException.class)
                .build()),
            coordinator);

        InFlightRecord<byte[], byte[]> r = rec(2);
        p.process(r, ctxFor(r));

        coordinator.commitPendingAcks(consumer);
        org.mockito.Mockito.verify(consumer).acknowledge(
            argThat(cr -> cr.topic().equals("t") && cr.partition() == 0 && cr.offset() == 2L),
            eq(AcknowledgeType.REJECT));
    }

    @Test
    void inlineRetryEventuallySucceedsAndAccepts() {
        AtomicInteger attempts = new AtomicInteger();
        RecordListener<byte[], byte[]> listener = (r, ctx) -> {
            if (attempts.incrementAndGet() < 3) throw new IOException("transient");
        };
        ListenerInvoker invoker = ListenerInvoker.forImplicit(listener, RecordDeserializer.bytes(), RecordDeserializer.bytes());
        WorkerProcessor p = new WorkerProcessor(
            invoker,
            new RetryEngine(RetryPolicy.exponential()
                .maxAttempts(5)
                .initialDelay(Duration.ofMillis(20))
                .multiplier(1.0)
                .jitter(0.0)
                .retryOn(IOException.class)
                .build()),
            coordinator);

        InFlightRecord<byte[], byte[]> r = rec(3);
        p.process(r, ctxFor(r));

        assertThat(attempts.get()).isEqualTo(3);
        assertThat(r.attempt()).isEqualTo(2); // incremented twice on the 2 retries
        coordinator.commitPendingAcks(consumer);
        org.mockito.Mockito.verify(consumer).acknowledge(
            argThat(cr -> cr.topic().equals("t") && cr.partition() == 0 && cr.offset() == 3L),
            eq(AcknowledgeType.ACCEPT));
    }

    @Test
    void delayedRetryQueuesReleaseImmediately() {
        RecordListener<byte[], byte[]> listener = (r, ctx) -> { throw new IOException("transient"); };
        ListenerInvoker invoker = ListenerInvoker.forImplicit(listener, RecordDeserializer.bytes(), RecordDeserializer.bytes());
        WorkerProcessor p = new WorkerProcessor(
            invoker,
            new RetryEngine(RetryPolicy.exponential()
                .maxAttempts(3)
                .initialDelay(Duration.ofSeconds(5)) // > 1s → delayed
                .multiplier(1.0)
                .jitter(0.0)
                .retryOn(IOException.class)
                .build()),
            coordinator);

        InFlightRecord<byte[], byte[]> r = rec(4);
        long start = System.nanoTime();
        p.process(r, ctxFor(r));
        long elapsedMs = (System.nanoTime() - start) / 1_000_000;
        assertThat(elapsedMs).isLessThan(500);

        coordinator.commitPendingAcks(consumer);
        org.mockito.Mockito.verify(consumer).acknowledge(
            argThat(cr -> cr.topic().equals("t") && cr.partition() == 0 && cr.offset() == 4L),
            eq(AcknowledgeType.RELEASE));
    }

    @Test
    void exhaustedRetryQueuesRejectAndLogs() {
        AtomicInteger attempts = new AtomicInteger();
        RecordListener<byte[], byte[]> listener = (r, ctx) -> {
            attempts.incrementAndGet();
            throw new IOException("always");
        };
        ListenerInvoker invoker = ListenerInvoker.forImplicit(listener, RecordDeserializer.bytes(), RecordDeserializer.bytes());
        WorkerProcessor p = new WorkerProcessor(
            invoker,
            new RetryEngine(RetryPolicy.exponential()
                .maxAttempts(2)
                .initialDelay(Duration.ofMillis(10))
                .multiplier(1.0)
                .jitter(0.0)
                .retryOn(IOException.class)
                .build()),
            coordinator);

        InFlightRecord<byte[], byte[]> r = rec(5);
        p.process(r, ctxFor(r));

        // attempt 0 (initial) + attempt 1 (retry) + attempt 2 (final, exhausted) = 3 calls
        assertThat(attempts.get()).isEqualTo(3);
        coordinator.commitPendingAcks(consumer);
        org.mockito.Mockito.verify(consumer).acknowledge(
            argThat(cr -> cr.topic().equals("t") && cr.partition() == 0 && cr.offset() == 5L),
            eq(AcknowledgeType.REJECT));
    }

    @Test
    void exhaustedWithDltRouterRoutesAndAccepts() throws Exception {
        org.apache.kafka.clients.producer.MockProducer<byte[], byte[]> mockProducer =
            new org.apache.kafka.clients.producer.MockProducer<>(
                true, null,
                new org.apache.kafka.common.serialization.ByteArraySerializer(),
                new org.apache.kafka.common.serialization.ByteArraySerializer());
        java.util.Properties pp = new java.util.Properties();
        pp.put("bootstrap.servers", "ignored");
        io.plurima.kafka.dlt.DltConfig dltCfg = io.plurima.kafka.dlt.DltConfig.builder()
            .producerProperties(pp).build();
        DltRouter dlt = new DltRouter(mockProducer, dltCfg);

        java.util.concurrent.atomic.AtomicInteger attempts = new java.util.concurrent.atomic.AtomicInteger();
        io.plurima.kafka.RecordListener<byte[], byte[]> listener = (r, ctx) -> {
            attempts.incrementAndGet();
            throw new java.io.IOException("always");
        };
        ListenerInvoker invoker = ListenerInvoker.forImplicit(listener, RecordDeserializer.bytes(), RecordDeserializer.bytes());
        WorkerProcessor p = new WorkerProcessor(
            invoker,
            new RetryEngine(io.plurima.kafka.retry.RetryPolicy.exponential()
                .maxAttempts(1)
                .initialDelay(java.time.Duration.ofMillis(10))
                .multiplier(1.0)
                .jitter(0.0)
                .retryOn(java.io.IOException.class)
                .build()),
            coordinator, dlt);

        InFlightRecord<byte[], byte[]> r = rec(6);
        p.process(r, ctxFor(r));

        long start = System.nanoTime();
        while (mockProducer.history().isEmpty()
            && (System.nanoTime() - start) < java.util.concurrent.TimeUnit.SECONDS.toNanos(2)) {
            Thread.sleep(10);
        }
        assertThat(mockProducer.history()).hasSize(1);
        assertThat(mockProducer.history().get(0).topic()).isEqualTo("t.DLT");

        coordinator.commitPendingAcks(consumer);
        org.mockito.Mockito.verify(consumer).acknowledge(
            argThat(cr -> cr.topic().equals("t") && cr.partition() == 0 && cr.offset() == 6L),
            eq(org.apache.kafka.clients.consumer.AcknowledgeType.ACCEPT));

        dlt.close();
    }

    @Test
    void manualAckThenThrowSkipsRetryAndDlt() throws Exception {
        // The user's manual-ack listener decided ACCEPT before throwing. Plurima must NOT
        // run retry/DLT — that would either reprocess an accepted record (inline retry),
        // queue a wasteful RELEASE that the first-wins guard drops (delayed retry), or
        // produce a DLT record for an accepted message. The exception is still recorded
        // via metrics.recordsFailed, but the user-chosen ack stands.
        org.apache.kafka.clients.producer.MockProducer<byte[], byte[]> dltProducer =
            new org.apache.kafka.clients.producer.MockProducer<>(
                true, null,
                new org.apache.kafka.common.serialization.ByteArraySerializer(),
                new org.apache.kafka.common.serialization.ByteArraySerializer());
        java.util.Properties pp = new java.util.Properties();
        pp.put("bootstrap.servers", "ignored");
        io.plurima.kafka.dlt.DltConfig dltCfg = io.plurima.kafka.dlt.DltConfig.builder()
            .producerProperties(pp).build();
        DltRouter dlt = new DltRouter(dltProducer, dltCfg);

        AtomicInteger listenerInvocations = new AtomicInteger();
        io.plurima.kafka.ack.ManualAckListener<byte[], byte[]> manual = (rec, ack) -> {
            listenerInvocations.incrementAndGet();
            ack.acknowledge(AcknowledgeType.ACCEPT);   // user accepts FIRST
            throw new IOException("post-ack failure");  // then throws
        };
        ListenerInvoker invoker = ListenerInvoker.forManual(
            manual, RecordDeserializer.bytes(), RecordDeserializer.bytes());
        WorkerProcessor p = new WorkerProcessor(
            invoker,
            new RetryEngine(io.plurima.kafka.retry.RetryPolicy.exponential()
                .maxAttempts(5)
                .initialDelay(Duration.ofMillis(10))
                .multiplier(2.0)
                .jitter(0.0)
                .retryOn(IOException.class)
                .build()),
            coordinator, dlt);

        InFlightRecord<byte[], byte[]> r = rec(100);
        p.process(r, ctxFor(r));

        assertThat(listenerInvocations.get())
            .as("listener must run exactly once — no inline retry after the user already acked")
            .isEqualTo(1);
        assertThat(dltProducer.history())
            .as("no DLT record must be published — user accepted before the throw")
            .isEmpty();

        coordinator.commitPendingAcks(consumer);
        org.mockito.Mockito.verify(consumer).acknowledge(
            argThat(cr -> cr.topic().equals("t") && cr.partition() == 0 && cr.offset() == 100L),
            eq(AcknowledgeType.ACCEPT));

        dlt.close();
    }

    @Test
    void recordsHandlerLatencyIntoWindowOnSuccess() {
        HandlerLatencyWindow window = new HandlerLatencyWindow(1024);
        io.plurima.kafka.RecordListener<byte[], byte[]> listener = (r, ctx) -> {
            try { Thread.sleep(5); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        };
        ListenerInvoker invoker = ListenerInvoker.forImplicit(
            listener, RecordDeserializer.bytes(), RecordDeserializer.bytes());
        WorkerProcessor p = new WorkerProcessor(
            invoker, new RetryEngine(io.plurima.kafka.retry.RetryPolicy.noRetry()),
            coordinator, null, io.plurima.kafka.metrics.PlurimaMetrics.noOp(), window);

        InFlightRecord<byte[], byte[]> r = rec(1);
        p.process(r, ctxFor(r));

        assertThat(window.sampleCount()).isEqualTo(1);
        assertThat(window.percentileMillis(1.0)).isGreaterThanOrEqualTo(5.0);
    }

    @Test
    void exhaustedWithDltProduceFailureReleases() throws Exception {
        org.apache.kafka.clients.producer.MockProducer<byte[], byte[]> failProducer =
            new org.apache.kafka.clients.producer.MockProducer<>(
                false, null,
                new org.apache.kafka.common.serialization.ByteArraySerializer(),
                new org.apache.kafka.common.serialization.ByteArraySerializer());
        java.util.Properties pp = new java.util.Properties();
        pp.put("bootstrap.servers", "ignored");
        io.plurima.kafka.dlt.DltConfig dltCfg = io.plurima.kafka.dlt.DltConfig.builder()
            .producerProperties(pp).build();
        DltRouter dlt = new DltRouter(failProducer, dltCfg);

        io.plurima.kafka.RecordListener<byte[], byte[]> listener = (r, ctx) -> {
            throw new java.io.IOException("always");
        };
        ListenerInvoker invoker2 = ListenerInvoker.forImplicit(listener, RecordDeserializer.bytes(), RecordDeserializer.bytes());
        WorkerProcessor p = new WorkerProcessor(
            invoker2,
            new RetryEngine(io.plurima.kafka.retry.RetryPolicy.exponential()
                .maxAttempts(1)
                .initialDelay(java.time.Duration.ofMillis(10))
                .multiplier(1.0)
                .jitter(0.0)
                .retryOn(java.io.IOException.class)
                .build()),
            coordinator, dlt);

        InFlightRecord<byte[], byte[]> r = rec(7);

        // process() now synchronously blocks waiting for the DLT future — run it in a thread
        Thread worker = new Thread(() -> p.process(r, ctxFor(r)));
        worker.start();

        // Wait for the send to be enqueued in MockProducer, then inject the failure
        long start = System.nanoTime();
        while (failProducer.history().isEmpty()
            && (System.nanoTime() - start) < java.util.concurrent.TimeUnit.SECONDS.toNanos(2)) {
            Thread.sleep(10);
        }
        failProducer.errorNext(new RuntimeException("dlt broker down"));

        worker.join(5_000);
        assertThat(worker.isAlive()).isFalse();

        coordinator.commitPendingAcks(consumer);
        org.mockito.Mockito.verify(consumer).acknowledge(
            argThat(cr -> cr.topic().equals("t") && cr.partition() == 0 && cr.offset() == 7L),
            eq(org.apache.kafka.clients.consumer.AcknowledgeType.RELEASE));

        dlt.close();
    }
}
