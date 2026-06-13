package io.plurima.kafka.internal;

import org.apache.kafka.clients.consumer.AcknowledgeType;
import org.apache.kafka.clients.consumer.AcknowledgementCommitCallback;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ShareConsumer;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.metrics.KafkaMetric;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Minimal ShareConsumer fake for unit tests. Only implements methods PollLoop touches.
 * All other declared methods throw UnsupportedOperationException so a missed assumption surfaces.
 */
final class FakeShareConsumer implements ShareConsumer<byte[], byte[]> {

    private final LinkedBlockingDeque<ConsumerRecord<byte[], byte[]>> pending = new LinkedBlockingDeque<>();
    private final List<AckCall> acks = new ArrayList<>();
    private int commitAsyncCalls;
    private final AtomicBoolean closed = new AtomicBoolean();
    private final AtomicBoolean wakeup = new AtomicBoolean();
    private Set<String> subscribed = Set.of();
    private volatile RuntimeException nextPollThrow;

    record AckCall(String topic, int partition, long offset, AcknowledgeType type) {}

    void enqueue(ConsumerRecord<byte[], byte[]> r) {
        pending.add(r);
    }

    void throwOnNextPoll(RuntimeException ex) { this.nextPollThrow = ex; }

    synchronized List<AckCall> acks() { return List.copyOf(acks); }
    synchronized int commitAsyncCalls() { return commitAsyncCalls; }
    boolean isClosed() { return closed.get(); }

    @Override
    public Set<String> subscription() { return subscribed; }

    @Override
    public void subscribe(Collection<String> topics) {
        this.subscribed = Set.copyOf(topics);
    }

    @Override
    public void unsubscribe() { this.subscribed = Set.of(); }

    @Override
    public ConsumerRecords<byte[], byte[]> poll(Duration timeout) {
        if (wakeup.getAndSet(false)) throw new WakeupException();
        RuntimeException throwOnce = nextPollThrow;
        if (throwOnce != null) {
            nextPollThrow = null;
            throw throwOnce;
        }
        Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> batch = new HashMap<>();
        long deadlineNanos = System.nanoTime() + timeout.toNanos();
        ConsumerRecord<byte[], byte[]> first = pending.poll();
        if (first == null) {
            try {
                long remainingMs = Math.max(0, (deadlineNanos - System.nanoTime()) / 1_000_000);
                first = pending.poll(remainingMs, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        if (first != null) {
            batch.computeIfAbsent(new TopicPartition(first.topic(), first.partition()),
                tp -> new ArrayList<>()).add(first);
            ConsumerRecord<byte[], byte[]> r;
            while ((r = pending.poll()) != null) {
                batch.computeIfAbsent(new TopicPartition(r.topic(), r.partition()),
                    tp -> new ArrayList<>()).add(r);
            }
        }
        if (batch.isEmpty()) {
            return ConsumerRecords.empty();
        }
        return new ConsumerRecords<>(batch, Map.of());
    }

    @Override
    public synchronized void acknowledge(ConsumerRecord<byte[], byte[]> record) {
        acks.add(new AckCall(record.topic(), record.partition(), record.offset(), AcknowledgeType.ACCEPT));
    }

    @Override
    public synchronized void acknowledge(ConsumerRecord<byte[], byte[]> record, AcknowledgeType type) {
        acks.add(new AckCall(record.topic(), record.partition(), record.offset(), type));
    }

    @Override
    public synchronized void acknowledge(String topic, int partition, long offset, AcknowledgeType type) {
        acks.add(new AckCall(topic, partition, offset, type));
    }

    @Override
    public Map<TopicIdPartition, Optional<KafkaException>> commitSync() {
        return Map.of();
    }

    @Override
    public Map<TopicIdPartition, Optional<KafkaException>> commitSync(Duration timeout) {
        return Map.of();
    }

    @Override
    public synchronized void commitAsync() {
        commitAsyncCalls++;
    }

    @Override
    public void setAcknowledgementCommitCallback(AcknowledgementCommitCallback callback) {
        // accepted; not invoked by the fake
    }

    @Override
    public Uuid clientInstanceId(Duration timeout) {
        throw new UnsupportedOperationException();
    }

    private volatile Optional<Integer> acquisitionLockTimeoutMs = Optional.empty();

    /** Test hook — lets a test set the broker-reported acquisition lock duration. */
    public void setAcquisitionLockTimeoutMs(int ms) {
        this.acquisitionLockTimeoutMs = Optional.of(ms);
    }

    @Override
    public Optional<Integer> acquisitionLockTimeoutMs() {
        return acquisitionLockTimeoutMs;
    }

    @Override
    public Map<MetricName, ? extends Metric> metrics() { return Map.of(); }

    @Override
    public void registerMetricForSubscription(KafkaMetric metric) {
        // no-op
    }

    @Override
    public void unregisterMetricFromSubscription(KafkaMetric metric) {
        // no-op
    }

    @Override
    public void close() { closed.set(true); }

    @Override
    public void close(Duration timeout) {
        lastCloseTimeout = timeout;
        closed.set(true);
    }

    /** Last {@code Duration} passed to {@link #close(Duration)}, or {@code null} if only the no-arg form was used. */
    Duration lastCloseTimeout() { return lastCloseTimeout; }

    private volatile Duration lastCloseTimeout;

    @Override
    public void wakeup() { wakeup.set(true); }
}
