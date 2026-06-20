package io.plurima.kafka.benchmarks;

import io.plurima.kafka.internal.AckCoordinator;
import io.plurima.kafka.internal.InFlightRecord;
import io.plurima.kafka.internal.InFlightRegistry;
import org.apache.kafka.clients.consumer.AcknowledgeType;
import org.apache.kafka.clients.consumer.AcknowledgementCommitCallback;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ShareConsumer;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Measures the throughput of queueing acks + draining via {@link AckCoordinator}.
 * Uses a no-op {@link ShareConsumer} stub to isolate the AckCoordinator's overhead from
 * any Kafka client work.
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
public class AckCoordinatorBenchmark {

    private AckCoordinator coordinator;
    private InFlightRegistry registry;
    private NoOpShareConsumer consumer;
    private final AtomicLong offset = new AtomicLong();

    @Setup(Level.Trial)
    public void setUp() {
        registry = new InFlightRegistry();
        coordinator = new AckCoordinator(registry);
        consumer = new NoOpShareConsumer();
    }

    @Benchmark
    public void queueAndCommit() {
        long o = offset.incrementAndGet();
        ConsumerRecord<byte[], byte[]> cr = new ConsumerRecord<>(
            "bench", 0, o, new byte[0], new byte[0]);
        InFlightRecord<byte[], byte[]> r = new InFlightRecord<>(cr);
        registry.register(r);
        coordinator.queueAck(r, AcknowledgeType.ACCEPT);
        coordinator.commitPendingAcks(consumer);
    }

    /** Minimal ShareConsumer that no-ops every call. */
    private static final class NoOpShareConsumer implements ShareConsumer<byte[], byte[]> {
        @Override public Set<String> subscription() { return Set.of(); }
        @Override public void subscribe(Collection<String> topics) {}
        @Override public void unsubscribe() {}
        @Override public ConsumerRecords<byte[], byte[]> poll(Duration timeout) { return ConsumerRecords.empty(); }
        @Override public void acknowledge(ConsumerRecord<byte[], byte[]> record) {}
        @Override public void acknowledge(ConsumerRecord<byte[], byte[]> record, AcknowledgeType type) {}
        @Override public void acknowledge(String topic, int partition, long offset, AcknowledgeType type) {}
        @Override public Map<TopicIdPartition, Optional<KafkaException>> commitSync() { return Map.of(); }
        @Override public Map<TopicIdPartition, Optional<KafkaException>> commitSync(Duration timeout) { return Map.of(); }
        @Override public void commitAsync() {}
        @Override public void setAcknowledgementCommitCallback(AcknowledgementCommitCallback callback) {}
        @Override public Optional<Integer> acquisitionLockTimeoutMs() { return Optional.empty(); }
        @Override public Uuid clientInstanceId(Duration timeout) { throw new UnsupportedOperationException(); }
        @Override public Map<MetricName, ? extends Metric> metrics() { return Map.of(); }
        @Override public void registerMetricForSubscription(KafkaMetric metric) {}
        @Override public void unregisterMetricFromSubscription(KafkaMetric metric) {}
        @Override public void wakeup() {}
        @Override public void close() {}
        @Override public void close(Duration timeout) {}
    }
}
