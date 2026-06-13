package io.plurima.kafka.internal;

import io.plurima.kafka.annotation.Internal;
import io.plurima.kafka.dlt.DltConfig;
import io.plurima.kafka.metrics.PlurimaMetrics;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

@Internal
public final class DltRouter implements AutoCloseable {

    private static final int STACK_TRACE_MAX_BYTES = 4096;

    private final Producer<byte[], byte[]> producer;
    private final DltConfig config;
    private final PlurimaMetrics metrics;

    /** Production constructor: builds a {@link KafkaProducer} from {@code config}. */
    public DltRouter(DltConfig config) {
        this(buildProducer(config), config, PlurimaMetrics.noOp());
    }

    /** Production constructor with metrics. */
    public DltRouter(DltConfig config, PlurimaMetrics metrics) {
        this(buildProducer(config), config, metrics);
    }

    /** Test seam: inject an in-memory or mock producer. Package-private. */
    DltRouter(Producer<byte[], byte[]> producer, DltConfig config) {
        this(producer, config, PlurimaMetrics.noOp());
    }

    /** Full constructor: inject producer and metrics. Package-private. */
    DltRouter(Producer<byte[], byte[]> producer, DltConfig config, PlurimaMetrics metrics) {
        this.producer = producer;
        this.config = config;
        this.metrics = metrics;
    }

    private static Producer<byte[], byte[]> buildProducer(DltConfig config) {
        Properties props = config.producerProperties();
        props.put("key.serializer", ByteArraySerializer.class.getName());
        props.put("value.serializer", ByteArraySerializer.class.getName());
        return new KafkaProducer<>(props);
    }

    /**
     * Outcome handle for a single {@link #route} call. Carries the producer's
     * pending future plus a CAS guard ({@link #metricEmitted}) so the route's
     * dltRouted / dltFailed metric is emitted EXACTLY once even when the
     * producer's async callback races with a caller-side timeout.
     *
     * <p>Whoever first {@code metricEmitted.compareAndSet(false, true)} succeeds
     * is responsible for emitting the metric. The other side checks the flag
     * and skips its emission. Without this guard a slow DLT broker could
     * produce a "first dltFailed (timeout) then dltRouted (callback eventually
     * landed)" sequence — two metrics fired for one record with conflicting
     * semantics.
     */
    @Internal
    public record DltRoute(CompletableFuture<Void> future, AtomicBoolean metricEmitted) {}

    public DltRoute route(InFlightRecord<?, ?> r, Throwable cause) {
        String dltTopic = config.namingStrategy().dltTopicFor(r.coord().topic());

        ProducerRecord<byte[], byte[]> dltRecord = new ProducerRecord<>(
            dltTopic,
            null,
            (byte[]) r.consumerRecord().key(),
            (byte[]) r.consumerRecord().value());

        for (Header h : r.consumerRecord().headers()) {
            dltRecord.headers().add(h.key(), h.value());
        }

        addHeader(dltRecord, "plurima-dlt-original-topic", r.coord().topic());
        addHeader(dltRecord, "plurima-dlt-original-partition", String.valueOf(r.coord().partition()));
        addHeader(dltRecord, "plurima-dlt-original-offset", String.valueOf(r.coord().offset()));
        addHeader(dltRecord, "plurima-dlt-failure-class", cause.getClass().getName());
        addHeader(dltRecord, "plurima-dlt-failure-message", String.valueOf(cause.getMessage()));
        // Use effective attempt = max(in-process attempt, brokerDeliveryCount - 1) so that
        // exhausted retries driven by broker redelivery report the true attempt count.
        // Without this, delayed retries (which RELEASE to the broker and create a fresh
        // InFlightRecord on the next poll) reset r.attempt() to 0, and the DLT header
        // misleadingly publishes plurima-dlt-attempt-count=0 for a record that has actually
        // been redelivered many times. Same calculation WorkerProcessor uses for retry
        // decisions; see WorkerProcessor effective-attempt comment.
        int dc = r.consumerRecord().deliveryCount().orElse((short) 1);
        int effectiveAttempt = Math.max(r.attempt(), dc - 1);
        addHeader(dltRecord, "plurima-dlt-attempt-count", String.valueOf(effectiveAttempt));
        addHeader(dltRecord, "plurima-dlt-routed-at", Instant.now().toString());
        if (config.includeStackTrace()) {
            addHeader(dltRecord, "plurima-dlt-stack-trace", truncatedStackTrace(cause));
        }

        CompletableFuture<Void> future = new CompletableFuture<>();
        AtomicBoolean metricEmitted = new AtomicBoolean(false);
        try {
            producer.send(dltRecord, (md, ex) -> {
                // First-completion-wins: if a caller-side timeout already emitted
                // dltFailed, skip our own emission. Either way the future completes.
                if (metricEmitted.compareAndSet(false, true)) {
                    if (ex == null) {
                        metrics.dltRouted(r.coord().topic(), dltTopic);
                    } else {
                        metrics.dltFailed(r.coord().topic(), ex.getClass().getSimpleName());
                    }
                }
                if (ex == null) future.complete(null); else future.completeExceptionally(ex);
            });
        } catch (Throwable t) {
            // producer.send can throw SYNCHRONOUSLY before the callback ever fires
            // (SerializationException, BufferExhaustedException with max.block.ms=0,
            // IllegalStateException after producer.close, KafkaException on broker
            // discovery failure when max.block.ms is tight, etc.). The synchronous
            // throw means no callback will ever fire, so this is unconditionally the
            // first completion — we CAS to be safe but it should always succeed here.
            if (metricEmitted.compareAndSet(false, true)) {
                metrics.dltFailed(r.coord().topic(), t.getClass().getSimpleName());
            }
            future.completeExceptionally(t);
        }
        return new DltRoute(future, metricEmitted);
    }

    private static void addHeader(ProducerRecord<byte[], byte[]> r, String key, String value) {
        r.headers().add(key, value.getBytes(StandardCharsets.UTF_8));
    }

    private static String truncatedStackTrace(Throwable t) {
        StringWriter sw = new StringWriter();
        t.printStackTrace(new PrintWriter(sw));
        String full = sw.toString();
        byte[] bytes = full.getBytes(StandardCharsets.UTF_8);
        if (bytes.length <= STACK_TRACE_MAX_BYTES) {
            return full;
        }
        // Truncate at a UTF-8 char boundary (continuation bytes have the high two bits == 10).
        int cutAt = STACK_TRACE_MAX_BYTES;
        while (cutAt > 0 && (bytes[cutAt] & 0xC0) == 0x80) {
            cutAt--;
        }
        return new String(bytes, 0, cutAt, StandardCharsets.UTF_8);
    }

    @Override
    public void close() {
        producer.close();
    }
}
