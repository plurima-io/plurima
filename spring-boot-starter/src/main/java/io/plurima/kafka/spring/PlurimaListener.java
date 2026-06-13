package io.plurima.kafka.spring;

import io.plurima.kafka.ConsumerEngine;
import io.plurima.kafka.OrderingMode;
import io.plurima.kafka.annotation.Stable;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a method on a Spring bean as a Plurima record listener. The method must accept a
 * single {@code org.apache.kafka.clients.consumer.ConsumerRecord<byte[], byte[]>}. The
 * starter constructs a {@code PlurimaConsumer} per annotated method when the Spring
 * context refreshes, and shuts them down on close.
 *
 * <p>The annotation exposes the auto-ack listener path plus opt-in retry, DLT, and
 * metrics wiring. The following remain <b>programmatic-only</b> (build a
 * {@code PlurimaConsumer} directly and register it as a bean):
 * <ul>
 *   <li>Manual ack ({@link io.plurima.kafka.ack.ManualAckListener}) — per-record
 *       RELEASE / REJECT semantics, SHARE engine only.</li>
 *   <li>Typed key/value deserializers — annotation methods can't carry generic
 *       type information through reflection.</li>
 * </ul>
 */
@Stable(since = "0.1.0")
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface PlurimaListener {

    /** Topics to subscribe to. Currently exactly one topic is supported. */
    String[] topics();

    /** Share group id (SHARE engine) or consumer group id (CLASSIC_BASIC engine). Required. */
    String groupId();

    /** Ordering mode. Default UNORDERED. */
    OrderingMode ordering() default OrderingMode.UNORDERED;

    /**
     * Max in-flight records. Default 50.
     *
     * <p>On {@link ConsumerEngine#SHARE SHARE}: bounds in-flight via a semaphore;
     * also caps {@code max.poll.records} when the user hasn't set it.
     *
     * <p>On {@link ConsumerEngine#CLASSIC_BASIC CLASSIC_BASIC}: drives the pause/resume
     * backpressure threshold (pause all assigned partitions when in-flight ≥ concurrency,
     * resume at ≤ concurrency / 2) and is also the default seed for {@code shardCount}
     * under KEY mode ({@code shardCount = concurrency × 4}).
     */
    int concurrency() default 50;

    /**
     * Underlying consumer engine. Default {@link ConsumerEngine#SHARE}; use
     * {@link ConsumerEngine#CLASSIC_BASIC} when you need cross-cluster STRICT ordering
     * for KEY/PARTITION modes. See {@link ConsumerEngine}'s Javadoc and the user guide
     * § Engines.
     *
     * @since 0.1.0
     */
    ConsumerEngine engine() default ConsumerEngine.SHARE;

    /**
     * Optional name of a {@link io.plurima.kafka.retry.RetryPolicy RetryPolicy} bean to
     * apply to this listener. Empty default means {@code RetryPolicy.noRetry()} —
     * every listener exception is treated as <b>non-retriable</b> and immediately
     * routes to {@code REJECT} (logged WARN; record committed past).
     *
     * <p>DLT routing requires a {@code RetryPolicy} that classifies the failing
     * exception as retriable AND a {@code maxAttempts} count that the record can
     * exhaust — only the {@code Exhausted} retry decision reaches the DLT path.
     * Without a configured retry policy, the {@link #dltConfigBeanName} bean is
     * never consulted because non-retriable rejections short-circuit before DLT.
     *
     * @since 0.1.0
     */
    String retryPolicyBeanName() default "";

    /**
     * Optional name of a {@link io.plurima.kafka.dlt.DltConfig DltConfig} bean to wire
     * for retry-exhaustion routing. Only reached when the configured
     * {@link #retryPolicyBeanName retry policy} marks an exception as retriable AND
     * the record exhausts {@code maxAttempts}. Empty default means no DLT —
     * exhausted records REJECT with an ERROR log (see UserGuide § Dead-letter topic).
     *
     * <p>If you set {@code dltConfigBeanName} without also setting
     * {@code retryPolicyBeanName}, the DLT will never receive any records — the
     * default no-retry policy classifies every exception as non-retriable, so the
     * retry pipeline produces {@code Reject} (not {@code Exhausted}) and the DLT
     * router is bypassed entirely.
     *
     * @since 0.1.0
     */
    String dltConfigBeanName() default "";
}
