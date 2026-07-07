/**
 * Plurima's public API: the {@link io.plurima.kafka.PlurimaConsumerBuilder} entry point, the
 * {@link io.plurima.kafka.PlurimaConsumer} runtime handle it builds, and the
 * Kafka-decoupled record/handler types ({@link io.plurima.kafka.Message},
 * {@link io.plurima.kafka.RecordListener}, {@link io.plurima.kafka.MessageListener}) an
 * application wires together to consume a topic. See the User Guide for a walkthrough and
 * {@code io.plurima.kafka.ack}, {@code io.plurima.kafka.retry}, {@code io.plurima.kafka.dlt},
 * and {@code io.plurima.kafka.metrics} for the companion configuration types.
 */
@NullMarked
package io.plurima.kafka;

import org.jspecify.annotations.NullMarked;
