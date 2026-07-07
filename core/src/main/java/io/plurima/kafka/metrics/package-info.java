/**
 * The {@link io.plurima.kafka.metrics.PlurimaMetrics} observability hook interface Plurima
 * calls on the hot path (records polled/processed/failed, retry attempts, DLT routing,
 * in-flight gauges). All methods default to no-op, so an unconfigured consumer pays zero
 * overhead; the {@code metrics} module ships a Micrometer-backed implementation, and users
 * may supply their own via {@link io.plurima.kafka.PlurimaConsumerBuilder#metrics}.
 */
@NullMarked
package io.plurima.kafka.metrics;

import org.jspecify.annotations.NullMarked;
