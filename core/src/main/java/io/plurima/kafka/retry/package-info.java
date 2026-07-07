/**
 * Retry configuration for handler failures: {@link io.plurima.kafka.retry.RetryPolicy}
 * (attempt cap, exponential backoff with jitter) and
 * {@link io.plurima.kafka.retry.ExceptionClassifier} (which failures are eligible for retry
 * at all). Set on a consumer via {@link io.plurima.kafka.PlurimaConsumerBuilder#retry}; once
 * attempts are exhausted (or a non-retriable exception is thrown), the record is rejected or,
 * if configured, routed to the dead-letter topic.
 */
@NullMarked
package io.plurima.kafka.retry;

import org.jspecify.annotations.NullMarked;
