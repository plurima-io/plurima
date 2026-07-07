package io.plurima.kafka.retry;

import io.plurima.kafka.annotation.Stable;

/**
 * Decides whether a throwable a handler threw is eligible for retry at all. Set via
 * {@link RetryPolicy.ExponentialBuilder#classifier}; if omitted, {@link RetryPolicy#exponential()}
 * derives one from {@link RetryPolicy.ExponentialBuilder#retryOn}'s exception classes
 * ({@code isInstance} matching, so subclasses of a listed class are retriable too), or — if
 * neither is called — a classifier that never retries.
 *
 * <p>A non-retriable classification skips backoff entirely: the record is rejected (or routed
 * to the dead-letter topic, if configured) on the very first failure, regardless of
 * {@link RetryPolicy#maxAttempts()}. A retriable classification still exhausts
 * {@link RetryPolicy#maxAttempts()} before falling back to rejection/DLT.
 */
@Stable(since = "0.1.0")
@FunctionalInterface
public interface ExceptionClassifier {

    /**
     * @param t the throwable a handler invocation threw
     * @return {@code true} if this failure should be retried (subject to
     *     {@link RetryPolicy#maxAttempts()}); {@code false} to reject/DLT immediately
     */
    boolean isRetriable(Throwable t);
}
