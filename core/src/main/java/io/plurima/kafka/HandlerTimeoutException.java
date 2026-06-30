package io.plurima.kafka;

import io.plurima.kafka.annotation.Stable;

/**
 * Thrown into the retry/DLT pipeline when a listener exceeds the configured
 * {@link PlurimaConsumerBuilder#handlerTimeout(java.time.Duration) handler timeout}. The
 * worker thread is interrupted at the timeout; the resulting failure is surfaced as this
 * exception (wrapping whatever the interrupted handler threw, if anything) so it can be
 * classified explicitly — e.g. {@code RetryPolicy.exponential().retryOn(HandlerTimeoutException.class)}
 * to retry timeouts, or left non-retriable to route them straight to the DLT.
 *
 * <p>For the timeout to take effect the handler must be interruptible (most blocking I/O is).
 * A handler that swallows interrupts and runs to completion is treated as a normal success.
 *
 * @since 0.2.0
 */
@Stable(since = "0.2.0")
public final class HandlerTimeoutException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public HandlerTimeoutException(String message, Throwable cause) {
        super(message, cause);
    }
}
