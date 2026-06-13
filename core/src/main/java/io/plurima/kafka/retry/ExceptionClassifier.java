package io.plurima.kafka.retry;

import io.plurima.kafka.annotation.Stable;

@Stable(since = "0.1.0")
@FunctionalInterface
public interface ExceptionClassifier {
    boolean isRetriable(Throwable t);
}
