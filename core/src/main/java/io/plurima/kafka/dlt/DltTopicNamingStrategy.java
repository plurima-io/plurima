package io.plurima.kafka.dlt;

import io.plurima.kafka.annotation.Stable;

@Stable(since = "0.1.0")
@FunctionalInterface
public interface DltTopicNamingStrategy {
    String dltTopicFor(String originalTopic);
}
