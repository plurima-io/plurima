package io.plurima.kafka.dlt;

import io.plurima.kafka.PropertiesSupport;
import io.plurima.kafka.annotation.Stable;

import java.util.Objects;
import java.util.Properties;

@Stable(since = "0.1.0")
public final class DltConfig {

    private final Properties producerProperties;
    private final DltTopicNamingStrategy namingStrategy;
    private final boolean includeStackTrace;

    private DltConfig(Properties producerProperties,
                      DltTopicNamingStrategy namingStrategy,
                      boolean includeStackTrace) {
        this.producerProperties = producerProperties;
        this.namingStrategy = namingStrategy;
        this.includeStackTrace = includeStackTrace;
    }

    /** Returns a defensive copy of the producer properties. */
    public Properties producerProperties() {
        return PropertiesSupport.copy(producerProperties);
    }

    public DltTopicNamingStrategy namingStrategy() { return namingStrategy; }
    public boolean includeStackTrace() { return includeStackTrace; }

    public static Builder builder() { return new Builder(); }

    public static final class Builder {
        private Properties producerProperties;
        private DltTopicNamingStrategy namingStrategy = t -> t + ".DLT";
        private boolean includeStackTrace = false;

        Builder() {}

        public Builder producerProperties(Properties props) {
            this.producerProperties = PropertiesSupport.copy(
                Objects.requireNonNull(props, "producerProperties"));
            return this;
        }

        public Builder namingStrategy(DltTopicNamingStrategy strategy) {
            this.namingStrategy = Objects.requireNonNull(strategy, "namingStrategy");
            return this;
        }

        public Builder includeStackTrace(boolean enable) {
            this.includeStackTrace = enable;
            return this;
        }

        public DltConfig build() {
            Objects.requireNonNull(producerProperties, "producerProperties");
            return new DltConfig(producerProperties, namingStrategy, includeStackTrace);
        }
    }
}
