package io.plurima.kafka.dlt;

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
        return defensiveCopy(producerProperties);
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
            this.producerProperties = defensiveCopy(
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

    /**
     * Mirrors {@code internal.PropertiesCopy.copy} — duplicated here because the
     * arch rule (ADR-012) forbids public-API types from depending on the
     * {@code ..internal..} package. Preserves entries from the {@code defaults}
     * chain that {@code Hashtable.putAll} silently drops.
     */
    private static Properties defensiveCopy(Properties src) {
        Properties out = new Properties();
        out.putAll(src);  // explicit entries (incl. non-String values)
        for (String key : src.stringPropertyNames()) {
            if (!out.containsKey(key)) {
                String value = src.getProperty(key);
                if (value != null) out.setProperty(key, value);
            }
        }
        return out;
    }
}
