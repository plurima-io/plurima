package io.plurima.kafka.dlt;

import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DltConfigTest {

    @Test
    void defaultNamingStrategyAppendsDot_DLT() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        DltConfig cfg = DltConfig.builder()
            .producerProperties(props)
            .build();

        assertThat(cfg.namingStrategy().dltTopicFor("orders")).isEqualTo("orders.DLT");
        assertThat(cfg.namingStrategy().dltTopicFor("payments.events")).isEqualTo("payments.events.DLT");
    }

    @Test
    void customNamingStrategyOverridesDefault() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        DltConfig cfg = DltConfig.builder()
            .producerProperties(props)
            .namingStrategy(t -> "dlt." + t)
            .build();

        assertThat(cfg.namingStrategy().dltTopicFor("orders")).isEqualTo("dlt.orders");
    }

    @Test
    void includeStackTraceDefaultsToFalse() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        DltConfig cfg = DltConfig.builder()
            .producerProperties(props)
            .build();

        assertThat(cfg.includeStackTrace()).isFalse();
    }

    @Test
    void includeStackTraceCanBeEnabled() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        DltConfig cfg = DltConfig.builder()
            .producerProperties(props)
            .includeStackTrace(true)
            .build();

        assertThat(cfg.includeStackTrace()).isTrue();
    }

    @Test
    void producerPropertiesIsRequired() {
        assertThatThrownBy(() -> DltConfig.builder().build())
            .isInstanceOf(NullPointerException.class)
            .hasMessageContaining("producerProperties");
    }

    @Test
    void producerPropertiesAreDefensivelyCopied() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        DltConfig cfg = DltConfig.builder()
            .producerProperties(props)
            .build();

        props.put("bootstrap.servers", "changed:9092");
        assertThat(cfg.producerProperties().getProperty("bootstrap.servers"))
            .isEqualTo("localhost:9092");
    }

    @Test
    void defaultsBackedProducerPropertiesArePreserved() {
        // Mirrors PropertiesCopyTest.defaultsBackedEntriesArePreserved for the public
        // PropertiesSupport.copy path: DltConfig.Builder.producerProperties(...) and
        // DltConfig.producerProperties() both route through PropertiesSupport.copy, which
        // must walk the Properties(defaults) chain rather than a bare putAll (that would
        // silently drop bootstrap.servers/group.id here).
        Properties defaults = new Properties();
        defaults.setProperty("bootstrap.servers", "broker:9092");
        defaults.setProperty("group.id", "g");
        Properties live = new Properties(defaults);
        live.setProperty("client.id", "c");          // shadowed in `live`, not in defaults

        DltConfig cfg = DltConfig.builder()
            .producerProperties(live)
            .build();

        Properties copy = cfg.producerProperties();
        assertThat(copy.getProperty("bootstrap.servers"))
            .as("defaults-backed value must be copied")
            .isEqualTo("broker:9092");
        assertThat(copy.getProperty("group.id"))
            .as("defaults-backed value must be copied")
            .isEqualTo("g");
        assertThat(copy.getProperty("client.id"))
            .as("explicit value must be copied")
            .isEqualTo("c");
    }
}
