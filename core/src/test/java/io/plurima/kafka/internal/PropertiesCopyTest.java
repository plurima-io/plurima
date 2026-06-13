package io.plurima.kafka.internal;

import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies {@link PropertiesCopy#copy} preserves values reachable through the
 * defaults chain — the exact case that {@code new Properties().putAll(src)}
 * silently drops.
 */
class PropertiesCopyTest {

    @Test
    void nullSourceProducesEmpty() {
        Properties out = PropertiesCopy.copy(null);
        assertThat(out).isEmpty();
    }

    @Test
    void explicitEntriesAreCopied() {
        Properties src = new Properties();
        src.setProperty("bootstrap.servers", "broker:9092");
        src.setProperty("group.id", "g");

        Properties copy = PropertiesCopy.copy(src);

        assertThat(copy).hasSize(2);
        assertThat(copy.getProperty("bootstrap.servers")).isEqualTo("broker:9092");
        assertThat(copy.getProperty("group.id")).isEqualTo("g");
    }

    @Test
    void defaultsBackedEntriesArePreserved() {
        // This is the bug fix: putAll() only copies the explicit hashtable entries,
        // it does NOT walk the Properties(defaults) chain. So a config built like
        //   Properties defaults = new Properties();
        //   defaults.setProperty("bootstrap.servers", "broker:9092");
        //   Properties live = new Properties(defaults);
        // and passed in here would arrive at the runtime missing bootstrap.servers
        // — a silent misconfiguration. stringPropertyNames() walks the defaults
        // chain so PropertiesCopy.copy preserves every reachable key.
        Properties defaults = new Properties();
        defaults.setProperty("bootstrap.servers", "broker:9092");
        defaults.setProperty("group.id", "g");
        Properties live = new Properties(defaults);
        live.setProperty("client.id", "c");          // shadowed in `live`, not in defaults

        Properties copy = PropertiesCopy.copy(live);

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

    @Test
    void explicitOverrideShadowsDefault() {
        Properties defaults = new Properties();
        defaults.setProperty("k", "default-value");
        Properties live = new Properties(defaults);
        live.setProperty("k", "explicit-value");

        Properties copy = PropertiesCopy.copy(live);

        assertThat(copy.getProperty("k"))
            .as("explicit value must win over a same-key default — matches Properties.getProperty semantics")
            .isEqualTo("explicit-value");
    }

    @Test
    void copyIsDetachedFromSource() {
        Properties src = new Properties();
        src.setProperty("k", "v1");
        Properties copy = PropertiesCopy.copy(src);

        // Mutate source — copy must not see the change.
        src.setProperty("k", "v2");
        src.setProperty("new", "added");

        assertThat(copy.getProperty("k")).isEqualTo("v1");
        assertThat(copy.getProperty("new")).isNull();
    }
}
