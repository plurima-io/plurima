package io.plurima.kafka.spring;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import io.plurima.kafka.OrderingMode;
import io.plurima.kafka.RecordListener;
import io.plurima.kafka.spring.internal.PlurimaListenerContainer;
import io.plurima.kafka.spring.internal.PlurimaListenerEndpoint;
import io.plurima.kafka.spring.internal.PlurimaListenerPostProcessor;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Direct test of the property-assembly precedence in {@link PlurimaListenerContainer}: a
 * user setting {@code plurima.properties.group.id=...} in application.yml must NOT
 * override the {@code @PlurimaListener(groupId = "...")} value. Listener identity wins.
 */
class PlurimaListenerContainerPropsTest {

    @Test
    void listenerIdentityWinsOverPluralPropertiesGroupId() throws Exception {
        // Stage an endpoint whose listener metadata says groupId=real-group; the user has
        // also set plurima.properties.group.id=hijacker-group. After buildKafkaProperties,
        // group.id MUST be real-group.
        PlurimaProperties properties = new PlurimaProperties();
        properties.setBootstrapServers("localhost:9092");
        properties.setClientId("test-client");
        Map<String, String> overrides = new HashMap<>();
        overrides.put("group.id", "hijacker-group");
        overrides.put("bootstrap.servers", "wrong-host:1");
        overrides.put("client.id", "wrong-client");
        overrides.put("client.dns.lookup", "use_all_dns_ips");  // legitimate user setting
        properties.setProperties(overrides);

        RecordListener<byte[], byte[]> listener = (r, ctx) -> {};
        PlurimaListenerEndpoint endpoint = new PlurimaListenerEndpoint(
            "topic1", "real-group", OrderingMode.UNORDERED, 10,
            io.plurima.kafka.ConsumerEngine.SHARE, listener, "bean", "method",
            /* retryPolicyBeanName */ "", /* dltConfigBeanName */ "");

        PlurimaListenerContainer container = new PlurimaListenerContainer(
            new PlurimaListenerPostProcessor(), properties,
            /* beanFactory */ null, /* metrics */ null);

        Method buildMethod = PlurimaListenerContainer.class
            .getDeclaredMethod("buildKafkaProperties", PlurimaListenerEndpoint.class);
        buildMethod.setAccessible(true);
        Properties result = (Properties) buildMethod.invoke(container, endpoint);

        assertThat(result.getProperty("group.id"))
            .as("listener identity must win over plurima.properties.group.id")
            .isEqualTo("real-group");
        assertThat(result.getProperty("bootstrap.servers"))
            .as("plurima.bootstrap-servers must win over plurima.properties.bootstrap.servers")
            .isEqualTo("localhost:9092");
        assertThat(result.getProperty("client.id"))
            .as("plurima.client-id must win over plurima.properties.client.id, and carry the "
                + "per-endpoint bean/method suffix so multiple listeners don't collide on one MBean")
            .isEqualTo("test-client-bean-method");
        assertThat(result.getProperty("client.dns.lookup"))
            .as("non-identity user properties are still applied")
            .isEqualTo("use_all_dns_ips");
    }

    @Test
    void groupIdOverrideLogsAWarningNamingBothTheStrayOverrideAndTheWinningValue() throws Exception {
        // Companion to listenerIdentityWinsOverPluralPropertiesGroupId() above: that test
        // asserts the FUNCTIONAL outcome (real-group wins); this asserts the WARN a user
        // actually sees is fired, at the right level, and names both values so the message
        // is actionable rather than just "something was overridden".
        Logger containerLogger = (Logger) LoggerFactory.getLogger(PlurimaListenerContainer.class);
        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.start();
        containerLogger.addAppender(appender);
        try {
            PlurimaProperties properties = new PlurimaProperties();
            properties.setBootstrapServers("localhost:9092");
            Map<String, String> overrides = new HashMap<>();
            overrides.put("group.id", "hijacker-group");
            properties.setProperties(overrides);

            RecordListener<byte[], byte[]> listener = (r, ctx) -> {};
            PlurimaListenerEndpoint endpoint = new PlurimaListenerEndpoint(
                "topic1", "real-group", OrderingMode.UNORDERED, 10,
                io.plurima.kafka.ConsumerEngine.SHARE, listener, "bean", "method", "", "");

            PlurimaListenerContainer container = new PlurimaListenerContainer(
                new PlurimaListenerPostProcessor(), properties,
                /* beanFactory */ null, /* metrics */ null);

            Method buildMethod = PlurimaListenerContainer.class
                .getDeclaredMethod("buildKafkaProperties", PlurimaListenerEndpoint.class);
            buildMethod.setAccessible(true);
            buildMethod.invoke(container, endpoint);

            assertThat(appender.list)
                .as("plurima.properties.group.id='hijacker-group' colliding with the "
                    + "listener's real groupId must log a WARN naming both values")
                .anySatisfy(event -> {
                    assertThat(event.getLevel()).isEqualTo(Level.WARN);
                    assertThat(event.getFormattedMessage())
                        .contains("hijacker-group")
                        .contains("real-group");
                });
        } finally {
            containerLogger.detachAppender(appender);
        }
    }

    @Test
    void noGroupIdOverrideWarningWhenPropertiesGroupIdMatchesTheListenerGroupId() throws Exception {
        // Sanity check on the WARN condition itself: it must fire only on an actual
        // mismatch, not merely because plurima.properties happens to contain group.id at
        // all (e.g. a user redundantly restating the same value they also set via the
        // annotation).
        Logger containerLogger = (Logger) LoggerFactory.getLogger(PlurimaListenerContainer.class);
        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.start();
        containerLogger.addAppender(appender);
        try {
            PlurimaProperties properties = new PlurimaProperties();
            properties.setBootstrapServers("localhost:9092");
            Map<String, String> overrides = new HashMap<>();
            overrides.put("group.id", "real-group");
            properties.setProperties(overrides);

            RecordListener<byte[], byte[]> listener = (r, ctx) -> {};
            PlurimaListenerEndpoint endpoint = new PlurimaListenerEndpoint(
                "topic1", "real-group", OrderingMode.UNORDERED, 10,
                io.plurima.kafka.ConsumerEngine.SHARE, listener, "bean", "method", "", "");

            PlurimaListenerContainer container = new PlurimaListenerContainer(
                new PlurimaListenerPostProcessor(), properties,
                /* beanFactory */ null, /* metrics */ null);

            Method buildMethod = PlurimaListenerContainer.class
                .getDeclaredMethod("buildKafkaProperties", PlurimaListenerEndpoint.class);
            buildMethod.setAccessible(true);
            buildMethod.invoke(container, endpoint);

            assertThat(appender.list)
                .as("no mismatch occurred — the WARN must not fire")
                .noneSatisfy(event -> assertThat(event.getLevel()).isEqualTo(Level.WARN));
        } finally {
            containerLogger.detachAppender(appender);
        }
    }

    @Test
    void perEndpointClientIdSuffixDisambiguatesDifferentListeners() throws Exception {
        // Two distinct @PlurimaListener methods sharing one plurima.client-id must NOT end up
        // with the same client.id — that would collide on the same Kafka client MBean /
        // Micrometer gauge tag set. Each endpoint's bean+method name must produce a
        // distinct suffix (spring-kafka style: "-<beanName>-<methodName>").
        PlurimaProperties properties = new PlurimaProperties();
        properties.setBootstrapServers("localhost:9092");
        properties.setClientId("shared-client");

        RecordListener<byte[], byte[]> listener = (r, ctx) -> {};
        PlurimaListenerEndpoint first = new PlurimaListenerEndpoint(
            "topic1", "group1", OrderingMode.UNORDERED, 1,
            io.plurima.kafka.ConsumerEngine.SHARE, listener, "orderHandler", "onOrder",
            "", "");
        PlurimaListenerEndpoint second = new PlurimaListenerEndpoint(
            "topic2", "group2", OrderingMode.UNORDERED, 1,
            io.plurima.kafka.ConsumerEngine.SHARE, listener, "shipmentHandler", "onShipment",
            "", "");

        PlurimaListenerContainer container = new PlurimaListenerContainer(
            new PlurimaListenerPostProcessor(), properties, /* beanFactory */ null, /* metrics */ null);

        Method buildMethod = PlurimaListenerContainer.class
            .getDeclaredMethod("buildKafkaProperties", PlurimaListenerEndpoint.class);
        buildMethod.setAccessible(true);

        String firstClientId = ((Properties) buildMethod.invoke(container, first)).getProperty("client.id");
        String secondClientId = ((Properties) buildMethod.invoke(container, second)).getProperty("client.id");

        assertThat(firstClientId).isEqualTo("shared-client-orderHandler-onOrder");
        assertThat(secondClientId).isEqualTo("shared-client-shipmentHandler-onShipment");
        assertThat(firstClientId).isNotEqualTo(secondClientId);
    }

    @Test
    void clientIdIsUnsetWhenNoGlobalClientIdIsConfigured() throws Exception {
        PlurimaProperties properties = new PlurimaProperties();
        properties.setBootstrapServers("localhost:9092");
        // clientId left null (the default) — no client.id should be suffixed onto nothing.

        RecordListener<byte[], byte[]> listener = (r, ctx) -> {};
        PlurimaListenerEndpoint endpoint = new PlurimaListenerEndpoint(
            "topic1", "group1", OrderingMode.UNORDERED, 1,
            io.plurima.kafka.ConsumerEngine.SHARE, listener, "bean", "method", "", "");

        PlurimaListenerContainer container = new PlurimaListenerContainer(
            new PlurimaListenerPostProcessor(), properties, /* beanFactory */ null, /* metrics */ null);

        Method buildMethod = PlurimaListenerContainer.class
            .getDeclaredMethod("buildKafkaProperties", PlurimaListenerEndpoint.class);
        buildMethod.setAccessible(true);
        Properties result = (Properties) buildMethod.invoke(container, endpoint);

        assertThat(result.containsKey("client.id")).isFalse();
    }
}
