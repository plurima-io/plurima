package io.plurima.kafka.spring;

import io.plurima.kafka.OrderingMode;
import io.plurima.kafka.RecordListener;
import io.plurima.kafka.spring.internal.PlurimaListenerContainer;
import io.plurima.kafka.spring.internal.PlurimaListenerEndpoint;
import io.plurima.kafka.spring.internal.PlurimaListenerPostProcessor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;

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
            .as("plurima.client-id must win over plurima.properties.client.id")
            .isEqualTo("test-client");
        assertThat(result.getProperty("client.dns.lookup"))
            .as("non-identity user properties are still applied")
            .isEqualTo("use_all_dns_ips");
    }

    /** Suppresses an unused-import warning for ConsumerRecord (referenced via test
     *  classpath in this package but not used here). */
    @SuppressWarnings("unused")
    private ConsumerRecord<byte[], byte[]> _unused;
}
