package io.plurima.kafka.spring;

import io.plurima.kafka.annotation.Stable;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.HashMap;
import java.util.Map;

/**
 * Binds {@code plurima.*} properties from {@code application.yml}/{@code application.properties}.
 *
 * <p>Example:
 * <pre>{@code
 * plurima:
 *   enabled: true
 *   bootstrap-servers: localhost:9092
 *   client-id: my-app
 *   properties:
 *     client.dns.lookup: use_all_dns_ips
 * }</pre>
 *
 * <p><b>Note on {@code auto.offset.reset}:</b>
 * <ul>
 *   <li>{@link io.plurima.kafka.ConsumerEngine#SHARE SHARE} engine — rejected by the
 *       builder. To set the initial offset, configure {@code share.auto.offset.reset}
 *       broker-side via
 *       {@code kafka-share-groups.sh --alter --config share.auto.offset.reset=earliest}.</li>
 *   <li>{@link io.plurima.kafka.ConsumerEngine#CLASSIC_BASIC CLASSIC_BASIC} engine —
 *       supported as a legitimate {@code KafkaConsumer} setting. Pass it through
 *       {@link #getProperties() properties}: {@code plurima.properties.auto.offset.reset:
 *       earliest}.</li>
 * </ul>
 * Several other configs differ between engines (share-only:
 * {@code share.acknowledgement.mode}, {@code share.acquire.mode}; classic-only:
 * {@code isolation.level}, {@code partition.assignment.strategy},
 * {@code session.timeout.ms}, etc.); the builder's per-engine deny-list rejects each
 * with an explanatory error message.
 */
@Stable(since = "0.1.0")
@ConfigurationProperties(prefix = "plurima")
public class PlurimaProperties {

    /**
     * Master switch for the whole starter, checked by
     * {@link io.plurima.kafka.spring.PlurimaAutoConfiguration}'s
     * {@code @ConditionalOnProperty}. Defaults to {@code true}; set
     * {@code plurima.enabled=false} to suppress autoconfiguration entirely (e.g. in a
     * test slice that doesn't want any Kafka consumer beans registered).
     */
    private boolean enabled = true;
    private String bootstrapServers = "localhost:9092";
    private String clientId;
    private Map<String, String> properties = new HashMap<>();

    public boolean isEnabled() { return enabled; }
    public void setEnabled(boolean enabled) { this.enabled = enabled; }

    public String getBootstrapServers() { return bootstrapServers; }
    public void setBootstrapServers(String bootstrapServers) { this.bootstrapServers = bootstrapServers; }

    public String getClientId() { return clientId; }
    public void setClientId(String clientId) { this.clientId = clientId; }

    public Map<String, String> getProperties() { return properties; }
    public void setProperties(Map<String, String> properties) { this.properties = properties; }
}
