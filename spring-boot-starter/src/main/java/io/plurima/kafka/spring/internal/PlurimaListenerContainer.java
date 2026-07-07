package io.plurima.kafka.spring.internal;

import io.plurima.kafka.PlurimaConsumer;
import io.plurima.kafka.PlurimaConsumerBuilder;
import io.plurima.kafka.annotation.Internal;
import io.plurima.kafka.dlt.DltConfig;
import io.plurima.kafka.metrics.PlurimaMetrics;
import io.plurima.kafka.retry.RetryPolicy;
import io.plurima.kafka.spring.PlurimaProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.context.SmartLifecycle;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Spring lifecycle bean that holds one {@link PlurimaConsumer} per
 * {@link PlurimaListenerEndpoint}. Starts on context refresh, stops on close.
 *
 * <p>The container resolves optional {@link RetryPolicy}, {@link DltConfig}, and
 * {@link PlurimaMetrics} beans from the Spring context and wires them into each
 * {@link PlurimaConsumerBuilder}. Retry and DLT are opt-in per listener via the
 * annotation's {@code retryPolicyBeanName} / {@code dltConfigBeanName} fields;
 * metrics are auto-discovered as a singleton bean if one exists in the context.
 */
@Internal
public class PlurimaListenerContainer implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(PlurimaListenerContainer.class);

    private final PlurimaListenerPostProcessor postProcessor;
    private final PlurimaProperties properties;
    private final BeanFactory beanFactory;
    private final PlurimaMetrics metrics;  // null when no PlurimaMetrics bean present
    private final List<PlurimaConsumer<byte[], byte[]>> consumers = new java.util.concurrent.CopyOnWriteArrayList<>();
    private final AtomicBoolean running = new AtomicBoolean();

    public PlurimaListenerContainer(PlurimaListenerPostProcessor postProcessor,
                                    PlurimaProperties properties,
                                    BeanFactory beanFactory,
                                    PlurimaMetrics metrics) {
        this.postProcessor = postProcessor;
        this.properties = properties;
        this.beanFactory = beanFactory;
        this.metrics = metrics;
    }

    @Override
    public void start() {
        if (!running.compareAndSet(false, true)) return;
        try {
            for (PlurimaListenerEndpoint endpoint : postProcessor.endpoints()) {
                Properties props = buildKafkaProperties(endpoint);
                PlurimaConsumerBuilder<byte[], byte[]> builder = PlurimaConsumer.builder()
                    .kafkaProperties(props)
                    .topic(endpoint.topic())
                    .engine(endpoint.engine())
                    .ordering(endpoint.ordering())
                    .concurrency(endpoint.concurrency())
                    .listener(endpoint.listener());

                if (!endpoint.retryPolicyBeanName().isEmpty()) {
                    builder.retry(beanFactory.getBean(endpoint.retryPolicyBeanName(), RetryPolicy.class));
                }
                if (!endpoint.dltConfigBeanName().isEmpty()) {
                    builder.deadLetter(beanFactory.getBean(endpoint.dltConfigBeanName(), DltConfig.class));
                }
                if (metrics != null) {
                    // Wrap the SHARED metrics bean per endpoint in a close-suppressing
                    // delegate: each consumer runtime calls metrics.close() in its own
                    // cleanup, and (with the Micrometer adapter) that would deregister
                    // EVERY endpoint's gauges the first time any one consumer closes or
                    // fails. The shared bean's lifecycle belongs to Spring / the meter
                    // registry — never to an individual consumer. See
                    // CloseSuppressingPlurimaMetrics for the full rationale. The
                    // container's own stop() likewise never calls the adapter's close().
                    builder.metrics(new CloseSuppressingPlurimaMetrics(metrics));
                }

                PlurimaConsumer<byte[], byte[]> consumer = builder.build();
                consumer.start();
                consumers.add(consumer);
                log.info("Started PlurimaConsumer for {}", endpoint);
            }
        } catch (RuntimeException e) {
            log.error("Partial Spring start failure; rolling back already-started consumers", e);
            for (PlurimaConsumer<byte[], byte[]> c : consumers) {
                try { c.close(); }
                catch (Exception closeEx) { log.warn("Failed to close consumer during rollback", closeEx); }
            }
            consumers.clear();
            running.set(false);
            throw e;
        }
    }

    private Properties buildKafkaProperties(PlurimaListenerEndpoint endpoint) {
        Properties props = new Properties();
        // Order matters: putAll(user properties) FIRST, then write the identity properties
        // so they cannot be overridden by stray plurima.properties.* entries.
        //
        // Previously the order was reversed — a user setting
        //   plurima:
        //     properties:
        //       group.id: surprise-group
        // would silently override the @PlurimaListener(groupId = "real-group") value, joining
        // the wrong share group at runtime. Same risk for bootstrap.servers and client.id.
        props.putAll(properties.getProperties());

        if (props.containsKey("group.id")
            && !endpoint.groupId().equals(props.getProperty("group.id"))) {
            log.warn("plurima.properties.group.id='{}' on '{}' will be overridden by "
                + "@PlurimaListener(groupId='{}') — listener identity always wins. "
                + "Remove the property override.",
                props.getProperty("group.id"), endpoint, endpoint.groupId());
        }
        props.put("bootstrap.servers", properties.getBootstrapServers());
        props.put("group.id", endpoint.groupId());
        if (properties.getClientId() != null) {
            // Suffix per endpoint (spring-kafka style) — one global plurima.client-id shared
            // across multiple @PlurimaListener methods would otherwise produce identical
            // client.id values, colliding on the same Kafka client MBean name and the same
            // Micrometer gauge tag set (registerInFlightGauge/registerBarrierTimeoutGauge are
            // tagged by client_id). beanName#methodName is already unique per registered
            // endpoint and human-readable in JMX, so it's used directly rather than a bare
            // running index.
            props.put("client.id",
                properties.getClientId() + "-" + endpoint.beanName() + "-" + endpoint.methodName());
        }
        return props;
    }

    @Override
    public void stop() {
        if (!running.compareAndSet(true, false)) return;
        for (PlurimaConsumer<byte[], byte[]> consumer : consumers) {
            try { consumer.close(); }
            catch (Exception e) { log.warn("Failed to close consumer", e); }
        }
        consumers.clear();
    }

    @Override
    public boolean isRunning() {
        return running.get();
    }

    /** Visible for tests. */
    public List<PlurimaConsumer<byte[], byte[]>> consumers() {
        return List.copyOf(consumers);
    }
}
