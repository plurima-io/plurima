package io.plurima.kafka.spring;

import io.plurima.kafka.OrderingMode;
import io.plurima.kafka.spring.internal.PlurimaListenerContainer;
import io.plurima.kafka.spring.internal.PlurimaListenerEndpoint;
import io.plurima.kafka.spring.internal.PlurimaListenerPostProcessor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

class PlurimaAutoConfigurationTest {

    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
        .withConfiguration(AutoConfigurations.of(PlurimaAutoConfiguration.class));

    @Test
    void registersPostProcessorAndContainer() {
        contextRunner
            .withUserConfiguration(NoStartConfig.class, ListenerConfig.class)
            .withPropertyValues(
                "plurima.bootstrap-servers=localhost:9092",
                "plurima.client-id=test-client")
            .run(ctx -> {
                assertThat(ctx).hasSingleBean(PlurimaListenerPostProcessor.class);
                assertThat(ctx).hasSingleBean(PlurimaListenerContainer.class);
                assertThat(ctx).hasSingleBean(PlurimaProperties.class);

                PlurimaListenerPostProcessor pp = ctx.getBean(PlurimaListenerPostProcessor.class);
                assertThat(pp.endpoints()).hasSize(1);
                PlurimaListenerEndpoint endpoint = pp.endpoints().get(0);
                assertThat(endpoint.topic()).isEqualTo("orders");
                assertThat(endpoint.groupId()).isEqualTo("order-group");
                assertThat(endpoint.ordering()).isEqualTo(OrderingMode.KEY);
                assertThat(endpoint.concurrency()).isEqualTo(10);
            });
    }

    @Test
    void listenerMethodInvocationDispatchesToBean() {
        contextRunner
            .withUserConfiguration(NoStartConfig.class, ListenerConfig.class)
            .withPropertyValues("plurima.bootstrap-servers=localhost:9092")
            .run(ctx -> {
                MyHandler handler = ctx.getBean(MyHandler.class);
                PlurimaListenerEndpoint endpoint = ctx.getBean(PlurimaListenerPostProcessor.class)
                    .endpoints().get(0);

                ConsumerRecord<byte[], byte[]> record =
                    new ConsumerRecord<>("orders", 0, 1L, "k".getBytes(), "v".getBytes());
                endpoint.listener().onRecord(record, null);

                assertThat(handler.invoked.get()).isTrue();
            });
    }

    @Test
    void retryAndDltBeanNamesAreCarriedThroughTheEndpoint() {
        contextRunner
            .withUserConfiguration(NoStartConfig.class, AnnotatedRetryDltConfig.class)
            .withPropertyValues("plurima.bootstrap-servers=localhost:9092")
            .run(ctx -> {
                PlurimaListenerEndpoint endpoint = ctx.getBean(PlurimaListenerPostProcessor.class)
                    .endpoints().get(0);
                assertThat(endpoint.retryPolicyBeanName())
                    .as("annotation retry bean name must reach the endpoint")
                    .isEqualTo("myRetry");
                assertThat(endpoint.dltConfigBeanName())
                    .as("annotation dlt bean name must reach the endpoint")
                    .isEqualTo("myDlt");

                // Verify the beans are resolvable from the context — the container
                // looks them up at start() time.
                assertThat(ctx.getBean("myRetry", io.plurima.kafka.retry.RetryPolicy.class))
                    .isNotNull();
                assertThat(ctx.getBean("myDlt", io.plurima.kafka.dlt.DltConfig.class))
                    .isNotNull();
            });
    }

    @Test
    void metricsBeanIsAutoDiscoveredByContainer() {
        contextRunner
            .withUserConfiguration(NoStartConfig.class, ListenerConfig.class, MetricsConfig.class)
            .withPropertyValues("plurima.bootstrap-servers=localhost:9092")
            .run(ctx -> {
                // The container resolves PlurimaMetrics from ObjectProvider — verify
                // a single bean is exposed for it to find.
                assertThat(ctx.getBeansOfType(io.plurima.kafka.metrics.PlurimaMetrics.class))
                    .as("metrics bean must be available for container auto-wiring")
                    .hasSize(1);
            });
    }

    @Test
    void propertiesBindFromApplicationConfig() {
        contextRunner
            .withUserConfiguration(NoStartConfig.class, ListenerConfig.class)
            .withPropertyValues(
                "plurima.bootstrap-servers=broker:9094",
                "plurima.client-id=my-app",
                "plurima.properties.client.dns.lookup=use_all_dns_ips")
            .run(ctx -> {
                PlurimaProperties props = ctx.getBean(PlurimaProperties.class);
                assertThat(props.getBootstrapServers()).isEqualTo("broker:9094");
                assertThat(props.getClientId()).isEqualTo("my-app");
                assertThat(props.getProperties())
                    .containsEntry("client.dns.lookup", "use_all_dns_ips");
            });
    }

    @Configuration
    static class ListenerConfig {
        @Bean
        MyHandler myHandler() {
            return new MyHandler();
        }
    }

    @Configuration
    static class AnnotatedRetryDltConfig {
        @Bean
        AnnotatedRetryDltHandler annotatedRetryDltHandler() {
            return new AnnotatedRetryDltHandler();
        }

        @Bean("myRetry")
        io.plurima.kafka.retry.RetryPolicy myRetry() {
            return io.plurima.kafka.retry.RetryPolicy.exponential()
                .maxAttempts(3)
                .initialDelay(java.time.Duration.ofMillis(50))
                .multiplier(2.0)
                .jitter(0.0)
                .retryOn(RuntimeException.class)
                .build();
        }

        @Bean("myDlt")
        io.plurima.kafka.dlt.DltConfig myDlt() {
            java.util.Properties props = new java.util.Properties();
            props.put("bootstrap.servers", "localhost:9092");
            return io.plurima.kafka.dlt.DltConfig.builder()
                .producerProperties(props)
                .build();
        }
    }

    @Component
    static class AnnotatedRetryDltHandler {
        @PlurimaListener(
            topics = "annotated",
            groupId = "annotated-group",
            retryPolicyBeanName = "myRetry",
            dltConfigBeanName = "myDlt")
        public void onRecord(ConsumerRecord<byte[], byte[]> record) {}
    }

    @Configuration
    static class MetricsConfig {
        @Bean
        io.plurima.kafka.metrics.PlurimaMetrics testMetrics() {
            return io.plurima.kafka.metrics.PlurimaMetrics.noOp();
        }
    }

    @Component
    static class MyHandler {
        final AtomicBoolean invoked = new AtomicBoolean();

        @PlurimaListener(
            topics = "orders",
            groupId = "order-group",
            ordering = OrderingMode.KEY,
            concurrency = 10)
        public void onOrder(ConsumerRecord<byte[], byte[]> record) {
            invoked.set(true);
        }
    }

    /** Replaces the real container with a stub that never actually starts a consumer. */
    @Configuration
    static class NoStartConfig {
        @Bean
        @org.springframework.context.annotation.Primary
        PlurimaListenerContainer noOpContainer(
            PlurimaListenerPostProcessor postProcessor,
            PlurimaProperties properties,
            org.springframework.beans.factory.BeanFactory beanFactory) {
            return new PlurimaListenerContainer(postProcessor, properties, beanFactory, null) {
                @Override
                public void start() {
                    // Don't actually start consumers — Kafka isn't available in this test.
                }
            };
        }
    }
}
