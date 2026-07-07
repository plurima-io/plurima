package io.plurima.kafka.spring;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import io.plurima.kafka.OrderingMode;
import io.plurima.kafka.spring.internal.PlurimaListenerContainer;
import io.plurima.kafka.spring.internal.PlurimaListenerEndpoint;
import io.plurima.kafka.spring.internal.PlurimaListenerPostProcessor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.BeanFactoryPostProcessor;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.autoconfigure.context.PropertyPlaceholderAutoConfiguration;
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

    @Test
    void placeholdersInTopicsAndGroupIdAreResolvedFromEnvironment() {
        // ${...} in @PlurimaListener(topics=..., groupId=...) must be resolved against the
        // Spring Environment before validation runs — previously the literal placeholder
        // text ("${app.topic}") would have been used verbatim as the topic name.
        contextRunner
            .withConfiguration(AutoConfigurations.of(PropertyPlaceholderAutoConfiguration.class))
            .withUserConfiguration(NoStartConfig.class, PlaceholderConfig.class)
            .withPropertyValues(
                "plurima.bootstrap-servers=localhost:9092",
                "app.topic=orders",
                "app.group=order-group")
            .run(ctx -> {
                PlurimaListenerEndpoint endpoint = ctx.getBean(PlurimaListenerPostProcessor.class)
                    .endpoints().get(0);
                assertThat(endpoint.topic()).isEqualTo("orders");
                assertThat(endpoint.groupId()).isEqualTo("order-group");
            });
    }

    @Test
    void placeholderInConcurrencyIsResolvedAndParsedAsAPositiveInteger() {
        contextRunner
            .withConfiguration(AutoConfigurations.of(PropertyPlaceholderAutoConfiguration.class))
            .withUserConfiguration(NoStartConfig.class, PlaceholderConcurrencyConfig.class)
            .withPropertyValues(
                "plurima.bootstrap-servers=localhost:9092",
                "app.concurrency=7")
            .run(ctx -> {
                PlurimaListenerEndpoint endpoint = ctx.getBean(PlurimaListenerPostProcessor.class)
                    .endpoints().get(0);
                assertThat(endpoint.concurrency()).isEqualTo(7);
            });
    }

    @Test
    void nonStaticBeanPostProcessorFactoryMethodTriggersSpringsOwnNonStaticBppWarning() {
        // This is the actual static-BPP regression test: it fails (goes RED) if `static` is
        // removed from PlurimaAutoConfiguration.plurimaListenerPostProcessor(), and passes
        // (GREEN) when it's static, as verified empirically while writing this test.
        //
        // Spring's PostProcessorRegistrationDelegate.BeanPostProcessorChecker logs a WARN,
        // by name, whenever a BeanPostProcessor bean was produced by a NON-static @Bean
        // factory method on a @Configuration class — because that forces Spring to fully
        // instantiate the enclosing @Configuration bean (here, PlurimaAutoConfiguration)
        // ahead of registerBeanPostProcessors(), so that instance itself can't be run through
        // all BeanPostProcessors. Capturing that logger and asserting the WARN is absent is a
        // direct, mechanical check on the `static` modifier — unlike assertions about
        // scanning/binding behavior (see the test below), which hold regardless of it.
        Logger checkerLogger = (Logger) LoggerFactory.getLogger(
            "org.springframework.context.support.PostProcessorRegistrationDelegate"
                + "$BeanPostProcessorChecker");
        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.start();
        checkerLogger.addAppender(appender);
        try {
            contextRunner
                .withUserConfiguration(NoStartConfig.class, ListenerConfig.class)
                .withPropertyValues(
                    "plurima.bootstrap-servers=localhost:9092",
                    "plurima.client-id=test-client")
                .run(ctx -> assertThat(ctx).hasNotFailed());

            assertThat(appender.list)
                .as("plurimaListenerPostProcessor() must stay a static @Bean method — a "
                    + "non-static factory method makes Spring log a WARN naming the bean and "
                    + "'consider declaring it as static instead'")
                .noneSatisfy(event -> assertThat(event.getFormattedMessage())
                    .contains("plurimaListenerPostProcessor")
                    .contains("consider declaring it as static instead"));
        } finally {
            checkerLogger.detachAppender(appender);
        }
    }

    @Test
    void enabledFalseSuppressesTheWholeAutoConfiguration() {
        // Deliberately no NoStartConfig/ListenerConfig here: NoStartConfig's noOpContainer
        // @Bean method requires a PlurimaListenerPostProcessor parameter, which won't exist
        // when the whole autoconfiguration is suppressed — including it would turn this into
        // an (irrelevant) UnsatisfiedDependencyException test instead of exercising
        // @ConditionalOnProperty. Since no container bean is created at all, there's also no
        // SmartLifecycle to start a real (unavailable) Kafka connection from.
        contextRunner
            .withPropertyValues(
                "plurima.bootstrap-servers=localhost:9092",
                "plurima.enabled=false")
            .run(ctx -> {
                assertThat(ctx).doesNotHaveBean(PlurimaListenerContainer.class);
                assertThat(ctx).doesNotHaveBean(PlurimaListenerPostProcessor.class);
                assertThat(ctx).doesNotHaveBean(PlurimaProperties.class);
            });
    }

    @Test
    void enabledDefaultsToTrueWhenUnset() {
        contextRunner
            .withUserConfiguration(NoStartConfig.class, ListenerConfig.class)
            .withPropertyValues("plurima.bootstrap-servers=localhost:9092")
            .run(ctx -> assertThat(ctx).hasSingleBean(PlurimaListenerContainer.class));
    }

    @Test
    void micrometerAdapterIsAutoRegisteredWhenAMeterRegistryBeanIsPresent() {
        contextRunner
            .withUserConfiguration(NoStartConfig.class, ListenerConfig.class, MeterRegistryConfig.class)
            .withPropertyValues("plurima.bootstrap-servers=localhost:9092")
            .run(ctx -> {
                assertThat(ctx).hasSingleBean(io.plurima.kafka.metrics.MicrometerPlurimaMetrics.class);
                assertThat(ctx.getBean(io.plurima.kafka.metrics.PlurimaMetrics.class))
                    .isInstanceOf(io.plurima.kafka.metrics.MicrometerPlurimaMetrics.class);
            });
    }

    @Test
    void micrometerAdapterBacksOffWhenNoMeterRegistryBeanIsPresent() {
        contextRunner
            .withUserConfiguration(NoStartConfig.class, ListenerConfig.class)
            .withPropertyValues("plurima.bootstrap-servers=localhost:9092")
            .run(ctx -> assertThat(ctx)
                .doesNotHaveBean(io.plurima.kafka.metrics.MicrometerPlurimaMetrics.class));
    }

    @Test
    void micrometerAdapterBacksOffWhenUserDefinesTheirOwnPlurimaMetricsBean() {
        contextRunner
            .withUserConfiguration(
                NoStartConfig.class, ListenerConfig.class, MeterRegistryConfig.class, MetricsConfig.class)
            .withPropertyValues("plurima.bootstrap-servers=localhost:9092")
            .run(ctx -> {
                assertThat(ctx).doesNotHaveBean(io.plurima.kafka.metrics.MicrometerPlurimaMetrics.class);
                assertThat(ctx).hasSingleBean(io.plurima.kafka.metrics.PlurimaMetrics.class);
                assertThat(ctx.getBean(io.plurima.kafka.metrics.PlurimaMetrics.class))
                    .isSameAs(ctx.getBean("testMetrics"));
            });
    }

    @Test
    void userDefinedPostProcessorBacksOffTheAutoConfiguredOne() {
        // @ConditionalOnMissingBean on plurimaListenerPostProcessor() must back off when the
        // user already registered a bean of that type under a different name — otherwise
        // two BeanPostProcessors would scan every bean's @PlurimaListener methods, double
        // registering every endpoint.
        contextRunner
            .withUserConfiguration(UserPostProcessorConfig.class)
            .withPropertyValues("plurima.bootstrap-servers=localhost:9092")
            .run(ctx -> {
                assertThat(ctx).hasSingleBean(PlurimaListenerPostProcessor.class);
                assertThat(ctx.getBean(PlurimaListenerPostProcessor.class))
                    .as("the auto-configured post-processor must NOT have been registered "
                        + "alongside the user's own bean of the same type")
                    .isSameAs(ctx.getBean("customPostProcessor"));
            });
    }

    @Test
    void userDefinedContainerBacksOffTheAutoConfiguredOne() {
        // Same @ConditionalOnMissingBean backoff, but for plurimaListenerContainer() — a user
        // supplying their own PlurimaListenerContainer (e.g. a customized SmartLifecycle
        // wrapper) must not end up with a second, auto-configured container also trying to
        // start consumers for the same endpoints.
        contextRunner
            .withUserConfiguration(UserContainerConfig.class)
            .withPropertyValues("plurima.bootstrap-servers=localhost:9092")
            .run(ctx -> {
                assertThat(ctx).hasSingleBean(PlurimaListenerContainer.class);
                assertThat(ctx.getBean(PlurimaListenerContainer.class))
                    .as("the auto-configured container must NOT have been registered "
                        + "alongside the user's own PlurimaListenerContainer bean")
                    .isSameAs(ctx.getBean("customContainer"));
            });
    }

    @Configuration
    static class UserPostProcessorConfig {
        // static for the same reason plurimaListenerPostProcessor() itself is static — see
        // PlurimaAutoConfiguration's javadoc on that method.
        @Bean
        static PlurimaListenerPostProcessor customPostProcessor() {
            return new PlurimaListenerPostProcessor();
        }
    }

    @Configuration
    static class UserContainerConfig {
        @Bean
        PlurimaListenerContainer customContainer(
            PlurimaListenerPostProcessor postProcessor,
            PlurimaProperties properties,
            org.springframework.beans.factory.BeanFactory beanFactory) {
            return new PlurimaListenerContainer(postProcessor, properties, beanFactory, null) {
                @Override
                public void start() {
                    // Don't actually start consumers — Kafka isn't available in this test;
                    // the point here is bean registration/backoff, not lifecycle behavior.
                }
            };
        }
    }

    @Test
    void beanForcedEarlyByABeanFactoryPostProcessorDoesNotBreakNormalConfigurationPropertiesDependentScanning() {
        // NOTE: despite the name, this test does NOT guard the `static` modifier specifically
        // — see nonStaticBeanPostProcessorFactoryMethodTriggersSpringsOwnNonStaticBppWarning()
        // above for that. Removing `static` from plurimaListenerPostProcessor() does not turn
        // this test red: the boundary below (BeanFactoryPostProcessor-forced beans predate ALL
        // BeanPostProcessor registration) holds unconditionally in Spring's lifecycle.
        //
        // What this test DOES guard is BFPP/BPP ordering more generally: EarlyForcingConfig's
        // BeanFactoryPostProcessor forces EarlyHandler — which depends on the
        // @ConfigurationProperties-bound PlurimaProperties bean — into existence during
        // invokeBeanFactoryPostProcessors(), strictly before registerBeanPostProcessors()
        // creates PlurimaListenerPostProcessor. Because that forcing happens ahead of ALL
        // bean-definition BeanPostProcessors (ours included, and
        // ConfigurationPropertiesBindingPostProcessor too), EarlyHandler's PlurimaProperties
        // comes back unbound (getClientId() is null) and its own @PlurimaListener method is
        // never scanned — a fundamental Spring lifecycle boundary, asserted explicitly here
        // rather than assumed away. The test also asserts that a normal (non-forced)
        // @PlurimaListener bean like MyHandler is still scanned correctly despite that
        // unrelated early-forcing BFPP being present in the same context.
        contextRunner
            .withUserConfiguration(NoStartConfig.class, ListenerConfig.class, EarlyForcingConfig.class)
            .withPropertyValues(
                "plurima.bootstrap-servers=localhost:9092",
                "plurima.client-id=eager-client")
            .run(ctx -> {
                assertThat(ctx).hasNotFailed();
                assertThat(ctx).hasSingleBean(PlurimaListenerPostProcessor.class);

                assertThat(ctx.getBean(EarlyHandler.class).properties.getClientId())
                    .as("bean forced during invokeBeanFactoryPostProcessors() predates "
                        + "@ConfigurationProperties binding — documents the boundary, not a bug")
                    .isNull();

                assertThat(ctx.getBean(PlurimaListenerPostProcessor.class).endpoints())
                    .as("normal @PlurimaListener beans must still be scanned even though an "
                        + "unrelated bean was forced early elsewhere in the context")
                    .anySatisfy(endpoint -> assertThat(endpoint.topic()).isEqualTo("orders"))
                    .noneSatisfy(endpoint -> assertThat(endpoint.topic()).isEqualTo("early-topic"));
            });
    }

    @Configuration
    static class PlaceholderConfig {
        @Bean
        PlaceholderHandler placeholderHandler() {
            return new PlaceholderHandler();
        }
    }

    static class PlaceholderHandler {
        @PlurimaListener(topics = "${app.topic}", groupId = "${app.group}")
        public void onRecord(ConsumerRecord<byte[], byte[]> record) {}
    }

    @Configuration
    static class PlaceholderConcurrencyConfig {
        @Bean
        PlaceholderConcurrencyHandler placeholderConcurrencyHandler() {
            return new PlaceholderConcurrencyHandler();
        }
    }

    static class PlaceholderConcurrencyHandler {
        @PlurimaListener(topics = "orders", groupId = "g", concurrency = "${app.concurrency}")
        public void onRecord(ConsumerRecord<byte[], byte[]> record) {}
    }

    @Configuration
    static class EarlyForcingConfig {
        @Bean
        EarlyHandler earlyHandler(PlurimaProperties properties) {
            return new EarlyHandler(properties);
        }

        // Static, matching the production BPP fix: this factory method must not depend on
        // anything to avoid the same early-materialization hazard it's protecting against.
        @Bean
        static BeanFactoryPostProcessor forceEarlyHandlerBeanFactoryPostProcessor() {
            return beanFactory -> beanFactory.getBean(EarlyHandler.class);
        }
    }

    static class EarlyHandler {
        final PlurimaProperties properties;

        EarlyHandler(PlurimaProperties properties) {
            this.properties = properties;
        }

        @PlurimaListener(topics = "early-topic", groupId = "early-group")
        public void onRecord(ConsumerRecord<byte[], byte[]> record) {}
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

    @Configuration
    static class MeterRegistryConfig {
        @Bean
        io.micrometer.core.instrument.MeterRegistry meterRegistry() {
            return new io.micrometer.core.instrument.simple.SimpleMeterRegistry();
        }
    }

    @Component
    static class MyHandler {
        final AtomicBoolean invoked = new AtomicBoolean();

        @PlurimaListener(
            topics = "orders",
            groupId = "order-group",
            ordering = OrderingMode.KEY,
            concurrency = "10")
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
