package io.plurima.kafka.spring;

import io.micrometer.core.instrument.MeterRegistry;
import io.plurima.kafka.PlurimaConsumer;
import io.plurima.kafka.annotation.Stable;
import io.plurima.kafka.metrics.MicrometerPlurimaMetrics;
import io.plurima.kafka.metrics.PlurimaMetrics;
import io.plurima.kafka.spring.internal.PlurimaListenerContainer;
import io.plurima.kafka.spring.internal.PlurimaListenerPostProcessor;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Spring Boot autoconfiguration entry point. Registers the Plurima bean post-processor
 * and lifecycle container when {@link PlurimaConsumer} is on the classpath.
 *
 * <p>The {@code @Bean} methods below are intentionally <b>package-private</b>: their
 * concrete return / parameter types are {@code @Internal} (see
 * {@code io.plurima.kafka.spring.internal}) and we don't want those types to appear in
 * this {@code @Stable} class's public binary surface. Spring discovers {@code @Bean}
 * methods reflectively regardless of access modifier, so this keeps DI working while
 * preventing user code from compile-time-coupling to internals via the autoconfig.
 */
@Stable(since = "0.1.0")
@AutoConfiguration
@ConditionalOnClass(PlurimaConsumer.class)
@ConditionalOnProperty(prefix = "plurima", name = "enabled", havingValue = "true", matchIfMissing = true)
@EnableConfigurationProperties(PlurimaProperties.class)
public class PlurimaAutoConfiguration {

    // BPP @Bean methods must be static: the container has to instantiate
    // PlurimaListenerPostProcessor during registerBeanPostProcessors(), which runs before
    // the rest of this @Configuration class's beans (and its @Autowired state, if any) are
    // resolved. A non-static factory method would force Spring to fully create the
    // PlurimaAutoConfiguration instance up front to invoke it, which can pull other beans
    // (e.g. @ConfigurationProperties) into existence ahead of normal post-processing and
    // bypass the proxy/enhancement Spring applies to @Configuration classes. Keeping the
    // method static means only the no-arg PlurimaListenerPostProcessor gets built early;
    // everything else in this class still follows the normal bean lifecycle.
    @Bean
    @ConditionalOnMissingBean
    static PlurimaListenerPostProcessor plurimaListenerPostProcessor() {
        return new PlurimaListenerPostProcessor();
    }

    @Bean
    @ConditionalOnMissingBean
    PlurimaListenerContainer plurimaListenerContainer(
        PlurimaListenerPostProcessor postProcessor,
        PlurimaProperties properties,
        BeanFactory beanFactory,
        ObjectProvider<PlurimaMetrics> metricsProvider) {
        // ObjectProvider so the metrics bean is OPTIONAL — when absent the container
        // falls through to the builder's no-op default. Resolving via
        // getIfAvailable() also tolerates multiple PlurimaMetrics beans by binding
        // to whichever Spring's preference rules pick (typically @Primary).
        return new PlurimaListenerContainer(
            postProcessor, properties, beanFactory, metricsProvider.getIfAvailable());
    }

    /**
     * Auto-registers a {@link MicrometerPlurimaMetrics} adapter when both Micrometer and
     * the adapter class are on the classpath and a {@link MeterRegistry} bean exists.
     *
     * <p>The starter only {@code compileOnly}-depends on {@code :metrics} (see
     * {@code build.gradle.kts}), so {@code @ConditionalOnClass} fails — silently, no
     * error — for users who never add {@code kafka-plurima-metrics} to their own runtime
     * classpath. Users who want this adapter add that artifact themselves; its presence
     * (plus micrometer-core, which it depends on) is what satisfies this condition and
     * activates the bean below.
     */
    @Configuration(proxyBeanMethods = false)
    @ConditionalOnClass({MeterRegistry.class, MicrometerPlurimaMetrics.class})
    static class MicrometerMetricsConfiguration {

        @Bean
        @ConditionalOnBean(MeterRegistry.class)
        @ConditionalOnMissingBean(PlurimaMetrics.class)
        MicrometerPlurimaMetrics plurimaMicrometerMetrics(MeterRegistry registry) {
            return new MicrometerPlurimaMetrics(registry);
        }
    }
}
