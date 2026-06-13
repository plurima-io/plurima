package io.plurima.kafka.spring;

import io.plurima.kafka.PlurimaConsumer;
import io.plurima.kafka.annotation.Stable;
import io.plurima.kafka.metrics.PlurimaMetrics;
import io.plurima.kafka.spring.internal.PlurimaListenerContainer;
import io.plurima.kafka.spring.internal.PlurimaListenerPostProcessor;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

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
@EnableConfigurationProperties(PlurimaProperties.class)
public class PlurimaAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    PlurimaListenerPostProcessor plurimaListenerPostProcessor() {
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
}
