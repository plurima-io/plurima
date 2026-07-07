package io.plurima.kafka.spring;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.plurima.kafka.ConsumerEngine;
import io.plurima.kafka.PlurimaConsumer;
import io.plurima.kafka.metrics.MicrometerPlurimaMetrics;
import io.plurima.kafka.spring.internal.PlurimaListenerContainer;
import io.plurima.kafka.spring.internal.PlurimaListenerPostProcessor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * F4 — the Spring starter shares ONE {@code PlurimaMetrics} adapter bean across every
 * {@code @PlurimaListener} endpoint, but each consumer runtime's {@code doClose()} calls
 * {@code metrics.close()}. With the Micrometer adapter, close() deregisters EVERY gauge
 * the adapter ever registered — so the first consumer to close (or fail fatally) used to
 * nuke all OTHER live consumers' {@code in_flight} gauges, and cleared the tracking lists
 * so the survivors' own later close became a no-op.
 *
 * <p>The container therefore wraps the shared bean per endpoint in a close-suppressing
 * delegate: the shared adapter's lifecycle belongs to Spring / the registry, not to any
 * single consumer. Gauges for stopped endpoints remain registered, reading their final
 * values.
 */
class PlurimaListenerContainerSharedMetricsTest {

    private static final String UNREACHABLE_BOOTSTRAP = "localhost:1";

    @Test
    void closingOneConsumerMustNotRemoveAnotherConsumersGauges() {
        PlurimaListenerPostProcessor pp = new PlurimaListenerPostProcessor();
        pp.postProcessAfterInitialization(new ListenerOne(), "listenerOne");
        pp.postProcessAfterInitialization(new ListenerTwo(), "listenerTwo");

        PlurimaProperties properties = new PlurimaProperties();
        properties.setBootstrapServers(UNREACHABLE_BOOTSTRAP);

        MeterRegistry registry = new SimpleMeterRegistry();
        MicrometerPlurimaMetrics sharedMetrics = new MicrometerPlurimaMetrics(registry);

        PlurimaListenerContainer container = new PlurimaListenerContainer(
            pp, properties, /* beanFactory */ null, sharedMetrics);

        container.start();
        try {
            List<PlurimaConsumer<byte[], byte[]>> consumers = container.consumers();
            assertThat(consumers).hasSize(2);
            assertThat(registry.find("plurima.consumer.records.in_flight").gauges())
                .as("each endpoint registers its own in_flight gauge on the shared registry")
                .hasSize(2);

            // Close ONE consumer (equivalently: it fails fatally and self-closes).
            consumers.get(0).close();

            assertThat(registry.find("plurima.consumer.records.in_flight")
                    .tag("group_id", "group-two").gauges())
                .as("the OTHER consumer is still live — its gauge must survive the first "
                    + "consumer's close (shared-adapter close() must be suppressed per "
                    + "endpoint; the shared bean's lifecycle belongs to Spring)")
                .hasSize(1);
        } finally {
            container.stop();
        }

        // After the container stops everything, the shared adapter still owns its gauges
        // (registry lifecycle owns them; stopped endpoints' gauges remain registered
        // reading their final values). A later registry/bean shutdown may clean up.
        assertThat(registry.find("plurima.consumer.records.in_flight").gauges())
            .as("container stop must not invoke the shared adapter's close() either")
            .hasSize(2);
    }

    static class ListenerOne {
        @PlurimaListener(
            topics = "topic-one",
            groupId = "group-one",
            engine = ConsumerEngine.CLASSIC_BASIC,
            concurrency = "1")
        public void onRecord(ConsumerRecord<byte[], byte[]> record) {}
    }

    static class ListenerTwo {
        @PlurimaListener(
            topics = "topic-two",
            groupId = "group-two",
            engine = ConsumerEngine.CLASSIC_BASIC,
            concurrency = "1")
        public void onRecord(ConsumerRecord<byte[], byte[]> record) {}
    }
}
