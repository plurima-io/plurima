package io.plurima.kafka.spring;

import io.plurima.kafka.ConsumerEngine;
import io.plurima.kafka.OrderingMode;
import io.plurima.kafka.PlurimaConsumer;
import io.plurima.kafka.dlt.DltConfig;
import io.plurima.kafka.spring.internal.PlurimaListenerContainer;
import io.plurima.kafka.spring.internal.PlurimaListenerPostProcessor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.support.StaticListableBeanFactory;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Exercises {@link PlurimaListenerContainer#start()}/{@code stop()} against REAL
 * {@link PlurimaConsumer} instances — deliberately not the {@code NoStartConfig} stub used
 * elsewhere in this package. {@code bootstrap.servers=localhost:1} guarantees a fast, local
 * connection-refused rather than a real broker dependency or a slow unreachable-host
 * timeout, so these stay fast, deterministic unit tests.
 *
 * <p>The "failing builder seam" used to trigger the partial-start failure below is
 * {@link io.plurima.kafka.PlurimaConsumerBuilder#build()}'s own pre-existing, always-on
 * validation that {@code engine=SHARE} rejects {@code ordering=KEY}/{@code PARTITION} — a
 * deterministic, network-free {@code IllegalArgumentException} with no mocking required.
 */
class PlurimaListenerContainerLifecycleTest {

    private static final String UNREACHABLE_BOOTSTRAP = "localhost:1";

    @Test
    void startRollsBackAlreadyStartedConsumersOnPartialFailure() {
        PlurimaListenerPostProcessor pp = new PlurimaListenerPostProcessor();
        // Registration order matters: GoodListener must be scanned (and therefore started)
        // BEFORE BadListener, so start()'s failure on the second endpoint has a
        // real, already-started consumer to roll back.
        pp.postProcessAfterInitialization(new GoodListener(), "goodListener");
        pp.postProcessAfterInitialization(new BadListener(), "badListener");

        PlurimaProperties properties = new PlurimaProperties();
        properties.setBootstrapServers(UNREACHABLE_BOOTSTRAP);

        PlurimaListenerContainer container = new PlurimaListenerContainer(
            pp, properties, /* beanFactory */ null, /* metrics */ null);

        assertThatThrownBy(container::start)
            .as("engine=SHARE + ordering=KEY is rejected by PlurimaConsumerBuilder.build() — "
                + "this must propagate out of start(), not be swallowed")
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("engine=SHARE does not support ordering=KEY");

        assertThat(container.consumers())
            .as("the GoodListener consumer, already started before BadListener's build() "
                + "failed, must be rolled back (closed and removed) rather than left running")
            .isEmpty();
        assertThat(container.isRunning())
            .as("a failed start() must leave the container in the not-running state so a "
                + "subsequent retry (e.g. after the user fixes the bad endpoint) is possible")
            .isFalse();

        // stop() on a container that never successfully started must be a safe no-op.
        container.stop();
        assertThat(container.consumers()).isEmpty();
    }

    @Test
    void stopClosesAllStartedConsumers() {
        PlurimaListenerPostProcessor pp = new PlurimaListenerPostProcessor();
        pp.postProcessAfterInitialization(new GoodListener(), "goodListener");

        PlurimaProperties properties = new PlurimaProperties();
        properties.setBootstrapServers(UNREACHABLE_BOOTSTRAP);

        PlurimaListenerContainer container = new PlurimaListenerContainer(
            pp, properties, /* beanFactory */ null, /* metrics */ null);

        container.start();
        try {
            List<PlurimaConsumer<byte[], byte[]>> started = container.consumers();
            assertThat(started).hasSize(1);
            PlurimaConsumer<byte[], byte[]> consumer = started.get(0);
            assertThat(consumer.state()).isEqualTo(PlurimaConsumer.State.RUNNING);
            assertThat(container.isRunning()).isTrue();

            container.stop();

            assertThat(container.consumers())
                .as("stop() must clear the tracked consumer list")
                .isEmpty();
            assertThat(container.isRunning()).isFalse();
            assertThat(consumer.state())
                .as("stop() must actually close each started consumer, not merely forget it")
                .isEqualTo(PlurimaConsumer.State.CLOSED);
        } finally {
            // Safety net: if an assertion above fails, still tear down the poll thread
            // rather than leaking it into the next test.
            container.stop();
        }
    }

    /**
     * G6 — {@code endpoint.dltConfigBeanName()} must resolve through the {@code BeanFactory}
     * and reach {@code builder.deadLetter(...)}. This asserts the CONSUMER that
     * {@code start()} actually builds carries that exact {@link DltConfig} instance —
     * stronger than merely observing that {@code beanFactory.getBean(...)} was called.
     */
    @Test
    void dltConfigBeanNameWiresTheResolvedDltConfigIntoTheBuiltConsumer() throws Exception {
        Properties dltProps = new Properties();
        dltProps.put("bootstrap.servers", UNREACHABLE_BOOTSTRAP);
        DltConfig dltConfig = DltConfig.builder().producerProperties(dltProps).build();

        StaticListableBeanFactory beanFactory = new StaticListableBeanFactory();
        beanFactory.addBean("myDlt", dltConfig);

        PlurimaListenerPostProcessor pp = new PlurimaListenerPostProcessor();
        pp.postProcessAfterInitialization(new DltListener(), "dltListener");

        PlurimaProperties properties = new PlurimaProperties();
        properties.setBootstrapServers(UNREACHABLE_BOOTSTRAP);

        PlurimaListenerContainer container = new PlurimaListenerContainer(
            pp, properties, beanFactory, /* metrics */ null);

        container.start();
        try {
            List<PlurimaConsumer<byte[], byte[]>> started = container.consumers();
            assertThat(started).hasSize(1);

            Field dltConfigField = PlurimaConsumer.class.getDeclaredField("dltConfig");
            dltConfigField.setAccessible(true);
            Object wired = dltConfigField.get(started.get(0));

            assertThat(wired)
                .as("the consumer built for the endpoint must carry the EXACT DltConfig "
                    + "bean resolved via dltConfigBeanName, not null and not some other "
                    + "instance")
                .isSameAs(dltConfig);
        } finally {
            container.stop();
        }
    }

    static class GoodListener {
        @PlurimaListener(
            topics = "topic1",
            groupId = "group1",
            engine = ConsumerEngine.CLASSIC_BASIC,
            concurrency = "1")
        public void onRecord(ConsumerRecord<byte[], byte[]> record) {}
    }

    static class BadListener {
        @PlurimaListener(
            topics = "topic2",
            groupId = "group2",
            ordering = OrderingMode.KEY,
            concurrency = "1")
        public void onRecord(ConsumerRecord<byte[], byte[]> record) {}
    }

    static class DltListener {
        @PlurimaListener(
            topics = "topic3",
            groupId = "group3",
            engine = ConsumerEngine.CLASSIC_BASIC,
            concurrency = "1",
            dltConfigBeanName = "myDlt")
        public void onRecord(ConsumerRecord<byte[], byte[]> record) {}
    }
}
