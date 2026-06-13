package io.plurima.kafka.spring;

import io.plurima.kafka.OrderingMode;
import io.plurima.kafka.spring.internal.PlurimaListenerPostProcessor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.aop.framework.ProxyFactory;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Up-front validation of {@link PlurimaListener} annotation metadata. Previously a blank
 * topic, blank groupId, or non-positive concurrency would pass through and fail much later
 * inside Kafka itself with a less actionable error. Now the post-processor refuses to
 * register the endpoint, surfacing the misconfiguration at application startup.
 */
class PlurimaListenerValidationTest {

    @Test
    void blankTopicIsRejected() {
        PlurimaListenerPostProcessor pp = new PlurimaListenerPostProcessor();
        assertThatThrownBy(() -> pp.postProcessAfterInitialization(new BlankTopicListener(), "blank"))
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("topic must not be blank");
    }

    @Test
    void blankGroupIdIsRejected() {
        PlurimaListenerPostProcessor pp = new PlurimaListenerPostProcessor();
        assertThatThrownBy(() -> pp.postProcessAfterInitialization(new BlankGroupListener(), "blank"))
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("groupId must not be blank");
    }

    @Test
    void zeroConcurrencyIsRejected() {
        PlurimaListenerPostProcessor pp = new PlurimaListenerPostProcessor();
        assertThatThrownBy(() -> pp.postProcessAfterInitialization(new ZeroConcurrencyListener(), "zero"))
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("concurrency must be > 0");
    }

    @Test
    void listenerAnnotationOnInheritedMethodIsFound() {
        // Previously the post-processor used targetClass.getDeclaredMethods() which only
        // returned methods declared directly on the bean's class. An @PlurimaListener
        // method inherited from a base class was silently skipped. The new
        // MethodIntrospector-based scan walks the full hierarchy.
        PlurimaListenerPostProcessor pp = new PlurimaListenerPostProcessor();
        pp.postProcessAfterInitialization(new ConcreteFromBase(), "inherited");
        assertThat(pp.endpoints())
            .as("inherited @PlurimaListener method must be registered")
            .hasSize(1);
        assertThat(pp.endpoints().get(0).topic()).isEqualTo("inherited-topic");
    }

    @Test
    void listenerAnnotationOnProxiedClassMethodIsInvocable() throws Exception {
        ProxiedListener target = new ProxiedListener();
        ProxyFactory factory = new ProxyFactory(target);
        factory.setProxyTargetClass(true);
        Object proxy = factory.getProxy();

        PlurimaListenerPostProcessor pp = new PlurimaListenerPostProcessor();
        pp.postProcessAfterInitialization(proxy, "proxied");

        assertThat(pp.endpoints()).hasSize(1);
        pp.endpoints().get(0).listener().onRecord(
            new ConsumerRecord<>("proxied-topic", 0, 0L, new byte[0], new byte[0]),
            null);

        assertThat(target.invoked.get()).isTrue();
    }

    static class AnnotatedBase {
        @PlurimaListener(topics = "inherited-topic", groupId = "inherited-group", concurrency = 1)
        public void onRecord(ConsumerRecord<byte[], byte[]> r) {}
    }

    /** No @PlurimaListener on this class itself — only inherited from {@link AnnotatedBase}. */
    static class ConcreteFromBase extends AnnotatedBase {}

    static class ProxiedListener {
        final AtomicBoolean invoked = new AtomicBoolean();

        @PlurimaListener(topics = "proxied-topic", groupId = "proxied-group", concurrency = 1)
        public void onRecord(ConsumerRecord<byte[], byte[]> r) {
            invoked.set(true);
        }
    }

    static class BlankTopicListener {
        @PlurimaListener(topics = "   ", groupId = "g1", ordering = OrderingMode.UNORDERED, concurrency = 1)
        public void onRecord(ConsumerRecord<byte[], byte[]> r) {}
    }

    static class BlankGroupListener {
        @PlurimaListener(topics = "t1", groupId = "", ordering = OrderingMode.UNORDERED, concurrency = 1)
        public void onRecord(ConsumerRecord<byte[], byte[]> r) {}
    }

    static class ZeroConcurrencyListener {
        @PlurimaListener(topics = "t1", groupId = "g1", ordering = OrderingMode.UNORDERED, concurrency = 0)
        public void onRecord(ConsumerRecord<byte[], byte[]> r) {}
    }
}
