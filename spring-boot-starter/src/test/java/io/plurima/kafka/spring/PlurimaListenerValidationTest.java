package io.plurima.kafka.spring;

import io.plurima.kafka.OrderingMode;
import io.plurima.kafka.spring.internal.PlurimaListenerPostProcessor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.aop.framework.ProxyFactory;
import org.springframework.core.annotation.AliasFor;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
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
    void nonNumericConcurrencyIsRejectedWithATeachingMessage() {
        PlurimaListenerPostProcessor pp = new PlurimaListenerPostProcessor();
        assertThatThrownBy(() -> pp.postProcessAfterInitialization(new NonNumericConcurrencyListener(), "nan"))
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("concurrency must be a positive integer")
            .hasMessageContaining("not-a-number");
    }

    @Test
    void twoParameterHandlerMethodIsRejectedWithATeachingMessage() {
        // buildListener() requires exactly one parameter. A method with a second
        // parameter (e.g. an Acknowledgment-style helper) previously would have failed
        // much later with an opaque reflection error at invocation time; the
        // post-processor now refuses to register the endpoint up front.
        PlurimaListenerPostProcessor pp = new PlurimaListenerPostProcessor();
        assertThatThrownBy(() -> pp.postProcessAfterInitialization(new TwoParamListener(), "twoParam"))
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("must accept a single ConsumerRecord<byte[],byte[]>");
    }

    @Test
    void nonConsumerRecordParameterHandlerMethodIsRejectedWithATeachingMessage() {
        // A single parameter that isn't a ConsumerRecord (e.g. a typed value the user
        // expected auto-deserialization for) must also be rejected with the same
        // teaching message, not an opaque ClassCastException from method.invoke().
        PlurimaListenerPostProcessor pp = new PlurimaListenerPostProcessor();
        assertThatThrownBy(() -> pp.postProcessAfterInitialization(new NonRecordParamListener(), "nonRecord"))
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("must accept a single ConsumerRecord<byte[],byte[]>");
    }

    @Test
    void moreThanOneTopicIsRejected() {
        PlurimaListenerPostProcessor pp = new PlurimaListenerPostProcessor();
        assertThatThrownBy(() -> pp.postProcessAfterInitialization(new MultipleTopicsListener(), "multiTopics"))
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("requires exactly one topic")
            .hasMessageContaining("got 2");
    }

    @Test
    void zeroTopicsIsRejected() {
        PlurimaListenerPostProcessor pp = new PlurimaListenerPostProcessor();
        assertThatThrownBy(() -> pp.postProcessAfterInitialization(new ZeroTopicsListener(), "zeroTopics"))
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("requires exactly one topic")
            .hasMessageContaining("got 0");
    }

    static class TwoParamListener {
        @PlurimaListener(topics = "t1", groupId = "g1", concurrency = "1")
        public void onRecord(ConsumerRecord<byte[], byte[]> r, String extra) {}
    }

    static class NonRecordParamListener {
        @PlurimaListener(topics = "t1", groupId = "g1", concurrency = "1")
        public void onRecord(String notARecord) {}
    }

    static class MultipleTopicsListener {
        @PlurimaListener(topics = {"t1", "t2"}, groupId = "g1", concurrency = "1")
        public void onRecord(ConsumerRecord<byte[], byte[]> r) {}
    }

    static class ZeroTopicsListener {
        @PlurimaListener(topics = {}, groupId = "g1", concurrency = "1")
        public void onRecord(ConsumerRecord<byte[], byte[]> r) {}
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
        @PlurimaListener(topics = "inherited-topic", groupId = "inherited-group", concurrency = "1")
        public void onRecord(ConsumerRecord<byte[], byte[]> r) {}
    }

    /** No @PlurimaListener on this class itself — only inherited from {@link AnnotatedBase}. */
    static class ConcreteFromBase extends AnnotatedBase {}

    static class ProxiedListener {
        final AtomicBoolean invoked = new AtomicBoolean();

        @PlurimaListener(topics = "proxied-topic", groupId = "proxied-group", concurrency = "1")
        public void onRecord(ConsumerRecord<byte[], byte[]> r) {
            invoked.set(true);
        }
    }

    static class BlankTopicListener {
        @PlurimaListener(topics = "   ", groupId = "g1", ordering = OrderingMode.UNORDERED, concurrency = "1")
        public void onRecord(ConsumerRecord<byte[], byte[]> r) {}
    }

    static class BlankGroupListener {
        @PlurimaListener(topics = "t1", groupId = "", ordering = OrderingMode.UNORDERED, concurrency = "1")
        public void onRecord(ConsumerRecord<byte[], byte[]> r) {}
    }

    static class ZeroConcurrencyListener {
        @PlurimaListener(topics = "t1", groupId = "g1", ordering = OrderingMode.UNORDERED, concurrency = "0")
        public void onRecord(ConsumerRecord<byte[], byte[]> r) {}
    }

    static class NonNumericConcurrencyListener {
        @PlurimaListener(topics = "t1", groupId = "g1", concurrency = "not-a-number")
        public void onRecord(ConsumerRecord<byte[], byte[]> r) {}
    }

    @Test
    void composedMetaAnnotationAttributesAreMergedViaAliasFor() {
        // AnnotationUtils.findAnnotation returns the @PlurimaListener meta-annotation
        // instance exactly as declared on @OrdersListener — i.e. topics=["unused-default"],
        // concurrency="50" — ignoring the @AliasFor overrides supplied on the composed
        // annotation. AnnotatedElementUtils.findMergedAnnotation synthesizes a merged
        // annotation that applies those @AliasFor overrides, so topics/concurrency come
        // from the composed annotation's own attributes while groupId (not aliased) falls
        // through from the meta-annotation's fixed value.
        PlurimaListenerPostProcessor pp = new PlurimaListenerPostProcessor();
        pp.postProcessAfterInitialization(new ComposedAnnotationListener(), "composed");

        assertThat(pp.endpoints()).hasSize(1);
        io.plurima.kafka.spring.internal.PlurimaListenerEndpoint endpoint = pp.endpoints().get(0);
        assertThat(endpoint.topic()).isEqualTo("composed-topic");
        assertThat(endpoint.groupId()).isEqualTo("composed-group");
        assertThat(endpoint.concurrency()).isEqualTo(5);
    }

    /** Composed/meta-annotation: fixes {@code groupId}, aliases {@code topics} and
     *  {@code concurrency} back onto {@link PlurimaListener} via {@code @AliasFor}. */
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.METHOD)
    @PlurimaListener(topics = "unused-default", groupId = "composed-group")
    @interface OrdersListener {

        @AliasFor(annotation = PlurimaListener.class, attribute = "topics")
        String[] topics();

        @AliasFor(annotation = PlurimaListener.class, attribute = "concurrency")
        String concurrency() default "5";
    }

    static class ComposedAnnotationListener {
        @OrdersListener(topics = "composed-topic")
        public void onRecord(ConsumerRecord<byte[], byte[]> r) {}
    }
}
