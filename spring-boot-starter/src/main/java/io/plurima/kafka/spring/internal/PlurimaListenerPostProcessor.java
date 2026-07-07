package io.plurima.kafka.spring.internal;

import io.plurima.kafka.RecordListener;
import io.plurima.kafka.annotation.Internal;
import io.plurima.kafka.spring.PlurimaListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.aop.support.AopUtils;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.context.EmbeddedValueResolverAware;
import org.springframework.core.MethodIntrospector;
import org.springframework.core.annotation.AnnotatedElementUtils;
import org.springframework.util.StringValueResolver;

import java.lang.reflect.Method;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Scans Spring beans at post-processing time for {@link PlurimaListener}-annotated
 * methods and records a {@link PlurimaListenerEndpoint} for each. The endpoints are
 * exposed via {@link #endpoints()} for the {@link PlurimaListenerContainer} to consume.
 */
@Internal
public class PlurimaListenerPostProcessor implements BeanPostProcessor, EmbeddedValueResolverAware {

    // CopyOnWriteArrayList: this bean is registered as a BeanPostProcessor, so
    // postProcessAfterInitialization (writer) can run concurrently with endpoints()
    // (reader, invoked by PlurimaListenerContainer.start()) if a user context refreshes
    // eagerly while lazily-initialized beans are still being created on another thread.
    // A plain ArrayList is not safe for that read/write overlap.
    private final List<PlurimaListenerEndpoint> endpoints = new CopyOnWriteArrayList<>();

    // Null until the owning ApplicationContext calls setEmbeddedValueResolver — which
    // happens during this bean's own initialization (ApplicationContextAwareProcessor runs
    // ahead of user BeanPostProcessors), so it is always set before
    // postProcessAfterInitialization is invoked on any other bean.
    private StringValueResolver embeddedValueResolver;

    @Override
    public void setEmbeddedValueResolver(StringValueResolver resolver) {
        this.embeddedValueResolver = resolver;
    }

    @Override
    public Object postProcessAfterInitialization(Object bean, String beanName) throws BeansException {
        Class<?> targetClass = AopUtils.getTargetClass(bean);
        // Walk the FULL class hierarchy (superclasses + interfaces) using Spring's
        // MethodIntrospector. The previous targetClass.getDeclaredMethods() only saw
        // methods declared directly on the bean's user class, so listener methods
        // inherited from a superclass or default-implemented on an interface were
        // silently skipped. Use AopUtils.getTargetClass so proxied beans are scanned
        // by their user type instead of the generated proxy class.
        //
        // AnnotatedElementUtils.findMergedAnnotation (not AnnotationUtils.findAnnotation)
        // so @PlurimaListener also resolves through a user-defined composed/meta-annotation
        // that carries @AliasFor overrides — findAnnotation only looks for the annotation
        // literally present (directly or on a superclass method), it does not merge
        // attributes from a meta-annotation stack.
        Map<Method, PlurimaListener> annotatedMethods = MethodIntrospector.selectMethods(
            targetClass,
            (MethodIntrospector.MetadataLookup<PlurimaListener>) m ->
                AnnotatedElementUtils.findMergedAnnotation(m, PlurimaListener.class));
        for (Map.Entry<Method, PlurimaListener> entry : annotatedMethods.entrySet()) {
            Method method = AopUtils.selectInvocableMethod(entry.getKey(), bean.getClass());
            PlurimaListener annotation = entry.getValue();

            String where = beanName + "#" + method.getName();
            if (annotation.topics().length != 1) {
                throw new IllegalStateException(
                    "@PlurimaListener requires exactly one topic; got " +
                    annotation.topics().length + " on " + where);
            }
            // Resolve ${...} placeholders BEFORE validating, so e.g. topics = "${app.topic}"
            // is checked against its resolved value, not the literal placeholder text.
            String topic = resolvePlaceholders(annotation.topics()[0]);
            if (topic == null || topic.isBlank()) {
                throw new IllegalStateException(
                    "@PlurimaListener topic must not be blank — got '" + topic + "' on " + where);
            }
            String groupId = resolvePlaceholders(annotation.groupId());
            if (groupId == null || groupId.isBlank()) {
                throw new IllegalStateException(
                    "@PlurimaListener groupId must not be blank — got '" + groupId + "' on " + where);
            }
            int concurrency = parseConcurrency(resolvePlaceholders(annotation.concurrency()), where);
            String retryPolicyBeanName = resolvePlaceholders(annotation.retryPolicyBeanName());
            String dltConfigBeanName = resolvePlaceholders(annotation.dltConfigBeanName());

            method.setAccessible(true);
            RecordListener<byte[], byte[]> listener = buildListener(bean, method);

            endpoints.add(new PlurimaListenerEndpoint(
                topic,
                groupId,
                annotation.ordering(),
                concurrency,
                annotation.engine(),
                listener,
                beanName,
                method.getName(),
                retryPolicyBeanName,
                dltConfigBeanName));
        }
        return bean;
    }

    /**
     * Resolves {@code ${...}} property placeholders in an annotation attribute. Returns the
     * input unchanged when it is {@code null} or when no {@link StringValueResolver} has been
     * set (e.g. this post-processor is used standalone, outside a Spring context, as some
     * unit tests do).
     */
    private String resolvePlaceholders(String value) {
        if (value == null || embeddedValueResolver == null) {
            return value;
        }
        return embeddedValueResolver.resolveStringValue(value);
    }

    private int parseConcurrency(String raw, String where) {
        String trimmed = raw == null ? "" : raw.trim();
        int concurrency;
        try {
            concurrency = Integer.parseInt(trimmed);
        } catch (NumberFormatException ex) {
            throw new IllegalStateException(
                "@PlurimaListener concurrency must be a positive integer literal or a "
                + "'${...}' placeholder resolving to one (e.g. \"50\" or "
                + "\"${orders.concurrency:50}\") — got '" + raw + "' on " + where, ex);
        }
        if (concurrency <= 0) {
            throw new IllegalStateException(
                "@PlurimaListener concurrency must be > 0 — got " + concurrency + " on " + where);
        }
        return concurrency;
    }

    private RecordListener<byte[], byte[]> buildListener(Object bean, Method method) {
        Class<?>[] params = method.getParameterTypes();
        if (params.length != 1 || !ConsumerRecord.class.isAssignableFrom(params[0])) {
            throw new IllegalStateException(
                "@PlurimaListener method " + method + " must accept a single ConsumerRecord<byte[],byte[]>");
        }
        return (record, ctx) -> {
            try {
                method.invoke(bean, record);
            } catch (java.lang.reflect.InvocationTargetException ite) {
                Throwable cause = ite.getCause();
                if (cause instanceof RuntimeException re) throw re;
                if (cause instanceof Error err) throw err;
                throw new RuntimeException(cause);
            } catch (IllegalAccessException iae) {
                throw new IllegalStateException("Cannot invoke @PlurimaListener method " + method, iae);
            }
        };
    }

    public List<PlurimaListenerEndpoint> endpoints() {
        return Collections.unmodifiableList(endpoints);
    }
}
