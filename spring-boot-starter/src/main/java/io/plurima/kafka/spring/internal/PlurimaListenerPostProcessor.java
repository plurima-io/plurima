package io.plurima.kafka.spring.internal;

import io.plurima.kafka.RecordListener;
import io.plurima.kafka.annotation.Internal;
import io.plurima.kafka.spring.PlurimaListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.aop.support.AopUtils;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.core.MethodIntrospector;
import org.springframework.core.annotation.AnnotationUtils;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Scans Spring beans at post-processing time for {@link PlurimaListener}-annotated
 * methods and records a {@link PlurimaListenerEndpoint} for each. The endpoints are
 * exposed via {@link #endpoints()} for the {@link PlurimaListenerContainer} to consume.
 */
@Internal
public class PlurimaListenerPostProcessor implements BeanPostProcessor {

    private final List<PlurimaListenerEndpoint> endpoints = new ArrayList<>();

    @Override
    public Object postProcessAfterInitialization(Object bean, String beanName) throws BeansException {
        Class<?> targetClass = AopUtils.getTargetClass(bean);
        // Walk the FULL class hierarchy (superclasses + interfaces) using Spring's
        // MethodIntrospector. The previous targetClass.getDeclaredMethods() only saw
        // methods declared directly on the bean's user class, so listener methods
        // inherited from a superclass or default-implemented on an interface were
        // silently skipped. Use AopUtils.getTargetClass so proxied beans are scanned
        // by their user type instead of the generated proxy class.
        Map<Method, PlurimaListener> annotatedMethods = MethodIntrospector.selectMethods(
            targetClass,
            (MethodIntrospector.MetadataLookup<PlurimaListener>) m ->
                AnnotationUtils.findAnnotation(m, PlurimaListener.class));
        for (Map.Entry<Method, PlurimaListener> entry : annotatedMethods.entrySet()) {
            Method method = AopUtils.selectInvocableMethod(entry.getKey(), bean.getClass());
            PlurimaListener annotation = entry.getValue();

            String where = beanName + "#" + method.getName();
            if (annotation.topics().length != 1) {
                throw new IllegalStateException(
                    "@PlurimaListener requires exactly one topic; got " +
                    annotation.topics().length + " on " + where);
            }
            String topic = annotation.topics()[0];
            if (topic == null || topic.isBlank()) {
                throw new IllegalStateException(
                    "@PlurimaListener topic must not be blank — got '" + topic + "' on " + where);
            }
            String groupId = annotation.groupId();
            if (groupId == null || groupId.isBlank()) {
                throw new IllegalStateException(
                    "@PlurimaListener groupId must not be blank — got '" + groupId + "' on " + where);
            }
            int concurrency = annotation.concurrency();
            if (concurrency <= 0) {
                throw new IllegalStateException(
                    "@PlurimaListener concurrency must be > 0 — got " + concurrency + " on " + where);
            }

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
                annotation.retryPolicyBeanName(),
                annotation.dltConfigBeanName()));
        }
        return bean;
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
