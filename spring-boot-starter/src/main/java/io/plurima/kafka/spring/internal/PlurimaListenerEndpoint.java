package io.plurima.kafka.spring.internal;

import io.plurima.kafka.ConsumerEngine;
import io.plurima.kafka.OrderingMode;
import io.plurima.kafka.RecordListener;
import io.plurima.kafka.annotation.Internal;
import io.plurima.kafka.spring.PlurimaListener;

import java.util.Objects;

/** Per-{@link PlurimaListener}-method registration data. */
@Internal
public final class PlurimaListenerEndpoint {

    private final String topic;
    private final String groupId;
    private final OrderingMode ordering;
    private final int concurrency;
    private final ConsumerEngine engine;
    private final RecordListener<byte[], byte[]> listener;
    private final String beanName;
    private final String methodName;
    private final String retryPolicyBeanName;       // "" when unset
    private final String dltConfigBeanName;         // "" when unset

    public PlurimaListenerEndpoint(
        String topic, String groupId, OrderingMode ordering, int concurrency,
        ConsumerEngine engine,
        RecordListener<byte[], byte[]> listener, String beanName, String methodName,
        String retryPolicyBeanName, String dltConfigBeanName) {
        this.topic = Objects.requireNonNull(topic, "topic");
        this.groupId = Objects.requireNonNull(groupId, "groupId");
        this.ordering = Objects.requireNonNull(ordering, "ordering");
        this.concurrency = concurrency;
        this.engine = Objects.requireNonNull(engine, "engine");
        this.listener = Objects.requireNonNull(listener, "listener");
        this.beanName = beanName;
        this.methodName = methodName;
        this.retryPolicyBeanName = retryPolicyBeanName == null ? "" : retryPolicyBeanName;
        this.dltConfigBeanName = dltConfigBeanName == null ? "" : dltConfigBeanName;
    }

    public String topic() { return topic; }
    public String groupId() { return groupId; }
    public OrderingMode ordering() { return ordering; }
    public int concurrency() { return concurrency; }
    public ConsumerEngine engine() { return engine; }
    public RecordListener<byte[], byte[]> listener() { return listener; }
    public String beanName() { return beanName; }
    public String methodName() { return methodName; }
    public String retryPolicyBeanName() { return retryPolicyBeanName; }
    public String dltConfigBeanName() { return dltConfigBeanName; }

    @Override
    public String toString() {
        return "PlurimaListenerEndpoint{" +
            "topic='" + topic + "', groupId='" + groupId + "', engine=" + engine
            + ", ordering=" + ordering +
            ", bean=" + beanName + "#" + methodName + "}";
    }
}
