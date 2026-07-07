package io.plurima.kafka;

import io.plurima.kafka.annotation.Stable;

import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * A read-only, Kafka-decoupled view of a delivered record's headers. Returned by
 * {@link Message#headers()} so handler code never needs to depend on Kafka's own
 * {@code org.apache.kafka.common.header.Headers} type.
 *
 * <p>Header names may repeat (Kafka headers are a multi-map): {@link #values(String)} returns
 * every value for a name in delivery order, {@link #lastValue(String)} returns only the one a
 * single-valued header convention would use (Kafka's own "last wins" rule), and {@link #names()}
 * lists the distinct header names present.
 *
 * <p>Byte arrays returned by this view may be the live buffers backing the underlying record
 * (see the implementing adapter's contract) — callers must treat them as read-only.
 */
@Stable(since = "0.3.0")
public interface MessageHeaders {

    /** All values for {@code name}, in delivery order, or an empty list when absent. */
    List<byte[]> values(String name);

    /** The last value for {@code name}, if present ("last wins", matching Kafka's own convention). */
    Optional<byte[]> lastValue(String name);

    /** The distinct header names present, in no particular order. */
    Set<String> names();
}
