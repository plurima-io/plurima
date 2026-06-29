# plurima

<p align="center">
  <img src="docs/assets/plurima.png" alt="plurima" width="720">
</p>

[![CI](https://github.com/plurima-io/plurima/actions/workflows/ci.yml/badge.svg)](https://github.com/plurima-io/plurima/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)
[![Java](https://img.shields.io/badge/Java-21%2B-orange.svg)](docs/UserGuide.md#prerequisites)
[![Kafka](https://img.shields.io/badge/Kafka-4.2%2B-231f20.svg)](docs/UserGuide.md#prerequisites)

Production-grade Kafka consumer abstraction over Kafka 4.2 share groups
(`KafkaShareConsumer`, KIP-932) and classic consumer groups (`KafkaConsumer`).
Plurima gives Java services a single API for high-concurrency message processing,
retry, dead-letter routing, metrics, and Spring Boot integration.

**Current version:** `0.2.0`

## Why Plurima

Kafka 4.2 introduces share groups: a broker-level queueing model designed for
parallel message consumption without assigning whole partitions to one consumer
instance. That is powerful, but the raw client still leaves application teams to
solve listener dispatch, ack discipline, retries, DLT routing, shutdown drain, and
operational metrics.

Plurima packages those concerns into a small library surface:

- **Two engines, one API**: Kafka 4.2 share groups or classic consumer groups.
- **High concurrency**: virtual-thread worker execution with bounded in-flight work.
- **Ordering choices**: unordered, partition-serial, or key-serial processing where
  the selected engine can support it.
- **Reliable completion**: explicit accept/release/reject handling for share groups
  and safe commit-frontier tracking for classic groups.
- **Retry and DLT**: exponential retry policy, delayed retry via broker redelivery,
  and configurable dead-letter topic publishing.
- **Operational visibility**: built-in metrics SPI plus Micrometer adapter.
- **Spring Boot starter**: annotation-driven listeners and property binding.

## Benchmark Highlights

A head-to-head local benchmark compared idiomatic Kafka 4.2 client loops with
Plurima using the same record counts and per-record handler latency.

| Scenario | Vanilla Kafka | Plurima | Result |
|---|---:|---:|---:|
| SHARE throughput, 200 records with 100 ms handlers | 20,741 ms | 1,684 ms | **12.3x** |
| CLASSIC per-key FIFO, 100 records across 20 keys | 10,360 ms | 1,039 ms | **10.0x** |
| CLASSIC partition FIFO, 200 records across 4 partitions | 20,755 ms | 12,194 ms | **1.7x** |
| Slow-handler fencing scenario | Timing-dependent | Continuous-poll completion | Correctness |
| Permanently failing record | Partition stalled | Retried, DLT-routed, unblocked | Correctness |

The largest throughput gains appear when independent records or keys can run
concurrently. Partition-ordered processing is intentionally capped by the number
of assigned partitions. The fencing and DLT rows measure failure-handling behavior,
not meaningful throughput speedups.

See [Benchmark Results](docs/Benchmarks.md) for methodology, raw output, limitations,
and scenario details. These figures are from one local run and are not a general
performance guarantee.

The benchmark implementation is included in this repository. Run it against a
local Kafka broker with:

```bash
./gradlew benchmark
```

## Modules

| Module | Maven coordinate | Purpose |
|---|---|---|
| `core` | `io.plurima:kafka-plurima-core:0.2.0` | Consumer runtime, retry, DLT, ordering, public API |
| `metrics` | `io.plurima:kafka-plurima-metrics:0.2.0` | Micrometer implementation of `PlurimaMetrics` |
| `spring-boot-starter` | `io.plurima:kafka-plurima-spring-boot-starter:0.2.0` | Spring Boot auto-configuration and `@PlurimaListener` |

## Requirements

| Requirement | Version |
|---|---|
| Java | 21 or newer |
| Apache Kafka | 4.2.0 or newer for share groups |
| Spring Boot | 3.4+ when using the starter |

For share groups, Kafka must have share support enabled on the broker. See the
[User Guide prerequisites](docs/UserGuide.md#prerequisites) for exact broker
properties and Docker environment variables.

### Five-Minute Docker Quick Start

Start Kafka 4.2 and run a real produce/consume round trip:

```bash
docker compose -f examples/docker-compose.yml up -d --wait
./gradlew quickstart
```

The app creates a topic, starts a Plurima share consumer, produces ten records,
verifies all ten were processed, and prints `QUICKSTART_OK`.

See [examples/README.md](examples/README.md) for cleanup and troubleshooting.

## Quick Start

### Gradle

```kotlin
dependencies {
    implementation("io.plurima:kafka-plurima-core:0.2.0")

    // Optional integrations
    implementation("io.plurima:kafka-plurima-metrics:0.2.0")
    implementation("io.plurima:kafka-plurima-spring-boot-starter:0.2.0")
}
```

### Minimal Consumer

```java
import io.plurima.kafka.PlurimaConsumer;

import java.time.Duration;
import java.util.Properties;

Properties props = new Properties();
props.put("bootstrap.servers", "localhost:9092");
props.put("group.id", "orders-share-group");
props.put("share.acknowledgement.mode", "explicit");

try (PlurimaConsumer<byte[], byte[]> consumer = PlurimaConsumer.<byte[], byte[]>builder()
    .kafkaProperties(props)
    .topic("orders")
    .concurrency(50)
    .pollTimeout(Duration.ofMillis(500))
    .listener((record, context) -> {
        handleOrder(record.key(), record.value());
    })
    .build()) {

    consumer.start();
    Thread.currentThread().join();
}
```

### Classic Engine With Per-Key FIFO

```java
import io.plurima.kafka.ConsumerEngine;
import io.plurima.kafka.OrderingGuarantee;
import io.plurima.kafka.OrderingMode;
import io.plurima.kafka.PlurimaConsumer;

PlurimaConsumer<byte[], byte[]> consumer = PlurimaConsumer.<byte[], byte[]>builder()
    .kafkaProperties(props)
    .topic("orders")
    .engine(ConsumerEngine.CLASSIC_BASIC)
    .ordering(OrderingMode.KEY)
    .orderingGuarantee(OrderingGuarantee.STRICT)
    .concurrency(64)
    .listener((record, context) -> handleOrder(record.key(), record.value()))
    .build();
```

## Engine Selection

| Engine | Best for | Ordering support |
|---|---|---|
| `SHARE` | Kafka 4.2 share groups, broker-managed redelivery, queue-like fan-out | `UNORDERED` |
| `CLASSIC_BASIC` | Existing consumer groups, partition assignment, cross-instance ordering control | `UNORDERED`, `PARTITION`, `KEY` |

The builder rejects unsupported combinations at build time. For example, key ordering
is classic-only because Kafka share groups do not provide cross-instance
per-key FIFO semantics.

## Retry And Dead-Letter Routing

```java
import io.plurima.kafka.dlt.DltConfig;
import io.plurima.kafka.retry.RetryPolicy;

PlurimaConsumer<byte[], byte[]> consumer = PlurimaConsumer.<byte[], byte[]>builder()
    .kafkaProperties(props)
    .topic("orders")
    .retry(RetryPolicy.exponential()
        .maxAttempts(3)
        .initialDelay(Duration.ofMillis(100))
        .multiplier(2.0)
        .jitter(0.2)
        .retryOn(RuntimeException.class)
        .build())
    .deadLetterTopic(DltConfig.builder()
        .producerProperties(dltProducerProperties)
        .namingStrategy(topic -> topic + ".DLT")
        .build())
    .listener((record, context) -> handleOrder(record.key(), record.value()))
    .build();
```

## Metrics

The `metrics` module adapts Plurima's metrics SPI to Micrometer:

```java
import io.micrometer.core.instrument.MeterRegistry;
import io.plurima.kafka.metrics.MicrometerPlurimaMetrics;

MicrometerPlurimaMetrics metrics = new MicrometerPlurimaMetrics(meterRegistry);

PlurimaConsumer<byte[], byte[]> consumer = PlurimaConsumer.<byte[], byte[]>builder()
    .kafkaProperties(props)
    .topic("orders")
    .metrics(metrics)
    .listener((record, context) -> handleOrder(record.key(), record.value()))
    .build();
```

Meters use the `plurima.consumer.*` namespace. The documented set includes records
polled, processed, failed, in-flight gauges, retry attempts, DLT routing, and ack
commit outcomes.

## Spring Boot

Add the starter and declare listener methods with `@PlurimaListener`:

```java
import io.plurima.kafka.spring.PlurimaListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.stereotype.Component;

@Component
class OrderListeners {

    @PlurimaListener(topics = "orders", groupId = "orders-share-group", concurrency = 50)
    void onOrder(ConsumerRecord<byte[], byte[]> record) {
        handleOrder(record.key(), record.value());
    }
}
```

The starter auto-discovers `RetryPolicy`, `DltConfig`, and `PlurimaMetrics` beans
when they are present.

## Documentation

- [User Guide](docs/UserGuide.md): full API guide, engine behavior, broker setup,
  retry/DLT, metrics, Spring Boot, shutdown, and troubleshooting.
- [Benchmark Results](docs/Benchmarks.md): comparison with direct Kafka client
  implementations and interpretation of the measured results.
- [CHANGELOG](CHANGELOG.md): release notes for published versions.
- Javadocs: public API contracts for `PlurimaConsumer`, `ConsumerEngine`,
  `OrderingMode`, `RetryPolicy`, `DltConfig`, and `PlurimaMetrics`.

## Project Status

`0.2.0` builds on the first public release with a Kafka-decoupled `Message` handler
API, SHARE handler timeouts, lock-duration auto-alignment, and slowness-aware retry
budgeting. The project includes live Kafka integration coverage in CI.

## License

Apache License 2.0. See [LICENSE](LICENSE).

## Trademark Notice

KAFKA is a registered trademark of The Apache Software Foundation and has been
licensed for use by plurima. plurima has no affiliation with and is not endorsed
by The Apache Software Foundation.

See the [Apache Kafka trademark guidance](https://kafka.apache.org/community/trademark/).
See [TRADEMARKS.md](TRADEMARKS.md) for the repository trademark notice.
