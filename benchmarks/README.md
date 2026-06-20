# Plurima Benchmarks

JMH micro-benchmarks for hot paths in the core module.

## Run all benchmarks

```bash
./gradlew :benchmarks:jmh
```

Results land in `benchmarks/build/results/jmh/`.

## Run a specific benchmark

```bash
./gradlew :benchmarks:jmh -PjmhInclude=AckCoordinatorBenchmark
```

(The `me.champeau.jmh` plugin honors the `-PjmhInclude=<pattern>` Gradle property.)

## What's measured

| Benchmark | Hot path | Reports |
|---|---|---|
| `AckCoordinatorBenchmark.queueAndCommit` | `AckCoordinator.queueAck` + `commitPendingAcks` against a no-op consumer | ops/sec |

These are isolated from broker I/O. For end-to-end throughput against a real broker, use the integration test suite.

## Interpreting changes

When a PR changes the locking strategy, listener-invocation path, or ack pipeline, run before/after benchmarks. A regression of >10% on either benchmark warrants discussion in the PR.
