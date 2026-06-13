# kafka-plurima

> Production-grade Kafka consumer abstraction over `KafkaShareConsumer` (KIP-932) and
> vanilla `KafkaConsumer`. Cross-cluster per-key FIFO with intra-partition parallelism,
> exponential retry, dead-letter routing, and Spring Boot integration.

**Version:** v0.1.0 — first release with both share-group and classic consumer-group engines.

## Getting started

See the [User Guide](docs/UserGuide.md) for the full API + engine-choice guidance, and
[CHANGELOG.md](CHANGELOG.md) for the v0.1 changes.

## License

Apache 2.0 — see `LICENSE`.

## Design

Current behavior: see [docs/UserGuide.md](docs/UserGuide.md) and the Javadocs on
`ConsumerEngine`, `OrderingMode`, `OrderingGuarantee`, `RetryPolicy`, `RetryDecision`,
`DltConfig`, and `PlurimaMetrics`.
