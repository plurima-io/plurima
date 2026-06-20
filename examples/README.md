# Docker Quick Start

This example starts a single-node Kafka 4.2 broker with share groups enabled and
runs a real Plurima produce/consume round trip.

## Prerequisites

- Docker with Compose support
- Java 21

## Run

```bash
docker compose -f examples/docker-compose.yml up -d --wait
./gradlew quickstart
```

Expected output:

```text
QUICKSTART_OK bootstrap=localhost:9092 produced=10 processed=10
```

The app creates a unique topic, waits for the share-group assignment, produces ten
records, verifies all ten unique values were processed, and deletes the topic.

## Cleanup

```bash
docker compose -f examples/docker-compose.yml down -v
```

If port `9092` is already in use, stop the existing broker before starting this
example.
