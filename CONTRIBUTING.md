# Contributing to Plurima

Thanks for your interest in contributing! This document covers how to build the
project, run the test suites, and submit changes.

## Prerequisites

- **Java 21+** (the build toolchain targets 21; CI also verifies on 24)
- **Docker** with Compose support (only needed for integration tests and the
  quick-start example)

## Building

```bash
./gradlew build
```

This compiles all modules (`core`, `metrics`, `spring-boot-starter`) and runs
the **unit** test suite. Integration tests are excluded by default (see below).

## Running Tests

### Unit tests

```bash
./gradlew test
```

Unit tests need no broker and run everywhere.

### Integration tests

Live integration tests (JUnit tag `integration`) exercise a real Kafka 4.2
broker with share groups enabled. They are gated behind the
`integrationTests` Gradle property.

1. Start the broker (the quick-start compose file has share groups pre-enabled):

   ```bash
   docker compose -f examples/docker-compose.yml up -d --wait
   ```

2. Run the suites with the gate open:

   ```bash
   ./gradlew :core:test :metrics:test -PintegrationTests=true
   ```

3. Tear down when done:

   ```bash
   docker compose -f examples/docker-compose.yml down -v
   ```

By default the tests connect to `localhost:9092`. To point them at a different
broker (a remote test cluster, a CI service container on another port), set
either the system property or the environment variable — no source edits needed:

```bash
# System property takes precedence...
./gradlew :core:test -PintegrationTests=true -Dplurima.test.bootstrap=broker:19092

# ...then the environment variable, then the localhost:9092 default.
PLURIMA_TEST_BOOTSTRAP=broker:19092 ./gradlew :core:test -PintegrationTests=true
```

Note: when `-PintegrationTests=true` is set, the test tasks intentionally
opt out of Gradle's build cache and up-to-date checks so the live suite always
executes against the broker you spun up.

### Benchmarks

```bash
./gradlew benchmark   # requires the local broker from the compose file
```

## CI

Every push to any branch — and every pull request targeting `master` — runs
the full CI pipeline: build + unit tests on Java 21 and 24, followed by the
live integration suite against a Kafka 4.2 service container. Snapshot
publishing runs only on `master` pushes.

## Pull Request Conventions

- **Branch from `master`** and open PRs against `master`.
- **Keep PRs focused** — one logical change per PR. Split unrelated fixes.
- **Write tests** for behavior changes: unit tests always; add or extend an
  integration test when the change touches broker interaction (ack paths,
  rebalance, commit semantics, DLT publishing).
- **Commit messages**: short imperative summary line (e.g.
  `Fix commit-frontier resurrection race on revoke`), with body text explaining
  the why when it isn't obvious.
- **Public API changes** need a matching update to `docs/UserGuide.md`,
  `README.md` examples where relevant, and a `CHANGELOG.md` entry under
  `[Unreleased]`. Breaking changes must be flagged explicitly.
- **No stale docs**: every code block in README/UserGuide must compile against
  the current public API.
- Make sure `./gradlew build` passes locally before requesting review; run the
  integration suite too if your change touches the consumer runtime.

## Reporting Issues

Use GitHub issues for bugs and feature requests. For security vulnerabilities,
follow [SECURITY.md](SECURITY.md) instead — do not open a public issue.

## License

By contributing you agree that your contributions are licensed under the
[Apache License 2.0](LICENSE), the project's license.
