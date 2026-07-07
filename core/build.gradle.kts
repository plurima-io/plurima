plugins {
    `java-library`
}

dependencies {
    api(libs.kafka.clients)
    // implementation, not api: slf4j types (Logger/LoggerFactory) are used only internally
    // (PlurimaConsumer's private static logger field) — no public signature in :core exposes
    // org.slf4j.* — so downstream consumers don't need it on their compile classpath.
    implementation(libs.slf4j.api)
    api(libs.jspecify)

    testImplementation(platform(libs.junit.bom))
    testImplementation(libs.junit.jupiter)
    testRuntimeOnly(libs.junit.platform.launcher)
    testImplementation(libs.assertj.core)
    testImplementation(libs.mockito.core)
    testImplementation(libs.mockito.junit)
    testImplementation(libs.archunit.junit5)
    testImplementation(libs.logback.classic)
}

tasks.test {
    // Unit tests by default; live integration tests (those tagged `@integration`)
    // require a Kafka 4.x broker at localhost:9092 and are gated behind
    // -PintegrationTests=true.
    val runIntegration = providers.gradleProperty("integrationTests").orElse("false").get().toBoolean()
    useJUnitPlatform {
        if (!runIntegration) excludeTags("integration")
    }
    // CRITICAL: when -PintegrationTests=true is set, never satisfy this task from
    // Gradle's build cache or via the "up-to-date" check. The live suite asserts
    // behaviour against a real Kafka broker; a cache hit from a prior run on a
    // different broker (or no broker at all) would silently turn the live gate
    // into a no-op. Always force a fresh execution in CI/release.
    if (runIntegration) {
        outputs.upToDateWhen { false }
        outputs.cacheIf { false }
    }
}
