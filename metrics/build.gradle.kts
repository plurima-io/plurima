plugins {
    `java-library`
}

dependencies {
    api(project(":core"))
    annotationProcessor(project(":core"))
    api(libs.micrometer.core)

    testImplementation(platform(libs.junit.bom))
    testImplementation(libs.junit.jupiter)
    testImplementation(libs.assertj.core)
    testRuntimeOnly(libs.junit.platform.launcher)
    testRuntimeOnly(libs.logback.classic)
}

tasks.named<JavaCompile>("compileJava") {
    options.compilerArgs.addAll(listOf(
        "-processor",
        "io.plurima.kafka.annotation.processor.internal.StabilityProcessor"
    ))
}

tasks.withType<JavaCompile>().configureEach {
    // Suppress "No processor claimed annotations" warning: @Stable/@Experimental/@Internal have
    // CLASS retention and appear on both the classpath and processorpath; javac's -processing
    // lint fires spuriously in this configuration and is promoted to an error by -Werror.
    options.compilerArgs.add("-Xlint:-processing")
}

tasks.test {
    // Mirror :core's gate so live integration tests need explicit opt-in.
    val runIntegration = providers.gradleProperty("integrationTests").orElse("false").get().toBoolean()
    useJUnitPlatform {
        if (!runIntegration) excludeTags("integration")
    }
    // CRITICAL: when -PintegrationTests=true is set, force a fresh execution
    // (no cache, no up-to-date). See :core:test for the rationale — a cached
    // result would silently turn the live gate into a no-op.
    if (runIntegration) {
        outputs.upToDateWhen { false }
        outputs.cacheIf { false }
    }
}
