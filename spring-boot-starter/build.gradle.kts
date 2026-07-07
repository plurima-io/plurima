plugins {
    `java-library`
}

dependencies {
    api(project(":core"))
    annotationProcessor(project(":core"))
    api(libs.spring.boot.autoconfigure)
    // Used directly by the internal PlurimaListenerContainer logger. Previously arrived
    // transitively via :core's `api(libs.slf4j.api)`; now that :core exposes slf4j only as
    // `implementation` (nothing in :core's public API references it), modules that use slf4j
    // themselves must declare the dependency explicitly.
    implementation(libs.slf4j.api)

    // Micrometer auto-registration (see PlurimaAutoConfiguration.MicrometerMetricsConfiguration)
    // is compiled against :metrics but deliberately NOT bundled at runtime: compileOnly means
    // users who never add kafka-plurima-metrics to their own runtime classpath don't pull in
    // micrometer-core transitively just for using the starter. Users who DO want the adapter
    // add `kafka-plurima-metrics` themselves; that satisfies the nested class's
    // @ConditionalOnClass check and activates the bean.
    compileOnly(project(":metrics"))
    annotationProcessor(libs.spring.boot.configuration.processor)

    testImplementation(platform(libs.junit.bom))
    testImplementation(libs.junit.jupiter)
    testImplementation(libs.assertj.core)
    testImplementation(libs.spring.boot.test)
    testImplementation(libs.archunit.junit5)
    // compileOnly is not inherited by the test source set, so :metrics (and transitively
    // micrometer-core) must be re-declared here to compile/run the auto-registration tests.
    testImplementation(project(":metrics"))
    testRuntimeOnly(libs.junit.platform.launcher)
    // testImplementation (not testRuntimeOnly): PlurimaAutoConfigurationTest attaches a
    // logback ListAppender directly, which requires ch.qos.logback.classic types at
    // compile time (matches the :core module's testImplementation usage).
    testImplementation(libs.logback.classic)
}

tasks.named<JavaCompile>("compileJava") {
    // Explicitly passing -processor OVERRIDES javac's normal SPI auto-discovery of annotation
    // processors found on the processor path — ONLY the classes named here run. That's fine
    // for StabilityProcessor, which is deliberately NOT SPI-registered (see its javadoc) and
    // therefore must be named explicitly to run at all. But spring-boot-configuration-processor
    // IS SPI-registered in its jar (META-INF/services/javax.annotation.processing.Processor) —
    // with an explicit -processor list present, it would be silently skipped (no error, no
    // spring-configuration-metadata.json) unless its class is ALSO named here. Both must be
    // listed, or the interaction above disables it.
    options.compilerArgs.addAll(listOf(
        "-processor",
        "io.plurima.kafka.annotation.processor.internal.StabilityProcessor,"
            + "org.springframework.boot.configurationprocessor.ConfigurationMetadataAnnotationProcessor"
    ))
}

tasks.withType<JavaCompile>().configureEach {
    // Suppress "No processor claimed annotations" warning: @Stable/@Experimental/@Internal have
    // CLASS retention and appear on both the classpath and processorpath; javac's -processing
    // lint fires spuriously in this configuration and is promoted to an error by -Werror.
    options.compilerArgs.add("-Xlint:-processing")
}

tasks.test {
    useJUnitPlatform()
}
