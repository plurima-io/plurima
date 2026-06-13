plugins {
    `java-library`
}

dependencies {
    api(project(":core"))
    annotationProcessor(project(":core"))
    api(libs.spring.boot.autoconfigure)

    testImplementation(platform(libs.junit.bom))
    testImplementation(libs.junit.jupiter)
    testImplementation(libs.assertj.core)
    testImplementation(libs.spring.boot.test)
    testImplementation(libs.archunit.junit5)
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
    useJUnitPlatform()
}
