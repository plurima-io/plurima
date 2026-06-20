plugins {
    `java-library`
    alias(libs.plugins.jmh)
}

dependencies {
    // IntelliJ can attach src/jmh/java to the module's main compile classpath during
    // Gradle import. Mirror the JMH compile dependencies as compileOnly so IDE symbol
    // resolution sees Kafka/JMH types even though this module has no main sources.
    compileOnly(project(":core"))
    compileOnly(libs.kafka.clients)
    compileOnly(libs.jmh.core)

    jmh(project(":core"))
    jmh(libs.kafka.clients)
    jmh(libs.jmh.core)
    jmhAnnotationProcessor(libs.jmh.generator.annprocess)
}

jmh {
    warmupIterations.set(3)
    iterations.set(5)
    fork.set(1)
    timeOnIteration.set("1s")
    warmup.set("1s")
    failOnError.set(true)
}

tasks.named("check") {
    dependsOn(tasks.named("jmhClasses"))
}
