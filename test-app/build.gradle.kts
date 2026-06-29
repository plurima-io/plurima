plugins {
    application
}

dependencies {
    implementation(project(":core"))
    implementation(libs.kafka.clients)
    implementation(libs.slf4j.api)
    runtimeOnly(libs.logback.classic)
}

application {
    mainClass.set("io.plurima.testapp.PlurimaTestApp")
}

// The test-app is compiled to the Java 21 toolchain. Custom JavaExec tasks must LAUNCH with a
// matching JVM — otherwise, when Gradle itself runs on an older JVM, they fail with
// UnsupportedClassVersionError. The `application` `run` task already uses the toolchain launcher;
// pin the custom tasks to it too.
val appLauncher = javaToolchains.launcherFor {
    languageVersion.set(JavaLanguageVersion.of(21))
}

tasks.named<JavaExec>("run") {
    // Pass-through for ad-hoc args: ./gradlew :test-app:run --args="--bootstrap localhost:9092"
    standardInput = System.`in`
    args = (findProperty("appArgs") as String?)?.split(" ") ?: emptyList()
}

tasks.register<JavaExec>("runBench") {
    group = "application"
    description = "Run the Plurima-vs-vanilla benchmark against a live Kafka broker"
    mainClass.set("io.plurima.testapp.bench.PlurimaVsVanillaBench")
    classpath = sourceSets["main"].runtimeClasspath
    javaLauncher.set(appLauncher)
    standardInput = System.`in`
    args = (findProperty("appArgs") as String?)?.split(" ") ?: emptyList()
}

tasks.register<JavaExec>("runOrdering") {
    group = "application"
    description = "Verify PARTITION and KEY ordering invariants against a live Kafka broker"
    mainClass.set("io.plurima.testapp.ordering.OrderingTestApp")
    classpath = sourceSets["main"].runtimeClasspath
    javaLauncher.set(appLauncher)
    standardInput = System.`in`
    args = (findProperty("appArgs") as String?)?.split(" ") ?: emptyList()
}

tasks.register<JavaExec>("runQuickstart") {
    group = "application"
    description = "Run a ten-record SHARE produce/consume quick start against a live Kafka broker"
    mainClass.set("io.plurima.testapp.QuickstartApp")
    classpath = sourceSets["main"].runtimeClasspath
    javaLauncher.set(appLauncher)
    args = (findProperty("appArgs") as String?)?.split(" ") ?: emptyList()
}
