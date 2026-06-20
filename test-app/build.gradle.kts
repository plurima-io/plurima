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
    standardInput = System.`in`
    args = (findProperty("appArgs") as String?)?.split(" ") ?: emptyList()
}

tasks.register<JavaExec>("runOrdering") {
    group = "application"
    description = "Verify PARTITION and KEY ordering invariants against a live Kafka broker"
    mainClass.set("io.plurima.testapp.ordering.OrderingTestApp")
    classpath = sourceSets["main"].runtimeClasspath
    standardInput = System.`in`
    args = (findProperty("appArgs") as String?)?.split(" ") ?: emptyList()
}

tasks.register<JavaExec>("runQuickstart") {
    group = "application"
    description = "Run a ten-record SHARE produce/consume quick start against a live Kafka broker"
    mainClass.set("io.plurima.testapp.QuickstartApp")
    classpath = sourceSets["main"].runtimeClasspath
    args = (findProperty("appArgs") as String?)?.split(" ") ?: emptyList()
}
