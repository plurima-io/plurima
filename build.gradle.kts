// Gradle 8.14+ compatible configuration (core plugins are automatically available)

import org.gradle.testing.jacoco.tasks.JacocoReport

val publishedModules = setOf("core", "metrics", "spring-boot-starter")

tasks.register("quickstart") {
    group = "application"
    description = "Run the Docker quick-start produce/consume example"
    dependsOn(":test-app:runQuickstart")
}

tasks.register("benchmark") {
    group = "application"
    description = "Run the end-to-end Plurima-vs-vanilla Kafka benchmark"
    dependsOn(":test-app:runBench")
}

allprojects {
    group = "io.plurima"
    // master branch carries the NEXT version as -SNAPSHOT. To cut a release:
    //   1. checkout the release branch, change to e.g. "0.3.0" (no suffix),
    //   2. tag v0.3.0 and push the tag — release.yml verifies tag matches version
    //      and publishes to Sonatype releases
    //   3. bump master to the next patch snapshot, e.g. "0.3.1-SNAPSHOT"
    // The ci.yml publish-snapshot job FAILS the build (not skips) if master ever
    // carries a non-SNAPSHOT version, so a misconfigured branch on master cannot
    // silently skip publishing or accidentally publish a real release.
    version = "0.3.0-SNAPSHOT"
}

subprojects {
    plugins.withId("java") {
        extensions.configure<JavaPluginExtension> {
            toolchain {
                languageVersion.set(JavaLanguageVersion.of(21))
            }
        }
        tasks.withType<JavaCompile>().configureEach {
            options.release.set(21)
            options.compilerArgs.addAll(listOf("-Xlint:all", "-Werror", "-parameters"))
        }
        tasks.withType<Test>().configureEach {
            useJUnitPlatform()
            testLogging {
                events("passed", "skipped", "failed")
                showStandardStreams = false
            }
        }
        tasks.withType<ProcessResources>().configureEach {
            includeEmptyDirs = false
        }
        // Reproducible archives: strip timestamps and normalize entry order so `jar`/`sourcesJar`/
        // `javadocJar` outputs are byte-for-byte identical across runs given identical inputs —
        // a prerequisite for build reproducibility / supply-chain verification tooling.
        tasks.withType<AbstractArchiveTask>().configureEach {
            isPreserveFileTimestamps = false
            isReproducibleFileOrder = true
        }
        // Published Javadocs document the PUBLIC API only. Skip the io.plurima.kafka.internal
        // package (everything inside is @Internal — explicitly not for downstream use) so the
        // generated docs match the actual stability contract.
        //
        // Doclint is left ENABLED for accuracy (syntax/reference/html), but "missing tag"
        // checks are off — many record components and obviously-named builder setters have
        // self-documenting names; forcing @param tags on every one of them adds boilerplate
        // without information. Real Javadoc bugs (broken @link refs, malformed HTML) still
        // fail the build.
        tasks.withType<Javadoc>().configureEach {
            exclude("io/plurima/kafka/internal/**")
            exclude("io/plurima/kafka/spring/internal/**")
            exclude("io/plurima/kafka/annotation/processor/internal/**")
            (options as org.gradle.external.javadoc.StandardJavadocDocletOptions)
                .addStringOption("Xdoclint:all,-missing", "-quiet")
        }
    }

    plugins.withId("java-library") {
        if (name !in publishedModules) {
            return@withId
        }

        // Coverage reporting for the published modules (core, metrics, spring-boot-starter).
        // Report-only for now: `check` runs `jacocoTestReport` so every build produces an
        // up-to-date coverage report under build/reports/jacoco/, but there is deliberately
        // NO `jacocoTestCoverageVerification` / minimum-coverage gate yet. This branch has no
        // agreed coverage baseline to enforce against — an arbitrary threshold today would
        // either be too low to mean anything or fail the build on modules nobody has actually
        // measured. Follow-up: once a few report runs establish a real baseline, add
        // verification rules gating `check` on regressions.
        apply(plugin = "jacoco")
        tasks.named<JacocoReport>("jacocoTestReport") {
            dependsOn(tasks.named("test"))
            reports {
                xml.required.set(true)
                html.required.set(true)
            }
        }
        tasks.named("check") {
            dependsOn(tasks.named("jacocoTestReport"))
        }

        extensions.configure<JavaPluginExtension> {
            withSourcesJar()
            withJavadocJar()
        }

        apply(plugin = "maven-publish")
        apply(plugin = "signing")

        extensions.configure<PublishingExtension> {
            publications {
                create<MavenPublication>("library") {
                    from(components["java"])
                    // Module dirs are `core`, `metrics`, `spring-boot-starter`. The published
                    // artifact id must be prefixed `kafka-plurima-` so consumers don't depend
                    // on the generic `io.plurima:core` etc. — see UserGuide § Gradle dependency.
                    artifactId = "kafka-plurima-${project.name}"
                    pom {
                        name.set("kafka-plurima :: ${project.name}")
                        description.set("Production-grade Kafka consumer abstraction over KafkaShareConsumer "
                            + "(KIP-932) and vanilla KafkaConsumer. Cross-cluster per-key FIFO with "
                            + "intra-partition parallelism, exponential retry, dead-letter routing, and "
                            + "Spring Boot integration.")
                        url.set("https://github.com/plurima-io/plurima")
                        licenses {
                            license {
                                name.set("The Apache License, Version 2.0")
                                url.set("https://www.apache.org/licenses/LICENSE-2.0.txt")
                            }
                        }
                        developers {
                            developer {
                                id.set("sangreal")
                                name.set("Martyn Ye")
                            }
                        }
                        scm {
                            connection.set("scm:git:git://github.com/plurima-io/plurima.git")
                            developerConnection.set("scm:git:ssh://github.com/plurima-io/plurima.git")
                            url.set("https://github.com/plurima-io/plurima")
                        }
                    }
                }
            }
            repositories {
                // No mavenLocal() here: `publishToMavenLocal` is still available (it's wired
                // implicitly by maven-publish regardless of the `repositories {}` block below),
                // but we don't want it listed as a `publish` target — that would make a plain
                // `./gradlew publish` silently write to ~/.m2 in addition to Sonatype.
                //
                // Sonatype Central Portal compatibility endpoints. OSSRH was shut down in
                // 2025; the staging API below accepts Maven-style PUT uploads and the
                // release workflow performs the required Portal handoff after publishing.
                maven {
                    name = "sonatype"
                    val releasesUrl = uri("https://ossrh-staging-api.central.sonatype.com/service/local/staging/deploy/maven2/")
                    val snapshotsUrl = uri("https://central.sonatype.com/repository/maven-snapshots/")
                    url = if (version.toString().endsWith("SNAPSHOT")) snapshotsUrl else releasesUrl
                    credentials {
                        username = providers.environmentVariable("SONATYPE_USERNAME").orNull
                            ?: providers.gradleProperty("sonatypeUsername").orNull
                        password = providers.environmentVariable("SONATYPE_PASSWORD").orNull
                            ?: providers.gradleProperty("sonatypePassword").orNull
                    }
                }
            }
        }

        extensions.configure<SigningExtension> {
            // Sign only when a GPG key is configured. CI/release builds set these via env or
            // -Psigning.key=... -Psigning.password=...; local dev builds skip signing entirely.
            val signingKey = providers.environmentVariable("SIGNING_KEY").orNull
                ?: providers.gradleProperty("signing.key").orNull
            val signingPassword = providers.environmentVariable("SIGNING_PASSWORD").orNull
                ?: providers.gradleProperty("signing.password").orNull
            if (signingKey != null && signingPassword != null) {
                useInMemoryPgpKeys(signingKey, signingPassword)
                sign(extensions.getByType<PublishingExtension>().publications["library"])
            }
        }
    }
}
