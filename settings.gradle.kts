import org.gradle.api.initialization.resolve.RepositoriesMode

plugins {
    id("org.gradle.toolchains.foojay-resolver-convention") version "1.0.0"
}

rootProject.name = "plurima"

include("core")
include("metrics")
include("spring-boot-starter")
include("test-app")
include("benchmarks")

dependencyResolutionManagement {
    // FAIL_ON_PROJECT_REPOS: dependency repositories are declared centrally here, not in
    // per-project build scripts. No subproject declares its own `repositories {}` block for
    // dependency resolution today (the root build.gradle.kts `repositories {}` under
    // PublishingExtension is a separate DSL for *publishing targets*, not dependency lookup,
    // and is unaffected by this setting) — if one is ever added, the build now fails loudly
    // instead of silently allowing an unreviewed repository into the dependency graph.
    repositoriesMode.set(RepositoriesMode.FAIL_ON_PROJECT_REPOS)
    repositories {
        mavenCentral()
    }
}
