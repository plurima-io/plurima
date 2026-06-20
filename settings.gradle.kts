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
    repositories {
        mavenCentral()
    }
}
