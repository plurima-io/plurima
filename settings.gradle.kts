plugins {
    id("org.gradle.toolchains.foojay-resolver-convention") version "1.0.0"
}

rootProject.name = "kafka-plurima"

include("core")
include("metrics")
include("spring-boot-starter")

dependencyResolutionManagement {
    repositories {
        mavenCentral()
    }
}
