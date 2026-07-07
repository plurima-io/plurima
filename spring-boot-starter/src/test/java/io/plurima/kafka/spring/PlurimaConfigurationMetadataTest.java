package io.plurima.kafka.spring;

import org.junit.jupiter.api.Test;

import java.io.File;
import java.net.URL;
import java.nio.file.Files;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Guards two build-time facts that are easy to silently break:
 *
 * <ol>
 *   <li>{@code spring-boot-configuration-processor} actually runs alongside
 *   {@code StabilityProcessor} — see the explanatory comment on the {@code -processor}
 *   compiler arg in {@code build.gradle.kts}. Explicitly listing {@code -processor}
 *   disables javac's normal SPI auto-discovery of annotation processors, so adding the
 *   configuration-processor dependency alone is not sufficient; its processor class must
 *   also be named in that list, or IDE/editor metadata for {@code plurima.*} properties
 *   silently stops being generated.</li>
 *   <li>{@code AutoConfiguration.imports} names the real, fully-qualified
 *   {@code PlurimaAutoConfiguration} class. A typo in that file compiles fine (it's a
 *   plain text resource) but silently ships a starter that never autoconfigures
 *   anything — Spring Boot has no build-time way to catch that on its own.</li>
 * </ol>
 */
class PlurimaConfigurationMetadataTest {

    @Test
    void springConfigurationMetadataJsonIsGeneratedForPlurimaProperties() throws Exception {
        URL resource = getClass().getClassLoader().getResource("META-INF/spring-configuration-metadata.json");
        assertThat(resource)
            .as("spring-boot-configuration-processor must be on the -processor list in "
                + "build.gradle.kts (alongside StabilityProcessor) for this file to be "
                + "generated into build/classes/java/main/META-INF/")
            .isNotNull();

        File file = new File(resource.toURI());
        assertThat(file).exists();

        String content = Files.readString(file.toPath());
        assertThat(content)
            .as("generated metadata must document the plurima.enabled property")
            .contains("\"name\": \"plurima.enabled\"");
    }

    @Test
    void autoConfigurationImportsFileNamesTheRealClassExactly() throws Exception {
        URL resource = getClass().getClassLoader()
            .getResource("META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports");
        assertThat(resource).as("AutoConfiguration.imports resource must exist").isNotNull();

        String content = Files.readString(new File(resource.toURI()).toPath()).strip();
        assertThat(content)
            .as("a typo here compiles fine (plain text resource) but silently ships a "
                + "starter that never autoconfigures anything")
            .isEqualTo("io.plurima.kafka.spring.PlurimaAutoConfiguration");
    }
}
