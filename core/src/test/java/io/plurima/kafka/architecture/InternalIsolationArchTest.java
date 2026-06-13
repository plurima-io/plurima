package io.plurima.kafka.architecture;

import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.lang.ArchRule;
import org.junit.jupiter.api.Test;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.classes;
import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;

/**
 * Enforces the convention from ADR-012: classes in {@code io.plurima.kafka.*.internal.*}
 * packages are not part of the public API. External code in non-internal subpackages
 * (e.g. {@code io.plurima.kafka.ack}, {@code .dlt}, {@code .retry}, …) must not depend
 * on internal classes.
 *
 * <p><b>Composition roots are exempted.</b> {@code PlurimaConsumer} and
 * {@code PlurimaConsumerBuilder} live in the root {@code io.plurima.kafka} package and
 * are by design the boundary that wires internal components into a user-facing
 * facade. Their dependencies on internals are private (fields, constructor calls,
 * method bodies); none leak through the public API contract (return types, public
 * method signatures, public fields are all non-internal).
 *
 * <p>This test fails the build if a non-composition-root class accidentally couples
 * to an internal one, surfacing the leak before it ships.
 */
class InternalIsolationArchTest {

    private static final JavaClasses CORE_CLASSES = new ClassFileImporter()
        .withImportOption(ImportOption.Predefined.DO_NOT_INCLUDE_TESTS)
        .importPackages("io.plurima.kafka");

    @Test
    void publicApiMustNotDependOnInternalPackages() {
        ArchRule rule = noClasses()
            .that().resideOutsideOfPackages("..internal..")
            .and().resideInAPackage("io.plurima.kafka..")
            // Composition roots: explicit exception per ADR-012 — they wire internals into a facade.
            .and().haveSimpleNameNotStartingWith("PlurimaConsumer")
            .should().dependOnClassesThat()
            .resideInAPackage("..internal..")
            .because("public API surface (excluding composition roots) must not depend on internals");

        rule.check(CORE_CLASSES);
    }

    @Test
    void internalClassesMustResideInInternalPackages() {
        ArchRule rule = classes()
            .that().areAnnotatedWith(io.plurima.kafka.annotation.Internal.class)
            .should().resideInAPackage("..internal..")
            .because("classes annotated @Internal must live in a *.internal.* package (ADR-012)");

        rule.check(CORE_CLASSES);
    }
}
