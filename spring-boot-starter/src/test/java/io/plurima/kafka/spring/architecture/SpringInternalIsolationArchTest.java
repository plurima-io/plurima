package io.plurima.kafka.spring.architecture;

import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.lang.ArchRule;
import org.junit.jupiter.api.Test;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.classes;
import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;
import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noMethods;

/**
 * Mirror of core's {@code InternalIsolationArchTest} for the spring-boot-starter module.
 * Without this, @Internal classes in the spring package were not enforced to live in
 * {@code io.plurima.kafka.spring.internal} — only core's package set was checked.
 *
 * <p>The autoconfiguration entry point {@code PlurimaAutoConfiguration} is exempted as a
 * composition root (same justification as core's {@code PlurimaConsumer*}): it wires
 * internal beans into a user-facing Spring autoconfig but its own dependencies on
 * internals are private (return types and bean names are non-internal).
 */
class SpringInternalIsolationArchTest {

    private static final JavaClasses SPRING_CLASSES = new ClassFileImporter()
        .withImportOption(ImportOption.Predefined.DO_NOT_INCLUDE_TESTS)
        .importPackages("io.plurima.kafka.spring");

    @Test
    void publicApiMustNotDependOnInternalPackages() {
        ArchRule rule = noClasses()
            .that().resideOutsideOfPackages("..internal..")
            .and().resideInAPackage("io.plurima.kafka.spring..")
            // Composition root: autoconfig wires beans, same convention as core's PlurimaConsumer*.
            .and().haveSimpleNameNotStartingWith("PlurimaAutoConfiguration")
            .should().dependOnClassesThat()
            .resideInAPackage("..internal..")
            .because("public Spring API (excluding the autoconfig composition root) must not depend on internals");

        rule.check(SPRING_CLASSES);
    }

    @Test
    void internalClassesMustResideInInternalPackages() {
        ArchRule rule = classes()
            .that().areAnnotatedWith(io.plurima.kafka.annotation.Internal.class)
            .should().resideInAPackage("..internal..")
            .because("classes annotated @Internal must live in a *.internal.* package (ADR-012)");

        rule.check(SPRING_CLASSES);
    }

    /**
     * The class-level dependency rule above is composition-root-exempt — PlurimaAutoConfiguration
     * can reference internal classes in its bytecode because it wires them. But the BINARY
     * SURFACE of PlurimaAutoConfiguration (its {@code public} method signatures) MUST NOT
     * expose internal types, or downstream user code would compile-time-couple to them via
     * the autoconfig. {@code @Bean} methods that return internal beans must be
     * package-private; Spring still discovers them reflectively.
     */
    @Test
    void publicMethodsOutsideInternalMustNotReturnInternalTypes() {
        ArchRule rule = noMethods()
            .that().arePublic()
            .and().areDeclaredInClassesThat().resideInAPackage("io.plurima.kafka.spring..")
            .and().areDeclaredInClassesThat().resideOutsideOfPackages("..internal..")
            .should().haveRawReturnType(
                com.tngtech.archunit.base.DescribedPredicate.describe(
                    "a class in an internal package",
                    c -> c.getPackageName().contains(".internal")))
            .because("public method signatures on non-internal Spring classes must not expose "
                + "internal return types — keep @Bean methods package-private if they return @Internal beans");

        rule.check(SPRING_CLASSES);
    }

    /**
     * The companion check for parameter types. Without this, a public method
     * {@code public void foo(PlurimaListenerPostProcessor pp)} on a non-internal class
     * would slip through the return-type-only rule above, still exposing the internal
     * type in the autoconfig's binary surface (a caller would have to depend on
     * {@code PlurimaListenerPostProcessor} to invoke it).
     */
    @Test
    void publicMethodsOutsideInternalMustNotAcceptInternalParameterTypes() {
        com.tngtech.archunit.base.DescribedPredicate<com.tngtech.archunit.core.domain.JavaClass> inInternalPackage =
            com.tngtech.archunit.base.DescribedPredicate.describe(
                "a class in an internal package",
                c -> c.getPackageName().contains(".internal"));

        ArchRule rule = noMethods()
            .that().arePublic()
            .and().areDeclaredInClassesThat().resideInAPackage("io.plurima.kafka.spring..")
            .and().areDeclaredInClassesThat().resideOutsideOfPackages("..internal..")
            .should(new com.tngtech.archunit.lang.ArchCondition<com.tngtech.archunit.core.domain.JavaMethod>(
                "have a parameter type in an internal package") {
                @Override
                public void check(com.tngtech.archunit.core.domain.JavaMethod method,
                                  com.tngtech.archunit.lang.ConditionEvents events) {
                    for (com.tngtech.archunit.core.domain.JavaClass paramType : method.getRawParameterTypes()) {
                        if (inInternalPackage.test(paramType)) {
                            events.add(com.tngtech.archunit.lang.SimpleConditionEvent.satisfied(
                                method,
                                method.getFullName() + " accepts internal type "
                                    + paramType.getName()));
                        }
                    }
                }
            })
            .because("public method signatures on non-internal Spring classes must not accept "
                + "internal parameter types — same reasoning as the return-type rule");

        rule.check(SPRING_CLASSES);
    }
}
