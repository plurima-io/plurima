package io.plurima.kafka.architecture;

import com.tngtech.archunit.core.domain.JavaClass;
import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.domain.JavaMethod;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.lang.ArchCondition;
import com.tngtech.archunit.lang.ArchRule;
import com.tngtech.archunit.lang.ConditionEvents;
import com.tngtech.archunit.lang.SimpleConditionEvent;
import org.junit.jupiter.api.Test;

import java.util.function.Predicate;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.classes;
import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.methods;
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

    /**
     * Task B2: Kafka's {@code AcknowledgeType} exposes {@code RENEW}, which Plurima rejects at
     * runtime, and forces handler code to depend on the Kafka client just to name an ack type.
     * Plurima's own {@link io.plurima.kafka.ack.AckType} replaces it everywhere on the public
     * surface — this rule guards against a future public method quietly reintroducing the Kafka
     * type. Internal plumbing (the actual {@code ShareConsumer.acknowledge} boundary) is exempt.
     */
    @Test
    void publicApiMustNotReferenceKafkaAcknowledgeType() {
        String kafkaAcknowledgeType = "org.apache.kafka.clients.consumer.AcknowledgeType";
        checkPublicApiDoesNotReference(
            type -> type.getFullName().equals(kafkaAcknowledgeType),
            kafkaAcknowledgeType,
            "public API surface (excluding internals) must depend on Plurima's own "
                + "AckType, not Kafka's AcknowledgeType (which exposes the invalid RENEW value "
                + "and forces a Kafka-client dependency on handler code)");
    }

    /**
     * Task B3: {@link io.plurima.kafka.Message#headers()} now returns Plurima's own
     * {@link io.plurima.kafka.MessageHeaders} view instead of Kafka's {@code Headers}/{@code
     * Header}, so handler code that only reads headers never needs the Kafka client on its
     * classpath. This rule guards against a future public method quietly reintroducing any type
     * from Kafka's header package. Internal plumbing (record construction, DLT header
     * propagation, etc.) is exempt.
     */
    @Test
    void publicApiMustNotReferenceKafkaHeaderTypes() {
        String kafkaHeaderPackage = "org.apache.kafka.common.header";
        checkPublicApiDoesNotReference(
            type -> type.getPackageName().equals(kafkaHeaderPackage)
                || type.getPackageName().startsWith(kafkaHeaderPackage + "."),
            kafkaHeaderPackage + "..",
            "public API surface (excluding internals) must depend on Plurima's own "
                + "MessageHeaders view, not any type from Kafka's header package (which would "
                + "force a Kafka-client dependency on handler code that only reads headers)");
    }

    /**
     * Shared machinery for {@code publicApiMustNotReference*} rules: fails the build if any
     * public method declared in a non-internal {@code io.plurima.kafka..} class references (as
     * return type or parameter type) a type matched by {@code forbidden}.
     */
    private static void checkPublicApiDoesNotReference(
            Predicate<JavaClass> forbidden,
            String forbiddenDescription,
            String reason) {
        ArchRule rule = methods()
            .that().arePublic()
            .and().areDeclaredInClassesThat().resideInAPackage("io.plurima.kafka..")
            .and().areDeclaredInClassesThat().resideOutsideOfPackage("..internal..")
            .should(new ArchCondition<JavaMethod>(
                "not reference " + forbiddenDescription + " in their signature") {
                @Override
                public void check(JavaMethod method, ConditionEvents events) {
                    boolean referencesIt = forbidden.test(method.getRawReturnType())
                        || method.getRawParameterTypes().stream().anyMatch(forbidden);
                    if (referencesIt) {
                        events.add(SimpleConditionEvent.violated(method,
                            method.getFullName() + " references " + forbiddenDescription));
                    } else {
                        events.add(SimpleConditionEvent.satisfied(method,
                            method.getFullName() + " does not reference " + forbiddenDescription));
                    }
                }
            })
            .because(reason);

        rule.check(CORE_CLASSES);
    }
}
