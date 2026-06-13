package io.plurima.kafka.annotation.processor.internal;

import io.plurima.kafka.annotation.Internal;
import javax.annotation.processing.AbstractProcessor;
import javax.annotation.processing.RoundEnvironment;
import javax.annotation.processing.SupportedAnnotationTypes;
import javax.annotation.processing.SupportedSourceVersion;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.Element;
import javax.lang.model.element.ElementKind;
import javax.lang.model.element.Modifier;
import javax.lang.model.element.PackageElement;
import javax.lang.model.element.TypeElement;
import javax.tools.Diagnostic;
import java.util.Set;

/**
 * Compile-time enforcement of ADR-011: every public top-level type in a
 * non-{@code internal} package must carry one of {@code @Stable},
 * {@code @Experimental}, or {@code @Internal}.
 *
 * <p>Repository modules activate this processor explicitly via Gradle
 * {@code annotationProcessor} dependencies and {@code -processor}. The published runtime
 * jar does not auto-register it, so downstream users are not affected by this internal
 * release-contract check unless they opt in deliberately.
 *
 * <p><b>Limitation:</b> The processor does not self-apply during {@code core}'s own
 * compilation due to the bootstrapping constraint (the processor class must be compiled
 * before it can run). Downstream modules ({@code metrics}, {@code spring-boot-starter})
 * run it explicitly.
 */
@SupportedAnnotationTypes("*")
@SupportedSourceVersion(SourceVersion.RELEASE_21)
@Internal
public class StabilityProcessor extends AbstractProcessor {

    private static final String STABLE = "io.plurima.kafka.annotation.Stable";
    private static final String EXPERIMENTAL = "io.plurima.kafka.annotation.Experimental";
    private static final String INTERNAL = "io.plurima.kafka.annotation.Internal";

    @Override
    public boolean process(Set<? extends TypeElement> annotations, RoundEnvironment env) {
        if (env.processingOver()) {
            return false;
        }
        for (Element root : env.getRootElements()) {
            if (root.getKind() == ElementKind.PACKAGE) continue;
            if (!isTopLevelType(root)) continue;
            if (!root.getModifiers().contains(Modifier.PUBLIC)) continue;

            TypeElement type = (TypeElement) root;
            String pkg = packageOf(type);
            if (pkg.contains(".internal") || pkg.endsWith(".internal")) continue;
            if (!pkg.startsWith("io.plurima.kafka")) continue;

            if (!hasStabilityAnnotation(type)) {
                processingEnv.getMessager().printMessage(
                    Diagnostic.Kind.ERROR,
                    "Public type " + type.getQualifiedName()
                        + " must carry one of @Stable, @Experimental, or @Internal "
                        + "(see design § 13.2 / ADR-011).",
                    type);
            }
        }
        return false;
    }

    private static boolean isTopLevelType(Element e) {
        return switch (e.getKind()) {
            case CLASS, INTERFACE, ENUM, ANNOTATION_TYPE, RECORD -> e.getEnclosingElement() instanceof PackageElement;
            default -> false;
        };
    }

    private static String packageOf(TypeElement type) {
        Element e = type.getEnclosingElement();
        return (e instanceof PackageElement pe) ? pe.getQualifiedName().toString() : "";
    }

    private static boolean hasStabilityAnnotation(TypeElement type) {
        return type.getAnnotationMirrors().stream()
            .map(a -> a.getAnnotationType().toString())
            .anyMatch(n -> n.equals(STABLE) || n.equals(EXPERIMENTAL) || n.equals(INTERNAL));
    }
}
