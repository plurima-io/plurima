package io.plurima.kafka.annotation;

import io.plurima.kafka.annotation.processor.internal.StabilityProcessor;
import org.junit.jupiter.api.Test;

import javax.tools.DiagnosticCollector;
import javax.tools.JavaCompiler;
import javax.tools.JavaFileObject;
import javax.tools.SimpleJavaFileObject;
import javax.tools.ToolProvider;
import java.net.URI;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class StabilityProcessorTest {

    @Test
    void publicTypeOutsideInternalWithoutAnnotationIsRejected() {
        String src = """
            package io.plurima.kafka.test;
            public class WithoutAnnotation {}
            """;

        DiagnosticCollector<JavaFileObject> diagnostics = compile("WithoutAnnotation", src);

        assertThat(diagnostics.getDiagnostics())
            .anySatisfy(d -> {
                assertThat(d.getKind()).isEqualTo(javax.tools.Diagnostic.Kind.ERROR);
                assertThat(d.getMessage(null)).contains("must carry one of @Stable");
            });
    }

    @Test
    void publicTypeWithStableAnnotationPasses() {
        String src = """
            package io.plurima.kafka.test;
            import io.plurima.kafka.annotation.Stable;
            @Stable(since = "0.1.0")
            public class WithStable {}
            """;

        DiagnosticCollector<JavaFileObject> diagnostics = compile("WithStable", src);

        assertThat(diagnostics.getDiagnostics())
            .noneMatch(d -> d.getKind() == javax.tools.Diagnostic.Kind.ERROR);
    }

    @Test
    void publicTypeWithExperimentalAnnotationPasses() {
        String src = """
            package io.plurima.kafka.test;
            import io.plurima.kafka.annotation.Experimental;
            @Experimental
            public class WithExperimental {}
            """;

        DiagnosticCollector<JavaFileObject> diagnostics = compile("WithExperimental", src);

        assertThat(diagnostics.getDiagnostics())
            .noneMatch(d -> d.getKind() == javax.tools.Diagnostic.Kind.ERROR);
    }

    @Test
    void typeInInternalPackageDoesNotNeedAnnotation() {
        String src = """
            package io.plurima.kafka.test.internal;
            public class InternalClass {}
            """;

        DiagnosticCollector<JavaFileObject> diagnostics = compile("InternalClass", src);

        // Internal packages don't require @Stable/@Experimental even on the public type.
        assertThat(diagnostics.getDiagnostics())
            .noneMatch(d -> d.getKind() == javax.tools.Diagnostic.Kind.ERROR
                && d.getMessage(null).contains("must carry one of @Stable"));
    }

    @Test
    void packagePrivateTypeDoesNotNeedAnnotation() {
        String src = """
            package io.plurima.kafka.test;
            class PackagePrivate {}
            """;

        DiagnosticCollector<JavaFileObject> diagnostics = compile("PackagePrivate", src);

        // Package-private types are not part of the public API.
        assertThat(diagnostics.getDiagnostics())
            .noneMatch(d -> d.getKind() == javax.tools.Diagnostic.Kind.ERROR
                && d.getMessage(null).contains("must carry one of @Stable"));
    }

    private static DiagnosticCollector<JavaFileObject> compile(String className, String source) {
        JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
        DiagnosticCollector<JavaFileObject> diagnostics = new DiagnosticCollector<>();

        JavaFileObject sourceFile = new SimpleJavaFileObject(
                URI.create("string:///" + className + ".java"), JavaFileObject.Kind.SOURCE) {
            @Override
            public CharSequence getCharContent(boolean ignoreEncodingErrors) { return source; }
        };

        var task = compiler.getTask(
            null, null, diagnostics,
            List.of("-proc:only"),  // only run annotation processing, don't actually compile
            null,
            List.of(sourceFile));
        task.setProcessors(List.of(new StabilityProcessor()));
        task.call();
        return diagnostics;
    }
}
