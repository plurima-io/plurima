package io.plurima.kafka.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Experimental API. May change or be removed in any minor release.
 *
 * <p>The annotation type itself is committed as part of the stability vocabulary, so it
 * is {@link Stable @Stable(since = "0.1.0")} even though it labels other APIs as
 * experimental. Without this manual marker, the processor would skip it (processors
 * cannot self-apply to their own compilation unit).
 */
@Stable(since = "0.1.0")
@Retention(RetentionPolicy.CLASS)
@Target({ElementType.TYPE, ElementType.METHOD, ElementType.CONSTRUCTOR})
public @interface Experimental {
    /** Free-form description of what may change. */
    String reason() default "";
}
