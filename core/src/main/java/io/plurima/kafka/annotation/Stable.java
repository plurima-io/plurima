package io.plurima.kafka.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Stable API. Subject to the compatibility guarantees of the design § 13.1.
 *
 * <p>The annotation type itself is self-stable: the annotation processor cannot apply to
 * its own compilation unit, so {@code @Stable} is declared manually here.
 */
@Stable(since = "0.1.0")
@Retention(RetentionPolicy.CLASS)
@Target({ElementType.TYPE, ElementType.METHOD, ElementType.CONSTRUCTOR})
public @interface Stable {
    /** Minimum version since this API has been stable. */
    String since();
}
