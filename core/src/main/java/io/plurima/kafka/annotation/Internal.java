package io.plurima.kafka.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Internal API. Not for external use. Lives in {@code *.internal} packages.
 *
 * <p>The annotation type itself is {@link Stable @Stable(since = "0.1.0")} — it's part of
 * the committed stability vocabulary so tooling (including downstream linters) can rely
 * on the name. The processor can't self-apply to its own compilation unit, so this is
 * declared manually.
 *
 * <p>{@link ElementType#PACKAGE} is included so an {@code internal} package's
 * {@code package-info.java} can carry {@code @Internal} at the package level; the
 * {@code io.plurima.kafka.annotation.processor.internal.StabilityProcessor} explicitly skips
 * {@code PACKAGE}-kind elements, so this widening does not change what the processor enforces.
 */
@Stable(since = "0.1.0")
@Retention(RetentionPolicy.CLASS)
@Target({ElementType.TYPE, ElementType.METHOD, ElementType.CONSTRUCTOR, ElementType.PACKAGE})
public @interface Internal {}
