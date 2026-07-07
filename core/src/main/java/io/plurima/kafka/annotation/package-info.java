/**
 * Plurima's stability vocabulary — {@link io.plurima.kafka.annotation.Stable},
 * {@link io.plurima.kafka.annotation.Experimental}, and
 * {@link io.plurima.kafka.annotation.Internal} — used to mark every public top-level type's
 * compatibility guarantee per design § 13.2 / ADR-011. The
 * {@code io.plurima.kafka.annotation.processor} sub-package holds the compile-time checker
 * that enforces every public non-{@code internal} type carries one of these three.
 */
@NullMarked
package io.plurima.kafka.annotation;

import org.jspecify.annotations.NullMarked;
