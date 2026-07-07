/**
 * Runtime internals — poll loops, dispatchers, ack/commit bookkeeping, retry/DLT plumbing —
 * wired together by {@link io.plurima.kafka.PlurimaConsumerBuilder#build}. This package
 * carries no compatibility guarantees: types and members here may change or be removed in
 * any release, including patch releases, without notice. Application code must not depend
 * on anything in this package directly; use the public API in {@code io.plurima.kafka} and
 * its non-{@code internal} sub-packages instead.
 */
@Internal
@NullMarked
package io.plurima.kafka.internal;

import io.plurima.kafka.annotation.Internal;
import org.jspecify.annotations.NullMarked;
