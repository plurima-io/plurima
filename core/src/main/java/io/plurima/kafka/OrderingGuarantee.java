package io.plurima.kafka;

import io.plurima.kafka.annotation.Stable;

/**
 * User-asserted scope of the ordering promise. Acts as a build-time check that
 * the configured {@link ConsumerEngine} can actually deliver what the user
 * expects.
 *
 * <p>The guarantee is OPTIONAL on the builder. If unset, it is inferred from
 * {@code (engine, ordering)}:
 *
 * <table>
 *   <caption>Inferred guarantee matrix (v0.1+)</caption>
 *   <tr><th>Engine</th><th>OrderingMode</th><th>Inferred guarantee</th></tr>
 *   <tr><td>SHARE</td><td>UNORDERED</td><td>LOCAL (vacuous — nothing to order)</td></tr>
 *   <tr><td>SHARE</td><td>KEY or PARTITION</td><td><b>rejected at build time</b> — share
 *     groups can deliver same-key / same-partition records to any member of the group,
 *     so in-process ordering cannot extend to the cluster. The builder throws
 *     {@code IllegalArgumentException}; use {@link ConsumerEngine#CLASSIC_BASIC} for
 *     cross-cluster KEY or PARTITION ordering.</td></tr>
 *   <tr><td>CLASSIC_BASIC</td><td>UNORDERED</td><td>LOCAL (vacuous — nothing to order;
 *     inferring STRICT here would conflict with the {@code UNORDERED + STRICT}
 *     rejection below)</td></tr>
 *   <tr><td>CLASSIC_BASIC</td><td>KEY or PARTITION</td><td>STRICT (cross-cluster, via
 *     consumer-group exclusive partition ownership)</td></tr>
 * </table>
 *
 * <p>When the user sets the guarantee explicitly, the builder validates against the
 * matrix. The remaining checkable rejection on this path is
 * {@code (any-engine, UNORDERED, STRICT)} — STRICT requires an ordering relation,
 * which UNORDERED disclaims by definition. (Pre-v0.1 the builder also rejected
 * {@code (SHARE, KEY|PARTITION, STRICT)} via this path, but in v0.1 the SHARE+KEY /
 * SHARE+PARTITION combinations are rejected outright, before the guarantee check
 * runs at all.)
 *
 * @since 0.1.0
 */
@Stable(since = "0.1.0")
public enum OrderingGuarantee {

    /**
     * Ordering applies only within a single consumer instance. The inferred guarantee
     * for any {@code UNORDERED} combination (both engines) — vacuous, since there is
     * no ordering relation to scope. Asserting LOCAL explicitly on UNORDERED is
     * allowed but adds nothing.
     *
     * <p>Pre-v0.1, this was also the inferred guarantee for {@code SHARE + KEY} and
     * {@code SHARE + PARTITION} — but those combinations are now rejected at build
     * time, so LOCAL is no longer reachable through them.
     */
    LOCAL,

    /**
     * Ordering applies across all consumer instances in the same group. Only
     * achievable with {@link ConsumerEngine#CLASSIC_BASIC} + {@link OrderingMode#KEY}
     * or {@link OrderingMode#PARTITION}, where the broker pins each partition to
     * exactly one consumer at a time.
     */
    STRICT
}
