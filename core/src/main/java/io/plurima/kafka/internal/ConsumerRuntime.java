package io.plurima.kafka.internal;

import io.plurima.kafka.PlurimaConsumer;
import io.plurima.kafka.annotation.Internal;

/**
 * Engine-agnostic lifecycle handle for the underlying consumer pipeline.
 *
 * <p>{@link io.plurima.kafka.PlurimaConsumer} is a thin facade: at start time it
 * picks a runtime implementation based on the configured
 * {@link io.plurima.kafka.ConsumerEngine ConsumerEngine} and delegates the
 * lifecycle here. Two implementations:
 *
 * <ul>
 *   <li>{@link ShareConsumerRuntime} — KIP-932 {@code KafkaShareConsumer} pipeline.
 *       The original Plurima engine; per-record acquisition + RELEASE, broker-side
 *       delivery counting, and the drain barrier. UNORDERED mode only as of v0.1.</li>
 *   <li>{@code ClassicBasicRuntime} — vanilla {@code KafkaConsumer} pipeline.
 *       Continuous-poll loop with pause/resume backpressure and mode-tailored
 *       dispatch (per-record for UNORDERED, key-sharded for KEY, partition-serial
 *       for PARTITION). Provides cross-cluster STRICT ordering on KEY/PARTITION at
 *       the cost of share-group features.</li>
 * </ul>
 *
 * <p>The interface is intentionally minimal: {@code start()} and {@code close()}.
 * Everything else (state, threads, metrics wiring, etc.) is the runtime's private
 * concern. {@code PlurimaConsumer}'s public surface does not depend on which
 * runtime is active.
 */
@Internal
public interface ConsumerRuntime extends AutoCloseable {

    /**
     * Begins consuming. Must be called exactly once per runtime instance and
     * before {@link #close()}. Idempotency is the caller's responsibility
     * ({@code PlurimaConsumer} holds a lifecycle lock + started/closed flags).
     *
     * @throws RuntimeException if the underlying client cannot be constructed or
     *     subscribed; the runtime should roll back any partially-acquired
     *     resources before throwing.
     */
    void start();

    /**
     * Shuts down the runtime. Implementations MUST tolerate being called from a
     * different thread than {@code start()}, and MUST be safe to invoke even if
     * {@code start()} threw partway through (i.e. release whatever was created).
     */
    @Override
    void close();

    /**
     * Current lifecycle state, backing {@link PlurimaConsumer#state()}. Implementations
     * start at {@code RUNNING} once {@link #start()} has published all its fields and the
     * poll thread is live, move to {@code CLOSED} on a clean {@link #close()}, or to
     * {@code FAILED} when the poll thread hits an unrecoverable error and self-closes
     * (see {@code PollLoop} / {@code ClassicPollLoop}'s fatal-error handling). A
     * {@code FAILED} transition always wins over a later {@code close()} call, and a
     * fatal error observed after an already-completed clean close leaves the state at
     * {@code CLOSED} — whichever transition CAS'd the runtime's close-guard first is
     * authoritative. {@code NEW} is never returned here; {@code PlurimaConsumer} reports
     * it itself before a runtime exists.
     */
    PlurimaConsumer.State state();
}
