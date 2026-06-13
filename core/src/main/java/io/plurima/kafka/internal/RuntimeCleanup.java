package io.plurima.kafka.internal;

import io.plurima.kafka.annotation.Internal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Small helpers for runtime shutdown / partial-startup cleanup. Keeps
 * {@link ClassicBasicRuntime} and {@link ShareConsumerRuntime} free of the
 * repeated {@code try { x.close(); } catch (Exception ignored) {}} pattern.
 */
@Internal
final class RuntimeCleanup {

    private static final Logger log = LoggerFactory.getLogger(RuntimeCleanup.class);

    private RuntimeCleanup() {}

    @FunctionalInterface
    interface ThrowingAction {
        void run() throws Exception;
    }

    /** Run an action, swallowing any exception. Use only on cleanup paths where the caller is about to rethrow. */
    static void quietly(ThrowingAction action) {
        try {
            action.run();
        } catch (Exception ignored) {
            // cleanup path — the original failure is being rethrown by the caller
        }
    }

    /** Run an action, logging at WARN if it throws. Use for resource-close on the normal shutdown path. */
    static void logIfRaised(String name, ThrowingAction action) {
        try {
            action.run();
        } catch (Exception e) {
            log.warn("{} close raised", name, e);
        }
    }

    /** Join a thread with the given timeout, restoring the interrupt flag if interrupted. */
    static void joinQuietly(Thread thread, long millis) {
        try {
            thread.join(millis);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }
    }
}
