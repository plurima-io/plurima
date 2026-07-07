package io.plurima.kafka.internal;

import io.plurima.kafka.ConsumerEngine;
import io.plurima.kafka.OrderingMode;
import io.plurima.kafka.PlurimaConsumer;
import io.plurima.kafka.RecordListener;
import io.plurima.kafka.deserializer.RecordDeserializer;
import io.plurima.kafka.metrics.PlurimaMetrics;
import io.plurima.kafka.retry.RetryPolicy;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Task B6 — liveness + fatal-error surface. Verifies the {@code onFatal} hook that
 * {@link PollLoop} (SHARE engine) and {@link ClassicPollLoop} (CLASSIC_BASIC engine) invoke
 * from their poll thread's {@code run()} finally block: it fires INSTEAD OF the plain
 * {@code onLoopExit} callback, ONLY when the loop exited via the generic
 * {@code catch (Throwable t)} branch (an unrecoverable error), carrying that exact
 * {@code Throwable}. {@code ShareConsumerRuntime.handleFatal} / {@code ClassicBasicRuntime
 * .handleFatal} wire this hook to the FAILED-state transition, self-close, and the user's
 * {@code PlurimaConsumerBuilder#onFatalError} callback (in that order) — those runtime classes
 * aren't directly unit-testable without a real broker (their {@code start()} constructs a real
 * {@code KafkaConsumer}/{@code KafkaShareConsumer}), so this test exercises the mechanism one
 * layer down, at the fake/mock-driven poll-loop level both runtimes delegate to.
 */
class ConsumerStateTest {

    private Thread loopThread;

    @AfterEach
    void tearDown() throws InterruptedException {
        if (loopThread != null) {
            loopThread.interrupt();
            loopThread.join(5_000);
        }
    }

    // ---------------------------------------------------------------- SHARE engine (PollLoop)

    @Test
    void shareFatalPollErrorInvokesOnFatalInsteadOfOnLoopExit() throws Exception {
        FakeShareConsumer consumer = new FakeShareConsumer();
        consumer.subscribe(List.of("t"));
        RuntimeException boom = new RuntimeException("boom");
        consumer.throwOnNextPoll(boom);

        InFlightRegistry registry = new InFlightRegistry();
        AckCoordinator coordinator = new AckCoordinator(registry);
        BackpressureGate gate = new BackpressureGate(10);
        WorkDispatcher noop = r -> { };

        AtomicReference<Throwable> capturedFatal = new AtomicReference<>();
        AtomicBoolean onLoopExitCalled = new AtomicBoolean();

        PollLoop loop = new PollLoop(
            consumer, noop, coordinator, registry, gate,
            Duration.ofMillis(50), Duration.ofSeconds(30), Duration.ofSeconds(5),
            Duration.ofSeconds(30), PlurimaMetrics.noOp(), "t", "g1",
            /* onLoopExit */ () -> onLoopExitCalled.set(true),
            /* latencyWindow */ null, /* adaptiveConfig */ null,
            /* lockDurationExplicitlySet */ true,
            /* onFatal */ capturedFatal::set);

        loopThread = new Thread(loop, "share-fatal-poll-loop");
        loopThread.start();
        loopThread.join(5_000);

        assertThat(loopThread.isAlive())
            .as("poll thread must exit after the fatal Throwable")
            .isFalse();
        assertThat(capturedFatal.get())
            .as("onFatal must receive the exact Throwable that killed the loop")
            .isSameAs(boom);
        assertThat(onLoopExitCalled.get())
            .as("onFatal takes over from onLoopExit on the fatal path — calling both would "
                + "double-run ShareConsumerRuntime's close logic and race the state transition")
            .isFalse();
    }

    @Test
    void shareCleanShutdownInvokesOnLoopExitNotOnFatal() throws Exception {
        FakeShareConsumer consumer = new FakeShareConsumer();
        consumer.subscribe(List.of("t"));

        InFlightRegistry registry = new InFlightRegistry();
        AckCoordinator coordinator = new AckCoordinator(registry);
        BackpressureGate gate = new BackpressureGate(10);
        WorkDispatcher noop = r -> { };

        AtomicReference<Throwable> capturedFatal = new AtomicReference<>();
        AtomicBoolean onLoopExitCalled = new AtomicBoolean();

        PollLoop loop = new PollLoop(
            consumer, noop, coordinator, registry, gate,
            Duration.ofMillis(50), Duration.ofSeconds(30), Duration.ofSeconds(5),
            Duration.ofSeconds(30), PlurimaMetrics.noOp(), "t", "g1",
            /* onLoopExit */ () -> onLoopExitCalled.set(true),
            /* latencyWindow */ null, /* adaptiveConfig */ null,
            /* lockDurationExplicitlySet */ true,
            /* onFatal */ capturedFatal::set);

        loopThread = new Thread(loop, "share-clean-shutdown-loop");
        loopThread.start();
        Thread.sleep(100);
        loop.shutdown();
        loopThread.join(5_000);

        assertThat(loopThread.isAlive()).isFalse();
        assertThat(onLoopExitCalled.get())
            .as("a normal shutdown must still run the plain onLoopExit cleanup")
            .isTrue();
        assertThat(capturedFatal.get())
            .as("onFatal must not fire on a clean shutdown — there was no fatal error")
            .isNull();
    }

    @Test
    void shareOnFatalCallbackThrowingIsCaughtAndLoggedNotPropagated() throws Exception {
        FakeShareConsumer consumer = new FakeShareConsumer();
        consumer.subscribe(List.of("t"));
        consumer.throwOnNextPoll(new RuntimeException("boom"));

        InFlightRegistry registry = new InFlightRegistry();
        AckCoordinator coordinator = new AckCoordinator(registry);
        BackpressureGate gate = new BackpressureGate(10);
        WorkDispatcher noop = r -> { };

        AtomicBoolean callbackInvoked = new AtomicBoolean();
        Consumer<Throwable> throwingCallback = t -> {
            callbackInvoked.set(true);
            throw new IllegalStateException("callback boom");
        };

        PollLoop loop = new PollLoop(
            consumer, noop, coordinator, registry, gate,
            Duration.ofMillis(50), Duration.ofSeconds(30), Duration.ofSeconds(5),
            Duration.ofSeconds(30), PlurimaMetrics.noOp(), "t", "g1",
            /* onLoopExit */ () -> { },
            /* latencyWindow */ null, /* adaptiveConfig */ null,
            /* lockDurationExplicitlySet */ true,
            /* onFatal */ throwingCallback);

        AtomicReference<Throwable> uncaught = new AtomicReference<>();
        loopThread = new Thread(loop, "share-fatal-callback-throws-loop");
        loopThread.setUncaughtExceptionHandler((thread, ex) -> uncaught.set(ex));
        loopThread.start();
        loopThread.join(5_000);

        assertThat(loopThread.isAlive()).isFalse();
        assertThat(callbackInvoked.get()).isTrue();
        assertThat(uncaught.get())
            .as("a throwing onFatal callback must be caught+logged inside PollLoop, never "
                + "propagate out of the poll thread")
            .isNull();
    }

    // -------------------------------------------------------- CLASSIC_BASIC engine (ClassicPollLoop)

    @Test
    @SuppressWarnings("unchecked")
    void classicFatalPollErrorInvokesOnFatalInsteadOfOnLoopExit() throws Exception {
        KafkaConsumer<byte[], byte[]> consumer = mock(KafkaConsumer.class);
        RuntimeException boom = new RuntimeException("classic boom");
        when(consumer.poll(any(Duration.class))).thenThrow(boom);
        WorkerLauncher launcher = new WorkerLauncher();
        try {
            RecordListener<byte[], byte[]> listener = (r, ctx) -> { };
            AtomicReference<Throwable> capturedFatal = new AtomicReference<>();
            AtomicBoolean onLoopExitCalled = new AtomicBoolean();

            ClassicPollLoop<byte[], byte[]> loop = new ClassicPollLoop<>(
                consumer, "t", "g1", listener,
                RecordDeserializer.bytes(), RecordDeserializer.bytes(),
                OrderingMode.UNORDERED,
                new RetryEngine(RetryPolicy.noRetry()),
                /* dltRouter */ null,
                Duration.ofMillis(50),
                /* shutdownDrainTimeoutMs */ 1_000L,
                /* concurrency */ 8,
                /* shardCount */ 16,
                PlurimaMetrics.noOp(), launcher,
                /* onLoopExit */ () -> onLoopExitCalled.set(true),
                /* onFatal */ capturedFatal::set);

            loopThread = new Thread(loop, "classic-fatal-poll-loop");
            loopThread.start();
            loopThread.join(5_000);

            assertThat(loopThread.isAlive())
                .as("poll thread must exit after the fatal Throwable")
                .isFalse();
            assertThat(capturedFatal.get())
                .as("onFatal must receive the exact Throwable that killed the loop")
                .isSameAs(boom);
            assertThat(onLoopExitCalled.get())
                .as("onFatal takes over from onLoopExit on the fatal path")
                .isFalse();
        } finally {
            launcher.close();
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    void classicCleanShutdownInvokesOnLoopExitNotOnFatal() throws Exception {
        KafkaConsumer<byte[], byte[]> consumer = mock(KafkaConsumer.class);
        when(consumer.poll(any(Duration.class))).thenReturn(
            new org.apache.kafka.clients.consumer.ConsumerRecords<>(java.util.Map.of(), java.util.Map.of()));
        WorkerLauncher launcher = new WorkerLauncher();
        try {
            RecordListener<byte[], byte[]> listener = (r, ctx) -> { };
            AtomicReference<Throwable> capturedFatal = new AtomicReference<>();
            AtomicBoolean onLoopExitCalled = new AtomicBoolean();

            ClassicPollLoop<byte[], byte[]> loop = new ClassicPollLoop<>(
                consumer, "t", "g1", listener,
                RecordDeserializer.bytes(), RecordDeserializer.bytes(),
                OrderingMode.UNORDERED,
                new RetryEngine(RetryPolicy.noRetry()),
                /* dltRouter */ null,
                Duration.ofMillis(50),
                /* shutdownDrainTimeoutMs */ 1_000L,
                /* concurrency */ 8,
                /* shardCount */ 16,
                PlurimaMetrics.noOp(), launcher,
                /* onLoopExit */ () -> onLoopExitCalled.set(true),
                /* onFatal */ capturedFatal::set);

            loopThread = new Thread(loop, "classic-clean-shutdown-loop");
            loopThread.start();
            Thread.sleep(100);
            loop.shutdown();
            loopThread.join(5_000);

            assertThat(loopThread.isAlive()).isFalse();
            assertThat(onLoopExitCalled.get())
                .as("a normal shutdown must still run the plain onLoopExit cleanup")
                .isTrue();
            assertThat(capturedFatal.get())
                .as("onFatal must not fire on a clean shutdown — there was no fatal error")
                .isNull();
        } finally {
            launcher.close();
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    void classicOnFatalCallbackThrowingIsCaughtAndLoggedNotPropagated() throws Exception {
        KafkaConsumer<byte[], byte[]> consumer = mock(KafkaConsumer.class);
        when(consumer.poll(any(Duration.class))).thenThrow(new RuntimeException("classic boom"));
        WorkerLauncher launcher = new WorkerLauncher();
        try {
            RecordListener<byte[], byte[]> listener = (r, ctx) -> { };
            AtomicBoolean callbackInvoked = new AtomicBoolean();
            Consumer<Throwable> throwingCallback = t -> {
                callbackInvoked.set(true);
                throw new IllegalStateException("callback boom");
            };

            ClassicPollLoop<byte[], byte[]> loop = new ClassicPollLoop<>(
                consumer, "t", "g1", listener,
                RecordDeserializer.bytes(), RecordDeserializer.bytes(),
                OrderingMode.UNORDERED,
                new RetryEngine(RetryPolicy.noRetry()),
                /* dltRouter */ null,
                Duration.ofMillis(50),
                /* shutdownDrainTimeoutMs */ 1_000L,
                /* concurrency */ 8,
                /* shardCount */ 16,
                PlurimaMetrics.noOp(), launcher,
                /* onLoopExit */ () -> { },
                /* onFatal */ throwingCallback);

            AtomicReference<Throwable> uncaught = new AtomicReference<>();
            loopThread = new Thread(loop, "classic-fatal-callback-throws-loop");
            loopThread.setUncaughtExceptionHandler((thread, ex) -> uncaught.set(ex));
            loopThread.start();
            loopThread.join(5_000);

            assertThat(loopThread.isAlive()).isFalse();
            assertThat(callbackInvoked.get()).isTrue();
            assertThat(uncaught.get())
                .as("a throwing onFatal callback must be caught+logged inside ClassicPollLoop, "
                    + "never propagate out of the poll thread")
                .isNull();
        } finally {
            launcher.close();
        }
    }

    // ------------------------------------------------------------ public API surface (builder)

    @Test
    void builderOnFatalErrorRejectsNullCallback() {
        var builder = io.plurima.kafka.PlurimaConsumer.builder();
        assertThat(org.junit.jupiter.api.Assertions.assertThrows(
            NullPointerException.class, () -> builder.onFatalError(null)))
            .hasMessageContaining("onFatalError");
    }

    // ------------------------------------------------------- runtime state machine (B6 core)
    //
    // The tests above cover the onFatal *hook wiring* one layer below the runtimes. These
    // cover the actual deliverable: PlurimaConsumer.State transitions on the two concrete
    // ConsumerRuntime implementations (ClassicBasicRuntime, ShareConsumerRuntime), which is
    // what PlurimaConsumer#state()/#isRunning() report.
    //
    // start() constructs a real KafkaConsumer/KafkaShareConsumer, but never blocks on
    // broker connectivity: both runtimes publish state = RUNNING and every other field
    // BEFORE starting the poll thread (see the "publish runtime fields BEFORE t.start()"
    // comments in both classes), and the underlying Kafka client resolves/connects lazily
    // inside poll() on the poll thread, not in the constructor or subscribe(). So pointing
    // bootstrap.servers at a closed local port (localhost:1) reaches RUNNING deterministically
    // with no broker, and close()/shutdown() use consumer.wakeup() to interrupt an in-flight
    // poll() immediately rather than waiting out pollTimeout — both engines close in single-
    // digit-to-low-double-digit milliseconds in this configuration (measured while writing
    // this test), so no test here should be able to hang.

    private static Properties bogusBrokerProps(boolean share) {
        Properties p = new Properties();
        p.put("bootstrap.servers", "localhost:1"); // syntactically valid, nothing listening
        p.put("group.id", "consumer-state-test-" + UUID.randomUUID());
        p.put("request.timeout.ms", "2000");
        if (!share) {
            // share groups reject session.timeout.ms/heartbeat.interval.ms outright
            // (ShareConsumerConfig.checkUnsupportedConfigsPreProcess) — classic-only.
            p.put("session.timeout.ms", "6000");
            p.put("heartbeat.interval.ms", "2000");
        }
        p.put("reconnect.backoff.ms", "50");
        p.put("reconnect.backoff.max.ms", "200");
        return p;
    }

    /**
     * Invokes the private {@code handleFatal(Throwable)} on a runtime directly. Neither
     * runtime exposes a way to make a real {@code KafkaConsumer}/{@code KafkaShareConsumer}
     * throw fatally from inside {@code poll()} without a live broker (an unreachable broker
     * makes {@code poll()} retry/return-empty, never throw — see the class comment on
     * {@link #shareFatalPollErrorInvokesOnFatalInsteadOfOnLoopExit()} above for the
     * lower-level mechanism, which IS exercised with a fake/mocked consumer). Reflection on
     * the already-verified {@code handleFatal} is the only remaining way to exercise the
     * runtime's own state-machine contract (the actual B6 deliverable) without adding a
     * production-only test seam or a real broker.
     */
    private static void invokeHandleFatal(Object runtime, Throwable fatal) throws Exception {
        Method m = runtime.getClass().getDeclaredMethod("handleFatal", Throwable.class);
        m.setAccessible(true);
        m.invoke(runtime, fatal);
    }

    @Test
    void classicRuntimeState_newRunningClosed() throws Exception {
        ClassicBasicRuntime<byte[], byte[]> rt = new ClassicBasicRuntime<>(
            bogusBrokerProps(false), "t", (r, ctx) -> { },
            RecordDeserializer.bytes(), RecordDeserializer.bytes(),
            OrderingMode.UNORDERED, 8, 16, RetryPolicy.noRetry(), null,
            Duration.ofMillis(50), Duration.ofSeconds(5), PlurimaMetrics.noOp(), t -> { });
        try {
            assertThat(rt.state()).as("before start()").isEqualTo(PlurimaConsumer.State.NEW);

            assertTimeoutPreemptively(Duration.ofSeconds(10), rt::start);
            assertThat(rt.state())
                .as("RUNNING must be reached without ever contacting a broker — the poll "
                    + "thread's poll() may retry/fail against the unreachable bootstrap "
                    + "address, but state is published before the poll thread even starts")
                .isEqualTo(PlurimaConsumer.State.RUNNING);

            assertTimeoutPreemptively(Duration.ofSeconds(15), rt::close);
            assertThat(rt.state()).as("after clean close()").isEqualTo(PlurimaConsumer.State.CLOSED);
        } finally {
            rt.close(); // idempotent; guarantees the poll thread/consumer are torn down
        }
    }

    @Test
    void shareRuntimeState_newRunningClosed() throws Exception {
        ShareConsumerRuntime<byte[], byte[]> rt = new ShareConsumerRuntime<>(
            bogusBrokerProps(true), "t", (r, ctx) -> { }, null,
            RecordDeserializer.bytes(), RecordDeserializer.bytes(),
            OrderingMode.UNORDERED, 8, 16, RetryPolicy.noRetry(), null,
            Duration.ofMillis(50), Duration.ofSeconds(30), Duration.ofSeconds(5),
            PlurimaMetrics.noOp(), null, true, null, t -> { });
        try {
            assertThat(rt.state()).as("before start()").isEqualTo(PlurimaConsumer.State.NEW);

            assertTimeoutPreemptively(Duration.ofSeconds(10), rt::start);
            assertThat(rt.state())
                .as("RUNNING must be reached without ever contacting a broker — same "
                    + "field-then-thread publish order as ClassicBasicRuntime")
                .isEqualTo(PlurimaConsumer.State.RUNNING);

            assertTimeoutPreemptively(Duration.ofSeconds(15), rt::close);
            assertThat(rt.state()).as("after clean close()").isEqualTo(PlurimaConsumer.State.CLOSED);
        } finally {
            rt.close();
        }
    }

    @Test
    void classicRuntimeState_fatalTransitionsToFailedAndCloseAfterFatalStaysFailed() throws Exception {
        AtomicReference<Throwable> capturedFatal = new AtomicReference<>();
        ClassicBasicRuntime<byte[], byte[]> rt = new ClassicBasicRuntime<>(
            bogusBrokerProps(false), "t", (r, ctx) -> { },
            RecordDeserializer.bytes(), RecordDeserializer.bytes(),
            OrderingMode.UNORDERED, 8, 16, RetryPolicy.noRetry(), null,
            Duration.ofMillis(50), Duration.ofSeconds(5), PlurimaMetrics.noOp(), capturedFatal::set);
        try {
            assertTimeoutPreemptively(Duration.ofSeconds(10), rt::start);

            RuntimeException boom = new RuntimeException("classic fatal");
            assertTimeoutPreemptively(Duration.ofSeconds(15), () -> invokeHandleFatal(rt, boom));
            assertThat(rt.state())
                .as("a fatal error must transition RUNNING -> FAILED")
                .isEqualTo(PlurimaConsumer.State.FAILED);
            assertThat(capturedFatal.get())
                .as("onFatalError callback must receive the exact fatal Throwable")
                .isSameAs(boom);

            rt.close();
            assertThat(rt.state())
                .as("close() arriving after a fatal error must NOT overwrite FAILED")
                .isEqualTo(PlurimaConsumer.State.FAILED);
        } finally {
            rt.close();
        }
    }

    @Test
    void shareRuntimeState_fatalTransitionsToFailedAndCloseAfterFatalStaysFailed() throws Exception {
        AtomicReference<Throwable> capturedFatal = new AtomicReference<>();
        ShareConsumerRuntime<byte[], byte[]> rt = new ShareConsumerRuntime<>(
            bogusBrokerProps(true), "t", (r, ctx) -> { }, null,
            RecordDeserializer.bytes(), RecordDeserializer.bytes(),
            OrderingMode.UNORDERED, 8, 16, RetryPolicy.noRetry(), null,
            Duration.ofMillis(50), Duration.ofSeconds(30), Duration.ofSeconds(5),
            PlurimaMetrics.noOp(), null, true, null, capturedFatal::set);
        try {
            assertTimeoutPreemptively(Duration.ofSeconds(10), rt::start);

            RuntimeException boom = new RuntimeException("share fatal");
            assertTimeoutPreemptively(Duration.ofSeconds(15), () -> invokeHandleFatal(rt, boom));
            assertThat(rt.state())
                .as("a fatal error must transition RUNNING -> FAILED")
                .isEqualTo(PlurimaConsumer.State.FAILED);
            assertThat(capturedFatal.get())
                .as("onFatalError callback must receive the exact fatal Throwable")
                .isSameAs(boom);

            rt.close();
            assertThat(rt.state())
                .as("close() arriving after a fatal error must NOT overwrite FAILED")
                .isEqualTo(PlurimaConsumer.State.FAILED);
        } finally {
            rt.close();
        }
    }

    @Test
    void classicRuntimeState_fatalAfterCloseStaysClosed() throws Exception {
        AtomicReference<Throwable> capturedFatal = new AtomicReference<>();
        ClassicBasicRuntime<byte[], byte[]> rt = new ClassicBasicRuntime<>(
            bogusBrokerProps(false), "t", (r, ctx) -> { },
            RecordDeserializer.bytes(), RecordDeserializer.bytes(),
            OrderingMode.UNORDERED, 8, 16, RetryPolicy.noRetry(), null,
            Duration.ofMillis(50), Duration.ofSeconds(5), PlurimaMetrics.noOp(), capturedFatal::set);
        try {
            assertTimeoutPreemptively(Duration.ofSeconds(10), rt::start);
            assertTimeoutPreemptively(Duration.ofSeconds(15), rt::close);
            assertThat(rt.state()).isEqualTo(PlurimaConsumer.State.CLOSED);

            RuntimeException boom = new RuntimeException("classic fatal after close");
            assertTimeoutPreemptively(Duration.ofSeconds(15), () -> invokeHandleFatal(rt, boom));
            assertThat(rt.state())
                .as("a fatal observed after an already-completed clean close must stay CLOSED, "
                    + "never flip to FAILED — the close CAS already won")
                .isEqualTo(PlurimaConsumer.State.CLOSED);
            assertThat(capturedFatal.get())
                .as("onFatalError must NOT fire once close() already won the CAS — there is "
                    + "nothing new to report, the consumer was already deliberately stopped")
                .isNull();
        } finally {
            rt.close();
        }
    }

    @Test
    void shareRuntimeState_fatalAfterCloseStaysClosed() throws Exception {
        AtomicReference<Throwable> capturedFatal = new AtomicReference<>();
        ShareConsumerRuntime<byte[], byte[]> rt = new ShareConsumerRuntime<>(
            bogusBrokerProps(true), "t", (r, ctx) -> { }, null,
            RecordDeserializer.bytes(), RecordDeserializer.bytes(),
            OrderingMode.UNORDERED, 8, 16, RetryPolicy.noRetry(), null,
            Duration.ofMillis(50), Duration.ofSeconds(30), Duration.ofSeconds(5),
            PlurimaMetrics.noOp(), null, true, null, capturedFatal::set);
        try {
            assertTimeoutPreemptively(Duration.ofSeconds(10), rt::start);
            assertTimeoutPreemptively(Duration.ofSeconds(15), rt::close);
            assertThat(rt.state()).isEqualTo(PlurimaConsumer.State.CLOSED);

            RuntimeException boom = new RuntimeException("share fatal after close");
            assertTimeoutPreemptively(Duration.ofSeconds(15), () -> invokeHandleFatal(rt, boom));
            assertThat(rt.state())
                .as("a fatal observed after an already-completed clean close must stay CLOSED, "
                    + "never flip to FAILED — the close CAS already won")
                .isEqualTo(PlurimaConsumer.State.CLOSED);
            assertThat(capturedFatal.get())
                .as("onFatalError must NOT fire once close() already won the CAS")
                .isNull();
        } finally {
            rt.close();
        }
    }

    // ------------------------------------------------ PlurimaConsumer facade (public surface)

    /**
     * Exercises {@link PlurimaConsumer#state()} / {@link PlurimaConsumer#isRunning()} — the
     * actual public API named in the B6 finding — end to end through the builder, on top of
     * the CLASSIC_BASIC runtime already verified in isolation above.
     */
    @Test
    void plurimaConsumerFacade_stateAndIsRunningTrackLifecycle() throws Exception {
        PlurimaConsumer<byte[], byte[]> consumer = io.plurima.kafka.PlurimaConsumer.builder()
            .kafkaProperties(bogusBrokerProps(false))
            .topic("t")
            .engine(ConsumerEngine.CLASSIC_BASIC)
            .listener((r, ctx) -> { })
            .build();
        try {
            assertThat(consumer.state()).isEqualTo(PlurimaConsumer.State.NEW);
            assertThat(consumer.isRunning()).isFalse();

            assertTimeoutPreemptively(Duration.ofSeconds(10), consumer::start);
            assertThat(consumer.state()).isEqualTo(PlurimaConsumer.State.RUNNING);
            assertThat(consumer.isRunning()).isTrue();

            assertTimeoutPreemptively(Duration.ofSeconds(15), consumer::close);
            assertThat(consumer.state()).isEqualTo(PlurimaConsumer.State.CLOSED);
            assertThat(consumer.isRunning())
                .as("isRunning() is state() == RUNNING; CLOSED must report false")
                .isFalse();
        } finally {
            consumer.close();
        }
    }
}
