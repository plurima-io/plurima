package io.plurima.kafka.internal;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import io.plurima.kafka.OrderingMode;
import io.plurima.kafka.RecordListener;
import io.plurima.kafka.deserializer.RecordDeserializer;
import io.plurima.kafka.metrics.PlurimaMetrics;
import io.plurima.kafka.retry.RetryPolicy;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that {@link PollLoop} compares the user-configured local lockDuration against
 * the broker's reported {@code acquisitionLockTimeoutMs} and logs the right level
 * exactly once.
 *
 * <p>The point is to catch misconfiguration — a local timeout ≥ broker timeout gives no
 * early-recovery benefit, so we surface that loudly at startup.
 */
class PollLoopLocalLockDurationTest {

    private Logger pollLoopLogger;
    private ListAppender<ILoggingEvent> appender;

    @BeforeEach
    void setUp() {
        pollLoopLogger = (Logger) LoggerFactory.getLogger(PollLoop.class);
        appender = new ListAppender<>();
        appender.start();
        pollLoopLogger.addAppender(appender);
        // The check itself logs at INFO (good) or ERROR (bad). Make sure we capture both.
        pollLoopLogger.setLevel(Level.DEBUG);
    }

    @AfterEach
    void tearDown() {
        pollLoopLogger.detachAppender(appender);
    }

    @Test
    void logsErrorWhenLocalLockIsNotSmallerThanBrokerLock() throws Exception {
        runOnePoll(Duration.ofSeconds(30), 30_000);  // local == broker

        ILoggingEvent error = findEvent(appender, Level.ERROR,
            "is NOT smaller than the broker");
        assertThat(error)
            .as("PollLoop must log ERROR when local lockDuration >= broker lock")
            .isNotNull();
        assertThat(error.getFormattedMessage()).contains("PT30S");  // both Durations in message
    }

    @Test
    void logsInfoWhenLocalLockIsSmallerThanBrokerLock() throws Exception {
        runOnePoll(Duration.ofSeconds(24), 30_000);

        ILoggingEvent info = findEvent(appender, Level.INFO,
            "PollLoop lockDuration check OK");
        assertThat(info)
            .as("PollLoop must log INFO when local lockDuration < broker lock")
            .isNotNull();
        assertThat(info.getFormattedMessage()).contains("6000ms");  // 30 - 24 = 6 seconds early
    }

    @Test
    void doesNotEmitWhenBrokerDoesNotReportLockDuration() throws Exception {
        // Default FakeShareConsumer returns Optional.empty(); PollLoop should NOT
        // emit either log line — we can only compare against a real broker value.
        runOnePoll(Duration.ofSeconds(30), null);

        boolean any = appender.list.stream().anyMatch(e ->
            e.getFormattedMessage().contains("NOT smaller than the broker")
                || e.getFormattedMessage().contains("lockDuration check OK"));
        assertThat(any)
            .as("no compare-log should fire when the broker hasn't reported its value")
            .isFalse();
    }

    @Test
    void logsOnlyOnceAcrossManyPollIterations() throws Exception {
        FakeShareConsumer consumer = new FakeShareConsumer();
        consumer.setAcquisitionLockTimeoutMs(30_000);
        // Push enough records that the poll loop iterates multiple times.
        for (int i = 0; i < 5; i++) {
            consumer.enqueue(new ConsumerRecord<>("t", 0, i, new byte[0], new byte[0]));
        }
        consumer.subscribe(List.of("t"));

        runLoopUntilDrained(consumer, Duration.ofSeconds(24), 5);

        long errors = appender.list.stream()
            .filter(e -> e.getLevel() == Level.INFO)
            .filter(e -> e.getFormattedMessage().contains("lockDuration check OK"))
            .count();
        assertThat(errors)
            .as("compare-log must fire exactly once even across many poll iterations")
            .isEqualTo(1L);
    }

    // ---------------------------------------------------------------------------------

    /** Run the poll loop just long enough for one batch + the compare-log decision, then stop. */
    private void runOnePoll(Duration localLock, Integer brokerLockMs) throws Exception {
        FakeShareConsumer consumer = new FakeShareConsumer();
        if (brokerLockMs != null) {
            consumer.setAcquisitionLockTimeoutMs(brokerLockMs);
        }
        // One record so the poll loop has something to dispatch and exercises the
        // brokerLockDuration cache + compare branch.
        consumer.enqueue(new ConsumerRecord<>("t", 0, 1L, new byte[0], new byte[0]));
        consumer.subscribe(List.of("t"));

        runLoopUntilDrained(consumer, localLock, 1);
    }

    private void runLoopUntilDrained(
        FakeShareConsumer consumer, Duration localLock, int expectedRecords) throws Exception {
        InFlightRegistry registry = new InFlightRegistry();
        AckCoordinator coordinator = new AckCoordinator(registry);
        BackpressureGate gate = new BackpressureGate(8);
        WorkerLauncher launcher = new WorkerLauncher();

        AtomicBoolean done = new AtomicBoolean();
        int[] seen = {0};
        RecordListener<byte[], byte[]> listener = (r, ctx) -> {
            seen[0]++;
            if (seen[0] >= expectedRecords) done.set(true);
        };

        ListenerInvoker invoker = ListenerInvoker.forImplicit(
            listener, RecordDeserializer.bytes(), RecordDeserializer.bytes());
        WorkerProcessor processor = new WorkerProcessor(
            invoker, new RetryEngine(RetryPolicy.noRetry()), coordinator);
        UnorderedDispatcher dispatcher = new UnorderedDispatcher(processor, registry, coordinator, launcher, gate, OrderingMode.UNORDERED);

        PollLoop loop = new PollLoop(
            consumer, dispatcher, coordinator, registry, gate,
            Duration.ofMillis(50), localLock, Duration.ofSeconds(2),
            localLock, PlurimaMetrics.noOp(), "t", "g1");

        Thread t = new Thread(loop, "poll-loop-test");
        t.start();
        try {
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (!done.get() && System.nanoTime() < deadline) {
                Thread.sleep(20);
            }
        } finally {
            loop.shutdown();
            t.join(2_000);
            launcher.close();
        }
    }

    private static ILoggingEvent findEvent(
        ListAppender<ILoggingEvent> appender, Level level, String contains) {
        return appender.list.stream()
            .filter(e -> e.getLevel() == level)
            .filter(e -> e.getFormattedMessage().contains(contains))
            .findFirst()
            .orElse(null);
    }
}
