package dev.engnotes.fes.marketdatasimulator;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CompletableFuture;

import dev.engnotes.fes.events.MarketDataTickEvent;
import dev.engnotes.fes.marketdatasimulator.MarketDataSimulatorProperties.Generation;
import dev.engnotes.fes.marketdatasimulator.MarketDataSimulatorProperties.Model;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@DisplayName("TickGenerationDriver")
class TickGenerationDriverTest {

    @Mock
    private MarketDataTickPublisher publisher;

    @Test
    @DisplayName("should size the batch from the rate and the wake interval")
    void should_size_the_batch_from_the_rate_and_the_wake_interval() {
        // Millisecond-resolution parking is why the batch exists: at the FR-01.4 ceiling a
        // one-per-wake driver would be twenty times short.
        assertThat(driver(50_000).batchSize()).isEqualTo(500);
        assertThat(driver(1_000).batchSize()).isEqualTo(10);
    }

    @Test
    @DisplayName("should emit at least one tick per wake even below one tick per interval")
    void should_emit_at_least_one_tick_per_wake_even_below_one_tick_per_interval() {
        assertThat(driver(10).batchSize()).isEqualTo(1);
    }

    @Test
    @DisplayName("should publish ticks while running")
    void should_publish_ticks_while_running() throws Exception {
        stubPublish();
        TickGenerationDriver driver = driver(1_000);

        driver.start();
        try {
            assertThat(driver.isRunning()).isTrue();
            awaitEmitted(driver, 50);
        } finally {
            driver.stop();
        }
    }

    @Test
    @DisplayName("should stop publishing once stopped")
    void should_stop_publishing_once_stopped() throws Exception {
        stubPublish();
        TickGenerationDriver driver = driver(1_000);

        driver.start();
        awaitEmitted(driver, 10);
        driver.stop();

        long atStop = driver.emitted();
        Thread.sleep(Duration.ofMillis(100));

        assertThat(driver.isRunning()).isFalse();
        assertThat(driver.emitted()).isEqualTo(atStop);
    }

    @Test
    @DisplayName("should ignore a second start")
    void should_ignore_a_second_start() throws Exception {
        stubPublish();
        TickGenerationDriver driver = driver(1_000);

        driver.start();
        try {
            driver.start();
            // Waiting for a tick rather than asserting on the flag alone: a driver that stopped
            // generating would still report running, and stopping before the first publish made
            // this test fail intermittently on the unused stub.
            awaitEmitted(driver, 1);
            assertThat(driver.isRunning()).isTrue();
        } finally {
            driver.stop();
        }
    }

    private static void awaitEmitted(TickGenerationDriver driver, long target) throws Exception {
        long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
        while (System.nanoTime() < deadline) {
            if (driver.emitted() >= target) {
                return;
            }
            Thread.sleep(Duration.ofMillis(10));
        }
        throw new AssertionError(
                "Driver emitted " + driver.emitted() + " ticks, expected at least " + target);
    }

    private void stubPublish() {
        when(publisher.publish(any(MarketDataTickEvent.class)))
                .thenReturn(CompletableFuture.completedFuture(null));
    }

    private TickGenerationDriver driver(int ratePerSecond) {
        Model model = new Model(Map.of("RELIANCE", 2_500.00d), 0.08d, 0.25d, 1.0d, 5.0d, 1.5d, 100L);
        TickGenerator generator = new TickGenerator(
                model, new Random(13L), Clock.fixed(Instant.parse("2026-08-16T09:15:00Z"), ZoneOffset.UTC));
        return new TickGenerationDriver(
                generator, publisher, new Generation(true, ratePerSecond, Duration.ofMillis(10)));
    }
}
