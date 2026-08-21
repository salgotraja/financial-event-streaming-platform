package dev.engnotes.fes.tradeproducer;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import dev.engnotes.fes.events.TradeEvent;
import dev.engnotes.fes.tradeproducer.TradeProducerProperties.Generation;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@DisplayName("trade generation driver")
class TradeGenerationDriverTest {

    private static final Generation GENERATION =
            new Generation(true, 100, Duration.ofMillis(50), List.of("AAPL", "MSFT"));

    @Test
    @DisplayName("should publish the batch due for each wake")
    void should_publish_the_batch_due_for_each_wake() {
        AtomicInteger published = new AtomicInteger();
        TradeEventPublisher publisher = mock(TradeEventPublisher.class);
        when(publisher.publish(any(TradeEvent.class))).thenAnswer(invocation -> {
            published.incrementAndGet();
            return CompletableFuture.completedFuture(null);
        });

        TradeGenerationDriver driver = new TradeGenerationDriver(
                new TradeGenerator(GENERATION, java.util.random.RandomGenerator.getDefault(),
                        java.time.Clock.systemUTC()),
                publisher, GENERATION);
        driver.start();
        try {
            await().atMost(Duration.ofSeconds(5)).until(() -> published.get() >= 10);
        } finally {
            driver.stop();
        }

        assertThat(driver.emitted()).isGreaterThanOrEqualTo(10);
    }

    @Test
    @DisplayName("should derive the batch size from the rate and the wake interval")
    void should_derive_the_batch_size_from_the_rate_and_the_wake_interval() {
        TradeGenerationDriver driver = new TradeGenerationDriver(
                new TradeGenerator(GENERATION, java.util.random.RandomGenerator.getDefault(),
                        java.time.Clock.systemUTC()),
                mock(TradeEventPublisher.class), GENERATION);

        assertThat(driver.batchSize())
                .as("100 per second on a 50ms wake is 5 per wake")
                .isEqualTo(5);
    }

    @Test
    @DisplayName("should generate a trade that satisfies the schema's required fields")
    void should_generate_a_trade_that_satisfies_the_schemas_required_fields() {
        TradeEvent trade = new TradeGenerator(GENERATION,
                java.util.random.RandomGenerator.getDefault(), java.time.Clock.systemUTC()).next();

        assertThat(trade.getTradeId()).isNotNull();
        assertThat(trade.getTicker()).isNotNull();
        assertThat(trade.getCorrelationId()).isNotNull();
        assertThat(GENERATION.tickers()).contains(trade.getTicker().toString());
    }
}
