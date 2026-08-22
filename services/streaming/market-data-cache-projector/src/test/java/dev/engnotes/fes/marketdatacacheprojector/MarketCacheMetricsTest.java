package dev.engnotes.fes.marketdatacacheprojector;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;

import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("MarketCacheMetrics")
class MarketCacheMetricsTest {

    private static final Instant NOW = Instant.ofEpochMilli(10_000L);

    private final PrometheusMeterRegistry registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
    private final MarketCacheMetrics metrics =
            new MarketCacheMetrics(registry, Clock.fixed(NOW, ZoneOffset.UTC));

    @Test
    @DisplayName("should publish the series names the specification requires")
    void should_publish_the_series_names_the_specification_requires() {
        metrics.record("RELIANCE", 8_000L, new ProjectionResult(ProjectionOutcome.APPLIED, true, 1));

        String scrape = registry.scrape();

        assertThat(scrape).contains("market_cache_projection_lag_seconds");
        assertThat(scrape).contains("market_cache_entry_age_seconds");
        assertThat(scrape).contains("market_cache_stale_writes_total");
    }

    @Test
    @DisplayName("should report the lag between the source event and the write")
    void should_report_the_lag_between_the_source_event_and_the_write() {
        metrics.record("RELIANCE", 8_000L, new ProjectionResult(ProjectionOutcome.APPLIED, true, 1));

        assertThat(registry.get("market.cache.projection.lag.seconds").gauge().value())
                .isEqualTo(2.0);
    }

    @Test
    @DisplayName("should keep reporting an entry age after the recording call has returned")
    void should_keep_reporting_an_entry_age_after_the_recording_call_has_returned() {
        metrics.record("RELIANCE", 8_000L, new ProjectionResult(ProjectionOutcome.APPLIED, true, 1));
        System.gc();

        // Micrometer holds the observed object weakly. A gauge registered over a captured local
        // reports NaN once that local is collected, and a NaN age is indistinguishable from a
        // healthy feed on a dashboard.
        assertThat(registry.get("market.cache.entry.age.seconds").tag("ticker", "RELIANCE")
                .gauge().value())
                .isNotNaN()
                .isEqualTo(2.0);
    }

    @Test
    @DisplayName("should count a duplicate and an older tick under different reasons")
    void should_count_a_duplicate_and_an_older_tick_under_different_reasons() {
        metrics.record("RELIANCE", 8_000L, new ProjectionResult(ProjectionOutcome.DUPLICATE, true, 1));
        metrics.record("RELIANCE", 8_000L, new ProjectionResult(ProjectionOutcome.OLDER, true, 1));
        metrics.record("RELIANCE", 8_000L, new ProjectionResult(ProjectionOutcome.OLDER, true, 1));

        assertThat(registry.get("market.cache.stale.writes").tag("reason", "duplicate")
                .counter().count()).isEqualTo(1.0);
        assertThat(registry.get("market.cache.stale.writes").tag("reason", "older")
                .counter().count()).isEqualTo(2.0);
    }

    @Test
    @DisplayName("should not advance the entry age from a tick that was not applied")
    void should_not_advance_the_entry_age_from_a_tick_that_was_not_applied() {
        metrics.record("RELIANCE", 8_000L, new ProjectionResult(ProjectionOutcome.APPLIED, true, 1));
        metrics.record("RELIANCE", 9_500L, new ProjectionResult(ProjectionOutcome.OLDER, true, 1));

        // The stored entry is still the 8_000 one. An age computed from a rejected tick would
        // report the cache as fresher than it is.
        assertThat(registry.get("market.cache.entry.age.seconds").tag("ticker", "RELIANCE")
                .gauge().value()).isEqualTo(2.0);
    }

    @Test
    @DisplayName("should publish the window series names the specification requires")
    void should_publish_the_window_series_names_the_specification_requires() {
        metrics.record("RELIANCE", 8_000L, new ProjectionResult(ProjectionOutcome.APPLIED, true, 4));

        String scrape = registry.scrape();

        assertThat(scrape).contains("market_cache_window_buckets");
        assertThat(scrape).contains("market_cache_window_skipped_total");
    }

    @Test
    @DisplayName("should report the bucket count the window reported")
    void should_report_the_bucket_count_the_window_reported() {
        metrics.record("RELIANCE", 8_000L, new ProjectionResult(ProjectionOutcome.APPLIED, true, 4));

        assertThat(registry.get("market.cache.window.buckets").tag("ticker", "RELIANCE")
                .gauge().value()).isEqualTo(4.0);
    }

    @Test
    @DisplayName("should count a tick the offset guard rejected")
    void should_count_a_tick_the_offset_guard_rejected() {
        metrics.record("RELIANCE", 8_000L, new ProjectionResult(ProjectionOutcome.DUPLICATE, false, 4));

        assertThat(registry.get("market.cache.window.skipped").counter().count()).isEqualTo(1.0);
    }

    @Test
    @DisplayName("should keep reporting a bucket count after the recording call has returned")
    void should_keep_reporting_a_bucket_count_after_the_recording_call_has_returned() {
        metrics.record("RELIANCE", 8_000L, new ProjectionResult(ProjectionOutcome.APPLIED, true, 4));
        System.gc();

        // Same weak-reference trap as the age gauge: a gauge over a captured local reports NaN once
        // that local is collected.
        assertThat(registry.get("market.cache.window.buckets").tag("ticker", "RELIANCE")
                .gauge().value()).isNotNaN().isEqualTo(4.0);
    }
}
