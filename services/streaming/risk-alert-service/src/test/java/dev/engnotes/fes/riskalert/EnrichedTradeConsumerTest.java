package dev.engnotes.fes.riskalert;

import java.time.Instant;
import java.util.List;

import dev.engnotes.fes.events.EnrichedTradeEvent;
import dev.engnotes.fes.events.RiskAlertEvent;
import dev.engnotes.fes.riskalert.rules.EnrichedTrades;
import dev.engnotes.fes.riskalert.rules.RiskRuleEngine;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import org.springframework.kafka.support.Acknowledgment;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class EnrichedTradeConsumerTest {

    private final RiskRuleEngine engine = mock(RiskRuleEngine.class);
    private final RiskAlertPublisher publisher = mock(RiskAlertPublisher.class);
    private final RiskAlertMetrics metrics = mock(RiskAlertMetrics.class);
    private final Acknowledgment acknowledgment = mock(Acknowledgment.class);

    private final EnrichedTradeConsumer consumer =
            new EnrichedTradeConsumer(engine, publisher, metrics);

    private static ConsumerRecord<String, EnrichedTradeEvent> record() {
        return new ConsumerRecord<>("trades.enriched", 0, 0L, "RELIANCE",
                EnrichedTrades.withDeviationAt(6.0, Instant.ofEpochMilli(1_000L)));
    }

    private static RiskAlertEvent alert(String alertId) {
        return RiskAlertEvent.newBuilder()
                .setAlertId(alertId)
                .setCorrelationId("corr-1")
                .setTriggeringTradeId("trade-1")
                .setAlertType(dev.engnotes.fes.events.AlertType.PRICE_DEVIATION)
                .setSeverity(dev.engnotes.fes.events.Severity.CRITICAL)
                .setTicker("RELIANCE")
                .setTraderId("trader-1")
                .setDescription("test")
                .setRuleParameters(java.util.Map.of())
                .setMeasuredValues(java.util.Map.of())
                .setRuleId("pd")
                .setRuleVersion(1L)
                .setAlertTimestamp(Instant.ofEpochMilli(1_000L))
                .build();
    }

    @Test
    void the_alert_is_published_before_the_offset_is_acknowledged() {
        ConsumerRecord<String, EnrichedTradeEvent> record = record();
        when(engine.evaluate(record.value())).thenReturn(List.of(alert("a-1")));

        consumer.consume(record, acknowledgment);

        // Contractual under at-least-once (ADR-019). Acknowledging first would advance the offset
        // past a trade whose alert never reached notifications.alerts, and no retry would ever
        // revisit it.
        InOrder order = inOrder(publisher, acknowledgment);
        order.verify(publisher).publish(record, alert("a-1"));
        order.verify(acknowledgment).acknowledge();
    }

    @Test
    void a_trade_that_breaches_nothing_is_acknowledged_without_publishing() {
        ConsumerRecord<String, EnrichedTradeEvent> record = record();
        when(engine.evaluate(record.value())).thenReturn(List.of());

        consumer.consume(record, acknowledgment);

        // Silence is the normal case, not an error, and the offset must still advance or the
        // partition stalls on the first non-breaching trade.
        verify(publisher, never()).publish(any(), any());
        verify(acknowledgment).acknowledge();
    }

    @Test
    void two_alerts_from_one_trade_are_both_published_before_the_acknowledgement() {
        ConsumerRecord<String, EnrichedTradeEvent> record = record();
        when(engine.evaluate(record.value())).thenReturn(List.of(alert("a-1"), alert("a-2")));

        consumer.consume(record, acknowledgment);

        // Two in-force rules of one type both alert. An acknowledgement after the first send would
        // lose the second one on a crash between them.
        InOrder order = inOrder(publisher, acknowledgment);
        order.verify(publisher).publish(record, alert("a-1"));
        order.verify(publisher).publish(record, alert("a-2"));
        order.verify(acknowledgment).acknowledge();
    }

    @Test
    void a_metrics_failure_after_a_successful_publish_does_not_prevent_the_acknowledgement() {
        ConsumerRecord<String, EnrichedTradeEvent> record = record();
        when(engine.evaluate(record.value())).thenReturn(List.of(alert("a-1")));
        doThrow(new IllegalStateException("meter registry closed"))
                .when(metrics).recordAlert(any());

        consumer.consume(record, acknowledgment);

        // By the time metrics runs the alert is already on the topic, so letting this propagate
        // would re-evaluate the trade and publish the same alert a second time. Same reasoning as
        // RawTradeConsumer in trade-enrichment-service.
        verify(publisher, times(1)).publish(record, alert("a-1"));
        verify(acknowledgment).acknowledge();
    }
}
