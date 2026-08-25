package dev.engnotes.fes.riskalert;

import java.util.List;

import dev.engnotes.fes.events.EnrichedTradeEvent;
import dev.engnotes.fes.events.RiskAlertEvent;
import dev.engnotes.fes.riskalert.rules.RiskRuleEngine;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

/**
 * Evaluates one trade, publishes whatever alerts it raised, then commits the offset.
 *
 * <p>The order is contractual under at-least-once (ADR-019): acknowledging before the sends would
 * advance the offset past a trade whose alert never reached {@code notifications.alerts}. A metrics
 * failure is not in that class, because by the time metrics runs the alerts are already on the
 * topic, so letting it propagate would re-evaluate the trade and publish them twice.
 *
 * <p>What gates this listener is the blocking {@code SmartInitializingSingleton} that loads the rule
 * timelines, not {@code autoStartup = false}. See {@code RiskAlertKafkaConfiguration}. Starting
 * before the fold completes would evaluate trades against the ungoverned bootstrap set while a
 * governed version was in force, which is a wrong verdict rather than a delayed one.
 */
@Component
public class EnrichedTradeConsumer {

    public static final String LISTENER_ID = "trades-enriched";

    private static final Logger log = LoggerFactory.getLogger(EnrichedTradeConsumer.class);

    private final RiskRuleEngine engine;
    private final RiskAlertPublisher publisher;
    private final RiskAlertMetrics metrics;

    public EnrichedTradeConsumer(RiskRuleEngine engine,
                                 RiskAlertPublisher publisher,
                                 RiskAlertMetrics metrics) {
        this.engine = engine;
        this.publisher = publisher;
        this.metrics = metrics;
    }

    // idIsGroup = false, for the reason spelled out in EnrichedTradeConsumerGroupIdTest.
    @KafkaListener(id = LISTENER_ID,
            idIsGroup = false,
            topics = "${fes.risk-alert-service.topic}",
            autoStartup = "false")
    public void consume(ConsumerRecord<String, EnrichedTradeEvent> record,
                        Acknowledgment acknowledgment) {

        List<RiskAlertEvent> alerts = engine.evaluate(record.value());
        for (RiskAlertEvent alert : alerts) {
            publisher.publish(record, alert);
        }

        try {
            alerts.forEach(metrics::recordAlert);
        } catch (RuntimeException e) {
            log.warn("Metrics recording failed for tradeId={} partition={} offset={}",
                    record.value().getTrade().getTradeId(), record.partition(), record.offset(), e);
        }

        // DEBUG rather than INFO: at the platform's target rate an INFO line per record is the
        // dominant cost of the service.
        log.debug("Evaluated trade tradeId={} ticker={} alerts={} partition={} offset={}",
                record.value().getTrade().getTradeId(), record.key(), alerts.size(),
                record.partition(), record.offset());

        acknowledgment.acknowledge();
    }
}
