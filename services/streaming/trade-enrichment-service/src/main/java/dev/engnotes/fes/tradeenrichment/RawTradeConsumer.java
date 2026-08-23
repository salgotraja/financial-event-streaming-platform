package dev.engnotes.fes.tradeenrichment;

import dev.engnotes.fes.events.EnrichedTradeEvent;
import dev.engnotes.fes.events.TradeEvent;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

/**
 * Enriches one trade, publishes it, then commits the offset.
 *
 * <p>The order is contractual under at-least-once (ADR-019): acknowledging before the send would
 * advance the offset past a trade that never reached {@code trades.enriched}.
 *
 * <p>Failures propagate to the container's error handler, which separates the two classes ADR-027
 * distinguishes. A metrics failure is neither: by the time metrics runs the record is already on the
 * topic, so letting it propagate would retry the enrichment and publish the trade twice.
 *
 * <p><strong>What actually gates this listener is not {@code autoStartup = false}.</strong> The
 * real guarantee is that {@link InstrumentCacheLoader#loadInitialSnapshot()} runs inside a blocking
 * {@code SmartInitializingSingleton} in {@code EnrichmentKafkaConfiguration}, and every
 * {@code SmartInitializingSingleton} in a Spring context completes during
 * {@code finishBeanFactoryInitialization()}, strictly before {@code finishRefresh()}, the later
 * phase in which any container's auto-start actually runs. So by the time this or any other listener
 * could start, the instrument master has already finished loading or the loader has already thrown
 * and aborted context startup. {@code autoStartup = false} is a second, redundant belt: it is what
 * lets {@code EnrichmentKafkaConfiguration}'s {@code SmartLifecycle} bean start this specific
 * container explicitly, once loading succeeds, rather than racing the framework's own auto-start,
 * but it is not the thing preventing an early delivery. Starting this listener before the master has
 * loaded, however it happened, would dead-letter every trade whose instrument the process has not
 * reached yet.
 */
@Component
public class RawTradeConsumer {

    public static final String LISTENER_ID = "trades-raw";

    private static final Logger log = LoggerFactory.getLogger(RawTradeConsumer.class);

    private final TradeEnricher enricher;
    private final EnrichedTradePublisher publisher;
    private final EnrichmentMetrics metrics;

    public RawTradeConsumer(TradeEnricher enricher,
                            EnrichedTradePublisher publisher,
                            EnrichmentMetrics metrics) {
        this.enricher = enricher;
        this.publisher = publisher;
        this.metrics = metrics;
    }

    // idIsGroup = false: id names the listener container bean for the readiness gate's
    // SmartLifecycle lookup and for the test registry, not the consumer group. Left at its default
    // of true, @KafkaListener silently overrides the configured group.id with this id, so the
    // running consumer would join a group named "trades-raw" instead of "trade-enrichment-service",
    // the one name the committed Kafka policy's GROUP grant actually authorizes.
    @KafkaListener(id = LISTENER_ID,
            idIsGroup = false,
            topics = "${fes.trade-enrichment-service.topic}",
            autoStartup = "false")
    public void consume(ConsumerRecord<String, TradeEvent> record, Acknowledgment acknowledgment) {
        long consumeStart = System.currentTimeMillis();

        EnrichedTradeEvent enriched = enricher.enrich(record.value(), consumeStart);
        publisher.publish(record, enriched);

        try {
            metrics.recordEnriched(enriched);
        } catch (RuntimeException e) {
            log.warn("Metrics recording failed for tradeId={} partition={} offset={}",
                    record.value().getTradeId(), record.partition(), record.offset(), e);
        }

        // DEBUG rather than INFO: at the platform's target rate an INFO line per record is the
        // dominant cost of the service. The identity stack test raises this logger instead.
        log.debug("Enriched trade tradeId={} ticker={} ageMs={} partition={} offset={}",
                record.value().getTradeId(), record.key(), enriched.getMarketDataAgeMs(),
                record.partition(), record.offset());

        acknowledgment.acknowledge();
    }
}
