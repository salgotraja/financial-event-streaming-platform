package dev.engnotes.fes.tradeproducer;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import dev.engnotes.fes.events.TradeEvent;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Headers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

/**
 * Publishes {@link TradeEvent} records to {@code trades.raw}.
 *
 * <p>Two things here are contractual rather than incidental.
 *
 * <p><strong>The key is the ticker, not the trade id.</strong> Downstream risk evaluation keeps a
 * running position per trader per ticker, and the risk service relies on all trades for a ticker
 * landing on one partition so a single consumer instance owns that state without cross-instance
 * coordination. Keying on {@code tradeId} would spread a ticker across all 12 partitions and break
 * that assumption silently, since nothing would fail, the totals would just be wrong.
 *
 * <p><strong>Trace context is written to headers, not only to the payload.</strong> The payload
 * carries {@code traceContext} for consumers that rebuild a span from the event itself, but
 * NFR-04.1 requires end-to-end tracing across services that may never deserialise the body, so the
 * W3C headers go on the record.
 */
@Component
public class TradeEventPublisher {

    private static final Logger log = LoggerFactory.getLogger(TradeEventPublisher.class);

    static final String TRACEPARENT_HEADER = "traceparent";
    static final String TRACESTATE_HEADER = "tracestate";
    static final String CORRELATION_ID_HEADER = "correlationId";

    private final KafkaTemplate<String, TradeEvent> kafkaTemplate;
    private final String topic;

    public TradeEventPublisher(KafkaTemplate<String, TradeEvent> kafkaTemplate,
                               TradeProducerProperties properties) {
        this.kafkaTemplate = kafkaTemplate;
        this.topic = properties.topic();
    }

    public CompletableFuture<SendResult<String, TradeEvent>> publish(TradeEvent trade) {
        ProducerRecord<String, TradeEvent> record =
                new ProducerRecord<>(topic, trade.getTicker().toString(), trade);
        applyHeaders(record.headers(), trade);

        return kafkaTemplate.send(record).whenComplete((result, failure) -> {
            if (failure != null) {
                // Producer-side failure is a delivery failure, not a poison record: there is no
                // consumer offset to advance and no DLQ semantics here. Surface it and let the
                // caller decide, rather than swallowing it into a fire-and-forget send.
                log.error("Failed to publish trade tradeId={} ticker={} correlationId={}",
                        trade.getTradeId(), trade.getTicker(), trade.getCorrelationId(), failure);
            } else if (log.isDebugEnabled()) {
                log.debug("Published trade tradeId={} ticker={} partition={} offset={}",
                        trade.getTradeId(), trade.getTicker(),
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset());
            }
        });
    }

    private static void applyHeaders(Headers headers, TradeEvent trade) {
        headers.add(CORRELATION_ID_HEADER, utf8(trade.getCorrelationId()));

        Map<String, String> traceContext = trade.getTraceContext();
        if (traceContext == null) {
            return;
        }
        copyIfPresent(headers, traceContext, TRACEPARENT_HEADER);
        copyIfPresent(headers, traceContext, TRACESTATE_HEADER);
    }

    private static void copyIfPresent(Headers headers, Map<String, String> traceContext, String name) {
        String value = traceContext.get(name);
        if (value != null) {
            headers.add(name, utf8(value));
        }
    }

    private static byte[] utf8(CharSequence value) {
        return value.toString().getBytes(StandardCharsets.UTF_8);
    }
}
