package dev.engnotes.fes.marketdatasimulator;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import dev.engnotes.fes.events.MarketDataTickEvent;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Headers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

/**
 * Publishes {@link MarketDataTickEvent} records to {@code market-data.ticks}.
 *
 * <p><strong>The key is the ticker.</strong> The market-data cache projector maintains the latest
 * price per ticker in Redis, and a last-write-wins projection is only correct if every tick for a
 * ticker is ordered, which Kafka guarantees within a partition and nowhere else. A null or rotating
 * key would let an older tick overwrite a newer one under rebalance with nothing failing.
 *
 * <p><strong>Trace context is written to headers, not only to the payload.</strong> NFR-04.1
 * requires end-to-end tracing across services that may never deserialise the body, so the W3C
 * headers go on the record.
 */
@Component
public class MarketDataTickPublisher {

    private static final Logger log = LoggerFactory.getLogger(MarketDataTickPublisher.class);

    static final String TRACEPARENT_HEADER = "traceparent";
    static final String TRACESTATE_HEADER = "tracestate";
    static final String CORRELATION_ID_HEADER = "correlationId";

    private final KafkaTemplate<String, MarketDataTickEvent> kafkaTemplate;
    private final String topic;

    public MarketDataTickPublisher(KafkaTemplate<String, MarketDataTickEvent> kafkaTemplate,
                                   MarketDataSimulatorProperties properties) {
        this.kafkaTemplate = kafkaTemplate;
        this.topic = properties.topic();
    }

    public CompletableFuture<SendResult<String, MarketDataTickEvent>> publish(MarketDataTickEvent tick) {
        ProducerRecord<String, MarketDataTickEvent> record =
                new ProducerRecord<>(topic, tick.getTicker().toString(), tick);
        applyHeaders(record.headers(), tick);

        return kafkaTemplate.send(record).whenComplete((result, failure) -> {
            if (failure != null) {
                // Producer-side failure is a delivery failure, not a poison record: there is no
                // consumer offset to advance and no DLQ semantics here. Surface it and let the
                // caller decide, rather than swallowing it into a fire-and-forget send.
                log.error("Failed to publish tick ticker={} correlationId={}",
                        tick.getTicker(), tick.getCorrelationId(), failure);
            } else if (log.isDebugEnabled()) {
                log.debug("Published tick ticker={} partition={} offset={}",
                        tick.getTicker(),
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset());
            }
        });
    }

    private static void applyHeaders(Headers headers, MarketDataTickEvent tick) {
        headers.add(CORRELATION_ID_HEADER, utf8(tick.getCorrelationId()));

        Map<String, String> traceContext = tick.getTraceContext();
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
