package dev.engnotes.fes.tradeenrichment;

import java.util.List;

import dev.engnotes.fes.events.EnrichedTradeEvent;
import dev.engnotes.fes.events.TradeEvent;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/**
 * Publishes to {@code trades.enriched}, preserving two things from the consumed record.
 *
 * <p><strong>The key stays the ticker.</strong> Downstream risk evaluation keeps a running position
 * per trader per ticker and relies on all trades for a ticker landing on one partition, which is why
 * {@code trade-producer} keys the raw topic that way. Re-keying here would break that silently:
 * nothing would fail, the totals would just be wrong.
 *
 * <p><strong>The trace headers are copied.</strong> NFR-04.1 requires end-to-end tracing across
 * services that may never deserialise the body, so {@code traceparent}, {@code tracestate} and
 * {@code correlationId} have to survive the hop rather than living only in the payload's
 * traceContext map.
 *
 * <p>The send is awaited before the caller acknowledges. A fire-and-forget send with an early
 * acknowledgement would advance the offset past a trade that never reached the topic.
 */
@Component
public class EnrichedTradePublisher {

    private static final List<String> PROPAGATED_HEADERS =
            List.of("traceparent", "tracestate", "correlationId");

    private final KafkaTemplate<String, EnrichedTradeEvent> kafkaTemplate;
    private final String topic;

    public EnrichedTradePublisher(KafkaTemplate<String, EnrichedTradeEvent> kafkaTemplate,
                                  EnrichmentProperties properties) {
        this.kafkaTemplate = kafkaTemplate;
        this.topic = properties.outputTopic();
    }

    public void publish(ConsumerRecord<String, TradeEvent> source, EnrichedTradeEvent enriched) {
        ProducerRecord<String, EnrichedTradeEvent> record =
                new ProducerRecord<>(topic, source.key(), enriched);
        for (String name : PROPAGATED_HEADERS) {
            Header header = source.headers().lastHeader(name);
            if (header != null) {
                record.headers().add(header);
            }
        }
        kafkaTemplate.send(record).join();
    }
}
