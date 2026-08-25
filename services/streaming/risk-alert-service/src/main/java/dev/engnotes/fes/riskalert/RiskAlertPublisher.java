package dev.engnotes.fes.riskalert;

import java.util.List;

import dev.engnotes.fes.events.EnrichedTradeEvent;
import dev.engnotes.fes.events.RiskAlertEvent;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/**
 * Publishes one alert to {@code notifications.alerts}, keyed by ticker.
 *
 * <p>Keyed by ticker to match the input partitioning, so alerts for one ticker stay ordered relative
 * to each other.
 *
 * <p>The trace headers are copied from the consumed record, following {@code EnrichedTradePublisher}
 * in {@code trade-enrichment-service}: {@code traceparent}, {@code tracestate} and
 * {@code correlationId} have to survive the hop on the headers, because NFR-04.1 requires end-to-end
 * tracing across services that may never deserialise the body.
 *
 * <p><strong>Every severity is published immediately.</strong> architecture-v1.2 batches WARNING and
 * INFO on a five second timer. That cannot coexist with MANUAL_IMMEDIATE offset commits: the
 * listener acknowledges the record, and an alert still sitting in a buffer is lost if the process
 * dies. The batching is deferred rather than silently dropped, and if it returns it needs an outbox
 * or offset commits deferred to flush, which is a design change and not a tuning knob (ADR-035).
 */
@Component
public class RiskAlertPublisher {

    private static final List<String> PROPAGATED_HEADERS =
            List.of("traceparent", "tracestate", "correlationId");

    private final KafkaTemplate<String, RiskAlertEvent> kafkaTemplate;
    private final String topic;

    public RiskAlertPublisher(KafkaTemplate<String, RiskAlertEvent> kafkaTemplate,
                              RiskAlertProperties properties) {
        this.kafkaTemplate = kafkaTemplate;
        this.topic = properties.outputTopic();
    }

    public void publish(ConsumerRecord<String, EnrichedTradeEvent> source, RiskAlertEvent alert) {
        ProducerRecord<String, RiskAlertEvent> record =
                new ProducerRecord<>(topic, alert.getTicker(), alert);
        for (String name : PROPAGATED_HEADERS) {
            Header header = source.headers().lastHeader(name);
            if (header != null) {
                record.headers().add(header);
            }
        }
        kafkaTemplate.send(record).join();
    }
}
