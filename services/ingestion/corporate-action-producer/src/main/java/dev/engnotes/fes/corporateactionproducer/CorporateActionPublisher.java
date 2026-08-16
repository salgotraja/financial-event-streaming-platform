package dev.engnotes.fes.corporateactionproducer;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import dev.engnotes.fes.events.CorporateActionEvent;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Headers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

/**
 * Publishes {@link CorporateActionEvent} records to {@code corporate-actions}.
 *
 * <p><strong>The key is the ticker, not the corporate action id.</strong> Corporate actions for one
 * instrument are a sequence, not independent facts: a split is superseded by its correction, and a
 * dividend by a revised amount. The {@code anomaly-candidate-service} reads this stream as
 * scheduled-event context to reject false-positive traps, so it must see the latest state of a
 * ticker rather than whichever revision happened to land on the partition it read first. Keying on
 * {@code corporateActionId} would give every revision its own partition and lose that ordering
 * silently.
 *
 * <p><strong>Publishing validates.</strong> See {@link CorporateActionValidator} for why an
 * unvalidated attribute map is worse here than a rejected publish. Validation failure throws rather
 * than returning a failed future: it is a caller bug, detectable before any broker interaction, and
 * not a delivery outcome.
 *
 * <p><strong>Trace context is written to headers, not only to the payload.</strong> NFR-04.1
 * requires end-to-end tracing across services that may never deserialise the body.
 */
@Component
public class CorporateActionPublisher {

    private static final Logger log = LoggerFactory.getLogger(CorporateActionPublisher.class);

    static final String TRACEPARENT_HEADER = "traceparent";
    static final String TRACESTATE_HEADER = "tracestate";
    static final String CORRELATION_ID_HEADER = "correlationId";

    private final KafkaTemplate<String, CorporateActionEvent> kafkaTemplate;
    private final String topic;

    public CorporateActionPublisher(KafkaTemplate<String, CorporateActionEvent> kafkaTemplate,
                                    CorporateActionProducerProperties properties) {
        this.kafkaTemplate = kafkaTemplate;
        this.topic = properties.topic();
    }

    public CompletableFuture<SendResult<String, CorporateActionEvent>> publish(CorporateActionEvent action) {
        CorporateActionValidator.validate(action);

        ProducerRecord<String, CorporateActionEvent> record =
                new ProducerRecord<>(topic, action.getTicker().toString(), action);
        applyHeaders(record.headers(), action);

        return kafkaTemplate.send(record).whenComplete((result, failure) -> {
            if (failure != null) {
                // Producer-side failure is a delivery failure, not a poison record: there is no
                // consumer offset to advance and no DLQ semantics here. Surface it and let the
                // caller decide, rather than swallowing it into a fire-and-forget send.
                log.error("Failed to publish corporate action id={} ticker={} type={} correlationId={}",
                        action.getCorporateActionId(), action.getTicker(), action.getActionType(),
                        action.getCorrelationId(), failure);
            } else if (log.isDebugEnabled()) {
                log.debug("Published corporate action id={} ticker={} type={} partition={} offset={}",
                        action.getCorporateActionId(), action.getTicker(), action.getActionType(),
                        result.getRecordMetadata().partition(), result.getRecordMetadata().offset());
            }
        });
    }

    private static void applyHeaders(Headers headers, CorporateActionEvent action) {
        headers.add(CORRELATION_ID_HEADER, utf8(action.getCorrelationId()));

        Map<String, String> traceContext = action.getTraceContext();
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
