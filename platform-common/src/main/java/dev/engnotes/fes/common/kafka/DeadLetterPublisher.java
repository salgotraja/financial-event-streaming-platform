package dev.engnotes.fes.common.kafka;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;

import dev.engnotes.fes.events.DeadLetterEvent;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

/**
 * Publishes a quarantined record to {@code {source-topic}.dlq}.
 *
 * <p>Quarantine is per record (ADR-027). One poison record moves to its dead-letter topic, the
 * offset advances, and the next record on the same partition processes normally. There is no
 * event-type-wide circuit breaker here and there must not be one: a breaker that opens on a
 * malformed payload stops archiving every healthy record behind it, which is the failure the
 * quarantine exists to avoid.
 *
 * <p>The dead-letter topic name is derived, never configured per service, so a service cannot ship a
 * name that no replay tool looks at. The suffix is {@code .dlq}, lowercase, not Spring Kafka's
 * default {@code .DLT}.
 *
 * <p>The record keeps its original key so a quarantined record and the records that follow it for
 * the same key stay on one partition, and a replay preserves their order.
 */
public class DeadLetterPublisher {

    public static final String DLQ_SUFFIX = ".dlq";
    public static final String CORRELATION_ID_HEADER = "correlationId";

    private static final int STACK_TRACE_SUMMARY_LENGTH = 500;

    private static final Logger log = LoggerFactory.getLogger(DeadLetterPublisher.class);

    private final KafkaTemplate<String, DeadLetterEvent> kafkaTemplate;
    private final FailureTracker failureTracker;
    private final String consumerGroup;
    private final String consumerInstance;
    private final Clock clock;

    public DeadLetterPublisher(KafkaTemplate<String, DeadLetterEvent> kafkaTemplate,
                               FailureTracker failureTracker,
                               String consumerGroup,
                               String consumerInstance) {
        this(kafkaTemplate, failureTracker, consumerGroup, consumerInstance, Clock.systemUTC());
    }

    public DeadLetterPublisher(KafkaTemplate<String, DeadLetterEvent> kafkaTemplate,
                               FailureTracker failureTracker,
                               String consumerGroup,
                               String consumerInstance,
                               Clock clock) {
        this.kafkaTemplate = kafkaTemplate;
        this.failureTracker = failureTracker;
        this.consumerGroup = consumerGroup;
        this.consumerInstance = consumerInstance;
        this.clock = clock;
    }

    /**
     * @param record         the record that could not be processed
     * @param originalPayload the bytes the broker delivered, so the quarantined evidence is the
     *                        payload itself rather than a re-encoding of a value that may have
     *                        failed to decode in the first place
     */
    public CompletableFuture<SendResult<String, DeadLetterEvent>> publish(ConsumerRecord<String, ?> record,
                                                                          byte[] originalPayload,
                                                                          Throwable failure) {
        // The container reports its own wrapper, so the class and message name the root cause, which
        // is what an operator triaging the DLQ needs. failureReason keeps the whole chain, because
        // the layer in the middle is usually the one that says what the record did wrong.
        Throwable cause = rootCause(failure);
        DeadLetterEvent event = DeadLetterEvent.newBuilder()
                .setOriginalTopic(record.topic())
                .setOriginalPartition(record.partition())
                .setOriginalOffset(record.offset())
                .setOriginalPayload(ByteBuffer.wrap(originalPayload == null ? new byte[0] : originalPayload))
                .setFailureReason(causeChain(failure))
                .setExceptionClass(cause.getClass().getName())
                .setExceptionMessage(String.valueOf(cause.getMessage()))
                .setStackTraceSummary(stackTraceSummary(failure))
                .setRetryCount(failureTracker.retryCount(record))
                .setFirstFailureAt(failureTracker.firstFailureAt(record))
                .setLastFailureAt(clock.instant())
                .setConsumerGroup(consumerGroup)
                .setConsumerInstance(consumerInstance)
                .setCorrelationId(correlationId(record))
                .build();

        String dlqTopic = record.topic() + DLQ_SUFFIX;
        return kafkaTemplate.send(dlqTopic, record.key(), event).whenComplete((_, sendFailure) -> {
            if (sendFailure != null) {
                // Nothing is safe to swallow here: a failed quarantine means the offset is about to
                // advance past a record that was never archived anywhere.
                log.error("Failed to quarantine topic={} partition={} offset={} to {}",
                        record.topic(), record.partition(), record.offset(), dlqTopic, sendFailure);
            } else {
                log.warn("Quarantined topic={} partition={} offset={} to {} reason={}",
                        record.topic(), record.partition(), record.offset(), dlqTopic,
                        event.getFailureReason());
            }
        });
    }

    private String correlationId(ConsumerRecord<String, ?> record) {
        Header header = record.headers().lastHeader(CORRELATION_ID_HEADER);
        return header == null ? null : new String(header.value(), StandardCharsets.UTF_8);
    }

    private static String causeChain(Throwable failure) {
        StringBuilder chain = new StringBuilder(failure.toString());
        Throwable cause = failure;
        while (cause.getCause() != null && cause.getCause() != cause) {
            cause = cause.getCause();
            chain.append(" <- ").append(cause);
        }
        return chain.toString();
    }

    private static Throwable rootCause(Throwable failure) {
        Throwable cause = failure;
        while (cause.getCause() != null && cause.getCause() != cause) {
            cause = cause.getCause();
        }
        return cause;
    }

    private static String stackTraceSummary(Throwable failure) {
        StringWriter writer = new StringWriter();
        failure.printStackTrace(new PrintWriter(writer));
        String trace = writer.toString();
        return trace.length() <= STACK_TRACE_SUMMARY_LENGTH
                ? trace
                : trace.substring(0, STACK_TRACE_SUMMARY_LENGTH);
    }
}
