package dev.engnotes.fes.referencedata;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

import dev.engnotes.fes.events.InstrumentReferenceEvent;
import io.micrometer.tracing.Tracer;
import io.micrometer.tracing.propagation.Propagator;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Headers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

/**
 * Publishes {@link InstrumentReferenceEvent} records to the compacted
 * {@code reference-data.instruments} topic.
 *
 * <p><strong>The key is the instrument id, not the ticker.</strong> Every other topic on the trade
 * path keys on ticker to hold ordering. This one is compacted, so the key is not a routing hint but
 * the identity of the row: the broker keeps the latest record per key and discards the rest. Keying
 * on ticker would make the compacted view a per-ticker view, which is wrong the moment one ticker
 * carries two instruments, and it would silently drop history the enrichment cache rebuilds from.
 *
 * <p><strong>Never publishes a null value.</strong> On a compacted topic a null value is a tombstone
 * and deletes the instrument from every subsequent rebuild. This service has no delete path, so a
 * null is always a bug rather than an intent.
 *
 * <p><strong>Provenance is stamped, not accepted.</strong> FR-10.5 wants reference-data changes
 * traceable to the producing workload identity. A {@code producerIdentity} the caller supplies is a
 * claim about itself, so the configured identity overwrites whatever arrives.
 *
 * <p><strong>The reference version must increase per instrument.</strong> On a compacted topic a
 * version that goes backwards resurrects stale metadata into the enrichment cache, and nothing
 * fails, the values are just wrong. The guard is in-process: it catches a caller republishing an old
 * version within a run, and it cannot catch a regression across a restart because this service is
 * write-only on the topic and has no consumer to read the current state back. That limit is a
 * consequence of the permission boundary, not an oversight.
 *
 * <p><strong>Trace context comes from the active span, not the payload.</strong> The other producers
 * copy W3C headers out of a {@code traceContext} field; this schema has none, so NFR-04.1 is met by
 * injecting the current context through the {@link Propagator}. There is likewise no
 * {@code correlationId} field, so no correlation header is written: an invented one would correlate
 * nothing.
 */
@Component
public class InstrumentReferencePublisher {

    private static final Logger log = LoggerFactory.getLogger(InstrumentReferencePublisher.class);

    private final KafkaTemplate<String, InstrumentReferenceEvent> kafkaTemplate;
    private final Propagator propagator;
    private final Tracer tracer;
    private final String topic;
    private final String producerIdentity;
    private final Map<String, Long> highestVersion = new ConcurrentHashMap<>();

    public InstrumentReferencePublisher(KafkaTemplate<String, InstrumentReferenceEvent> kafkaTemplate,
                                        Propagator propagator,
                                        Tracer tracer,
                                        ReferenceDataProperties properties) {
        this.kafkaTemplate = kafkaTemplate;
        this.propagator = propagator;
        this.tracer = tracer;
        this.topic = properties.topic();
        this.producerIdentity = properties.producerIdentity();
    }

    public CompletableFuture<SendResult<String, InstrumentReferenceEvent>> publish(
            InstrumentReferenceEvent instrument) {

        // A null value on a compacted topic is a tombstone, so it is rejected here rather than
        // reaching the broker as a delete nobody asked for.
        Objects.requireNonNull(instrument, "instrument reference must not be null: a null value is a tombstone");

        String instrumentId = instrument.getInstrumentId().toString();
        acceptVersion(instrumentId, instrument.getReferenceVersion());

        InstrumentReferenceEvent stamped = InstrumentReferenceEvent.newBuilder(instrument)
                .setProducerIdentity(producerIdentity)
                .build();

        ProducerRecord<String, InstrumentReferenceEvent> record =
                new ProducerRecord<>(topic, instrumentId, stamped);
        injectTraceContext(record.headers());

        return kafkaTemplate.send(record).whenComplete((result, failure) -> {
            if (failure != null) {
                // Producer-side failure is a delivery failure, not a poison record: there is no
                // consumer offset to advance and no DLQ semantics here.
                log.error("Failed to publish instrument reference instrumentId={} version={}",
                        instrumentId, stamped.getReferenceVersion(), failure);
            } else if (log.isDebugEnabled()) {
                log.debug("Published instrument reference instrumentId={} version={} partition={} offset={}",
                        instrumentId, stamped.getReferenceVersion(),
                        result.getRecordMetadata().partition(), result.getRecordMetadata().offset());
            }
        });
    }

    private void acceptVersion(String instrumentId, long version) {
        if (version <= 0) {
            throw new IllegalArgumentException(
                    "Instrument %s has a non-positive referenceVersion %d".formatted(instrumentId, version));
        }
        highestVersion.compute(instrumentId, (_, highest) -> {
            if (highest != null && version <= highest) {
                throw new IllegalArgumentException(
                        "Instrument %s referenceVersion %d does not advance on the published %d; a compacted topic would keep the stale record"
                                .formatted(instrumentId, version, highest));
            }
            return version;
        });
    }

    private void injectTraceContext(Headers headers) {
        var context = tracer.currentTraceContext().context();
        if (context == null) {
            return;
        }
        propagator.inject(context, headers, (carrier, name, value) -> {
            if (carrier != null && value != null) {
                carrier.add(name, value.getBytes(StandardCharsets.UTF_8));
            }
        });
    }
}
