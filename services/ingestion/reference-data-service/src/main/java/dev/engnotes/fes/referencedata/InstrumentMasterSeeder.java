package dev.engnotes.fes.referencedata;

import java.time.Clock;

import dev.engnotes.fes.events.InstrumentReferenceEvent;
import dev.engnotes.fes.referencedata.ReferenceDataProperties.Instrument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;

/**
 * Publishes the configured instrument master once at startup (FR-10.1).
 *
 * <p>Reference data is static, so this is a one-shot announcement rather than a paced stream. Every
 * instrument is seeded at {@code referenceVersion} 1; a later change to an instrument is a new
 * version published through {@link InstrumentReferencePublisher}, which enforces that the version
 * advances.
 *
 * <p>Off unless enabled. A restart would otherwise re-announce the entire master, which on a
 * compacted topic is harmless to the final state but noisy for every consumer rebuilding a cache.
 *
 * <p>A seed that collides with the version guard is logged and skipped rather than thrown. Once an
 * instrument has been updated to a later version, re-seeding it at version 1 is exactly the stale
 * write the guard exists to stop, and the correct response is to leave the newer record in place.
 * Throwing here would instead fail application startup, taking the service down over reference data
 * that is already correct on the topic.
 */
public class InstrumentMasterSeeder implements ApplicationRunner {

    private static final Logger log = LoggerFactory.getLogger(InstrumentMasterSeeder.class);

    static final long INITIAL_VERSION = 1L;

    private final InstrumentReferencePublisher publisher;
    private final ReferenceDataProperties properties;
    private final Clock clock;

    public InstrumentMasterSeeder(InstrumentReferencePublisher publisher,
                                  ReferenceDataProperties properties,
                                  Clock clock) {
        this.publisher = publisher;
        this.properties = properties;
        this.clock = clock;
    }

    @Override
    public void run(ApplicationArguments args) {
        var instruments = properties.seed().instruments();
        long seeded = instruments.stream().filter(this::seed).count();
        log.info("Seeded {} of {} instruments to {}",
                seeded, instruments.size(), properties.topic());
    }

    private boolean seed(Instrument instrument) {
        try {
            publisher.publish(toEvent(instrument));
            return true;
        } catch (IllegalArgumentException e) {
            log.warn("Skipped seeding instrument {}: {}", instrument.instrumentId(), e.getMessage());
            return false;
        }
    }

    private InstrumentReferenceEvent toEvent(Instrument instrument) {
        return InstrumentReferenceEvent.newBuilder()
                .setInstrumentId(instrument.instrumentId())
                .setTicker(instrument.ticker())
                .setExchange(instrument.exchange())
                .setIsin(instrument.isin())
                .setSecurityType(instrument.securityType())
                .setCurrency(instrument.currency())
                .setSector(instrument.sector())
                .setSharesOutstanding(instrument.sharesOutstanding())
                .setReferenceVersion(INITIAL_VERSION)
                .setEffectiveAt(clock.instant())
                // Overwritten by the publisher with the configured identity. Set to satisfy the
                // non-nullable field, never to assert provenance from here.
                .setProducerIdentity(properties.producerIdentity())
                .build();
    }
}
