package dev.engnotes.fes.referencedata;

import java.time.Instant;

import dev.engnotes.fes.events.InstrumentReferenceEvent;

/** Factory methods for instrument reference test data. */
final class Instruments {

    static final Instant EFFECTIVE_AT = Instant.parse("2026-08-16T09:15:00Z");

    private Instruments() {
    }

    static InstrumentReferenceEvent instrument(String instrumentId, String ticker, long version) {
        return instrument(instrumentId, ticker, version, "reference-data-service");
    }

    static InstrumentReferenceEvent instrument(String instrumentId,
                                               String ticker,
                                               long version,
                                               String producerIdentity) {
        return InstrumentReferenceEvent.newBuilder()
                .setInstrumentId(instrumentId)
                .setTicker(ticker)
                .setExchange("NSE")
                .setIsin("INE002A01018")
                .setSecurityType("EQUITY")
                .setCurrency("INR")
                .setSector("ENERGY")
                .setSharesOutstanding(6_766_000_000L)
                .setReferenceVersion(version)
                .setEffectiveAt(EFFECTIVE_AT)
                .setProducerIdentity(producerIdentity)
                .build();
    }
}
