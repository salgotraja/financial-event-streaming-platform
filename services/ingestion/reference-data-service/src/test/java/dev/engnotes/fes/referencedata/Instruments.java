package dev.engnotes.fes.referencedata;

import java.time.Instant;
import java.util.Map;

import dev.engnotes.fes.events.InstrumentReferenceEvent;

/** Factory methods for instrument reference test data. */
final class Instruments {

    static final Instant EFFECTIVE_AT = Instant.parse("2026-08-16T09:15:00Z");

    // Per ticker, so a test comparing a factory-built instrument against a seeded one does not
    // disagree on a field that was only ever a copy-paste.
    private static final Map<String, String> ISINS = Map.of(
            "RELIANCE", "INE002A01018",
            "TCS", "INE467B01029",
            "INFY", "INE009A01021",
            "WIPRO", "INE075A01022");

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
                .setIsin(ISINS.getOrDefault(ticker, "INE000A01000"))
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
