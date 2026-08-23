package dev.engnotes.fes.tradeenrichment;

import java.time.Instant;

import dev.engnotes.fes.events.InstrumentReferenceEvent;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("InstrumentCache, the fold of the compacted instrument master")
class InstrumentCacheTest {

    private final InstrumentCache cache = new InstrumentCache();

    @Test
    @DisplayName("should expose an instrument keyed by ticker, not by instrument id")
    void should_expose_an_instrument_keyed_by_ticker_not_by_instrument_id() {
        // The topic is keyed by instrumentId, but every lookup this service makes arrives as a
        // ticker off a TradeEvent. Keying the map by instrumentId would make the cache unusable.
        cache.apply("INE002A01018", instrument("RELIANCE", 6_766_000_000L));

        assertThat(cache.find("RELIANCE")).contains(new Instrument("RELIANCE", 6_766_000_000L));
        assertThat(cache.find("INE002A01018")).isEmpty();
    }

    @Test
    @DisplayName("should let a later record for the same instrument replace the earlier one")
    void should_let_a_later_record_for_the_same_instrument_replace_the_earlier_one() {
        cache.apply("INE002A01018", instrument("RELIANCE", 1L));
        cache.apply("INE002A01018", instrument("RELIANCE", 2L));

        assertThat(cache.find("RELIANCE")).contains(new Instrument("RELIANCE", 2L));
        assertThat(cache.size()).isEqualTo(1);
    }

    @Test
    @DisplayName("should remove the instrument when a tombstone arrives")
    void should_remove_the_instrument_when_a_tombstone_arrives() {
        // A compacted topic deletes by publishing a null value against the key. The map has to
        // honour it, or a delisted instrument would keep enriching trades forever.
        cache.apply("INE002A01018", instrument("RELIANCE", 1L));

        cache.apply("INE002A01018", null);

        assertThat(cache.find("RELIANCE")).isEmpty();
        assertThat(cache.size()).isZero();
    }

    @Test
    @DisplayName("should ignore a tombstone for a key it never saw")
    void should_ignore_a_tombstone_for_a_key_it_never_saw() {
        cache.apply("INE-UNKNOWN", null);

        assertThat(cache.size()).isZero();
    }

    @Test
    @DisplayName("should follow the instrument when a record re-points its key to another ticker")
    void should_follow_the_instrument_when_a_record_re_points_its_key_to_another_ticker() {
        // A ticker change for a listed instrument is a real corporate event. Without tracking the
        // key's previous ticker the old entry would linger and enrich trades under a dead symbol.
        cache.apply("INE002A01018", instrument("OLDNAME", 5L));

        cache.apply("INE002A01018", instrument("NEWNAME", 5L));

        assertThat(cache.find("OLDNAME")).isEmpty();
        assertThat(cache.find("NEWNAME")).contains(new Instrument("NEWNAME", 5L));
        assertThat(cache.size()).isEqualTo(1);
    }

    @Test
    @DisplayName("should return empty for a ticker the master does not carry")
    void should_return_empty_for_a_ticker_the_master_does_not_carry() {
        assertThat(cache.find("NOSUCH")).isEmpty();
    }

    private static InstrumentReferenceEvent instrument(String ticker, long sharesOutstanding) {
        return InstrumentReferenceEvent.newBuilder()
                .setInstrumentId("INE002A01018")
                .setTicker(ticker)
                .setExchange("NSE")
                .setIsin("INE002A01018")
                .setSecurityType("EQUITY")
                .setCurrency("INR")
                .setSector("ENERGY")
                .setSharesOutstanding(sharesOutstanding)
                .setReferenceVersion(1L)
                .setEffectiveAt(Instant.ofEpochMilli(1_000L))
                .setProducerIdentity("reference-data-service")
                .build();
    }
}
