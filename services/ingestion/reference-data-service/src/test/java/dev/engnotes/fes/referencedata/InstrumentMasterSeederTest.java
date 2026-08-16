package dev.engnotes.fes.referencedata;

import java.time.Clock;
import java.time.ZoneOffset;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import dev.engnotes.fes.events.InstrumentReferenceEvent;
import dev.engnotes.fes.referencedata.ReferenceDataProperties.Instrument;
import dev.engnotes.fes.referencedata.ReferenceDataProperties.Seed;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@DisplayName("InstrumentMasterSeeder")
class InstrumentMasterSeederTest {

    private static final Clock CLOCK = Clock.fixed(Instruments.EFFECTIVE_AT, ZoneOffset.UTC);

    @Mock
    private InstrumentReferencePublisher publisher;

    @Captor
    private ArgumentCaptor<InstrumentReferenceEvent> eventCaptor;

    @Test
    @DisplayName("should publish every configured instrument at the initial version")
    void should_publish_every_configured_instrument_at_the_initial_version() {
        when(publisher.publish(any())).thenReturn(CompletableFuture.completedFuture(null));

        seeder(instrument("INS-RELIANCE", "RELIANCE"), instrument("INS-TCS", "TCS")).run(null);

        verify(publisher, times(2)).publish(eventCaptor.capture());
        assertThat(eventCaptor.getAllValues())
                .extracting(event -> event.getInstrumentId().toString())
                .containsExactly("INS-RELIANCE", "INS-TCS");
        assertThat(eventCaptor.getAllValues())
                .allSatisfy(event -> assertThat(event.getReferenceVersion())
                        .isEqualTo(InstrumentMasterSeeder.INITIAL_VERSION));
    }

    @Test
    @DisplayName("should skip an instrument the version guard rejects rather than failing startup")
    void should_skip_an_instrument_the_version_guard_rejects_rather_than_failing_startup() {
        // After a restart, an instrument already advanced past version 1 makes the seed a stale
        // write. Leaving the newer record in place is correct; taking the service down over it is
        // not, so the seeder logs and moves on.
        when(publisher.publish(any()))
                .thenThrow(new IllegalArgumentException("referenceVersion 1 does not advance on the published 3"))
                .thenReturn(CompletableFuture.completedFuture(null));

        InstrumentMasterSeeder seeder =
                seeder(instrument("INS-RELIANCE", "RELIANCE"), instrument("INS-TCS", "TCS"));

        assertThatCode(() -> seeder.run(null)).doesNotThrowAnyException();
        verify(publisher, times(2)).publish(any());
    }

    private InstrumentMasterSeeder seeder(Instrument... instruments) {
        ReferenceDataProperties properties = new ReferenceDataProperties(
                "reference-data.instruments", "reference-data-service",
                new Seed(true, List.of(instruments)));
        return new InstrumentMasterSeeder(publisher, properties, CLOCK);
    }

    private static Instrument instrument(String instrumentId, String ticker) {
        return new Instrument(instrumentId, ticker, "NSE", "INE000A01000", "EQUITY", "INR",
                "ENERGY", 1_000_000L);
    }
}
