package dev.engnotes.fes.corporateactionproducer;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import dev.engnotes.fes.corporateactionproducer.CorporateActionProducerProperties.Seed;
import dev.engnotes.fes.events.CorporateActionEvent;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@DisplayName("corporate action seeder")
class CorporateActionSeederTest {

    private static final Clock CLOCK =
            Clock.fixed(Instant.parse("2026-08-21T09:00:00Z"), ZoneOffset.UTC);

    private static final CorporateActionProducerProperties PROPERTIES =
            new CorporateActionProducerProperties("corporate-actions", Seed.defaults());

    @Test
    @DisplayName("should publish every configured action at startup")
    void should_publish_every_configured_action_at_startup() {
        CorporateActionPublisher publisher = mock(CorporateActionPublisher.class);
        when(publisher.publish(any(CorporateActionEvent.class)))
                .thenReturn(CompletableFuture.completedFuture(null));

        new CorporateActionSeeder(publisher, PROPERTIES, CLOCK).run(null);

        verify(publisher, times(PROPERTIES.seed().actions().size()))
                .publish(any(CorporateActionEvent.class));
    }

    @Test
    @DisplayName("should skip an action the validator rejects rather than failing startup")
    void should_skip_an_action_the_validator_rejects_rather_than_failing_startup() {
        CorporateActionPublisher publisher = mock(CorporateActionPublisher.class);
        when(publisher.publish(any(CorporateActionEvent.class)))
                .thenThrow(new IllegalArgumentException("missing required attributes"))
                .thenReturn(CompletableFuture.completedFuture(null));

        assertThatCode(() -> new CorporateActionSeeder(publisher, PROPERTIES, CLOCK).run(null))
                .as("seed data that is already wrong must not take the service down on boot")
                .doesNotThrowAnyException();

        verify(publisher, times(PROPERTIES.seed().actions().size()))
                .publish(any(CorporateActionEvent.class));
    }

    @Test
    @DisplayName("should build actions the validator accepts")
    void should_build_actions_the_validator_accepts() {
        CorporateActionPublisher publisher = mock(CorporateActionPublisher.class);
        when(publisher.publish(any(CorporateActionEvent.class)))
                .thenReturn(CompletableFuture.completedFuture(null));
        ArgumentCaptor<CorporateActionEvent> captor =
                ArgumentCaptor.forClass(CorporateActionEvent.class);

        new CorporateActionSeeder(publisher, PROPERTIES, CLOCK).run(null);
        verify(publisher, times(PROPERTIES.seed().actions().size())).publish(captor.capture());

        List<CorporateActionEvent> seeded = captor.getAllValues();
        assertThat(seeded).isNotEmpty();
        seeded.forEach(action -> assertThatCode(() -> CorporateActionValidator.validate(action))
                .as("a default seed the validator rejects publishes nothing, which in the "
                        + "identity test is indistinguishable from an authorization denial")
                .doesNotThrowAnyException());
    }
}
