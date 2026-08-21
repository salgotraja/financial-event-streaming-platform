package dev.engnotes.fes.corporateactionproducer;

import java.time.Clock;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;

import dev.engnotes.fes.corporateactionproducer.CorporateActionProducerProperties.Action;
import dev.engnotes.fes.events.CorporateActionEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;

/**
 * Announces the configured corporate actions once at startup (FR-01.3).
 *
 * <p>A one-shot announcement rather than a paced stream, because corporate actions are events in
 * the world and not a rate. This is the shape {@code InstrumentMasterSeeder} takes for the same
 * reason.
 *
 * <p>Off unless enabled. A restart would otherwise re-announce every action, and a downstream
 * consumer holding scheduled-event context would see a second announcement of something that has
 * already taken effect.
 *
 * <p>A seed the publisher's validator rejects is logged and skipped rather than thrown. Throwing
 * would fail application startup over seed data, taking the service down when the correct response
 * is to announce the actions that are well formed and say which were not.
 */
public class CorporateActionSeeder implements ApplicationRunner {

    private static final Logger log = LoggerFactory.getLogger(CorporateActionSeeder.class);

    private final CorporateActionPublisher publisher;
    private final CorporateActionProducerProperties properties;
    private final Clock clock;

    public CorporateActionSeeder(CorporateActionPublisher publisher,
                                 CorporateActionProducerProperties properties,
                                 Clock clock) {
        this.publisher = publisher;
        this.properties = properties;
        this.clock = clock;
    }

    @Override
    public void run(ApplicationArguments args) {
        var actions = properties.seed().actions();
        long announced = actions.stream().filter(this::announce).count();
        log.info("Announced {} of {} corporate actions to {}",
                announced, actions.size(), properties.topic());
    }

    private boolean announce(Action action) {
        try {
            publisher.publish(toEvent(action));
            return true;
        } catch (IllegalArgumentException e) {
            log.warn("Skipped seeding corporate action for {}: {}", action.ticker(), e.getMessage());
            return false;
        }
    }

    private CorporateActionEvent toEvent(Action action) {
        Instant announcedAt = clock.instant();
        return CorporateActionEvent.newBuilder()
                .setCorporateActionId(UUID.randomUUID().toString())
                .setTicker(action.ticker())
                .setActionType(action.actionType())
                .setAttributes(action.attributes())
                .setAnnouncedAt(announcedAt)
                .setEffectiveAt(announcedAt.plus(action.effectiveIn()))
                .setProducedAt(announcedAt)
                .setCorrelationId(UUID.randomUUID().toString())
                .setTraceContext(Map.of())
                .build();
    }
}
