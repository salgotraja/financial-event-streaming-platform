package dev.engnotes.fes.riskalert.governance;

import java.util.Map;

import dev.engnotes.fes.events.RuleState;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class RuleTimelineTest {

    private static final Map<String, String> BANDS =
            Map.of("warn-deviation-percent", "2.0", "critical-deviation-percent", "5.0");

    private static RuleTransition transition(long version, RuleState state, long inForceFrom) {
        return new RuleTransition("pd", "price-deviation", version, state, BANDS, inForceFrom);
    }

    @Test
    void an_empty_timeline_has_nothing_in_force() {
        assertThat(new RuleTimeline().inForceAt(1_000L)).isEmpty();
    }

    @Test
    void a_version_is_not_in_force_before_its_effective_instant() {
        RuleTimeline timeline = new RuleTimeline();
        timeline.apply(transition(1, RuleState.ACTIVE, 1_000L));

        assertThat(timeline.inForceAt(999L)).isEmpty();
        assertThat(timeline.inForceAt(1_000L)).isPresent();
    }

    @Test
    void the_highest_version_effective_at_the_instant_wins() {
        RuleTimeline timeline = new RuleTimeline();
        timeline.apply(transition(1, RuleState.ACTIVE, 1_000L));
        timeline.apply(transition(2, RuleState.ACTIVE, 2_000L));

        assertThat(timeline.inForceAt(1_500L)).get().extracting(ActiveRule::version).isEqualTo(1L);
        assertThat(timeline.inForceAt(2_500L)).get().extracting(ActiveRule::version).isEqualTo(2L);
    }

    @Test
    void a_retirement_turns_the_rule_off_from_its_own_instant_onward() {
        RuleTimeline timeline = new RuleTimeline();
        timeline.apply(transition(1, RuleState.ACTIVE, 1_000L));
        timeline.apply(transition(2, RuleState.RETIRED, 2_000L));

        // The load-bearing assertion. A trade that executed before the retirement is still evaluated
        // against the rule that was in force then, so replaying it after the retirement produces the
        // same alert and the same alertId. A current-active-set that dropped retired rules by latest
        // state would return empty here and make replay diverge.
        assertThat(timeline.inForceAt(1_500L)).get().extracting(ActiveRule::version).isEqualTo(1L);
        assertThat(timeline.inForceAt(2_500L)).isEmpty();
    }

    @Test
    void a_later_activation_reinstates_a_retired_rule() {
        RuleTimeline timeline = new RuleTimeline();
        timeline.apply(transition(1, RuleState.ACTIVE, 1_000L));
        timeline.apply(transition(2, RuleState.RETIRED, 2_000L));
        timeline.apply(transition(3, RuleState.ACTIVE, 3_000L));

        assertThat(timeline.inForceAt(3_500L)).get().extracting(ActiveRule::version).isEqualTo(3L);
    }

    @Test
    void a_draft_alongside_a_live_version_does_not_take_the_rule_out_of_force() {
        RuleTimeline timeline = new RuleTimeline();
        timeline.apply(transition(1, RuleState.ACTIVE, 1_000L));
        timeline.apply(transition(2, RuleState.DRAFT, 2_000L));
        timeline.apply(transition(2, RuleState.PENDING_APPROVAL, 2_100L));

        assertThat(timeline.inForceAt(2_500L)).get().extracting(ActiveRule::version).isEqualTo(1L);
    }

    @Test
    void a_rejected_proposal_does_not_retire_the_live_version() {
        RuleTimeline timeline = new RuleTimeline();
        timeline.apply(transition(1, RuleState.ACTIVE, 1_000L));
        timeline.apply(transition(2, RuleState.REJECTED, 2_000L));

        // REJECTED is version level and kills a proposal. RETIRED is rule level and turns the rule
        // off. Conflating them would let a rejected draft silently disable risk evaluation.
        assertThat(timeline.inForceAt(2_500L)).get().extracting(ActiveRule::version).isEqualTo(1L);
    }

    @Test
    void the_parameters_of_the_selected_version_are_returned() {
        RuleTimeline timeline = new RuleTimeline();
        timeline.apply(new RuleTransition("pd", "price-deviation", 1, RuleState.ACTIVE,
                Map.of("warn-deviation-percent", "2.0", "critical-deviation-percent", "5.0"), 1_000L));
        timeline.apply(new RuleTransition("pd", "price-deviation", 2, RuleState.ACTIVE,
                Map.of("warn-deviation-percent", "1.0", "critical-deviation-percent", "3.0"), 2_000L));

        assertThat(timeline.inForceAt(1_500L)).get()
                .extracting(rule -> rule.parameters().get("warn-deviation-percent")).isEqualTo("2.0");
        assertThat(timeline.inForceAt(2_500L)).get()
                .extracting(rule -> rule.parameters().get("warn-deviation-percent")).isEqualTo("1.0");
    }

    @Test
    void two_transitions_sharing_an_instant_are_ordered_by_version() {
        RuleTimeline timeline = new RuleTimeline();
        timeline.apply(transition(2, RuleState.ACTIVE, 1_000L));
        timeline.apply(transition(1, RuleState.ACTIVE, 1_000L));

        // Millisecond timestamps collide under any realistic governance burst, so the ordering must
        // not depend on which record arrived first.
        assertThat(timeline.inForceAt(1_000L)).get().extracting(ActiveRule::version).isEqualTo(2L);
    }

    @Test
    void applying_the_same_transition_twice_changes_nothing() {
        RuleTimeline timeline = new RuleTimeline();
        timeline.apply(transition(1, RuleState.ACTIVE, 1_000L));
        timeline.apply(transition(1, RuleState.ACTIVE, 1_000L));

        assertThat(timeline.inForceAt(1_500L)).get().extracting(ActiveRule::version).isEqualTo(1L);
        assertThat(timeline.size()).isEqualTo(1);
    }

    @Test
    void governs_at_is_false_for_a_transition_dated_after_the_instant() {
        RuleTimeline timeline = new RuleTimeline();
        timeline.apply(transition(1, RuleState.ACTIVE, 2_000L));

        assertThat(timeline.governsAt("price-deviation", 1_500L)).isFalse();
    }

    @Test
    void governs_at_is_true_for_a_past_active_transition() {
        RuleTimeline timeline = new RuleTimeline();
        timeline.apply(transition(1, RuleState.ACTIVE, 1_000L));

        assertThat(timeline.governsAt("price-deviation", 1_500L)).isTrue();
    }

    @Test
    void governs_at_stays_true_for_a_past_retirement_even_though_nothing_is_in_force() {
        RuleTimeline timeline = new RuleTimeline();
        timeline.apply(transition(1, RuleState.ACTIVE, 1_000L));
        timeline.apply(transition(2, RuleState.RETIRED, 2_000L));

        assertThat(timeline.inForceAt(2_500L)).isEmpty();
        assertThat(timeline.governsAt("price-deviation", 2_500L)).isTrue();
    }
}
