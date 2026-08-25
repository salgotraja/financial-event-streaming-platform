package dev.engnotes.fes.riskalert.governance;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import dev.engnotes.fes.events.RuleState;

/**
 * Every transition seen for one {@code ruleId}, answering which version is in force at an instant.
 *
 * <p><strong>Only ACTIVE and RETIRED decide what is in force.</strong> DRAFT, PENDING_APPROVAL and
 * REJECTED are folded and kept but ignored by selection, and that is load-bearing rather than
 * tidiness. A plain latest-transition-wins rule would read a v2 draft published alongside a live v1
 * as evidence that no rule is in force, and would read a rejected v2 as retiring v1. Neither is what
 * those states mean: REJECTED is version level and kills a proposal, RETIRED is rule level and turns
 * the rule off. Reinstatement after a retirement is a later ACTIVE transition at a higher version,
 * which this ordering handles with no special case, and which is why rollback is a new version
 * rather than a rewrite of history.
 *
 * <p>Ordering is {@code (inForceFrom, version)}. The version tiebreak matters: millisecond
 * timestamps collide under any realistic governance burst, and arrival order must not decide a
 * verdict that replay has to reproduce.
 *
 * <p>Not thread-safe on its own. {@code RiskRuleRegistry} owns the concurrency, publishing an
 * immutable snapshot the listener threads read.
 */
public class RuleTimeline {

    private static final Comparator<RuleTransition> ORDER =
            Comparator.comparingLong(RuleTransition::inForceFrom)
                    .thenComparingLong(RuleTransition::version);

    // Keyed by (version, state) so a replayed record is idempotent, while a version that legitimately
    // moves DRAFT then ACTIVE keeps both transitions.
    private final Map<String, RuleTransition> transitions = new HashMap<>();
    private volatile String ruleType;

    public void apply(RuleTransition transition) {
        this.ruleType = transition.ruleType();
        transitions.put(transition.version() + ":" + transition.state(), transition);
    }

    public int size() {
        return transitions.size();
    }

    // A transition dated in the future has not taken the type over yet, so the bootstrap must still
    // apply. Once a transition has taken effect by the instant, the type stays governed even if the
    // current resolution at that instant is a retirement, so a governed type never falls back to the
    // bootstrap once it has been governed as of that instant.
    public boolean governsAt(String candidateType, long instant) {
        return candidateType.equals(ruleType) && transitions.values().stream()
                .filter(transition -> transition.inForceFrom() <= instant)
                .anyMatch(transition -> transition.state() == RuleState.ACTIVE
                        || transition.state() == RuleState.RETIRED);
    }

    public Optional<ActiveRule> inForceAt(long instant) {
        return transitions.values().stream()
                .filter(transition -> transition.inForceFrom() <= instant)
                .filter(transition -> transition.state() == RuleState.ACTIVE
                        || transition.state() == RuleState.RETIRED)
                .max(ORDER)
                .filter(transition -> transition.state() == RuleState.ACTIVE)
                .map(transition -> new ActiveRule(transition.ruleId(), transition.ruleType(),
                        transition.version(), transition.parameters()));
    }
}
