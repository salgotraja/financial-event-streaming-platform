package dev.engnotes.fes.riskalert.governance;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Every rule timeline the fold has seen, plus the ungoverned bootstrap set.
 *
 * <p><strong>{@code ruleType} dispatches, {@code ruleId} is the governance identity.</strong>
 * {@code AlertType} is a fixed four-symbol enum, so the alert type is a property of the
 * implementation and not of the governed data. Several {@code ruleId}s may share a
 * {@code ruleType}, which is how per-ticker thresholds arrive later with no schema change, and each
 * is evaluated independently.
 *
 * <p><strong>The bootstrap is suppressed per rule type, not per rule id.</strong> Keying the
 * suppression on {@code ruleId} would let governance create a rule with a new id and never outrank
 * the YAML default, so both would alert on the same trade. Suppression is also not undone by a
 * retirement: if governance turned a rule type off, falling back to an ungoverned default would
 * re-enable alerting an approver deliberately retired.
 *
 * <p>Concurrency: the fold thread writes, listener threads read. {@link ConcurrentHashMap} plus the
 * per-timeline lock below is enough because a reader never sees a partially applied transition, and
 * a reader that misses a transition by microseconds simply evaluates against the version in force a
 * moment earlier, which is a legitimate answer under at-least-once rather than an error.
 */
public class RiskRuleRegistry {

    private final Map<String, RuleTimeline> timelines = new ConcurrentHashMap<>();
    private final Map<String, List<BootstrapRuleProperties.BootstrapRule>> bootstrapByType;

    public RiskRuleRegistry(BootstrapRuleProperties bootstrap) {
        this.bootstrapByType = bootstrap.rules().stream()
                .collect(Collectors.groupingBy(BootstrapRuleProperties.BootstrapRule::ruleType));
    }

    public void apply(RuleTransition transition) {
        RuleTimeline timeline = timelines.computeIfAbsent(transition.ruleId(), id -> new RuleTimeline());
        synchronized (timeline) {
            timeline.apply(transition);
        }
    }

    public int timelineCount() {
        return timelines.size();
    }

    public List<ActiveRule> inForceAt(String ruleType, long instant) {
        List<ActiveRule> governed = new ArrayList<>();
        boolean anyTimelineOfThisType = false;

        for (RuleTimeline timeline : timelines.values()) {
            ActiveRule resolved;
            boolean governsAt;
            synchronized (timeline) {
                resolved = timeline.inForceAt(instant).orElse(null);
                governsAt = timeline.governsAt(ruleType, instant);
            }
            if (resolved != null && resolved.ruleType().equals(ruleType)) {
                governed.add(resolved);
            }
            if (governsAt) {
                anyTimelineOfThisType = true;
            }
        }

        if (!governed.isEmpty() || anyTimelineOfThisType) {
            return List.copyOf(governed);
        }
        return bootstrapByType.getOrDefault(ruleType, List.of()).stream()
                .map(rule -> new ActiveRule(rule.ruleId(), rule.ruleType(), 0L,
                        Map.copyOf(rule.parameters())))
                .toList();
    }
}
