package dev.engnotes.fes.marketdatacacheprojector;

/** What the compare-and-set did with a tick (ADR-032). */
public enum ProjectionOutcome {

    APPLIED,
    /** The stored entry already carries this timestamp: ordinary at-least-once redelivery. */
    DUPLICATE,
    /** The tick is older than what is stored, so applying it would install a stale price. */
    OLDER;

    static ProjectionOutcome of(Long scriptResult) {
        if (scriptResult == null) {
            throw new IllegalStateException(
                    "project-tick.lua returned no value, so it is unknown whether the tick applied");
        }
        return switch (scriptResult.intValue()) {
            case 1 -> APPLIED;
            case 0 -> DUPLICATE;
            case -1 -> OLDER;
            default -> throw new IllegalStateException(
                    "project-tick.lua returned an unmapped value " + scriptResult);
        };
    }
}
