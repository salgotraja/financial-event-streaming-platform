package dev.engnotes.fes.marketdatacacheprojector;

import java.util.List;

/**
 * What one call to the projection script did.
 *
 * <p>The tick outcome and the window outcome are separate because the two structures are guarded
 * separately (ADR-033). A tick can be a DUPLICATE for the latest-price hash and still contribute its
 * volume to the window, which is the case that a single shared guard would get wrong.
 *
 * @param outcome       what the compare-and-set did with the latest-price hash
 * @param windowApplied whether the window accepted this record's volume
 * @param windowBuckets how many buckets the window holds after this call
 */
public record ProjectionResult(ProjectionOutcome outcome, boolean windowApplied, int windowBuckets) {

    static ProjectionResult of(List<?> scriptResult) {
        if (scriptResult == null || scriptResult.size() != 3) {
            throw new IllegalStateException(
                    "project-tick.lua must return three values, got " + scriptResult);
        }
        // Coerced rather than cast. A Lua array comes back through Spring Data's script executor
        // deserialised by the template's serializer, so the elements arrive as Long on one path and
        // as String on another depending on how the script bean is configured. Both are the same
        // number; a straight cast would fail on whichever path the configuration did not take.
        return new ProjectionResult(
                ProjectionOutcome.of(asLong(scriptResult.get(0))),
                asLong(scriptResult.get(1)) == 1L,
                (int) asLong(scriptResult.get(2)));
    }

    private static long asLong(Object value) {
        return switch (value) {
            case Number number -> number.longValue();
            case String text -> Long.parseLong(text);
            case null -> throw new IllegalStateException("project-tick.lua returned a null element");
            default -> throw new IllegalStateException(
                    "project-tick.lua returned an unmappable element of type "
                            + value.getClass().getName());
        };
    }
}
