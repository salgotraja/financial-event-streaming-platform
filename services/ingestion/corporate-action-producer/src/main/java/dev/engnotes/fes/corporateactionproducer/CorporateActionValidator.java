package dev.engnotes.fes.corporateactionproducer;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import dev.engnotes.fes.events.CorporateActionEvent;
import dev.engnotes.fes.events.CorporateActionType;

/**
 * Rejects corporate actions that are structurally valid Avro but useless to a consumer.
 *
 * <p>{@code attributes} is an open {@code map<string,string>}, so Avro accepts a {@code STOCK_SPLIT}
 * carrying no split ratio. Downstream that becomes a scheduled-event context the
 * {@code anomaly-candidate-service} cannot reason about, and the failure surfaces as a bad
 * investigation rather than as a rejected publish. Validating at the producer is the only place the
 * error is still cheap and attributable.
 *
 * <p>The attribute keys below are derived from the {@code doc} field on the schema, which names
 * {@code splitRatio} and {@code dividendPerShare} as examples. No document specifies the full set,
 * so the requirement for {@code RIGHTS_ISSUE} follows the same shape and
 * {@code EARNINGS_ANNOUNCEMENT} requires nothing beyond its dates: an announcement carries its
 * meaning in {@code effectiveAt}.
 */
final class CorporateActionValidator {

    private CorporateActionValidator() {
    }

    static Set<String> requiredAttributes(CorporateActionType actionType) {
        return switch (actionType) {
            case DIVIDEND_DECLARATION -> Set.of("dividendPerShare");
            case STOCK_SPLIT -> Set.of("splitRatio");
            case RIGHTS_ISSUE -> Set.of("ratio", "subscriptionPrice");
            case EARNINGS_ANNOUNCEMENT -> Set.of();
        };
    }

    static void validate(CorporateActionEvent action) {
        if (action.getEffectiveAt().isBefore(action.getAnnouncedAt())) {
            throw new IllegalArgumentException(
                    "Corporate action %s takes effect before it was announced: announcedAt=%s effectiveAt=%s"
                            .formatted(action.getCorporateActionId(), action.getAnnouncedAt(),
                                    action.getEffectiveAt()));
        }

        // The schema defaults attributes to {} and the field is not nullable. Named here rather than
        // left to the serializer, which would report it as an Avro encoding failure at send time.
        Map<String, String> attributes = Objects.requireNonNull(action.getAttributes(),
                "Corporate action " + action.getCorporateActionId() + " has null attributes");
        List<String> missing = requiredAttributes(action.getActionType()).stream()
                .filter(key -> attributes.get(key) == null || attributes.get(key).isBlank())
                .sorted()
                .toList();

        if (!missing.isEmpty()) {
            throw new IllegalArgumentException(
                    "Corporate action %s of type %s is missing required attributes %s"
                            .formatted(action.getCorporateActionId(), action.getActionType(), missing));
        }
    }
}
