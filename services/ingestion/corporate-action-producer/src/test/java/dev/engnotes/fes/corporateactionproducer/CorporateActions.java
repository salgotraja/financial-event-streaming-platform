package dev.engnotes.fes.corporateactionproducer;

import java.time.Instant;
import java.util.Map;

import dev.engnotes.fes.events.CorporateActionEvent;
import dev.engnotes.fes.events.CorporateActionType;

/** Factory methods for corporate action test data, one per action type. */
final class CorporateActions {

    static final String TRACEPARENT = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
    static final Instant ANNOUNCED_AT = Instant.parse("2026-08-16T09:15:00Z");
    static final Instant EFFECTIVE_AT = Instant.parse("2026-09-01T09:15:00Z");

    private CorporateActions() {
    }

    static CorporateActionEvent split(String id, String ticker) {
        return action(id, ticker, CorporateActionType.STOCK_SPLIT, Map.of("splitRatio", "2:1"));
    }

    static CorporateActionEvent dividend(String id, String ticker) {
        return action(id, ticker, CorporateActionType.DIVIDEND_DECLARATION,
                Map.of("dividendPerShare", "11.00"));
    }

    static CorporateActionEvent rightsIssue(String id, String ticker) {
        return action(id, ticker, CorporateActionType.RIGHTS_ISSUE,
                Map.of("ratio", "1:15", "subscriptionPrice", "1257.00"));
    }

    static CorporateActionEvent earnings(String id, String ticker) {
        return action(id, ticker, CorporateActionType.EARNINGS_ANNOUNCEMENT, Map.of());
    }

    static CorporateActionEvent action(String id,
                                       String ticker,
                                       CorporateActionType type,
                                       Map<String, String> attributes) {
        return CorporateActionEvent.newBuilder()
                .setCorporateActionId(id)
                .setTicker(ticker)
                .setActionType(type)
                .setAttributes(attributes)
                .setAnnouncedAt(ANNOUNCED_AT)
                .setEffectiveAt(EFFECTIVE_AT)
                .setProducedAt(ANNOUNCED_AT.plusMillis(4))
                .setCorrelationId("corr-1")
                .setTraceContext(Map.of("traceparent", TRACEPARENT))
                .build();
    }
}
