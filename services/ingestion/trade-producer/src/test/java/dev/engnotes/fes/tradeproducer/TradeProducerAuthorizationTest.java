package dev.engnotes.fes.tradeproducer;

import dev.engnotes.fes.testing.KafkaProducerAuthorizationContract;
import org.junit.jupiter.api.DisplayName;

@DisplayName("trade-producer workload identity")
class TradeProducerAuthorizationTest extends KafkaProducerAuthorizationContract {

    @Override
    protected String principal() {
        return "trade-producer";
    }

    @Override
    protected String ownTopic() {
        return "trades.raw";
    }

    @Override
    protected String foreignTopic() {
        // The enriched stream, which only trade-enrichment-service may write. Picking a downstream
        // topic rather than a sibling producer's makes the denial the one that matters: a producer
        // that could write trades.enriched could inject trades that skipped enrichment entirely.
        return "trades.enriched";
    }
}
