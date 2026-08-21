package dev.engnotes.fes.tradeproducer;

import java.util.Map;

import dev.engnotes.fes.events.TradeEvent;
import dev.engnotes.fes.testing.SecureKafkaStack;
import dev.engnotes.fes.testing.ServiceIdentityContract;
import org.junit.jupiter.api.DisplayName;

@DisplayName("trade-producer service identity")
class TradeProducerServiceIdentityStackTest extends ServiceIdentityContract {

    @Override
    protected String principal() {
        return "trade-producer";
    }

    @Override
    protected String authorizationErrorMarker() {
        return "TopicAuthorizationException";
    }

    @Override
    protected String successMarker() {
        // TradeEventPublisher logs this from the send callback, so it appears only once the broker
        // has accepted a record. A startup line would prove the process booted and nothing more.
        return "Published trade";
    }

    @Override
    protected Map<String, String> extraEnvironment() {
        return Map.of(
                "FES_TRADE_PRODUCER_GENERATION_ENABLED", "true",
                // The success line is at DEBUG. Raising the service's own logger is smaller than
                // adding a consumer to the contract, and it changes no shipped configuration.
                "LOGGING_LEVEL_DEV_ENGNOTES_FES_TRADEPRODUCER", "DEBUG");
    }

    @Override
    protected void prepareBroker() {
        SecureKafkaStack.registerSubject("trades.raw-value", TradeEvent.getClassSchema().toString());
    }
}
