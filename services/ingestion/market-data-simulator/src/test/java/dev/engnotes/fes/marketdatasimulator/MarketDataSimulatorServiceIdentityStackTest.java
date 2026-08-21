package dev.engnotes.fes.marketdatasimulator;

import java.util.Map;

import dev.engnotes.fes.events.MarketDataTickEvent;
import dev.engnotes.fes.testing.SecureKafkaStack;
import dev.engnotes.fes.testing.ServiceIdentityContract;
import org.junit.jupiter.api.DisplayName;

@DisplayName("market-data-simulator service identity")
class MarketDataSimulatorServiceIdentityStackTest extends ServiceIdentityContract {

    @Override
    protected String principal() {
        return "market-data-simulator";
    }

    @Override
    protected String authorizationErrorMarker() {
        return "TopicAuthorizationException";
    }

    @Override
    protected String successMarker() {
        // MarketDataTickPublisher logs this from the send callback, so it appears only once the
        // broker has accepted a record. A startup line would prove the process booted and nothing
        // more.
        return "Published tick";
    }

    @Override
    protected Map<String, String> extraEnvironment() {
        return Map.of(
                "FES_MARKET_DATA_SIMULATOR_GENERATION_ENABLED", "true",
                // The success line is at DEBUG. Raising the service's own logger is smaller than
                // adding a consumer to the contract, and it changes no shipped configuration.
                "LOGGING_LEVEL_DEV_ENGNOTES_FES_MARKETDATASIMULATOR", "DEBUG");
    }

    @Override
    protected void prepareBroker() {
        SecureKafkaStack.registerSubject(
                "market-data.ticks-value", MarketDataTickEvent.getClassSchema().toString());
    }
}
