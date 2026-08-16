package dev.engnotes.fes.marketdatasimulator;

import dev.engnotes.fes.testing.KafkaProducerAuthorizationContract;
import org.junit.jupiter.api.DisplayName;

@DisplayName("market-data-simulator workload identity")
class MarketDataSimulatorAuthorizationTest extends KafkaProducerAuthorizationContract {

    @Override
    protected String principal() {
        return "market-data-simulator";
    }

    @Override
    protected String ownTopic() {
        return "market-data.ticks";
    }

    @Override
    protected String foreignTopic() {
        // A simulator able to write trades could fabricate executions that no trader placed.
        return "trades.raw";
    }
}
