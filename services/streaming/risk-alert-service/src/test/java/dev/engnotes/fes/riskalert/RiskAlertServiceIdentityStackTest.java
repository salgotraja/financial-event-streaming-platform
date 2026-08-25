package dev.engnotes.fes.riskalert;

import java.time.Instant;
import java.util.Map;

import dev.engnotes.fes.events.EnrichedTradeEvent;
import dev.engnotes.fes.events.RiskAlertEvent;
import dev.engnotes.fes.events.Side;
import dev.engnotes.fes.events.TradeEvent;
import dev.engnotes.fes.testing.SecureKafkaStack;
import dev.engnotes.fes.testing.ServiceIdentityContract;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.junit.jupiter.api.DisplayName;

@DisplayName("risk-alert-service service identity")
class RiskAlertServiceIdentityStackTest extends ServiceIdentityContract {

    private static final String TRADE_TOPIC = "trades.enriched";
    private static final String OUTPUT_TOPIC = "notifications.alerts";
    private static final String TICKER = "RELIANCE";
    private static final long FIXED_EVENT_TIMESTAMP_MILLIS = 1_000L;
    // Above the bootstrap price-deviation rule's 5.0 percent critical band, so the granted run's
    // success line only ever appears once the trade has been evaluated and the resulting alert has
    // actually been published, not merely consumed.
    private static final double BREACHING_DEVIATION_PERCENT = 6.0;

    @Override
    protected String principal() {
        return "risk-alert-service";
    }

    @Override
    protected String authorizationErrorMarker() {
        // Deterministic here, unlike trade-enrichment-service. The blocking readiness gate reads
        // risk-rules.events, with no group, before the trade listener is ever started, so an
        // ungranted run is always denied on that read first: the trade listener never gets a chance
        // to run at all, and a GroupAuthorizationException on trades.enriched can never surface.
        return "TopicAuthorizationException";
    }

    @Override
    protected String successMarker() {
        // EnrichedTradeConsumer logs this only after the trade has been evaluated and every alert it
        // raised has been published to notifications.alerts: the publish call happens earlier in the
        // same method and would throw before this line if the write grant were missing. It is at
        // DEBUG because an INFO line per record is the dominant cost at the platform's target rate,
        // so extraEnvironment raises that one logger.
        return "Evaluated trade";
    }

    @Override
    protected Map<String, String> extraEnvironment() {
        return Map.of("LOGGING_LEVEL_DEV_ENGNOTES_FES_RISKALERT", "DEBUG");
    }

    @Override
    protected void prepareBroker() {
        // Both subjects, because this service both consumes and produces Avro records and ships
        // auto.register.schemas: false. Missing the output subject lets the granted run consume and
        // evaluate the trade, then fail the publish, which reports as a denial that never happened.
        SecureKafkaStack.registerSubject(TRADE_TOPIC + "-value", EnrichedTradeEvent.getClassSchema().toString());
        SecureKafkaStack.registerSubject(OUTPUT_TOPIC + "-value", RiskAlertEvent.getClassSchema().toString());

        // risk-rules.events is left empty: the ungoverned bootstrap price-deviation rule from
        // application.yml is enough to raise the alert this test needs, and an empty, already
        // created topic still satisfies the readiness gate's catch-up condition immediately.
        SecureKafkaStack.seed(TRADE_TOPIC, TICKER, avroTrade());
    }

    private static byte[] avroTrade() {
        try (KafkaAvroSerializer serializer = new KafkaAvroSerializer()) {
            serializer.configure(Map.of(
                    AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                    SecureKafkaStack.schemaRegistryUrl(),
                    AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS, false), false);
            return serializer.serialize(TRADE_TOPIC, EnrichedTradeEvent.newBuilder()
                    .setTrade(TradeEvent.newBuilder()
                            .setTradeId("identity-probe-trade")
                            .setCorrelationId("identity-probe")
                            .setTicker(TICKER)
                            .setQuantity(10L)
                            .setPrice(106.0)
                            .setSide(Side.BUY)
                            .setTraderId("TR-1")
                            .setAccountId("AC-1")
                            .setEventTimestamp(Instant.ofEpochMilli(FIXED_EVENT_TIMESTAMP_MILLIS))
                            .setProducedAt(Instant.ofEpochMilli(FIXED_EVENT_TIMESTAMP_MILLIS + 5))
                            .build())
                    .setMidPriceAtExecution(100.0)
                    .setSpreadAtExecution(1.0)
                    .setVwap5Min(100.5)
                    .setMarketCap(1_700_000.0)
                    .setPriceDeviation(BREACHING_DEVIATION_PERCENT)
                    .setEnrichedAt(Instant.ofEpochMilli(FIXED_EVENT_TIMESTAMP_MILLIS + 10))
                    .setEnrichmentLatencyMs(1L)
                    .setMarketDataAgeMs(50L)
                    .build());
        }
    }
}
