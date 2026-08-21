package dev.engnotes.fes.referencedata;

import java.util.Map;

import dev.engnotes.fes.events.InstrumentReferenceEvent;
import dev.engnotes.fes.testing.SecureKafkaStack;
import dev.engnotes.fes.testing.ServiceIdentityContract;
import org.junit.jupiter.api.DisplayName;

@DisplayName("reference-data-service service identity")
class ReferenceDataServiceIdentityStackTest extends ServiceIdentityContract {

    @Override
    protected String principal() {
        return "reference-data-service";
    }

    @Override
    protected String authorizationErrorMarker() {
        return "TopicAuthorizationException";
    }

    @Override
    protected String successMarker() {
        // InstrumentReferencePublisher logs this from the send callback, so it appears only once
        // the broker has accepted a record. The seeder's own summary line is logged unconditionally,
        // including on a denied run where every send is skipped, so it cannot serve as this marker.
        return "Published instrument reference";
    }

    @Override
    protected Map<String, String> extraEnvironment() {
        return Map.of(
                "FES_REFERENCE_DATA_SERVICE_SEED_ENABLED", "true",
                // The success line is at DEBUG. Raising the service's own logger is smaller than
                // adding a consumer to the contract, and it changes no shipped configuration.
                "LOGGING_LEVEL_DEV_ENGNOTES_FES_REFERENCEDATA", "DEBUG");
    }

    @Override
    protected void prepareBroker() {
        SecureKafkaStack.registerSubject(
                "reference-data.instruments-value", InstrumentReferenceEvent.getClassSchema().toString());
    }
}
