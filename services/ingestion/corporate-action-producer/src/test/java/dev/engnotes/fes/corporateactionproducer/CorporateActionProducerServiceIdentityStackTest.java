package dev.engnotes.fes.corporateactionproducer;

import java.util.Map;

import dev.engnotes.fes.events.CorporateActionEvent;
import dev.engnotes.fes.testing.SecureKafkaStack;
import dev.engnotes.fes.testing.ServiceIdentityContract;
import org.junit.jupiter.api.DisplayName;

@DisplayName("corporate-action-producer service identity")
class CorporateActionProducerServiceIdentityStackTest extends ServiceIdentityContract {

    @Override
    protected String principal() {
        return "corporate-action-producer";
    }

    @Override
    protected String authorizationErrorMarker() {
        return "TopicAuthorizationException";
    }

    @Override
    protected String successMarker() {
        // CorporateActionPublisher logs this from the send callback, so it appears only once the
        // broker has accepted a record. The seeder's own summary line is logged unconditionally,
        // including on a denied run where every send is skipped, so it cannot serve as this marker.
        return "Published corporate action";
    }

    @Override
    protected Map<String, String> extraEnvironment() {
        return Map.of(
                "FES_CORPORATE_ACTION_PRODUCER_SEED_ENABLED", "true",
                // The success line is at DEBUG. Raising the service's own logger is smaller than
                // adding a consumer to the contract, and it changes no shipped configuration.
                "LOGGING_LEVEL_DEV_ENGNOTES_FES_CORPORATEACTIONPRODUCER", "DEBUG");
    }

    @Override
    protected void prepareBroker() {
        SecureKafkaStack.registerSubject(
                "corporate-actions-value", CorporateActionEvent.getClassSchema().toString());
    }
}
