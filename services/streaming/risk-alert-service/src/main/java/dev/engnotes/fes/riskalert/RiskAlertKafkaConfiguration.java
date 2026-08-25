package dev.engnotes.fes.riskalert;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import dev.engnotes.fes.common.kafka.KafkaSaslProfile;
import dev.engnotes.fes.riskalert.governance.BootstrapRuleProperties;
import dev.engnotes.fes.riskalert.governance.RiskRuleRegistry;
import dev.engnotes.fes.riskalert.governance.RuleTimelineLoader;
import dev.engnotes.fes.riskalert.governance.RuleTransition;
import dev.engnotes.fes.riskalert.rules.PriceDeviationParameters;
import dev.engnotes.fes.riskalert.rules.PriceDeviationRule;
import dev.engnotes.fes.riskalert.rules.RiskRule;
import dev.engnotes.fes.riskalert.rules.RiskRuleEngine;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Rules-consumer wiring and the readiness gate for the governed rule fold (ADR-035).
 *
 * <p>The error handler for {@code trades.enriched} arrives in Task 9; this class carries only the
 * rule fold and the beans that make {@link RiskRuleRegistry}, the {@link RiskRule} implementations
 * and {@link RiskRuleEngine} usable as Spring beans. Task 6 deliberately shipped those three
 * unannotated, because the registry was not itself a bean yet and annotating them broke the
 * context; this configuration class is where that wiring belongs instead.
 */
@Configuration(proxyBeanMethods = false)
public class RiskAlertKafkaConfiguration {

    private static final Logger log = LoggerFactory.getLogger(RiskAlertKafkaConfiguration.class);

    @Bean
    RiskRuleRegistry riskRuleRegistry(BootstrapRuleProperties bootstrap) {
        return new RiskRuleRegistry(bootstrap);
    }

    @Bean
    PriceDeviationRule priceDeviationRule() {
        return new PriceDeviationRule();
    }

    @Bean
    RiskRuleEngine riskRuleEngine(RiskRuleRegistry registry, List<RiskRule> rules) {
        return new RiskRuleEngine(registry, rules);
    }

    /**
     * Dispatches a governed transition to the parameter type of the {@link RiskRule} that serves its
     * {@code ruleType}, so a malformed governed version is rejected before it ever reaches the
     * registry. A {@code ruleType} with no implementation in this increment is accepted unparsed,
     * because increment 1 cannot know what a later increment's parameters look like.
     */
    @Bean
    Consumer<RuleTransition> ruleTransitionValidator() {
        return transition -> {
            if (PriceDeviationRule.RULE_TYPE.equals(transition.ruleType())) {
                PriceDeviationParameters.from(transition.parameters());
            }
        };
    }

    /**
     * The readiness gate. See {@code EnrichmentKafkaConfiguration} for the full reasoning this class
     * copies: {@code SmartInitializingSingleton} rather than an {@code ApplicationReadyEvent}
     * listener because the wait must complete before the context reports ready, so a timeout fails
     * startup instead of leaving a running service with a partial fold.
     *
     * <p>Starting the trade listener once the fold completes is Task 8's {@code SmartLifecycle}
     * bean, not this callback: {@link org.springframework.kafka.config.KafkaListenerEndpointRegistry}
     * is populated by Spring Kafka's own {@code SmartInitializingSingleton}, and those callbacks run
     * in bean-definition registration order rather than dependency order, so a lookup from this
     * callback can return null. {@code onLoaded} here is a no-op until Task 10 wires the metrics
     * gauge, exactly as {@code EnrichmentKafkaConfiguration} leaves listener start to a separate
     * {@code SmartLifecycle} bean.
     */
    @Bean(destroyMethod = "close")
    RuleTimelineLoader ruleTimelineLoader(RiskRuleRegistry registry,
                                          RiskAlertProperties properties,
                                          KafkaProperties kafkaProperties,
                                          Consumer<RuleTransition> ruleTransitionValidator,
                                          ObjectProvider<KafkaSaslProfile> saslProfile) {

        // Boot 4.1 moved this class out of spring-boot-autoconfigure. The import is
        // org.springframework.boot.kafka.autoconfigure.KafkaProperties, and buildConsumerProperties
        // takes no argument on this version, read off the resolved spring-boot-kafka-4.1.0 jar via
        // EnrichmentKafkaConfiguration rather than assumed here.
        Map<String, Object> consumerProperties = new HashMap<>(kafkaProperties.buildConsumerProperties());
        // This consumer is built by hand rather than through Spring Boot's autoconfigured
        // ConsumerFactory, so KafkaSecurityConfiguration's DefaultKafkaConsumerFactoryCustomizer
        // never touches it. Without this, the rule consumer would connect as PLAINTEXT under the
        // secure-kafka profile regardless of the broker's own listener, and the readiness gate would
        // fail every startup with a metadata timeout rather than an authorization error.
        // KafkaSaslProfile only exists under that profile, hence the optional provider.
        saslProfile.ifAvailable(profile -> consumerProperties.putAll(profile.properties()));
        // No group and no committed offsets: this consumer assign()s every partition, so a group id
        // would be misleading and a GROUP grant for it would be an unnecessary permission.
        consumerProperties.remove(ConsumerConfig.GROUP_ID_CONFIG);
        // ErrorHandlingDeserializer wrapping KafkaAvroDeserializer, not the plain delegate the
        // Task 7 brief's Step 5 named. That instruction and the brief's own requirement that a
        // decode failure on this topic "must never fail startup" cannot both hold: KafkaConsumer
        // deserializes every record inside poll() itself, before any ConsumerRecords is returned,
        // so a plain KafkaAvroDeserializer throws SerializationException straight out of poll(),
        // never reaching RuleTimelineLoader.fold() at all, and loadInitialSnapshot()'s outer
        // catch (RuntimeException) closes the consumer and rethrows, failing the readiness gate
        // and therefore startup on one corrupt control-plane record. That is precisely the
        // streaming-plane outage ADR-027 and this class exist to prevent, so the requirement wins
        // over the mechanism copied from EnrichmentKafkaConfiguration, whose failure surface
        // differs because its trade listener runs under Spring Kafka's container and error
        // handler rather than a hand-rolled poll() loop. ErrorHandlingDeserializer catches the
        // SerializationException inside its own deserialize() call and returns null with the
        // failure recorded on a header instead of throwing, so poll() returns normally and
        // fold() sees an ordinary null value it can log, count, and skip. There is still no DLQ
        // for risk-rules.events and this identity still holds no write grant on it: the wrapper
        // exists to keep the exception inside this process rather than to quarantine the bytes
        // anywhere.
        consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                org.springframework.kafka.support.serializer.ErrorHandlingDeserializer.class);
        consumerProperties.put(
                org.springframework.kafka.support.serializer.ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS,
                io.confluent.kafka.serializers.KafkaAvroDeserializer.class);
        consumerProperties.put("specific.avro.reader", true);

        return new RuleTimelineLoader(registry, consumerProperties, properties.ruleTopic(),
                properties.ruleTimelineTimeout(), ruleTransitionValidator,
                reason -> log.warn("Rejected governed rule version, reason={}", reason),
                () -> { });
    }

    @Bean
    SmartInitializingSingleton openTheReadinessGate(RuleTimelineLoader loader) {
        return loader::loadInitialSnapshot;
    }
}
