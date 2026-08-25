package dev.engnotes.fes.riskalert;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import dev.engnotes.fes.common.kafka.DeadLetterPublisher;
import dev.engnotes.fes.common.kafka.FailureTracker;
import dev.engnotes.fes.common.kafka.KafkaSaslProfile;
import dev.engnotes.fes.events.DeadLetterEvent;
import dev.engnotes.fes.riskalert.governance.BootstrapRuleProperties;
import dev.engnotes.fes.riskalert.governance.RiskRuleRegistry;
import dev.engnotes.fes.riskalert.governance.RuleTimelineLoader;
import dev.engnotes.fes.riskalert.governance.RuleTransition;
import dev.engnotes.fes.riskalert.rules.PriceDeviationParameters;
import dev.engnotes.fes.riskalert.rules.PriceDeviationRule;
import dev.engnotes.fes.riskalert.rules.RiskRule;
import dev.engnotes.fes.riskalert.rules.RiskRuleEngine;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
import org.springframework.context.SmartLifecycle;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.DeserializationException;
import org.springframework.util.backoff.BackOff;
import org.springframework.util.backoff.ExponentialBackOff;

/**
 * Rules-consumer wiring, the readiness gate for the governed rule fold (ADR-035), and the
 * {@code trades.enriched} error handler that separates ADR-027's two failure classes.
 *
 * <p>A malformed payload or a failed validation is one quarantined record, with zero retries.
 * {@code DeserializationException} and {@code IllegalArgumentException} are registered as not
 * retryable, so the recoverer runs on the first attempt and publishes to {@code trades.enriched.dlq},
 * and the offset advances so the partition keeps moving. Retrying either does not help: the bytes do
 * not decode differently on a second attempt, and a non-finite {@code priceDeviation} is a validation
 * verdict on the payload that will not change either.
 *
 * <p>This service calls no external datastore, unlike {@code trade-enrichment-service}. There is no
 * Redis and no reference-data gap here, so there is no dependency-outage branch and no
 * {@code ContainerPausingBackOffHandler}: {@link #riskAlertErrorHandler} takes three arguments, not
 * the five {@code EnrichmentKafkaConfiguration.enrichmentErrorHandler} takes, and every failure other
 * than the two listed above falls through to the same bounded back-off.
 *
 * <p>{@code InvalidRuleParametersException} never reaches this handler. It is thrown during the fold,
 * on the loader's own thread, never on the listener thread, because parameters are validated when a
 * transition is folded rather than when a trade is evaluated.
 *
 * <p>The quarantined bytes come from the exception. {@code ErrorHandlingDeserializer} sets the record
 * value to null and carries the delivered bytes on the {@link DeserializationException}, and
 * {@link DeadLetterPublisher} substitutes an empty array for a null payload. Passing
 * {@code record.value()} through would quarantine nothing at all and lose the only copy of the
 * evidence.
 */
@Configuration(proxyBeanMethods = false)
public class RiskAlertKafkaConfiguration {

    private static final Logger log = LoggerFactory.getLogger(RiskAlertKafkaConfiguration.class);

    // Three attempts total, so two retries after the first failure, matching FR-03.4's shape.
    private static final long MAX_RETRIES = 2;
    private static final long INITIAL_BACKOFF_MS = 100;
    private static final long MAX_BACKOFF_MS = 5_000;
    private static final long MAX_ELAPSED_MS = 5_000;

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

    @Bean
    FailureTracker failureTracker() {
        return new FailureTracker();
    }

    @Bean
    DeadLetterPublisher deadLetterPublisher(KafkaTemplate<String, DeadLetterEvent> kafkaTemplate,
                                            FailureTracker failureTracker,
                                            RiskAlertProperties properties,
                                            @Value("${spring.kafka.consumer.group-id}") String consumerGroup) {

        return new DeadLetterPublisher(kafkaTemplate, failureTracker, consumerGroup,
                properties.consumerInstance());
    }

    /**
     * The two failure classes ADR-027 separates for {@code trades.enriched}. See the class javadoc
     * for the full reasoning; this differs from {@code EnrichmentKafkaConfiguration.
     * enrichmentErrorHandler} only in that there is no dependency to protect here, so there is no
     * back-off function, no {@code ListenerContainerRegistry} and no pausing handler.
     *
     * <p>{@code metrics} records the quarantine through {@link RiskAlertMetrics#recordQuarantined()},
     * added in Task 10.
     */
    @Bean
    DefaultErrorHandler riskAlertErrorHandler(DeadLetterPublisher deadLetterPublisher,
                                              FailureTracker failureTracker,
                                              RiskAlertMetrics metrics) {

        DefaultErrorHandler errorHandler = new DefaultErrorHandler(
                (record, exception) -> quarantine(deadLetterPublisher, metrics, record, exception),
                poisonBackOff());

        // Retrying either of these does not help. A DeserializationException's bytes do not improve
        // on a second attempt. An IllegalArgumentException is a validation verdict on the payload
        // that will not change either.
        errorHandler.addNotRetryableExceptions(DeserializationException.class, IllegalArgumentException.class);
        errorHandler.setRetryListeners(failureTracker);
        // The container commits the recovered record's offset, so one poison payload does not block
        // the partition behind it.
        errorHandler.setAckAfterHandle(true);
        return errorHandler;
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
     * callback can return null. {@code onLoaded} here only binds the {@link RiskAlertMetrics} gauge,
     * exactly as {@code EnrichmentKafkaConfiguration} leaves listener start to a separate
     * {@code SmartLifecycle} bean.
     */
    @Bean(destroyMethod = "close")
    RuleTimelineLoader ruleTimelineLoader(RiskRuleRegistry registry,
                                          RiskAlertProperties properties,
                                          KafkaProperties kafkaProperties,
                                          Consumer<RuleTransition> ruleTransitionValidator,
                                          RiskAlertMetrics metrics,
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
                reason -> {
                    // fold() already logs at ERROR on all three rejection paths; this call must
                    // never throw out of fold(), because the catch-up loop in loadInitialSnapshot()
                    // treats a RuntimeException as fatal and fails startup. A metrics failure here
                    // is a data-visibility problem, not a reason to turn a control-plane record
                    // error into a streaming-plane outage.
                    try {
                        metrics.recordRejectedRuleVersion(reason);
                    } catch (RuntimeException e) {
                        log.warn("Metrics recording failed for rejected rule version reason={}",
                                reason, e);
                    }
                },
                // Bind the gauge before the listener can possibly start, so it can never report a
                // partial fold as though it were complete. Unlike onRejected, this is not swallowed:
                // it runs once, before readiness, matching EnrichmentKafkaConfiguration's own
                // unguarded onLoaded callback.
                () -> metrics.bindRuleRegistry(registry));
    }

    @Bean
    SmartInitializingSingleton openTheReadinessGate(RuleTimelineLoader loader) {
        return loader::loadInitialSnapshot;
    }

    /**
     * See {@link #ruleTimelineLoader} for why this is a {@link SmartLifecycle} rather than part of
     * the loader's {@code onLoaded} callback: {@link KafkaListenerEndpointRegistry} is populated by
     * Spring Kafka's own {@code SmartInitializingSingleton}, and those callbacks run in
     * bean-definition registration order rather than dependency order, so a lookup from the loader's
     * callback can return null. {@code SmartLifecycle.start()} runs in {@code finishRefresh()},
     * unconditionally after every {@code SmartInitializingSingleton} callback in the same context,
     * including the one that runs {@link RuleTimelineLoader#loadInitialSnapshot()}, so the registry
     * is always populated by the time this runs. If the loader had timed out, its exception would
     * already have aborted {@code refresh()} before {@code finishRefresh()} is ever reached, so no
     * explicit dependency on the gate is needed here.
     */
    @Bean
    SmartLifecycle startTheTradeListenerOnceLoaded(KafkaListenerEndpointRegistry registry) {
        return new SmartLifecycle() {
            private volatile boolean running;

            @Override
            public void start() {
                registry.getListenerContainer(EnrichedTradeConsumer.LISTENER_ID).start();
                running = true;
            }

            @Override
            public void stop() {
                running = false;
            }

            @Override
            public boolean isRunning() {
                return running;
            }
        };
    }

    private static void quarantine(DeadLetterPublisher publisher,
                                   RiskAlertMetrics metrics,
                                   ConsumerRecord<?, ?> record,
                                   Exception exception) {

        @SuppressWarnings("unchecked")
        ConsumerRecord<String, ?> failed = (ConsumerRecord<String, ?>) record;
        publisher.publish(failed, originalPayload(failed, exception), exception).join();

        // The dead letter is already published by the time this runs. setAckAfterHandle(true) means
        // a metrics failure here would prevent the ack and redeliver the record, quarantining it a
        // second time, so this must be swallowed the same way EnrichmentKafkaConfiguration.quarantine
        // swallows its own metrics failure after a successful publish.
        try {
            metrics.recordQuarantined();
        } catch (RuntimeException e) {
            log.warn("Metrics recording failed for quarantined topic={} partition={} offset={}",
                    failed.topic(), failed.partition(), failed.offset(), e);
        }
    }

    private static byte[] originalPayload(ConsumerRecord<String, ?> record, Throwable failure) {
        for (Throwable cause = failure; cause != null && cause != cause.getCause();
             cause = cause.getCause()) {
            if (cause instanceof DeserializationException deserialization) {
                return deserialization.getData();
            }
        }
        return record.value() instanceof byte[] bytes ? bytes : null;
    }

    private static BackOff poisonBackOff() {
        ExponentialBackOff backOff = new ExponentialBackOff();
        backOff.setInitialInterval(INITIAL_BACKOFF_MS);
        backOff.setMultiplier(2.0);
        backOff.setMaxInterval(MAX_BACKOFF_MS);
        backOff.setMaxElapsedTime(MAX_ELAPSED_MS);
        backOff.setMaxAttempts(MAX_RETRIES);
        return backOff;
    }
}
