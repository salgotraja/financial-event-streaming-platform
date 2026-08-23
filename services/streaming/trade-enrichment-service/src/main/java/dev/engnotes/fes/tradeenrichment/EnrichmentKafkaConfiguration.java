package dev.engnotes.fes.tradeenrichment;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import dev.engnotes.fes.common.kafka.DeadLetterPublisher;
import dev.engnotes.fes.common.kafka.FailureTracker;
import dev.engnotes.fes.common.kafka.KafkaSaslProfile;
import dev.engnotes.fes.events.DeadLetterEvent;
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
import org.springframework.dao.QueryTimeoutException;
import org.springframework.data.redis.RedisConnectionFailureException;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ContainerPausingBackOffHandler;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.ListenerContainerPauseService;
import org.springframework.kafka.listener.ListenerContainerRegistry;
import org.springframework.kafka.support.serializer.DeserializationException;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.util.backoff.BackOff;
import org.springframework.util.backoff.ExponentialBackOff;
import org.springframework.util.backoff.FixedBackOff;

/**
 * The two failure classes ADR-027 separates, wired so that neither can be mistaken for the other.
 *
 * <p><strong>A malformed payload or a failed validation is one quarantined record, with zero
 * retries.</strong> {@code DeserializationException} and {@code IllegalArgumentException} are
 * registered as not retryable, so the recoverer runs on the first attempt and publishes to
 * {@code {topic}.dlq}, and the offset advances so the partition keeps moving. Retrying either does
 * not help: the bytes do not decode differently on a second attempt, and a validation verdict on the
 * payload does not change either.
 *
 * <p><strong>A reference-data gap gets the same bounded retry, except the one reason that cannot
 * improve.</strong> {@code ReferenceDataUnavailableException} is deliberately left out of the
 * not-retryable set. {@code age = trade.eventTimestamp - cachedTick.eventTimestamp}, and the
 * projection is monotonic in event time (ADR-032), so the cached tick's timestamp only ever moves
 * forward: {@code age} can only shrink across a retry. That makes four of its five reasons
 * recoverable inside the bounded window: {@code tick_absent} and {@code window_empty} resolve as
 * soon as the projector writes the ticker's first tick or bucket, {@code instrument_missing}
 * resolves as soon as the loader's follower thread folds the record, and {@code stale} shrinks with
 * every fresher tick. Only {@code future}, where the cached tick already postdates the trade, cannot
 * improve: the same monotonic guarantee that shrinks age for the other four makes it grow more
 * negative for this one. {@code setBackOffFunction} gives {@code future} a zero-attempt back-off and
 * gives every other {@code ReferenceDataUnavailableException} the same {@link #poisonBackOff()} the
 * other listener failures get, so a newly listed ticker, or the first trade after a projector
 * restart, gets a second chance instead of dead-lettering with zero attempts.
 *
 * <p><strong>A Redis outage pauses the container.</strong> The back-off function returns an
 * unlimited-attempt back-off for a connection failure or a command timeout, so the recoverer is never
 * reached and no dead letter is written for a record that was never bad. The handler is given a
 * {@link ContainerPausingBackOffHandler} rather than the default one, and the difference decides
 * whether this works: the default handler sleeps the consumer thread, so {@code poll()} stops being
 * called, and an unbounded sleep crosses {@code max.poll.interval.ms} and gets the consumer evicted
 * from the group. That turns an outage into a rebalance storm. Pausing keeps the consumer polling
 * and in the group while it declines to deliver records.
 *
 * <p><strong>The quarantined bytes come from the exception.</strong> {@code
 * ErrorHandlingDeserializer} sets the record value to null and carries the delivered bytes on the
 * {@link DeserializationException}, and {@link DeadLetterPublisher} substitutes an empty array for a
 * null payload. Passing {@code record.value()} through would quarantine nothing at all and lose the
 * only copy of the evidence.
 */
@Configuration(proxyBeanMethods = false)
public class EnrichmentKafkaConfiguration {

    private static final Logger log = LoggerFactory.getLogger(EnrichmentKafkaConfiguration.class);

    // Three attempts total, so two retries after the first failure.
    private static final long MAX_RETRIES = 2;
    private static final long INITIAL_BACKOFF_MS = 100;
    private static final long MAX_BACKOFF_MS = 5_000;
    private static final long MAX_ELAPSED_MS = 5_000;

    // How long the container stays paused between attempts while Redis is down.
    private static final long OUTAGE_PAUSE_MS = 5_000;

    @Bean
    FailureTracker failureTracker() {
        return new FailureTracker();
    }

    @Bean
    DeadLetterPublisher deadLetterPublisher(KafkaTemplate<String, DeadLetterEvent> kafkaTemplate,
                                            FailureTracker failureTracker,
                                            EnrichmentProperties properties,
                                            @Value("${spring.kafka.consumer.group-id}") String consumerGroup) {

        return new DeadLetterPublisher(kafkaTemplate, failureTracker, consumerGroup,
                properties.consumerInstance());
    }

    @Bean
    TaskScheduler enrichmentPauseScheduler() {
        ThreadPoolTaskScheduler scheduler = new ThreadPoolTaskScheduler();
        scheduler.setPoolSize(1);
        scheduler.setThreadNamePrefix("enrichment-pause-");
        scheduler.initialize();
        return scheduler;
    }

    @Bean
    DefaultErrorHandler enrichmentErrorHandler(DeadLetterPublisher deadLetterPublisher,
                                               FailureTracker failureTracker,
                                               ListenerContainerRegistry registry,
                                               TaskScheduler enrichmentPauseScheduler,
                                               EnrichmentMetrics metrics) {

        ContainerPausingBackOffHandler pausing = new ContainerPausingBackOffHandler(
                new ListenerContainerPauseService(registry, enrichmentPauseScheduler));

        DefaultErrorHandler errorHandler = new DefaultErrorHandler(
                (record, exception) -> quarantine(deadLetterPublisher, metrics, record, exception),
                poisonBackOff(),
                pausing);

        errorHandler.setBackOffFunction((record, exception) -> backOffFor(exception));

        // Retrying either of these does not help. A DeserializationException's bytes do not
        // improve on a second attempt. An IllegalArgumentException is a validation verdict on the
        // payload that will not change either. ReferenceDataUnavailableException is deliberately
        // absent: four of its five reasons can resolve within the bounded back-off (see the class
        // javadoc), so it is routed through setBackOffFunction instead, which still gives its one
        // reason that cannot improve, future, a zero-attempt back-off of its own.
        errorHandler.addNotRetryableExceptions(DeserializationException.class, IllegalArgumentException.class);
        errorHandler.setRetryListeners(failureTracker);
        // The container commits the recovered record's offset, so one poison payload does not block
        // the partition behind it.
        errorHandler.setAckAfterHandle(true);
        return errorHandler;
    }

    @Bean
    InstrumentCache instrumentCache() {
        return new InstrumentCache();
    }

    /**
     * The readiness gate. The trade listener is declared {@code autoStartup = false}, so nothing
     * starts it but {@link #startTheTradeListenerOnceLoaded}, and that runs only after every
     * reference partition has been read to its captured end offset (ADR-034).
     *
     * <p>{@code SmartInitializingSingleton} rather than an {@code @EventListener} on
     * {@code ApplicationReadyEvent}: the wait must complete before the context reports ready, so a
     * timeout fails startup instead of leaving a running service with a partial map.
     *
     * <p>The {@code onLoaded} callback only binds the metrics gauge, not the listener start seen in
     * the brief this class was copied from. {@link KafkaListenerEndpointRegistry} is populated by
     * Spring Kafka's own {@code SmartInitializingSingleton}, and {@code SmartInitializingSingleton}
     * callbacks run in bean-definition registration order, not dependency order: Spring Boot
     * registers its autoconfigured Kafka beans, including that one, through a deferred import
     * selector, which runs after this class's own bean definitions are registered. That put ours
     * first in practice, so {@code registry.getListenerContainer(...)} here returned null. Starting
     * the container is moved to {@link #startTheTradeListenerOnceLoaded}, a {@link SmartLifecycle}
     * bean, whose {@code start()} runs in {@code finishRefresh()}, strictly after every
     * {@code SmartInitializingSingleton} callback including Spring Kafka's own, so the registry is
     * always populated by the time it runs. If the loader had timed out, its exception would already
     * have aborted {@code refresh()} before {@code finishRefresh()} is ever reached, so no guard is
     * needed here for that case.
     */
    @Bean(destroyMethod = "close")
    InstrumentCacheLoader instrumentCacheLoader(InstrumentCache cache,
                                                EnrichmentProperties properties,
                                                KafkaProperties kafkaProperties,
                                                EnrichmentMetrics metrics,
                                                ObjectProvider<KafkaSaslProfile> saslProfile) {

        // Boot 4.1 moved this class out of spring-boot-autoconfigure. The import is
        // org.springframework.boot.kafka.autoconfigure.KafkaProperties, and buildConsumerProperties
        // takes no argument on this version. Both were read off the resolved
        // spring-boot-kafka-4.1.0 jar, not assumed.
        Map<String, Object> consumerProperties =
                new HashMap<>(kafkaProperties.buildConsumerProperties());
        // This consumer is built by hand rather than through Spring Boot's autoconfigured
        // ConsumerFactory, so KafkaSecurityConfiguration's DefaultKafkaConsumerFactoryCustomizer
        // never touches it. Without this, the instrument master would connect as PLAINTEXT under
        // the secure-kafka profile regardless of the broker's own listener, and the readiness gate
        // would fail every startup with a metadata timeout rather than an authorization error.
        // KafkaSaslProfile only exists under that profile, hence the optional provider.
        saslProfile.ifAvailable(profile -> consumerProperties.putAll(profile.properties()));
        // No group and no committed offsets: this consumer assign()s every partition, so a group id
        // would be misleading and a GROUP grant for it would be an unnecessary permission.
        consumerProperties.remove(ConsumerConfig.GROUP_ID_CONFIG);
        // The trade listener wraps its deserialiser so a poison payload can be quarantined. There is
        // no DLQ for the reference topic, so a wrapper here would hand the fold a null value that
        // InstrumentCache would read as a tombstone and use to DELETE a live instrument.
        consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                io.confluent.kafka.serializers.KafkaAvroDeserializer.class);
        consumerProperties.put("specific.avro.reader", true);

        return new InstrumentCacheLoader(cache, consumerProperties, properties.referenceTopic(),
                properties.instrumentCacheTimeout(),
                // Bind the gauge before the listener can possibly start, so it can never report a
                // partial map as though it were the whole master.
                () -> metrics.bindInstrumentCache(cache));
    }

    @Bean
    SmartInitializingSingleton openTheReadinessGate(InstrumentCacheLoader loader) {
        return loader::loadInitialSnapshot;
    }

    /**
     * See {@link #instrumentCacheLoader} for why this is a {@link SmartLifecycle} rather than part
     * of the loader's {@code onLoaded} callback. No explicit dependency on the readiness gate is
     * needed: {@code SmartLifecycle.start()} runs in {@code finishRefresh()}, unconditionally after
     * every {@code SmartInitializingSingleton} callback in the same context, including the one that
     * runs {@link InstrumentCacheLoader#loadInitialSnapshot()}.
     */
    @Bean
    SmartLifecycle startTheTradeListenerOnceLoaded(KafkaListenerEndpointRegistry registry) {
        return new SmartLifecycle() {
            private volatile boolean running;

            @Override
            public void start() {
                registry.getListenerContainer(RawTradeConsumer.LISTENER_ID).start();
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

    private static boolean isRedisOutage(Throwable failure) {
        for (Throwable cause = failure; cause != null && cause != cause.getCause();
             cause = cause.getCause()) {
            // Two types, because an unavailable Redis presents as either. A refused connection
            // raises RedisConnectionFailureException; a Redis that accepted the socket and then
            // stopped answering raises a command timeout, which Spring Data translates to
            // QueryTimeoutException. Matching only the first sends a valid record to the DLQ during
            // exactly the outage this branch exists to survive. ReferenceDataUnavailableException is
            // deliberately not matched here: a Redis read that succeeds and returns an old value is
            // not a failing call, and treating it as an outage would pause the whole container on one
            // stale ticker.
            if (cause instanceof RedisConnectionFailureException
                    || cause instanceof QueryTimeoutException) {
                return true;
            }
        }
        return false;
    }

    static void quarantine(DeadLetterPublisher publisher,
                           EnrichmentMetrics metrics,
                           ConsumerRecord<?, ?> record,
                           Exception exception) {

        @SuppressWarnings("unchecked")
        ConsumerRecord<String, ?> failed = (ConsumerRecord<String, ?>) record;
        publisher.publish(failed, originalPayload(failed, exception), exception).join();

        // The dead letter is already published by the time this runs. setAckAfterHandle(true) means
        // a metrics failure here would prevent the ack and redeliver the record, quarantining it a
        // second time, so this must be swallowed exactly like RawTradeConsumer swallows its own
        // metrics failure after a successful publish.
        try {
            metrics.recordQuarantined();
            reasonOf(exception).ifPresent(metrics::recordUnavailable);
        } catch (RuntimeException e) {
            log.warn("Metrics recording failed for quarantined topic={} partition={} offset={}",
                    failed.topic(), failed.partition(), failed.offset(), e);
        }
    }

    /**
     * The back-off for one listener failure, in priority order: a Redis outage always pauses the
     * container regardless of what triggered it, then {@code future} gets zero attempts because it
     * cannot improve, then everything else falls through to the bounded {@link #poisonBackOff()}.
     */
    static BackOff backOffFor(Throwable exception) {
        if (isRedisOutage(exception)) {
            return new FixedBackOff(OUTAGE_PAUSE_MS, FixedBackOff.UNLIMITED_ATTEMPTS);
        }
        if (reasonOf(exception).filter(reason -> reason == UnavailableReason.FUTURE).isPresent()) {
            // Age only grows more negative on retry (see the class javadoc), so no bounded wait
            // recovers this one; the recoverer should see it on the first attempt, same as a
            // not-retryable exception.
            return new FixedBackOff(0L, 0L);
        }
        return poisonBackOff();
    }

    private static Optional<UnavailableReason> reasonOf(Throwable failure) {
        for (Throwable cause = failure; cause != null && cause != cause.getCause();
             cause = cause.getCause()) {
            if (cause instanceof ReferenceDataUnavailableException unavailable) {
                return Optional.of(unavailable.reason());
            }
        }
        return Optional.empty();
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
