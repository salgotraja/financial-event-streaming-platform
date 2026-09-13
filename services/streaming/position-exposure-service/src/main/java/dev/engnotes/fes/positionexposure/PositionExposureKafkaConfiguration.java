package dev.engnotes.fes.positionexposure;

import dev.engnotes.fes.common.kafka.DeadLetterPublisher;
import dev.engnotes.fes.common.kafka.FailureTracker;
import dev.engnotes.fes.common.kafka.PoisonRecordPolicy;
import dev.engnotes.fes.events.DeadLetterEvent;
import dev.engnotes.fes.events.PositionSnapshotEvent;
import dev.engnotes.fes.positionexposure.snapshot.PositionSnapshotPublisher;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.dao.QueryTimeoutException;
import org.springframework.jdbc.CannotGetJdbcConnectionException;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ContainerPausingBackOffHandler;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.ListenerContainerPauseService;
import org.springframework.kafka.listener.ListenerContainerRegistry;
import org.springframework.kafka.support.serializer.DeserializationException;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.util.backoff.BackOff;
import org.springframework.util.backoff.FixedBackOff;

/**
 * The {@code trades.enriched} error handler that separates ADR-027's two failure classes, and the
 * snapshot publisher wiring.
 *
 * <p>A malformed payload or a failed validation is one quarantined record, with zero retries.
 * {@code DeserializationException} and {@code IllegalArgumentException} are registered as not
 * retryable, so the recoverer runs on the first attempt and publishes to {@code trades.enriched.dlq},
 * and the offset advances so the partition keeps moving. Retrying either does not help: the bytes do
 * not decode differently on a second attempt.
 *
 * <p><strong>A PostgreSQL outage pauses the container</strong> rather than dead-lettering a good
 * trade, following {@code RiskAlertKafkaConfiguration}. The back-off function returns an
 * unlimited-attempt back-off for a lost connection or a statement timeout, so the recoverer is never
 * reached. The handler is given a {@link ContainerPausingBackOffHandler} rather than the default one:
 * the default handler sleeps the consumer thread, which stops {@code poll()} from being called and
 * crosses {@code max.poll.interval.ms}, evicting the consumer from its group and turning an outage
 * into a rebalance storm. Pausing keeps the consumer polling and in the group while it declines to
 * deliver records.
 *
 * <p>There is no readiness gate here, unlike {@code RiskAlertKafkaConfiguration}: this service folds
 * no governance topic, so it has nothing to wait for and the listener starts as soon as the context
 * is up.
 */
@Configuration(proxyBeanMethods = false)
public class PositionExposureKafkaConfiguration {

    private static final Logger log = LoggerFactory.getLogger(PositionExposureKafkaConfiguration.class);

    // How long the container stays paused between attempts while PostgreSQL is down.
    private static final long OUTAGE_PAUSE_MS = 5_000;

    @Bean
    PositionSnapshotPublisher positionSnapshotPublisher(KafkaTemplate<String, PositionSnapshotEvent> kafkaTemplate,
                                                        PositionExposureProperties properties) {
        return new PositionSnapshotPublisher(kafkaTemplate, properties.outputTopic());
    }

    @Bean
    FailureTracker failureTracker() {
        return new FailureTracker();
    }

    @Bean
    DeadLetterPublisher deadLetterPublisher(KafkaTemplate<String, DeadLetterEvent> kafkaTemplate,
                                            FailureTracker failureTracker,
                                            PositionExposureProperties properties,
                                            @Value("${spring.kafka.consumer.group-id}") String consumerGroup) {

        return new DeadLetterPublisher(kafkaTemplate, failureTracker, consumerGroup,
                properties.consumerInstance());
    }

    @Bean
    TaskScheduler positionExposurePauseScheduler() {
        ThreadPoolTaskScheduler scheduler = new ThreadPoolTaskScheduler();
        scheduler.setPoolSize(1);
        scheduler.setThreadNamePrefix("position-exposure-pause-");
        scheduler.initialize();
        return scheduler;
    }

    /**
     * The two failure classes ADR-027 separates for {@code trades.enriched}. See the class javadoc
     * for the full reasoning; this matches {@code RiskAlertKafkaConfiguration.riskAlertErrorHandler}'s
     * five-argument shape.
     */
    @Bean
    DefaultErrorHandler positionExposureErrorHandler(DeadLetterPublisher deadLetterPublisher,
                                                     FailureTracker failureTracker,
                                                     ListenerContainerRegistry registry,
                                                     TaskScheduler positionExposurePauseScheduler,
                                                     PositionExposureMetrics metrics) {

        ContainerPausingBackOffHandler pausing = new ContainerPausingBackOffHandler(
                new ListenerContainerPauseService(registry, positionExposurePauseScheduler));

        DefaultErrorHandler errorHandler = new DefaultErrorHandler(
                (record, exception) -> quarantine(deadLetterPublisher, metrics, record, exception),
                PoisonRecordPolicy.poisonBackOff(),
                pausing);

        errorHandler.setBackOffFunction((record, exception) -> backOffFor(exception));

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

    private static boolean isPostgresOutage(Throwable failure) {
        for (Throwable cause = failure; cause != null && cause != cause.getCause();
             cause = cause.getCause()) {
            // Two types, because an unavailable database presents as either. A refused connection or
            // an exhausted pool raises CannotGetJdbcConnectionException; a database that accepted the
            // connection and then stopped answering raises a statement timeout, which Spring
            // translates to QueryTimeoutException. Matching only the first sends a valid trade to the
            // DLQ during exactly the outage this branch exists to survive.
            if (cause instanceof CannotGetJdbcConnectionException
                    || cause instanceof QueryTimeoutException) {
                return true;
            }
        }
        return false;
    }

    /**
     * The back-off for one listener failure, in priority order: a PostgreSQL outage always pauses the
     * container regardless of what triggered it, then everything else falls through to the bounded
     * {@link PoisonRecordPolicy#poisonBackOff()}.
     */
    static BackOff backOffFor(Throwable exception) {
        if (isPostgresOutage(exception)) {
            return new FixedBackOff(OUTAGE_PAUSE_MS, FixedBackOff.UNLIMITED_ATTEMPTS);
        }
        return PoisonRecordPolicy.poisonBackOff();
    }

    private static void quarantine(DeadLetterPublisher publisher,
                                   PositionExposureMetrics metrics,
                                   ConsumerRecord<?, ?> record,
                                   Exception exception) {

        @SuppressWarnings("unchecked")
        ConsumerRecord<String, ?> failed = (ConsumerRecord<String, ?>) record;
        publisher.publish(failed, PoisonRecordPolicy.originalPayload(failed, exception), exception).join();

        // The dead letter is already published by the time this runs. setAckAfterHandle(true) means
        // a metrics failure here would prevent the ack and redeliver the record, quarantining it a
        // second time, so this must be swallowed the same way RiskAlertKafkaConfiguration.quarantine
        // swallows its own metrics failure after a successful publish.
        try {
            metrics.recordQuarantined();
        } catch (RuntimeException e) {
            log.warn("Metrics recording failed for quarantined topic={} partition={} offset={}",
                    failed.topic(), failed.partition(), failed.offset(), e);
        }
    }
}
