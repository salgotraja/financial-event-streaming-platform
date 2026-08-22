package dev.engnotes.fes.marketdatacacheprojector;

import dev.engnotes.fes.common.kafka.DeadLetterPublisher;
import dev.engnotes.fes.common.kafka.FailureTracker;
import dev.engnotes.fes.events.DeadLetterEvent;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.dao.QueryTimeoutException;
import org.springframework.data.redis.RedisConnectionFailureException;
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
 * <p><strong>A malformed payload is one quarantined record, with zero retries.</strong> {@code
 * DeserializationException} is registered as not retryable, so the recoverer runs on the first
 * attempt and publishes to {@code {topic}.dlq}, and the offset advances so the partition keeps
 * moving. Retrying a decode failure does not help: the bytes do not improve. The bounded
 * {@link #poisonBackOff()} exists for the listener failures that are not a decode failure and not a
 * Redis outage, so a transient bug elsewhere in the listener still gets a few bounded attempts
 * before quarantine rather than being retried forever or quarantined on the first failure.
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
public class ProjectorKafkaConfiguration {

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
                                            ProjectorProperties properties,
                                            @Value("${spring.kafka.consumer.group-id}") String consumerGroup) {

        return new DeadLetterPublisher(kafkaTemplate, failureTracker, consumerGroup,
                properties.consumerInstance());
    }

    @Bean
    TaskScheduler projectorPauseScheduler() {
        ThreadPoolTaskScheduler scheduler = new ThreadPoolTaskScheduler();
        scheduler.setPoolSize(1);
        scheduler.setThreadNamePrefix("projector-pause-");
        scheduler.initialize();
        return scheduler;
    }

    @Bean
    DefaultErrorHandler projectorErrorHandler(DeadLetterPublisher deadLetterPublisher,
                                              FailureTracker failureTracker,
                                              ListenerContainerRegistry registry,
                                              TaskScheduler projectorPauseScheduler) {

        ContainerPausingBackOffHandler pausing = new ContainerPausingBackOffHandler(
                new ListenerContainerPauseService(registry, projectorPauseScheduler));

        DefaultErrorHandler errorHandler = new DefaultErrorHandler(
                (record, exception) -> quarantine(deadLetterPublisher, record, exception),
                poisonBackOff(),
                pausing);

        errorHandler.setBackOffFunction((record, exception) ->
                isRedisOutage(exception)
                        ? new FixedBackOff(OUTAGE_PAUSE_MS, FixedBackOff.UNLIMITED_ATTEMPTS)
                        : poisonBackOff());

        errorHandler.addNotRetryableExceptions(DeserializationException.class);
        errorHandler.setRetryListeners(failureTracker);
        // The container commits the recovered record's offset, so one poison payload does not block
        // the partition behind it.
        errorHandler.setAckAfterHandle(true);
        return errorHandler;
    }

    private static boolean isRedisOutage(Throwable failure) {
        for (Throwable cause = failure; cause != null && cause != cause.getCause();
             cause = cause.getCause()) {
            // Two types, because an unavailable Redis presents as either. A refused connection
            // raises RedisConnectionFailureException; a Redis that accepted the socket and then
            // stopped answering raises a command timeout, which Spring Data translates to
            // QueryTimeoutException. Matching only the first sends a valid record to the DLQ during
            // exactly the outage this branch exists to survive.
            if (cause instanceof RedisConnectionFailureException
                    || cause instanceof QueryTimeoutException) {
                return true;
            }
        }
        return false;
    }

    private static void quarantine(DeadLetterPublisher publisher,
                                   ConsumerRecord<?, ?> record,
                                   Exception exception) {

        @SuppressWarnings("unchecked")
        ConsumerRecord<String, ?> failed = (ConsumerRecord<String, ?>) record;
        publisher.publish(failed, originalPayload(failed, exception), exception).join();
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
