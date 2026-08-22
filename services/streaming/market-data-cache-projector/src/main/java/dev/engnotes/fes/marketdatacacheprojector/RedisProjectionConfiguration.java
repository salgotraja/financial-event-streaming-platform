package dev.engnotes.fes.marketdatacacheprojector;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.core.script.RedisScript;

/**
 * The script is committed in the module rather than loaded from configuration. It is part of the
 * security surface: the projector's Redis grant includes EVAL, so what the script may do is decided
 * here and reviewed with the code (ADR-032).
 */
@Configuration(proxyBeanMethods = false)
public class RedisProjectionConfiguration {

    @Bean
    RedisScript<Long> projectTickScript() {
        DefaultRedisScript<Long> script = new DefaultRedisScript<>();
        script.setLocation(new ClassPathResource("redis/project-tick.lua"));
        script.setResultType(Long.class);
        return script;
    }

    @Bean
    java.time.Clock projectorClock() {
        return java.time.Clock.systemUTC();
    }
}
