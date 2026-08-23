package dev.engnotes.fes.tradeenrichment;

import java.time.Clock;
import java.util.List;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.core.script.RedisScript;

/**
 * The script is committed in the module rather than loaded from configuration. It is part of the
 * security surface: this service's Redis grant includes EVAL, so what the script may do is decided
 * here and reviewed with the code (ADR-034).
 */
@Configuration(proxyBeanMethods = false)
public class RedisReadConfiguration {

    @Bean
    RedisScript<List> readMarketStateScript() {
        DefaultRedisScript<List> script = new DefaultRedisScript<>();
        script.setLocation(new ClassPathResource("redis/read-market-state.lua"));
        script.setResultType(List.class);
        return script;
    }

    @Bean
    Clock enrichmentClock() {
        return Clock.systemUTC();
    }
}
