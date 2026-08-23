package dev.engnotes.fes.tradeenrichment;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.List;
import java.util.Map;

import dev.engnotes.fes.common.cache.MarketCacheKeys;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.connection.RedisPassword;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.core.script.RedisScript;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The Redis ACL trade-enrichment-service actually runs under, proven against the committed template
 * rather than a copy of it (NFR-05.4, ADR-034).
 *
 * <p>The template is read from disk and rendered with a test password substituted for the shipped
 * placeholder, the same substitution {@code scripts/generate-dev-security-material.sh} performs, so
 * this test fails the moment the template and the shipped grant drift apart rather than passing
 * against a duplicated copy of the ACL line.
 *
 * <p>{@code deploy/compose/docker-compose.strict-security.yml} gains no second healthcheck for this
 * identity. The projector's healthcheck authenticates as the projector user, so it proves nothing
 * about whether {@code FES_REDIS_ENRICHMENT_SECRET} was rendered into the ACL file correctly; only
 * this class does, because there is one Redis service in the file and a second liveness probe would
 * add a container dependency without adding a proof.
 */
@DisplayName("trade-enrichment-service's Redis ACL, against the committed template")
class EnrichmentRedisAclIntegrationTest {

    private static final Path REPO_ROOT = Path.of("../../..").toAbsolutePath().normalize();
    private static final Path TEMPLATE = REPO_ROOT.resolve("deploy/compose/redis/users.acl.template");
    private static final String TEST_PASSWORD = "enrichment-acl-integration-test-password";
    private static final String PRINCIPAL = "trade-enrichment-service";
    private static final String PROJECTOR_PRINCIPAL = "market-data-cache-projector";
    private static final String PROJECTOR_TEST_PASSWORD = "projector-acl-integration-test-password";
    private static final String TICKER = "ACLPROOF";

    private static GenericContainer<?> redis;
    private static LettuceConnectionFactory grantedFactory;
    private static StringRedisTemplate grantedTemplate;
    private static RedisScript<List> readMarketState;

    @BeforeAll
    static void startRedisWithRenderedAcl() throws IOException {
        String template = Files.readString(TEMPLATE);
        assertThat(template)
                .as("the committed template must still name the enrichment identity")
                .contains("user " + PRINCIPAL + " on");

        String rendered = template.replace("__FES_REDIS_ENRICHMENT_SECRET__", TEST_PASSWORD)
                // The projector's placeholder is also in this file; substitute it too, exactly as
                // the generate script does for both users, so this test can seed a market key as
                // the identity that actually writes it rather than needing an admin user this ACL
                // file does not define.
                .replace("__FES_REDIS_PROJECTOR_SECRET__", PROJECTOR_TEST_PASSWORD);
        Path renderedAcl = Files.createTempFile("users-acl-enrichment-test", ".acl");
        Files.writeString(renderedAcl, rendered);
        // The redis image runs as a non-root user inside the container; a temp file created with the
        // default umask is owner-only and is copied in with that mode intact, so the container's
        // process gets "Permission denied" reading its own aclfile.
        Files.setPosixFilePermissions(renderedAcl, PosixFilePermissions.fromString("rw-r--r--"));

        redis = new GenericContainer<>(DockerImageName.parse("redis:8.10.1-alpine"))
                .withExposedPorts(6379)
                .withCopyFileToContainer(
                        MountableFile.forHostPath(renderedAcl), "/usr/local/etc/redis/users.acl")
                .withCommand("redis-server", "--aclfile", "/usr/local/etc/redis/users.acl")
                .waitingFor(Wait.forLogMessage(".*Ready to accept connections tcp.*\\n", 1));
        redis.start();

        grantedFactory = connectionFactory(PRINCIPAL, TEST_PASSWORD);
        grantedTemplate = new StringRedisTemplate(grantedFactory);
        grantedTemplate.afterPropertiesSet();

        DefaultRedisScript<List> script = new DefaultRedisScript<>();
        script.setLocation(new ClassPathResource("redis/read-market-state.lua"));
        script.setResultType(List.class);
        readMarketState = script;
    }

    @AfterAll
    static void tearDown() {
        if (grantedFactory != null) {
            grantedFactory.destroy();
        }
        if (redis != null) {
            redis.stop();
        }
    }

    private static LettuceConnectionFactory connectionFactory(String username, String password) {
        RedisStandaloneConfiguration config =
                new RedisStandaloneConfiguration(redis.getHost(), redis.getMappedPort(6379));
        if (username != null) {
            config.setUsername(username);
        }
        if (password != null) {
            config.setPassword(RedisPassword.of(password));
        }
        LettuceConnectionFactory factory = new LettuceConnectionFactory(config);
        factory.afterPropertiesSet();
        return factory;
    }

    /**
     * Writes the tick key as the projector identity would, using the projector's own grant, so the
     * enrichment identity's granted read has something to see. There is no admin user in this ACL
     * file, only the two named workload identities, so seeding is done as the one that actually
     * owns the write.
     */
    private static void seedAsProjector() {
        LettuceConnectionFactory projectorFactory =
                connectionFactory(PROJECTOR_PRINCIPAL, PROJECTOR_TEST_PASSWORD);
        StringRedisTemplate projector = new StringRedisTemplate(projectorFactory);
        projector.afterPropertiesSet();
        try {
            // Field by field with HSET, not putAll, which Spring Data maps to HMSET. The projector's
            // grant holds +hset and not +hmset, and Redis treats the two as distinct ACL commands.
            Map<String, String> fields = Map.of(
                    "eventTimestamp", "1740000304000", "bidPrice", "99.0", "askPrice", "101.0",
                    "lastTradedPrice", "100.0", "volume", "5");
            fields.forEach((field, value) ->
                    projector.opsForHash().put(MarketCacheKeys.tickKey(TICKER), field, value));
        } finally {
            projectorFactory.destroy();
        }
    }

    @Test
    @DisplayName("should allow the read the enrichment script actually issues")
    void should_allow_the_read_the_enrichment_script_actually_issues() {
        seedAsProjector();

        List<List<String>> result = grantedTemplate.execute(readMarketState,
                List.of(MarketCacheKeys.tickKey(TICKER), MarketCacheKeys.windowKey(TICKER)));

        assertThat(result).hasSize(2);
        assertThat(result.get(0)).contains("lastTradedPrice");
    }

    @Test
    @DisplayName("should deny a write inside EVAL, because the grant holds no write command")
    void should_deny_a_write_inside_eval_because_the_grant_holds_no_write_command() {
        // Command denial. The script is read-only today; this is what stops an edit adding a write
        // from silently succeeding, so the read-only property is enforced rather than merely
        // documented.
        //
        // Resolved empirically: Redis wraps a command denial raised inside EVAL as a script error
        // rather than the bare NOPERM a direct command gets, and the top-level Spring exception
        // message is "Error in execution" for both probes in this class, so the check reads the
        // full printed stack trace rather than either message. What this one actually says is "ERR
        // ACL failure in script: User trade-enrichment-service has no permissions to run the 'hset'
        // command", which names the ACL rather than "unknown command", and that is the distinction
        // that proves the probe reached the authorizer.
        RedisScript<Object> write = new DefaultRedisScript<>(
                "return redis.call('HSET', KEYS[1], 'lastTradedPrice', '1')", Object.class);

        assertThatThrownBy(() -> grantedTemplate.execute(write,
                List.of(MarketCacheKeys.tickKey(TICKER))))
                .hasStackTraceContaining("has no permissions to run the 'hset' command");
    }

    @Test
    @DisplayName("should deny reading outside market:* using a command the identity does hold")
    void should_deny_reading_outside_market_star_using_a_command_the_identity_does_hold() {
        // Key-space denial, and the probe has to use a GRANTED command or it proves nothing about
        // the key scope. The projector's first attempt at this used a command the user was never
        // granted and so proved only command denial; that mistake is not repeated here.
        //
        // This probe's failure does carry NOPERM verbatim, unlike the command-denial probe above.
        // The two probes producing two different denial messages is itself evidence they exercise
        // two different ACL checks rather than one generic failure path.
        RedisScript<Object> foreign = new DefaultRedisScript<>(
                "return redis.call('HGETALL', KEYS[1])", Object.class);

        assertThatThrownBy(() -> grantedTemplate.execute(foreign, List.of("trades:{X}:state")))
                .hasStackTraceContaining("NOPERM");
    }
}
