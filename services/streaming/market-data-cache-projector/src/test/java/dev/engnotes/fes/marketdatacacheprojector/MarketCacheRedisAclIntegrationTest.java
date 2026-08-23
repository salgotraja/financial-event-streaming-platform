package dev.engnotes.fes.marketdatacacheprojector;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
 * The Redis ACL the projector actually runs under, proven against the committed template rather
 * than a copy of it (NFR-05.4, ADR-032).
 *
 * <p>{@code guide/docs/projector.md} describes the NOPERM behaviour of {@code
 * deploy/compose/redis/users.acl.template} as demonstrated fact, but until this class nothing ran a
 * real Redis with that file as its aclfile. Every other module-level test starts Redis with no ACL at
 * all, so a drift between the template and what the projector's script actually issues would have
 * gone unnoticed by every test that runs.
 *
 * <p>The template is read from disk and rendered with a test password substituted for the shipped
 * placeholder, the same substitution {@code scripts/generate-dev-security-material.sh} performs, so
 * this test fails the moment the template and the shipped grant drift apart rather than passing
 * against a duplicated copy of the ACL line.
 */
@DisplayName("the projector's Redis ACL, against the committed template")
class MarketCacheRedisAclIntegrationTest {

    private static final Path REPO_ROOT = Path.of("../../..").toAbsolutePath().normalize();
    private static final Path TEMPLATE = REPO_ROOT.resolve("deploy/compose/redis/users.acl.template");
    private static final String TEST_PASSWORD = "acl-integration-test-password";
    private static final String TICKER = "ACLPROOF";

    private static GenericContainer<?> redis;
    private static String principal;
    private static LettuceConnectionFactory grantedFactory;
    private static StringRedisTemplate grantedTemplate;

    @BeforeAll
    static void startRedisWithRenderedAcl() throws IOException {
        String template = Files.readString(TEMPLATE);
        principal = principal(template);

        String rendered = template.replace("__FES_REDIS_PROJECTOR_SECRET__", TEST_PASSWORD);
        Path renderedAcl = Files.createTempFile("users-acl-integration-test", ".acl");
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

        grantedFactory = connectionFactory(principal, TEST_PASSWORD);
        grantedTemplate = new StringRedisTemplate(grantedFactory);
        grantedTemplate.afterPropertiesSet();
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

    /** {@code user default off} disables the unauthenticated user; every named grant is explicit. */
    private static String principal(String template) {
        Matcher matcher = Pattern.compile("(?m)^user\\s+(\\S+)\\s+on\\s").matcher(template);
        if (!matcher.find()) {
            throw new IllegalStateException("No enabled user found in " + TEMPLATE);
        }
        return matcher.group(1);
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

    @Test
    @DisplayName("should run the projection script against a market key as the granted user")
    void should_run_the_projection_script_against_a_market_key_as_the_granted_user() {
        RedisScript<List> script = new DefaultRedisScript<>() {
            {
                setLocation(new ClassPathResource("redis/project-tick.lua"));
                setResultType(List.class);
            }
        };

        @SuppressWarnings("unchecked")
        List<Long> result = grantedTemplate.execute(script,
                List.of(MarketCacheKeys.tickKey(TICKER), MarketCacheKeys.windowKey(TICKER)),
                "1000", "100.0", "101.0", "100.5", "10", "1005", "corr-acl", "1",
                Integer.toString(MarketCacheKeys.BUCKET_SECONDS),
                Integer.toString(MarketCacheKeys.WINDOW_SECONDS),
                Integer.toString(MarketStateProjection.WINDOW_TTL_SECONDS));

        assertThat(result).as("the grant must let the projector's own script run to completion")
                .hasSize(3);
        // HGET, not HGETALL: the shipped grant deliberately holds +hkeys and not +hgetall, so
        // reading the written field back for verification must use a command this user actually
        // holds.
        assertThat(grantedTemplate.opsForHash().get(MarketCacheKeys.tickKey(TICKER), "lastTradedPrice"))
                .isEqualTo("100.5");
    }

    @Test
    @DisplayName("should deny a granted command outside the market key space")
    void should_deny_a_granted_command_outside_the_market_key_space() {
        // HGET is a granted command for this user; only the key space can be refusing it here, which
        // is what demonstrates the ~market:* scope rather than a denial any ungranted command would
        // produce just as easily.
        // The message lands on a nested cause once Spring Data wraps it, so the check reads the
        // full printed stack trace rather than the top-level RedisSystemException message.
        assertThatThrownBy(() -> grantedTemplate.opsForHash().get("other:key", "somefield"))
                .hasStackTraceContaining("NOPERM");
    }

    @Test
    @DisplayName("should refuse an unauthenticated connection")
    void should_refuse_an_unauthenticated_connection() {
        LettuceConnectionFactory anonymous = connectionFactory(null, null);
        try {
            StringRedisTemplate anonymousTemplate = new StringRedisTemplate(anonymous);
            anonymousTemplate.afterPropertiesSet();

            // Resolved empirically: with user default off, an unauthenticated client is denied
            // before it can run any command, and Redis reports it as NOAUTH.
            assertThatThrownBy(() -> anonymousTemplate.opsForValue().get(MarketCacheKeys.tickKey(TICKER)))
                    .as("user default off must leave an unauthenticated client with no access at all")
                    .hasStackTraceContaining("NOAUTH");
        } finally {
            anonymous.destroy();
        }
    }
}
