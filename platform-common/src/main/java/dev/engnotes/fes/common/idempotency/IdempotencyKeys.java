package dev.engnotes.fes.common.idempotency;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.UUID;

/**
 * Deterministic identifiers for records that must survive redelivery.
 *
 * <p>Delivery is at-least-once (ADR-019), so a consumer will reprocess records it has already
 * handled. Any identifier it mints with {@link UUID#randomUUID()} differs on the second pass, and
 * every downstream consumer then sees two distinct records for one event. Deriving the identifier
 * from the inputs that define the record makes reprocessing produce the same identifier, which is
 * what lets a downstream consumer deduplicate at all.
 *
 * <p>The derivation is an RFC 4122 version 5 name-based UUID: SHA-1 over the joined components,
 * with the version and variant bits set. Version 5 rather than version 3 because MD5 is absent on
 * some hardened JVM configurations. This is an identifier, not a security primitive, and nothing
 * about it depends on SHA-1 being collision resistant.
 */
public final class IdempotencyKeys {

    // A unit separator cannot appear in any component this platform derives keys from, so it makes
    // component boundaries unambiguous. Joining on a printable character would let ("ab", "c") and
    // ("a", "bc") hash identical bytes and collide.
    private static final char SEPARATOR = '\u001F';

    private IdempotencyKeys() {
    }

    public static UUID deterministic(String... components) {
        if (components == null || components.length == 0) {
            throw new IllegalArgumentException("A deterministic key needs at least one component");
        }

        StringBuilder joined = new StringBuilder();
        for (int i = 0; i < components.length; i++) {
            if (components[i] == null) {
                throw new IllegalArgumentException(
                        "Component " + i + " is null. A null component would make the key depend on "
                                + "how null happens to be rendered, so it is rejected here instead.");
            }
            if (i > 0) {
                joined.append(SEPARATOR);
            }
            joined.append(components[i]);
        }

        byte[] digest = sha1(joined.toString().getBytes(StandardCharsets.UTF_8));
        digest[6] = (byte) ((digest[6] & 0x0f) | 0x50);
        digest[8] = (byte) ((digest[8] & 0x3f) | 0x80);

        ByteBuffer buffer = ByteBuffer.wrap(digest, 0, 16);
        return new UUID(buffer.getLong(), buffer.getLong());
    }

    private static byte[] sha1(byte[] input) {
        try {
            return MessageDigest.getInstance("SHA-1").digest(input);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-1 is mandatory on every JVM", e);
        }
    }
}
