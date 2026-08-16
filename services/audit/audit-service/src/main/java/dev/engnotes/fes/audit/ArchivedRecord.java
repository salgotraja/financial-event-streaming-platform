package dev.engnotes.fes.audit;

import java.time.Instant;

/**
 * One record on its way to the evidence archive.
 *
 * <p>{@code payload} is the exact byte sequence the broker delivered, Confluent wire-format prefix
 * included. The archive stores what was published, not a re-encoding of a decoded object: a
 * re-encoding is a claim about the payload rather than the payload, and it is the wrong thing to
 * hand to an integrity check (FR-16).
 *
 * <p>Array identity means this record's generated {@code equals} compares payloads by reference.
 * Nothing here relies on record equality; the identity of an archived record is
 * {@link #idempotencyKey()}.
 */
public record ArchivedRecord(String topic,
                             int partition,
                             long offset,
                             String key,
                             String eventType,
                             byte[] payload,
                             Instant timestamp) {

    /**
     * Topic, partition and offset, which {@code architecture-v1.2.md} line 231 fixes as the canonical
     * event idempotency key for the audit path. Delivery is at-least-once (ADR-019), so the same
     * record arrives again after a rebalance; the broker coordinates make the duplicate recognisable
     * without the archive having to understand any event's payload.
     */
    public String idempotencyKey() {
        return topic + "-" + partition + "-" + offset;
    }
}
