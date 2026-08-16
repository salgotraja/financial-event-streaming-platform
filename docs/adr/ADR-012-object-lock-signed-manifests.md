# ADR-012: S3 Object Lock Plus KMS-Signed Audit Manifests

**Status:** Accepted
**Date:** 2026-08-16

## Context
FR-05.3 requires immutable audit records and FR-16 requires tamper evidence. Immutability and
tamper-evidence are different properties: retention stops deletion, but it does not prove the bytes
written are the bytes read back.

## Decision
Combine both. S3 Object Lock with versioning, Block Public Access and an SSL-only bucket policy
provides retention. Each flush additionally writes a sidecar manifest carrying the object SHA-256,
event count, topic/partition offset ranges, schema versions and writer identity, with the manifest
digest signed by a dedicated asymmetric KMS `SIGN_VERIFY` key.

## Alternatives
- **Object Lock alone.** Rejected: proves nothing about content integrity, and offset coverage gaps
  stay invisible to the FR-14 reconciliation control.
- **Hash chain in a database.** Rejected: moves the root of trust into a mutable store.
- **QLDB or a managed ledger.** Rejected: additional service and cost for a property Object Lock plus
  signing already provides.

## Consequences
The Audit Service role holds `kms:Sign` and `s3:PutObject` and nothing else: no delete, no retention
bypass, no bucket-policy administration, no `kms:Verify`. Verification is a separate identity and can
run offline against the public key. A verification failure is both a reconciliation FAIL and a
CRITICAL security event.
