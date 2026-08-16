# ADR-009: MSK IAM Authentication and Authorization in AWS

**Status:** Accepted
**Date:** 2026-08-16

## Context
NFR-05.3 requires every workload to have a distinct identity and least-privilege permissions, and
NFR-05.2 prohibits long-lived AWS access keys. Kafka authorization must be expressible per workload,
per topic, per consumer group, and testable with negative cases.

## Decision
Use TLS plus `AWS_MSK_IAM` SASL authentication with IAM policies scoping `Connect`, topic
`ReadData`/`WriteData` and consumer-group access per workload identity. Kafka ACLs are reserved for
the local `strict-security` profile where IAM is unavailable.

## Alternatives
- **SASL/SCRAM with Secrets Manager.** Portable across Kafka distributions. Rejected: reintroduces a
  shared secret per service, which NFR-05.10 pushes to last resort.
- **mTLS with Kafka ACLs.** No AWS coupling. Rejected: certificate lifecycle becomes project work,
  and authorization lives in a second system rather than alongside the S3 and KMS policies.

## Consequences
Kafka authorization and AWS resource authorization are expressed in one policy language and validated
by one CI check (NFR-05.14). Local development cannot use IAM, so the `strict-security` profile
exists to test denial paths without AWS. Client configuration carries no keys, only the SASL mechanism.
