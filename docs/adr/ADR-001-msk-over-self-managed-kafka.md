# ADR-001: Amazon MSK Over Self-Managed Kafka for Cloud Deployments

**Status:** Accepted
**Date:** 2026-08-16

## Context
The platform needs a three-broker Kafka cluster across three availability zones for the cloud
validation profile, with TLS, encryption at rest, IAM-based authorization and broker-failure testing.
Operating brokers on EC2 would consume most of the project's effort budget on undifferentiated work.

## Decision
Use Amazon MSK for all cloud deployments. Self-managed brokers in Docker Compose remain the local
development path only.

## Alternatives
- **Self-managed on EC2.** Full control over broker configuration and lower per-hour cost. Rejected:
  broker patching, ZooKeeper/KRaft operations, certificate rotation and AZ placement are operational
  work the project is not trying to demonstrate.
- **Confluent Cloud.** Strong tooling and Schema Registry integration. Rejected: the project's IAM and
  workload-identity story is AWS-native, and MSK IAM authorization is the control being demonstrated.
- **MSK Serverless.** Simpler capacity model. Rejected: the broker-failure and partition-scaling
  evidence in NFR-02.2 requires visible broker topology.

## Consequences
Broker-level tuning is limited to what MSK exposes. MSK IAM becomes the authorization mechanism,
which is the intent: Kafka ACLs are reserved for the local `strict-security` profile.
