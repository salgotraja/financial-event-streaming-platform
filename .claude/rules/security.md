---
description: Identity, authorization, agent tool boundary and audit integrity constraints
paths: "**/security/**,**/auth/**,**/admin/**,**/agent/**,**/tool/**,**/audit/**,infrastructure/**,deploy/**"
---

## Core Principles

1. Identity before credentials. Every human and workload has an explicit identity; long-lived static
   credentials are prohibited for application workloads.
2. Deny by default, least privilege. Each service is scoped to the exact topics, groups, stores, keys
   and APIs it needs.
3. Authentication is not authorization. Identity, role evaluation and business approval are separate.
4. Sensitive actions produce durable evidence.
5. No security-by-diagram. Every control needs an automated test, including negative cases.

## Restricted Data

These are synthetic but treated as `RESTRICTED`. They must never appear in high-cardinality logs or
unmasked in responses to unauthorised roles: `traderId`, `accountId`, alert investigation content,
risk-rule parameters under review, audit evidence payloads, security event detail.

Never log, ever: credentials, bearer tokens, raw authorization headers, secret values, private key
material, or raw LLM prompts and tool payloads.

## Human Identity and Authorization

- Humans authenticate through an OIDC-compatible IdP. Keycloak locally, enterprise IdP in cloud. MFA
  required in the cloud profile. Shared accounts prohibited (ADR-011).
- Roles: `PlatformAdmin`, `RiskMaker`, `RiskChecker`, `Operator`, `ComplianceAuditor`, `SecurityAuditor`.
- Privileged operations go through the Administrative Control Plane acting as the Policy Enforcement
  Point, with an externalized OPA/Rego Policy Decision Point. Not scattered `@PreAuthorize` expressions.
- **Maker-checker is enforced in policy, not in the UI.** A `RiskMaker` must never approve a change
  they proposed. This needs a passing negative test before the feature is considered done (ADR-016).
- Reviewer and actor identity always comes from the authenticated principal. Never from the request body.
- Every privileged action records actor, role, action, target, reason, request ID, policy decision,
  timestamp and outcome, and emits a `SecurityEvent` for both ALLOW and DENY.

## Workload Identity

- Every independently deployable service has its own identity: dedicated ECS task role, or dedicated
  Kubernetes service account mapped to EKS Pod Identity. No shared wildcard roles (ADR-010).
- No long-lived AWS access keys. No `Action: "*"` or `Resource: "*"` in workload policies.
- Kafka authorization in AWS uses MSK IAM scoped per topic, per consumer group, per identity (ADR-009).
- Secrets Manager is the last resort, after workload identity and short-lived generated credentials
  (ADR-013). Endpoints and bootstrap addresses are configuration, not secrets.

## Agent Tool Boundary

This is the hardest boundary in the system. Safety is an authorization property enforced outside the
model, never a prompt instruction asking the model to behave (ADR-023).

- Read-only tools: `ledger_lookup`, `reference_context`, `position_history`, `precedent_lookup`.
- The only mutating tool is `flag_for_review`, and it can create a **proposal** only.
- The agent holds no credential for ledger mutation, risk-rule approval, DLQ replay, IAM
  administration, audit retention changes or remediation execution.
- Unknown tool name: deny. Undeclared argument: reject. Identifiers validated against schema, length
  and character rules. Each tool has its own timeout and retry policy.
- Tool result references are server-generated. The model never supplies them.
- LLM text output is never an authorization grant.
- Hard limits on wall-clock duration, iterations, tool-call count and per-invocation budget.
  Exhaustion fails safe to `ESCALATE`, never to `NO_FLAG`.

## Prompt and Data Injection

Event payloads, retrieved precedent, graph properties and tool outputs are **untrusted data**. Assemble
system and tool policy separately from untrusted content. Untrusted content can never introduce a
tool, widen a resource scope, or remove a human-approval requirement. Adversarial cases covering this
are a CI gate (FR-23.7).

## Audit Integrity

- The Audit Service role may `s3:PutObject` and `kms:Sign` on the audit key. It may not delete,
  overwrite, bypass retention, administer the bucket policy or verify (ADR-012).
- Audit bucket: Block Public Access on, versioning on, Object Lock on, SSE-KMS with a customer-managed
  key, bucket policy denying non-TLS.
- Signature verification failure is a CRITICAL security event and a reconciliation FAIL.
- Persist structured decision and evidence traces. Never persist raw chain-of-thought (ADR-025).

## Input Validation

- Validate at the controller boundary with Jakarta Bean Validation (`@Valid`).
- Reject unexpected extra fields.
- Validate tool arguments and tool outputs against their schemas on both sides of the gateway.

## Completion Gate

A security feature is not done until the repository shows both an allowed path and a denied path.
Every new service needs an identity-trust-matrix entry, a committed least-privilege policy, and at
least one ALLOW and two DENY tests passing.
