# Identity & Trust Matrix — v1.2

Identity names are the service module names from ADR-028. They are load-bearing: the same string is
the Gradle module name, the Kafka consumer group and the IAM policy subject. Do not rename one
without the others.

**Known incomplete.** Entries exist only for services designed in detail so far. The following still
need entries as their phase lands: `market-data-simulator`, `corporate-action-producer`,
`reference-data-service`, `market-data-cache-projector`, `position-exposure-service`,
`alert-case-service`, `risk-rule-governance-service`, `reconciliation-simulator`,
`reconciliation-service`, `admin-control-plane`, `human-review-service`. The per-service completion
gate in `docs/specification-v1.2.md` requires the entry before the service is considered done.

| Identity | Credential/issuer | Allowed | Explicitly denied | Evidence |
|---|---|---|---|---|
| trade-producer | ECS task role / EKS workload identity | write `trades.raw`; telemetry | read trade topics; audit delete; admin APIs | IAM/MSK auth events + trace |
| migration-normalizer | workload identity | read `legacy.trades.cdc`; write `trades.raw` | risk/admin/audit delete | CDC + MSK + dedup metrics |
| trade-enrichment-service | workload identity | read `trades.raw`; write enriched/DLQ; Redis read | audit/KMS sign; admin | MSK + cache + trace |
| risk-alert-service | workload identity | read enriched; write alerts; risk DB | audit delete; rule approval | MSK/DB + rule version |
| anomaly-candidate-service | workload identity | read selected signals; write candidates | review decisions; remediation | candidate evidence |
| agent-investigation-service | workload identity | read candidates; call approved tool gateway | direct DB creds; DLQ; risk-rule approval; remediation | tool authorization + decision trace |
| precedent-sync-service | workload identity | read review decisions; write graph | mutate authoritative case DB | graph projection checkpoint |
| audit-service | workload identity + KMS signing grant | read topics; write locked audit; KMS Sign | delete/retention bypass | signed manifest |
| RiskMaker | OIDC + MFA | propose risk rule | approve own proposal | human authz event |
| RiskChecker | OIDC + MFA | approve/reject pending rules | author+approve same change | human authz event |
| Reviewer | OIDC + MFA | review agent cases; synthetic remediation request per policy | financial ledger mutation | review decision |
| Operator | OIDC + MFA | approved DLQ replay | risk-rule approval/audit deletion | replay audit |
| Auditor | OIDC + MFA | read evidence/verify signatures | writes/admin | audit access log |
