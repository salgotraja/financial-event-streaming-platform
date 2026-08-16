# Identity & Trust Matrix — v1.2

| Identity | Credential/issuer | Allowed | Explicitly denied | Evidence |
|---|---|---|---|---|
| trade-producer | ECS task role / EKS workload identity | write `trades.raw`; telemetry | read trade topics; audit delete; admin APIs | IAM/MSK auth events + trace |
| migration-normalizer | workload identity | read `legacy.trades.cdc`; write `trades.raw` | risk/admin/audit delete | CDC + MSK + dedup metrics |
| enrichment-service | workload identity | read `trades.raw`; write enriched/DLQ; Redis read | audit/KMS sign; admin | MSK + cache + trace |
| risk-alert-service | workload identity | read enriched; write alerts; risk DB | audit delete; rule approval | MSK/DB + rule version |
| anomaly-candidate-service | workload identity | read selected signals; write candidates | review decisions; remediation | candidate evidence |
| agent-investigation-service | workload identity | read candidates; call approved tool gateway | direct DB creds; DLQ; risk-rule approval; remediation | tool authorization + decision trace |
| precedent-sync | workload identity | read review decisions; write graph | mutate authoritative case DB | graph projection checkpoint |
| audit-service | workload identity + KMS signing grant | read topics; write locked audit; KMS Sign | delete/retention bypass | signed manifest |
| RiskMaker | OIDC + MFA | propose risk rule | approve own proposal | human authz event |
| RiskChecker | OIDC + MFA | approve/reject pending rules | author+approve same change | human authz event |
| Reviewer | OIDC + MFA | review agent cases; synthetic remediation request per policy | financial ledger mutation | review decision |
| Operator | OIDC + MFA | approved DLQ replay | risk-rule approval/audit deletion | replay audit |
| Auditor | OIDC + MFA | read evidence/verify signatures | writes/admin | audit access log |
