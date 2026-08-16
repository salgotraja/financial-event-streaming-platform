# Reconciliation & Anomaly Investigation Agent — v1.2

## Role in the platform

The agent is an **asynchronous investigation assistant**, not the primary high-throughput detector.

```text
50k/sec deterministic plane
        |
        v
risk / reconciliation / statistical screening
        |
        v
anomaly.candidates  -- bounded, prioritized queue
        |
        v
agent investigation
        |
        v
structured decision + narrative
        |
        v
human review
```

This preserves the streaming system's deterministic latency/reliability while giving selected cases richer contextual reasoning.

## What the agent may do

Read-only:
- ledger/history lookup
- reference/calendar lookup
- position/history lookup
- Neo4j precedent retrieval

Mutating:
- `flag-for-review` only, creating a proposed review case

The agent cannot:
- mutate financial ledgers;
- approve risk rules;
- replay DLQs;
- administer IAM;
- bypass audit retention;
- execute remediation.

## Decision contract

```text
outcome: FLAG | NO_FLAG | ESCALATE | INCONCLUSIVE
severity: INFO | WARNING | CRITICAL
confidence: LOW | MEDIUM | HIGH | NOT_APPLICABLE
reasonCodes[]
evidenceRefs[]
precedentRefs[]
toolStatus{}
narrative
```

## Bounded critique

A second critique pass is allowed once when:
- confidence is medium/low;
- required evidence/tool is missing;
- the candidate is high impact;
- contradictory evidence exists; or
- the decision belongs to a configured high-confidence sample used to detect overconfidence.

The critique checks for systemic causes, scheduled causes, missed precedent, unsupported confidence, and tool/data failure.

## Auditability

Persist a structured decision/evidence trace:
- source candidate and deterministic trigger evidence;
- tool calls/status/result references;
- precedent references;
- draft and final typed decisions;
- critique trigger/outcome;
- model/provider/prompt/tool versions;
- token/cost/latency;
- human reviewer verdict.

Raw hidden chain-of-thought is not required or persisted.

## Failure behavior

Provider outage, tool failure, graph outage, timeout, iteration limit or budget exhaustion must produce an explicit degraded/escalated state. The agent never silently treats missing evidence as proof that an event is normal.

## Human-in-the-loop boundary

All flags/escalations enter a review queue. In v1.2 an approved remediation creates only a synthetic `remediation.requested` event/case transition. The project does not execute a real financial correction.

## Success evidence

- one end-to-end candidate -> agent -> review trace;
- one exported decision/audit document;
- one prompt/data-injection test denied;
- one tool-failure case escalated correctly;
- one deliberate prompt/model regression blocked in CI;
- cost and latency measured per candidate.
