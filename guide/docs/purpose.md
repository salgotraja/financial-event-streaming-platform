# What this platform is for

!!! note "This page describes intent, not delivered code"

    Everything else in this guide documents what exists. This page and
    [Target architecture](architecture.md) are the two exceptions: they explain what the system is
    meant to be, because a reader who only sees the delivered parts cannot tell why they are shaped
    the way they are. What actually runs today is in the
    [delivery state table](index.md#delivery-state-at-a-glance).

In one line: a security-first financial event streaming and control platform, with a tested
legacy-to-streaming migration path, workload identity, immutable audit, and a human-gated AI
investigation layer.

## The problem

A firm processing trades needs to answer three questions continuously, and the answers go stale in
seconds.

**Is this trade risky right now?** A position limit breach, an unusual volume spike, a price far off
the last tick, a wash trade between related accounts. Answering after the fact is a report.
Answering in under 200ms is a control.

**What is our exposure right now?** Positions per trader, per ticker, per account, derived from a
stream of executions rather than reconstructed nightly.

**Can we prove any of it later?** Not "we have logs", but an immutable, tamper-evident archive with
signed manifests, and a reconciliation control that proves the archive actually covers the stream.

Batch processing answers all three, slowly and after the fact. Streaming answers them continuously,
and introduces a different problem: once the pipeline is the control, the pipeline's own correctness,
authorization and evidence become the thing you have to prove.

That is the problem this platform is built around. Not throughput for its own sake, but a control
path fast enough to matter and honest enough to audit.

## Why financial event streaming is its own discipline

Generic event streaming advice stops being sufficient at a few specific points, and most of the
design decisions in this repository sit on one of them.

**Ordering is domain-specific, not a default.** Trades for one ticker must stay ordered because a
running position is computed from them. Corporate actions for one instrument must stay ordered
because a correction supersedes the action it corrects. Instrument reference data is keyed by
identity rather than by routing, because the topic is compacted. Three topics, three different
reasons, one mechanism. See [Topics and schemas](topics.md).

**Duplicates are permitted and losses are not.** At-least-once with a deterministic idempotency key
is a decision (ADR-019), not a compromise. Kafka transactions would offer stronger semantics inside
Kafka and would not extend to the S3 archive, the Redis cache or the position database, so the
platform pays for idempotent consumers instead of buying a guarantee that stops at the broker.

**One bad record must not stop the control.** A malformed payload in a batch job is a failed batch.
In a risk pipeline it is a stalled partition, and a stalled partition is an outage of the control
itself. Hence per-record quarantine and an explicit ban on event-type-wide breakers (ADR-027).

**The audit trail is a product, not a byproduct.** It has to store what was published rather than a
re-encoding of it, survive the service that wrote it, and be provably complete.

**Identity is per workload, not per cluster.** A producer that can read the topic it writes is not a
producer. An archive that can write the topics it archives can manufacture its own evidence. These
are not theoretical: both are denied by committed policy and proven by tests today. See
[Workload authorization](authorization.md).

## Who would run it

| Stakeholder | What they need from it |
| --- | --- |
| Risk analysts | Real-time alerts, positions and exposures, with the evidence behind each |
| Risk makers and checkers | Propose risk-rule changes, and independently approve them |
| Compliance and audit | Query immutable records, control evidence, case history, privileged actions |
| Operations and SRE | Pipeline health, incidents, scaling, replay, availability |
| Security and IAM | Identities, trust boundaries, least-privilege policy, security telemetry |
| Platform and application engineers | Build producers and consumers under contracts that make the safe thing the default |

The last row is the one that shapes the code most. The shared durability profile, the shared offset
profile and the derived dead-letter topic name all exist so that a service gets the correct behaviour
by omission rather than by remembering.

## Security-first, concretely

"Security-first" is usually decoration. Here it means seven rules that have consequences you can see
in the source:

1. **Identity before credentials.** Every human and workload has an explicit identity. Long-lived
   static credentials are avoided wherever temporary ones are possible.
2. **Deny by default, least privilege.** Each service is scoped to the exact topics, groups, stores,
   keys and APIs it needs, and nothing else.
3. **Authentication is not authorization.** Identity, role evaluation and business approval are
   modelled separately.
4. **Sensitive actions produce durable evidence.** Rule activation, privileged replay, case closure,
   policy changes and emergency access all leave a record.
5. **Security is observable.** Authentication failures, authorization denials, privilege use and
   policy changes are measurable signals, not just log lines.
6. **Compromise is assumed.** The design limits blast radius when a service identity, operator
   session, secret, container or node is compromised.
7. **No security-by-diagram.** Every control needs an automated test, including negative cases. A
   service is not complete until the repository shows both an allowed path and a denied one.

Rule 7 is why [Workload authorization](authorization.md) is the most finished part of the delivered
system, and why one test exists purely to prove that a denial arrives as a denial rather than as a
timeout.

## Scope

**In scope:** event production and schema governance, trade enrichment, deterministic real-time risk
evaluation, position and exposure read models, risk-rule governance with maker-checker, alert case
lifecycle, immutable audit and reconciliation controls, a mock legacy PostgreSQL source with CDC
migration, deterministic anomaly-candidate generation, bounded AI-assisted investigation with human
review, evaluation and regression testing, dead-letter handling and controlled replay, human and
workload identity, cloud IAM, security telemetry, observability, autoscaling, and infrastructure as
code.

**Out of scope, by decision rather than by backlog:** order management, order routing, execution
against real exchanges, settlement, money movement, automatic correction of financial ledgers,
regulatory reporting or certification, licensed market-data feeds, autonomous agent remediation
without human review, and storage of real personal data.

All market data and financial identifiers are synthetic. The platform moves no money and executes no
trades.

Identity scope is bounded separately and deliberately (ADR-030). Workload identity, authorization,
privileged control, agent identity, evidence and identity observability are in. Workforce identity,
identity governance, customer identity and federation breadth are excluded by decision.

## What this project is, and what it is not

It is a portfolio and learning artifact built to production shape: a worked example of streaming
architecture, schema governance, legacy migration, workload identity, immutable audit, and AI safety
boundaries, with the tests and gates that make each claim checkable.

It is not a certified or production-deployed financial system, and the guide will not describe it as
one. Some specific claims this project deliberately does not make:

- that it is production compliant;
- that the AI handles 50,000 events per second, which conflates two planes with separate objectives;
- that a 15-case golden dataset measures model accuracy, when it is a regression suite (ADR-024);
- that Neo4j was chosen for scale, when it is a derived, rebuildable projection (ADR-022);
- that delivery is exactly-once, when it is at-least-once with deduplication (ADR-019);
- that the agent fixes financial anomalies, when it can only propose (ADR-023).

Holding that line is the point of the exercise, and it is why this guide separates targets from
measurements everywhere it states a number.
