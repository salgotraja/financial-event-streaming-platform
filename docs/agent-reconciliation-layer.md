# Reconciliation & Anomaly-Detection Agent — Requirements & Architecture
### (Agentic layer on the Financial Event Streaming Platform)

## Why this, and why layered on the existing platform
Principal-level AI architecture roles are increasingly explicit about what
separates them from a toy agent demo: production deployment with
reliability and cost controls, and human-in-the-loop safety boundaries on
anything consequential. A standalone "book an appointment" agent doesn't
demonstrate that. Layering an agent onto the event streaming platform
does, because it forces the real questions: what does this agent do when
a tool call fails mid-reasoning, what stops it from looping forever on a
weird event, what does "acting on a financial anomaly" actually require
before it's allowed to happen.

It also means one coherent artifact instead of two disconnected ones —
distributed systems expertise (Kafka, KEDA, observability) and applied
agentic AI expertise, demonstrated together, which is closer to what the
market is actually asking for than either shown separately.

## Positioning: compliance-grade, not just "reconciliation"
Same artifact, sharper frame. What this system already does (detects
anomalies, drafts readable narratives, cites precedent, gates every
consequential action behind human approval, keeps a full audit trace) is
close to the definition of AI-native compliance tooling, which is a
category currently attracting real attention and real money. "Compliance
grade agentic anomaly detection with audit ready narratives" is a
stronger description than "reconciliation agent" for both an interview
panel and a consulting conversation, and the framing costs no additional
build time.

Two things to be careful about with this frame. First, it raises the bar
on evidence: claiming compliance grade means the audit trail has to
actually be complete and exportable, not merely logged somewhere, which
is why FR-A10 below exists. Second, it is positioning for work you can
demonstrate, not a claim of regulatory certification. Say "designed to
compliance grade auditability standards", never "compliant", which is a
legal determination nobody makes from a portfolio project.

## What it does
Consumes enriched events from the existing pipeline's output topic,
reasons over them to detect reconciliation anomalies (a price or volume
event inconsistent with recent history, a settlement mismatch pattern),
and proposes a flagged action. It never executes a correction directly —
see NFR-A5.

## Functional Requirements

| ID | Requirement |
|----|-------------|
| FR-A1 | Agent service consumes from the enrichment-service's output topic |
| FR-A2 | Agent reasons over each event (or a small batch) using an LLM with defined tool access to detect anomalies |
| FR-A3 | Tool: `ledger-lookup` — read-only reference/history lookup |
| FR-A4 | Tool: `flag-for-review` — writes a *proposed* action to a review queue/table; this is the only way the agent affects state |
| FR-A5 | All proposed actions require explicit human approval before anything executes |
| FR-A6 | Agent reasoning steps are logged with enough detail to reconstruct why a flag was raised |
| FR-A7 | Agent retrieves relevant precedent from resolved past cases via a Neo4j graph (Event/Instrument/Counterparty/AnomalyPattern/Resolution nodes, synced from the human-feedback memory store) and cites it in its reasoning — see ADR-001 for why a graph database was chosen over flat retrieval at this scale |
| FR-A8 | Every flagged event produces a short, readable case narrative — what happened, why it's flagged, what precedent it resembles, suggested next check — not a bare score |
| FR-A9 | Before finalizing a flag/no-flag decision and narrative, the agent performs one self-critique pass against the same failure modes the golden dataset's false-positive traps target (systemic/sector-wide cause missed, precedent not checked, confidence overstated relative to evidence), revising the output if the critique finds a gap |
| FR-A10 | Every decision produces an exportable audit record in a single structured document: the source event, tools called and their results, precedent cited, the draft decision, the reflection critique and any revision, the final decision with confidence, the human reviewer's verdict, and timestamps throughout. Assembles data FR-A6/A7/A8/A9 already capture, so the build cost is serialization, not new instrumentation |

## Non-Functional Requirements

| ID | Requirement |
|----|-------------|
| NFR-A1 (Cost) | Cost per agent invocation tracked and controlled via five techniques (see "Cost control, in practice" below), three already proven on NoteSensei and two newly applicable here |
| NFR-A2 (Termination safety) | Hard cap on reasoning iterations per event; a cost/time budget per invocation; exceeding either fails the task safely into the review queue rather than looping or hanging |
| NFR-A3 (Tool failure handling) | Tool calls have timeouts and retry with backoff; repeated failure escalates to human review rather than the agent guessing or the task silently dropping |
| NFR-A4 (Observability) | Every agent reasoning step is traced via OpenTelemetry under the *same trace ID* as the originating Kafka event — one continuous trace from raw tick through enrichment through agent reasoning through proposed action |
| NFR-A5 (Safety/permission boundary) | No state-mutating action executes without explicit human confirmation — and this is enforced architecturally, not just behaviorally. The agent's own tool credentials are scoped so `flag-for-review` can only ever write a proposal; the credential capable of executing a correction is held separately and invoked only by the human-approval step. This matters beyond good practice: documented 2025-2026 incidents (agent infrastructure RCE, hooks-injection attacks, a supply-chain package exfiltrating data after 15 clean releases) show that relying on an agent's reasoning to "choose" safety is not sufficient — the boundary needs to be one the agent's permissions physically cannot cross |

## Cost control, in practice
Five techniques, three reused from NoteSensei and two new to this domain:

1. **Prefix caching** — stable system prompt, tool schemas, and reference
   context form the cached prefix; only the event payload varies per
   call. The biggest single lever here, and nearly free to add given the
   prompt structure already supports a stable breakpoint.
2. **Tool-call result cache** — `ledger-lookup` results cached per
   instrument/time-window key, since repeated lookups for the same
   reference data within a short window are common and idempotent.
3. **Semantic response cache** — the highest-risk of the five. A
   false-positive cache hit means returning "not anomalous" for an event
   that actually is. Restricted use only: never cache the final
   flag/no-flag verdict, only cheaper reasoning sub-steps, with a
   conservative similarity threshold and mandatory re-verification before
   any decision ships.
4. **Rolling summarization** — caps the growing context of the rolling
   per-instrument history window that "inconsistent with recent history"
   (FR-A2) requires, instead of re-sending full history each call. Real
   tuning risk: too aggressive a summarization interval can discard the
   slow-drift signal that's often the actual anomaly — the interval needs
   deliberate testing, not a default assumption.
5. **Human-feedback memory** — persists every human review decision
   (approved/rejected) so the agent stops re-flagging patterns already
   dismissed as false positives. Does double duty: this is also exactly
   how the eval harness's golden dataset grows over time (see the
   evaluation & regression harness doc) — one mechanism, two purposes.

This is a legitimate "I've solved this problem before, here's a harder
version of it" story for the first three, and a real extension for the
last two — not a from-scratch claim either way.

## FR-A9 wiring — how reflection actually runs
Placed after the initial reasoning pass (FR-A2, using FR-A7 precedent)
produces a draft decision, confidence level, and narrative, and before
FR-A8 finalizes or FR-A4 writes to the review queue.

Runs as a second, independent LLM call, not folded into the same prompt
as the initial reasoning, so the critique isn't anchored to the same
blind spots that produced the draft. Triggered conditionally, not on
every event and not on flag/no-flag alone: it runs when the initial pass
reports medium or low confidence, in either direction. A high-confidence
flag or a high-confidence no-flag skips it. Gating on confidence rather
than on "did it decide to flag" matters because a flag-only trigger would
catch false-positive risk but do nothing for false negatives, and
NFR-E2 already treats a missed anomaly as the more expensive failure of
the two.

The critique checklist is the same five failure modes the golden
dataset's hardest cases test, not a separate list: systemic/sector-wide
cause missed, known scheduled cause missed, precedent under-weighted,
confidence overstated relative to evidence, and a tool failure treated as
complete data. If the critique finds a gap, the decision and narrative
are revised once — a single review loop, not an open-ended one, per
NFR-A2's termination-safety bound.

## Success criteria
- A real event flows through the full path: ingestion, enrichment, agent
  reasoning, a proposed flag with a generated case narrative and cited
  precedent, sitting in a review queue, a human approving or rejecting,
  and — only then — the action executing.
- One continuous trace capturing that entire path, screenshot-able for
  both an interview and a LinkedIn/engnotes.dev post.
- A real, measured cost-per-invocation figure, not an estimate.
- One complete audit record (FR-A10) exported for a real decision,
  showing the full chain from event through reflection to human verdict.
  This is the artifact that makes the compliance grade framing
  demonstrable rather than asserted.
- The generated case narrative itself is the primary showcase artifact —
  more legible to a non-technical audience (recruiter, LinkedIn reader)
  than a trace or a dashboard, and the clearest evidence that this agent
  helps a human act faster rather than just producing a score.

## Explicitly out of scope for v1
**Multi-agent orchestration** — considered deliberately, not skipped by
default. Mapped out as a hub-and-spoke design (orchestrator plus
detection, document cross-reference, narrative, and routing specialists)
and declined: the current single-agent design already covers the
detection, narrative, and routing roles, and splitting them out added
coordination complexity without a clear domain-specialization need
strong enough to justify it — the industry lesson from 2023-2024
multi-agent overreach (systems with 8-15 agents costing 10x more than one
well-designed agent, with unpredictable inter-agent behavior) was the
deciding factor. Revisit only if a genuinely distinct specialist domain
emerges that the single agent handles poorly.

Also out of scope: autonomous execution without human review (a safety
decision, not a missing feature). Fine-tuning a custom model — an
off-the-shelf model with solid tool-calling is sufficient for this scope
and keeps the artifact honest about what it actually required.
