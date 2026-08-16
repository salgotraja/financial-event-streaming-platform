# ADR-001: Graph Layer for Reconciliation Agent Precedent Retrieval

**Status:** Accepted
**Date:** 2026-07-23
**Deciders:** Jagdish (sole architect on this project)

## Context
FR-A7 requires the reconciliation agent to retrieve relevant precedent
from past reviewed cases when reasoning about a new event. At current and
near-term scale (tens to low hundreds of reviewed cases), flat vector
similarity search is technically sufficient for most retrieval needs.
Two things are true at once, though: some genuinely useful queries —
"everything connected to this counterparty," multi-hop entity
relationships — are structurally graph-shaped and answered poorly by
similarity search alone. And the primary strategic goal behind this
whole project is demonstrating AI engineering depth to move into
AI-focused roles, where knowledge-graph experience is a named,
explicitly-requested skill in the current market. Learning and
demonstrable skill acquisition is being weighted here as a first-class
decision input, not an afterthought — stated explicitly so the decision
can be defended honestly rather than dressed up as pure necessity.

## Decision
Build the graph layer using **Neo4j** — Community Edition for local dev,
Aura free tier or a short-lived containerized instance on ECS Fargate for
a one-time production validation pass, consistent with the platform's
existing "validate once, tear down" cost discipline. Scoped narrowly to
serve FR-A7 only. Postgres remains the source of truth for jobs, events,
and everything else; Neo4j holds only the case-history graph (Event,
Instrument, Counterparty, AnomalyPattern, and Resolution nodes and their
relationships).

## Options Considered

### Option A: Postgres edge table
| Dimension | Assessment |
|---|---|
| Complexity | Low — reuses the existing datastore, recursive CTEs for shallow traversal |
| Cost | Effectively free — no new infrastructure |
| Scalability | Fine for 2-3 hop queries at low hundreds of nodes |
| Team familiarity | High — SQL already used throughout the platform |
| Career/learning value | Low — doesn't produce a distinct, nameable skill |

**Pros:** Zero new operational surface, fastest to build, nothing new to operate or fail.
**Cons:** Doesn't demonstrate graph-database experience specifically — a recursive CTE isn't what a job posting means by "knowledge graph."

### Option B: Dedicated graph database — Neo4j (chosen)
| Dimension | Assessment |
|---|---|
| Complexity | Medium — a new datastore to run and learn, but the query surface (Cypher) is small and well-documented for this use case |
| Cost | Free for local dev; Aura free tier or a short-lived Fargate instance keeps production cost near zero |
| Scalability | Comfortably exceeds what this project needs — not the reason to pick it |
| Team familiarity | Low initially, deliberately — that's the point |
| Career/learning value | High — Neo4j is the name most GraphRAG tooling and BFSI/enterprise postings reference directly; Cypher is a concrete, listable skill |

**Pros:** Directly builds the named skill the market is asking for; native multi-hop traversal for the counterparty-linkage query that actually matters; strong alignment with GraphRAG tooling, most of which is Neo4j-first.
**Cons:** A second datastore to operate — connection management, a backup/restore story, one more thing that can fail — for a dataset that doesn't yet need graph-database scale.

### Option C: Amazon Neptune (considered, not chosen)
Stays inside the AWS ecosystem already used elsewhere in the platform
(MSK, ECS Fargate, DynamoDB), which has some appeal. Rejected mainly on
the career/learning criterion: smaller hiring-market footprint and a
thinner GraphRAG tooling ecosystem than Neo4j specifically, and the point
of this decision is the skill demonstrated, not staying inside one cloud
vendor.

## Trade-off Analysis
The complexity and operational cost of Option B are real and are being
knowingly accepted, not overlooked. This is the one place in the whole
platform where "minimal complexity" is deliberately not the deciding
value — the explicit goal of breaking into AI-focused roles makes the
learning value outweigh the engineering minimalism that governs every
other decision here. That trade needs to be stated plainly if asked in
an interview: "I chose Neo4j here specifically to build hands-on graph
experience, not because the data volume required it" is an honest,
defensible answer. Claiming it was pure technical necessity would not be.

Scope containment matters as much as the technology choice. Neo4j serves
exactly one bounded purpose — if it were removed, the agent falls back to
flat retrieval, degraded but functional, not broken. Nothing else in the
platform depends on it.

## Consequences
- **Easier:** precedent retrieval for structurally-connected entities
  (counterparty linkage); a concrete, demonstrable graph-database line
  for interviews and the portfolio narrative; real hands-on Cypher
  experience.
- **Harder:** one more service to run locally and validate in the cloud;
  a second data-consistency story — Postgres is authoritative for
  event/job state, Neo4j is authoritative for the case-history graph,
  and these need a deliberate sync mechanism, not an assumption they'll
  stay aligned on their own.
- **To revisit:** if reviewed-case volume genuinely grows into the
  thousands, revisit schema/indexing in Neo4j — not a v1 concern, noted
  so it isn't forgotten later.

## Action Items
1. [ ] Define the Neo4j schema — node labels and relationship types matching the entities above
2. [ ] Decide the sync mechanism from the human-feedback memory store (Postgres) into the graph — likely an event emitted on every human review decision, consumed by a small sync process, keeping this consistent with the platform's event-driven design rather than a bolted-on batch job
3. [ ] Add Neo4j (Community Edition) to docker-compose for local dev
4. [ ] Update FR-A7 and the architecture doc to reference this decision explicitly
