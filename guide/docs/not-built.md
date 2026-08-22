# Specified, not built

Everything on this page exists as a design decision, a requirement, or an Avro schema, and has no
implementation in the repository. It is here so the rest of the guide can stay free of plans.

The one thing on this page you *can* open today is the schema set: eleven of the sixteen files in
`contracts/src/main/avro/` are contracts for services that do not exist yet, and they are already under
the compatibility gate.

## Phase 2, deterministic streaming

Three services remain. [The market cache projector](projector.md) is built, and Redis joined the local
stack with it, so what follows is the rest of the queue.

**trade-enrichment-service.** Consumes `trades.raw`, joins market state and instrument reference data,
produces `trades.enriched`. One constraint is fixed in advance: on a cache miss it must not fall back
to a synchronous call to the simulator (ADR-027). A read path that reaches back into a producer turns
a cache miss into an availability dependency.

**risk-alert-service.** Evaluates governed, versioned rules against enriched trades and produces
`notifications.alerts`. Sub-5ms p99 is the target.

**position-exposure-service.** An event-driven read model, idempotent by `tradeId` and rebuildable
from event history (ADR-017).

## Phase 3, security enforcement

The authorization half of this phase is done and described in
[Workload authorization](authorization.md), and the binding between a running service and its Kafka
principal is done and described in [Service identity](identity.md). What remains:

- The same binding in a deployed environment. It is proven in CI, against a local broker with ACLs,
  by running each service's own image. MSK IAM is the cloud authorization path (ADR-009) and nothing
  exercises it yet, and the strict-security compose stack still does not run the services themselves.
- The durable audit sink: S3 Parquet, signed manifests, Object Lock, and verification. The audit role
  may `s3:PutObject` and `kms:Sign` and may not delete, overwrite, bypass retention, administer the
  bucket policy or verify (ADR-012).
- An OIDC control plane with an externalized OPA policy decision point (ADR-011).
- Time-bounded privileged elevation, with expiry as a policy input rather than a cron job
  (NFR-05.21).
- A break-glass procedure: bounded, separately audited, review-gated, and never suppressing audit
  (NFR-11.5).
- Negative authorization tests for the remaining workloads as they ship.

## Phase 4, CDC migration

A mock legacy PostgreSQL source and the real Debezium PostgreSQL connector into `legacy.trades.cdc`,
normalised into the canonical `TradeEvent` by `migration-normalizer` with a deterministic dedup key
and provenance in the four CDC fields the schema already carries. CDC is not simulated in application
code (ADR-020).

Evidence required: snapshot, coexistence, connector restart, and cutover.

## Phase 5, reconciliation and candidates

A reconciliation observation simulator, and a deterministic anomaly candidate service that promotes a
bounded subset of events to `anomaly.candidates`. Deterministic screening comes before any agent
involvement, and an LLM is never called per event on the hot path (ADR-021).

## Phase 6, agent investigation

A typed tool gateway offering read-only tools (`ledger_lookup`, `reference_context`,
`position_history`, `precedent_lookup`) plus `flag_for_review`, which can create a proposal only.

The boundary rules are fixed in ADR-023 and are worth reading before the code exists:

- Unknown tool name: deny. Undeclared argument: reject.
- Identifiers validated against schema, length and character rules.
- Tool result references are server-generated; the model never supplies them.
- LLM text output is never an authorization grant.
- Hard limits on wall-clock duration, iterations, tool-call count and per-invocation budget.
  Exhaustion fails safe to `ESCALATE`, never to `NO_FLAG`.
- Event payloads, retrieved precedent, graph properties and tool outputs are untrusted data. Untrusted
  content can never introduce a tool, widen a resource scope, or remove a human-approval requirement.

Decision and evidence traces are persisted structurally: model, prompt and tool versions, tool calls,
precedent ids, verdict, latency, tokens, cost. Raw chain-of-thought is not persisted (ADR-025).

A human review queue and synthetic remediation intent complete the phase.

## Phase 7, precedent graph and evaluation

Neo4j as a derived, rebuildable projection from reviewed cases. PostgreSQL stays authoritative for
cases, decisions and feedback, and the agent must degrade when the graph is unavailable (ADR-022).

Golden and adversarial regression suites with category-aware CI gates, and one deliberate regression
proven blocked in CI. The 15-case golden dataset proves regression discipline and failure-mode
coverage, not statistical accuracy: no accuracy percentage may be written from it (ADR-024).

## Phase 8, performance and failure evidence

The phase that turns the architecture's numbers into results.

- A 50,000 events/sec deterministic load test under the production durability profile, driven by the
  simulator's rate driver. This is where the FR-01.4 ceiling gets evidenced.
- Broker loss, connector restart, and agent provider outage isolation.
- Cost attribution per plane.

Until this phase runs, every throughput and latency figure in the specification is a target.

## Known gaps in what is built

These qualify claims made elsewhere in this guide:

- The topic inventory and subject map exist twice, in the architecture specification and in
  `deploy/compose/topics.tsv` and `subjects.tsv`, and nothing checks that the two agree.
- The audit archive has no durable sink, and the other 13 evidence topics in FR-05.1 are unsubscribed
  because nothing writes them.
- The identity trust matrix has entries for 11 services; 8 more arrive with their phases.
- There is no runtime validation of Schema Registry subject naming or per-subject compatibility
  configuration.
- PostgreSQL is absent from the local stack, so FR-09.1 is partly met. Redis is present in both profiles.
- Every IAM control is specified and none is built. Specified and Built are tracked as separate states
  so the distinction stays visible, and ADR-030 fixes what is in scope at all: workload identity, authorization,
  privileged control, agent identity, evidence and identity observability are in; workforce identity,
  identity governance, customer identity and federation breadth are excluded by decision rather than
  by backlog.
- The market cache projector's Redis identity has no automated test. The Kafka half is proven
  structurally, by a denied run and a granted run of the service's own container image
  (`MarketDataCacheProjectorServiceIdentityStackTest`); the Redis half, the `market-data-cache-projector`
  ACL user in `deploy/compose/redis/users.acl.template`, is proven only by a manual `redis-cli` probe
  recorded outside this repository. Do not read the Kafka proof as covering both connections.
- The actuator's `RedisHealthIndicator` issues `INFO`, a command the projector's Redis ACL does not
  grant. Nothing in the compose stack runs this service under `strict-security` yet, so this has not
  surfaced, but once it does the service's aggregate `/actuator/health` will report `DOWN` on the
  Redis indicator until the ACL is widened or the indicator is reconfigured.
