# Specified, not built

Everything on this page exists as a design decision, a requirement, or an Avro schema, and has no
implementation in the repository. It is here so the rest of the guide can stay free of plans.

The one thing on this page you *can* open today is the schema set: seven of the sixteen files in
`contracts/src/main/avro/` are contracts for services that do not exist yet, and they are already under
the compatibility gate.

## Phase 2, deterministic streaming

**Every Phase 2 service now exists, and FR-11.4 and FR-11.5 remain open.** [The market cache projector](projector.md) is built and Redis joined the
local stack with it, [trade enrichment](enrichment.md) is built and reads that cache on every trade,
[the risk alert service](risk-alerts.md) reads the enriched stream, and
[the position read model](positions.md) reads it too. What follows is what those services still do
not do.

**Two of FR-11's five requirements are unmet, and one of them is blocked rather than queued.**
[The position read model](positions.md) is built and idempotent by `tradeId`, but it cannot yet
rebuild itself from event history, so FR-11.5's rebuild-and-reconcile is absent and a lost store is
lost. Its read-only query API (FR-11.4) is sequenced after rebuild and blocked on something this
platform does not have yet: the requirement asks for authorised risk and compliance *users*, which is
human authorisation. ADR-030 puts workforce identity outside this platform's boundary by decision, so
the route is the OIDC control plane listed under Phase 3 below (ADR-011). The data is reachable
today only by a consumer holding a Kafka grant on `positions.snapshots`.

The risk service keeps a position total of its own as well, for its position-limit rule, narrower on
purpose: net quantity per trader and ticker, with no account dimension and none of the market value or
exposure figures FR-11.2 names. It cannot be rebuilt from Kafka history either, a capability FR-11.5
asks of the position read model rather than of the risk service. The two overlap and nothing
reconciles them.

All four of the rules FR-04.2 names now exist, but one of them is narrower than the requirement.
`WASH_TRADE_DETECTED` detects a trader crossing themselves, not related-party wash trading:
`contracts/` carries `accountId` and nothing relating two accounts, and ADR-030 puts that class of
identity data out of scope by decision rather than by backlog, so the source is not coming. The rule
was redefined against what exists rather than left unbuilt, and
[the risk alert service](risk-alerts.md#the-self-cross-rule-and-what-it-deliberately-does-not-detect)
sets out exactly what it does and does not catch. The sub-5ms p99 evaluation target is unmeasured,
and a default deployment now makes three database round trips per trade.

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
- The identity trust matrix has entries for 12 services; 7 more arrive with their phases.
- There is no runtime validation of Schema Registry subject naming or per-subject compatibility
  configuration.
- PostgreSQL is absent from the local stack, so FR-09.1 is partly met. Redis is present in both profiles.
- Every IAM control is specified and none is built. Specified and Built are tracked as separate states
  so the distinction stays visible, and ADR-030 fixes what is in scope at all: workload identity, authorization,
  privileged control, agent identity, evidence and identity observability are in; workforce identity,
  identity governance, customer identity and federation breadth are excluded by decision rather than
  by backlog.
- The market cache projector's two identities are now both tested, but not to the same standard. The
  Kafka half is proven structurally, by a denied run and a granted run of the service's own container
  image (`MarketDataCacheProjectorServiceIdentityStackTest`). The Redis half is proven by
  `MarketCacheRedisAclIntegrationTest`, which renders the committed ACL template and asserts a denial
  from the key space and a refusal of an unauthenticated connection. What the Redis side still lacks
  is the container-image half: the assertions run from the test's own client rather than from the
  service process, so they prove the policy rather than the binding.
- The actuator's `RedisHealthIndicator` issues `INFO`, a command the projector's Redis ACL does not
  grant. Nothing in the compose stack runs this service under `strict-security` yet, so this has not
  surfaced, but once it does the service's aggregate `/actuator/health` will report `DOWN` on the
  Redis indicator until the ACL is widened or the indicator is reconfigured.
