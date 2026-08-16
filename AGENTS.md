# financial-intelligence-platform

Serverless Real-Time Financial Intelligence Platform — 8-module AWS architecture
(EventBridge → Step Functions → Lambda → Bedrock → DynamoDB/S3 → API Gateway).

**Current state (2026-06-26): partially built. Read actual source before assuming.**
- CDK is fully implemented: FoundationStack (VPC, KMS, marketDataTable PK=ticker/SK=timestamp +GSI,
  insightTable PK=ticker/SK=generatedAt, S3 lake, SNS, budget), IngestionStack (full Step Functions
  chain + DLQ + EventBridge + SnapStart aliases), QueryStack (API Gateway + cache + p99 alarm).
- ingestion-function is fully coded (Yahoo Finance fetch, null-safe parse, DynamoDB + S3 store).
- STUBS remaining: insight-function (no generateInsight bean; BedrockInsightService empty) and
  query-function (no queryHandler bean; InsightQuery/MarketDataQuery empty).
- Tests are still contextLoads() only.

## Stack
Java 25 (LTS), Spring Boot 4.1.0, Spring Cloud 2025.1.2, Spring Cloud Function (AWS adapter).
AWS CDK (Java, lib 2.160.0). DynamoDB, S3, EventBridge, Step Functions, Bedrock.
Region: ap-south-1. Lambda runtime: java25 (Amazon Linux 2023, SnapStart-capable).

## Build (Maven multi-module reactor — always run from repo root)
./mvnw clean package                                    # build + test all modules
./mvnw test                                             # all unit tests
./mvnw test -pl functions/ingestion-function            # single module
./mvnw -Dtest=ClassName#method test -pl <module>        # single test method
./mvnw spotless:apply                                   # auto-format (run before committing)
./mvnw verify                                           # full check incl. spotless:check (CI gate)

## CDK (run from infrastructure/)
cdk synth --context env=dev
cdk diff  --all --context env=dev
cdk deploy --all --context env=dev

## Toolchain
- Java 25 (Corretto). Pinned in .sdkmanrc — run `sdk env` (or `sdk env install`) before working.
- Formatter: Spotless with palantir-java-format, 4-space indent. Never use google-java-format.
- sdkman has 3.9.12 not 3.9.16 — `sdk env install` is required, not just `sdk env`.

## Modules
| Module | Package root | Purpose |
|--------|-------------|---------|
| infrastructure/ | dev.engnotes.platform | AWS CDK stacks |
| functions/ingestion-function | dev.engnotes.ingestion | Market data fetch + store |
| functions/insight-function | dev.engnotes.insight | Bedrock insight generation |
| functions/query-function | dev.engnotes.query | API-serving query path |

## Always
- Lambda handlers are Spring Cloud Function beans selected via SPRING_CLOUD_FUNCTION_DEFINITION env var
- Each function packaged as Spring Boot fat JAR via spring-boot-maven-plugin
- CDK: IngestionStack and QueryStack take FoundationStack as constructor arg, call addDependency(foundation)
- Request/response types are Java records
- Run ./mvnw spotless:apply before committing

## Never
- Use google-java-format — Spotless manages formatting
- Assume any CDK stack, Lambda function, or service is implemented — read source first
- Bump Lambda runtime past versions AWS publishes managed runtimes for (forfeits SnapStart)
- Assume insight-function or query-function beans exist — they are stubs (read source first)

## When Compressing Context, Keep
- Which module is being modified
- Whether working on CDK wiring vs Lambda implementation
- Test results from the last run
- Any decisions made about stack dependencies or runtime topology
