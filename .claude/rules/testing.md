---
description: Test conventions and available infrastructure
paths: "src/test/**/*.java"
---

## Framework
- Unit tests: JUnit 5 + Mockito — no Spring context, pure logic
- Integration tests: @SpringBootTest or test slices (@WebMvcTest, @DataJpaTest)
- Kafka: @EmbeddedKafka
- Database: Testcontainers (PostgreSQL) — never H2 as production substitute

## Rules
- No PowerMock
- Use @MockitoBean (Spring Boot 3.4+) not @MockBean
- Build test data with builders — never raw constructors
- Name pattern: should_{expectedBehaviour}_when_{condition}
- Use AssertJ (assertThat) — not JUnit assertEquals

## Test Slice Guide
- @WebMvcTest: controller validation, HTTP mapping, auth filters
- @DataJpaTest: repository queries, migrations, entity constraints
- @SpringBootTest: full integration paths, Kafka end-to-end
- Plain JUnit: service logic, domain objects, pure functions
