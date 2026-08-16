---
name: test-first
description: Use when creating a new Java class, implementing a new service method, or adding a new feature. Checks for an existing test file before writing any implementation code.
allowed-tools: Read, Bash, Edit, Glob
---

## Steps

1. Identify target: class name, package, owning service module, and type
   (Service / Controller / Consumer / Producer / Repository / Utility)
2. Derive paths inside the owning module:
   - Implementation: `{module}/src/main/java/dev/engnotes/fes/{service}/{ClassName}.java`
   - Test: `{module}/src/test/java/dev/engnotes/fes/{service}/{ClassName}Test.java`
3. Check if the test exists: `find . -name "{ClassName}Test.java" -not -path "./build/*"`
4. If the test EXISTS: read it, note coverage, then proceed with implementation
5. If the test DOES NOT EXIST: create the skeleton BEFORE writing implementation

## Test Skeleton

```java
package dev.engnotes.fes.{service};

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.junit.jupiter.MockitoExtension;
import static org.assertj.core.api.Assertions.*;

@ExtendWith(MockitoExtension.class)
@DisplayName("{ClassName}")
class {ClassName}Test {

    @InjectMocks
    private {ClassName} {classNameCamelCase};

    @Test
    @DisplayName("should [expected] when [condition]")
    void should_expected_when_condition() {
        // given
        // when
        // then
    }
}
```

6. After creating the skeleton, stop and ask: "What behaviour should the first test verify?"
7. Write implementation only after at least one failing test exists

## Mandatory Cases by Class Type

Before the class is done, these must be covered. Do not treat the happy path as sufficient.

- **Kafka consumer**: same event processed twice yields one effect (at-least-once, ADR-019);
  a malformed record goes to `{topic}.dlq` and the partition keeps moving (ADR-027).
- **Kafka producer**: message key set, trace context injected into headers.
- **Controller on the admin control plane**: one ALLOW and two DENY authorization cases; actor
  identity taken from the authenticated principal, never from the request body.
- **Agent tool**: unknown tool name denied, undeclared argument rejected, tool failure escalates
  rather than returning `NO_FLAG` (ADR-023).
- **Read model**: rebuild from event history produces the same state and reports reconciliation.

## Rules

- Use AssertJ (`assertThat`), not JUnit `assertEquals`
- Use `@ExtendWith(MockitoExtension.class)`, not `@SpringBootTest`, for unit tests
- Use `@MockitoBean` where a Spring context is genuinely needed, not the removed `@MockBean`
- Kafka and PostgreSQL integration tests use Testcontainers, not `@EmbeddedKafka` and not H2
- Naming: `should_{expectedBehaviour}_when_{condition}` in snake_case
