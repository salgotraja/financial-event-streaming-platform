---
name: test-first
description: Use when creating a new Java class, implementing a new service method, or adding a new feature. Checks for an existing test file before writing any implementation code.
allowed-tools: Read, Bash, Edit, Glob
---

## Steps
1. Identify target: class name, package, type (Service/Controller/Repository/Utility)
2. Derive paths:
   - Implementation: `src/main/java/{package}/{ClassName}.java`
   - Test: `src/test/java/{package}/{ClassName}Test.java`
3. Check if test exists: `find src/test -name "{ClassName}Test.java" 2>/dev/null`
4. If test EXISTS: read it, note coverage, then proceed with implementation
5. If test DOES NOT EXIST: create skeleton BEFORE writing implementation

## Test Skeleton
```java
package {package};

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

6. After creating skeleton, stop and ask: "What behaviour should the first test verify?"
7. Write implementation only after at least one failing test exists

## Rules
- Use AssertJ (assertThat) — not JUnit assertEquals
- Use @ExtendWith(MockitoExtension.class) — not @SpringBootTest for unit tests
- Naming: should_{expectedBehaviour}_when_{condition} in snake_case
