---
description: Java 25 modern idioms. Enforce records, sealed types, pattern matching, stream gatherers, virtual threads, scoped values. Applies to all Java source files.
paths: "**/*.java"
---

## Version Baseline
Java 25 (LTS) is the target, pinned via the Gradle toolchain so the language level does not drift
with the installed JDK. All features below are finalized unless marked [PREVIEW].
Structured Concurrency and Primitive Patterns are [PREVIEW] and require --enable-preview, which this
build does not enable. Do not use them.

---

## Records — Mandatory for Data Carriers

When you encounter a class with private fields, getters, and no real behavior: refactor it to a record.

```java
// WRONG — never write this
public class OrderRequest {
    private final String userId;
    private final BigDecimal amount;
    public String getUserId() { return userId; }
    public BigDecimal getAmount() { return amount; }
    // + constructor + equals + hashCode + toString
}

// CORRECT
public record OrderRequest(String userId, BigDecimal amount) {}
```

Records auto-generate: canonical constructor, accessors (userId() not getUserId()),
equals, hashCode, toString. Fields are final by default.

Compact constructors for validation:
```java
public record Money(BigDecimal amount, String currency) {
    public Money {
        Objects.requireNonNull(currency);
        if (amount.signum() < 0) throw new IllegalArgumentException("negative amount");
    }
}
```

Use records for: DTOs, request/response types, events, commands, queries, value objects,
config snapshots. Never revert a record to a POJO for any reason.

Exception — do NOT use records for:
- Spring beans (@Service, @Repository, @Component, @Controller)
- Classes that must extend another class
- Avro-generated event classes (the Avro plugin generates these; hand-written duplicates drift)

This project uses Spring Data JDBC, not JPA, so persistence aggregates may be records where the
mapping allows it. The usual "Hibernate needs a no-arg constructor" exception does not apply here.

---

## Sealed Types + Record Patterns

Model closed domains with sealed interfaces + records:
```java
public sealed interface PaymentResult permits Success, Failure {}
public record Success(String txId, Instant at) implements PaymentResult {}
public record Failure(ErrorCode code, String reason) implements PaymentResult {}
```

Use exhaustive switch — no default needed when sealed:
```java
String msg = switch (result) {
    case Success(var txId, _) -> "Processed: " + txId;   // record pattern deconstruction
    case Failure(_, var reason) -> "Failed: " + reason;
};
```

Replace instanceof chains with pattern matching:
```java
// WRONG
if (shape instanceof Circle) { Circle c = (Circle) shape; ... }

// CORRECT
if (shape instanceof Circle c) { ... }

// BETTER — with guard
case Circle c when c.radius() > 100 -> "large";
```

Always use switch expressions (not switch statements). yield for multi-statement arms.

---

## Streams

Prefer method references and streams over loops:
```java
// WRONG
List<String> names = new ArrayList<>();
for (User u : users) { if (u.active()) names.add(u.name()); }

// CORRECT
List<String> names = users.stream()
    .filter(User::active)
    .map(User::name)
    .toList();                // toList() — unmodifiable, Java 16+
```

Stream Gatherers (finalized Java 24) for complex pipelines:
```java
stream.gather(Gatherers.windowFixed(100)).forEach(this::processBatch);
stream.gather(Gatherers.windowSliding(3)).map(this::movingAverage).toList();
```

Use List.of(), Map.of(), Set.of() for immutable collections.
Use List.copyOf() not Collections.unmodifiableList(new ArrayList<>(...)).

Sequenced Collections (Java 21):
```java
list.getFirst();  list.getLast();  list.reversed();  // not get(0) or get(size-1)
```

---

## Virtual Threads + Scoped Values

Virtual threads for all I/O-bound work:
```java
ExecutorService exec = Executors.newVirtualThreadPerTaskExecutor();
Thread.ofVirtual().name("worker-", 0).start(task);
```

Never use synchronized for long-running I/O in virtual threads — pins the carrier:
```java
// WRONG in virtual thread context
synchronized (this) { connection.execute(); }

// CORRECT
private final ReentrantLock lock = new ReentrantLock();
lock.lock(); try { connection.execute(); } finally { lock.unlock(); }
```

Scoped Values (finalized Java 25) — replace ThreadLocal for per-request context:
```java
// WRONG
private static final ThreadLocal<String> TENANT = new ThreadLocal<>();

// CORRECT
private static final ScopedValue<String> TENANT = ScopedValue.newInstance();
ScopedValue.where(TENANT, tenantId).run(() -> handleRequest());
String t = TENANT.get(); // safe within the scope, propagates to virtual thread children
```

---

## Syntax Idioms

Text blocks for multi-line strings (SQL, JSON, HTML):
```java
String sql = """
        SELECT id, name FROM users
        WHERE status = :status
        ORDER BY created_at DESC
        """;
```

Unnamed variables for intentionally unused values (Java 22):
```java
try { op(); } catch (IOException _) { log.warn("failed"); }
stream.filter(_ -> condition()).toList();
if (obj instanceof Point(_, int y)) { use(y); }
```

Use var for obvious local types — never for fields or parameters:
```java
var users = userRepository.findAllActive();   // type is obvious from the method name
```

---

## Anti-Patterns — Always Refactor When Found

| Found | Replace with |
|-------|-------------|
| POJO with getters/setters | record |
| ThreadLocal for request context | ScopedValue |
| if-else instanceof chain | switch pattern matching |
| new ArrayList<>() + loop add | stream + toList() |
| Collections.unmodifiableList() | List.copyOf() |
| new ArrayList<>(Arrays.asList()) | List.of() |
| Platform thread per I/O task | virtual thread |
| get(0) / get(size-1) | getFirst() / getLast() |
| Explicit cast after instanceof | pattern variable |
