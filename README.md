# kopi

Cloud bursting for Java and Scala — distributed parallel map via AWS ECS/Fargate.

**kopi** is part of the [burst-core](https://github.com/scttfrdmn/burst-core) family of cloud bursting libraries. The same JAR you build IS the worker: register functions, add the worker check at the top of `main`, then call `map` to distribute work across AWS Fargate.

## Modules

- **kopi-java** — core Java 17 library
- **kopi-scala** — thin Scala 3 wrapper with `Future`-based API

## Install

### Gradle (Kotlin DSL)

```kotlin
dependencies {
    implementation("dev.burst:kopi-java:0.1.0")
    // For Scala projects:
    implementation("dev.burst:kopi-scala:0.1.0")
}
```

### Maven

```xml
<dependency>
    <groupId>dev.burst</groupId>
    <artifactId>kopi-java</artifactId>
    <version>0.1.0</version>
</dependency>
```

## Quick Start — Java

```java
import dev.burst.kopi.Kopi;
import dev.burst.kopi.MapOptions;
import java.util.List;
import java.util.stream.IntStream;

public class MyApp {
    record Item(int value) {}
    record Result(int doubled) {}

    public static void main(String[] args) throws Exception {
        // 1. Register your function — same binary IS the worker
        Kopi.register("process", Item.class, Result.class,
                item -> new Result(item.value() * 2));

        // 2. Worker check — must be near the top of main()
        if (Kopi.isWorker()) {
            System.exit(Kopi.runWorker());
        }

        // 3. Normal application code — distribute work across Fargate
        List<Item> items = IntStream.range(0, 100)
                .mapToObj(Item::new)
                .toList();

        List<Result> results = Kopi.map("process", items, Result.class,
                MapOptions.defaults().workers(10).spot(true));

        System.out.printf("Got %d results%n", results.size());
    }
}
```

## Quick Start — Scala

```scala
import dev.burst.kopi.{Kopi, MapOptions}
import scala.concurrent.{Await, ExecutionContext}
import scala.concurrent.duration.*

given ExecutionContext = ExecutionContext.global

@main def run(): Unit =
  // 1. Register your function
  Kopi.register[Int, Int]("double", x => Right(x * 2))

  // 2. Worker check
  if Kopi.isWorker() then
    System.exit(Kopi.runWorker())

  // 3. Distribute work
  val items = (0 until 100).toSeq
  val future = Kopi.map[Int, Int]("double", items,
    MapOptions.defaults().workers(10))

  val results = Await.result(future, 5.minutes)
  println(s"Got ${results.size} results")
```

## Options

| Option | Default | Description |
|--------|---------|-------------|
| `workers(n)` | 10 | Number of Fargate tasks to launch |
| `cpu(n)` | 2 | vCPUs per worker |
| `memoryGb(n)` | 4 | Memory in GB per worker |
| `spot(true)` | false | Use Fargate Spot (lower cost, may be interrupted) |
| `maxCost(usd)` | 0.0 | Cost ceiling in USD/hr (0 = unlimited) |
| `timeout(secs)` | 0 | Timeout in seconds (0 = unlimited) |
| `region(r)` | config | Override AWS region |

## Error Handling

- `KopiException` — base exception for all kopi errors
- `PartialException extends KopiException` — some items failed; partial results available via `getResults()`

```java
try {
    List<Result> results = Kopi.map("process", items, Result.class);
} catch (PartialException e) {
    System.err.printf("%d failed, %d succeeded%n",
            e.getFailed(), e.getSucceeded());
    List<Object> partialResults = e.getResults(); // null for failed items
}
```

## Configuration

Place `~/.burst/config.json`:

```json
{
  "region": "us-west-2",
  "s3_bucket": "burst-us-west-2",
  "ecs_cluster": "burst-cluster",
  "ecr_base_uri": "123456789012.dkr.ecr.us-west-2.amazonaws.com",
  "execution_role_arn": "arn:aws:iam::123456789012:role/burst-execution-role",
  "task_role_arn": "arn:aws:iam::123456789012:role/burst-task-role",
  "default_workers": 10,
  "default_cpu": 2,
  "default_memory_gb": 4,
  "backend": "fargate",
  "spot": false
}
```

Or set `BURST_CONFIG_PATH` to override the path.

## License

Apache 2.0
