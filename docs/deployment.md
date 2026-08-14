# Deployment

StreamFusion currently supports exactly **Flink 2.2.0 and 2.2.1**. The loader fails closed for an
unknown or unversioned planner ABI. Install it into Flink's `lib` directory — never
into a job JAR — and it accelerates ordinary streaming SQL jobs with no application-side
`NativePlanner.install(...)` call and no query rewriting.

## Kubernetes or Docker

Build the universal release artifacts, then build and publish a job-neutral Flink base image:

```sh
bin/build-release.sh
bin/build-flink-image.sh --tag registry.example/streamfusion-flink:dev --push
```

Use that image as `spec.image` in a Flink Kubernetes Operator `FlinkDeployment`, or as
`kubernetes.container.image.ref` for Flink's native Kubernetes deployment. It works for either
mode:

- **Session** — run the JobManager, TaskManagers, and the SQL/client process from the
  StreamFusion image; submit job JARs through your normal REST, SQL Gateway, or `FlinkSessionJob`
  path.
- **Application** — derive a job image from the StreamFusion base image, place the job JAR in
  `/opt/flink/usrlib`, and use that image in the Application deployment. Remote job-artifact
  delivery remains supported too.

The pushed tag is a Linux x86_64/ARM64 manifest; the runtime picks the matching native library
inside each pod automatically.

### Layering connectors and formats

The base image is connector- and format-neutral: every optional connector or format is its own
`streamfusion-*` artifact, matching Flink's own connector/format module split. Derive a small image
and install Flink's connector and format JARs, the matching StreamFusion connector JAR, and only
the StreamFusion format JARs your jobs actually use into `/opt/flink/lib` — use that same image for
the JobManager, TaskManagers, and submission client. For example, JSON on Kafka needs four JARs:

```Dockerfile
FROM registry.example/streamfusion-flink:dev
COPY flink-connector-kafka-5.0.0-2.2.jar /opt/flink/lib/
COPY flink-json-2.2.1.jar /opt/flink/lib/
COPY streamfusion-kafka/target/streamfusion-kafka-0.1.0-alpha.1.jar /opt/flink/lib/
COPY streamfusion-json/target/streamfusion-json-0.1.0-alpha.1.jar /opt/flink/lib/
```

Replace `streamfusion-json` with `streamfusion-csv`, `streamfusion-raw`, `streamfusion-avro`, or
`streamfusion-protobuf` and add Flink's like-named format JAR — see [Connectors](connectors/index.md)
for the full per-format breakdown. `avro-confluent` uses both `streamfusion-avro` (the shared native
Avro codec) and `streamfusion-avro-confluent-registry` with Flink's
`flink-avro-confluent-registry`. Use
`fluss-flink-2.2` with `streamfusion-fluss`, or `flink-parquet` with `streamfusion-parquet`, the
same way. A missing optional module is always a normal planner fallback to stock Flink, never a
linkage failure — the core image doesn't require any of them.

## Bare metal

For a local Flink distribution instead:

```sh
bin/build-release.sh
sh bin/install-flink.sh "$FLINK_HOME"
```

Restart Flink after installation, then submit ordinary streaming SQL jobs as usual.

## Building from source

For local development, `mvn compile` is Java-only and does not invoke Cargo; `mvn test` builds the
host **debug** native library once before running tests — fast to iterate with, but roughly an
order of magnitude slower than release, so never benchmark against it. Build the portable optimized
artifacts only when needed for an image or release:

```sh
bin/build-release.sh
```

The release build enables `mimalloc` by default.

## Deployment JVM flags

Run the TaskManager JVM with Arrow's safety checks off, as Comet/Spark do — profiling showed
roughly a third of the transpose CPU was per-accessor bounds/refcount checks:

```
-Darrow.enable_unsafe_memory_access=true -Darrow.enable_null_check_for_get=false
```

See [Configuration](configuration.md) for the full `-Dstreamfusion.*` runtime flag surface,
including off-heap sizing for Arrow batches and native operator state.
