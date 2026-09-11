# Deployment

StreamFusion currently supports exactly **Flink 2.2.0 and 2.2.1**. The loader fails closed for an
unknown or unversioned planner ABI. Install the loader and core JARs into Flink's `lib` directory
— never into a job JAR — and it accelerates ordinary streaming SQL jobs with no application-side
`NativePlanner.install(...)` call and no query rewriting. Planning itself loads the native library
(the planner compiles each `Calc`'s expressions to verify their result types before admitting it),
so the JARs must be present on the submission client or JobManager as well as the TaskManagers —
which installing them into `lib` on one shared image already ensures.

Release artifacts are available from Maven Central and already contain the optimized native
libraries. Fetch the loader and the separate runtime-visible core payload directly into a Flink
distribution; installing StreamFusion does not require a source checkout, Rust, or a local build:

```sh
STREAMFUSION_VERSION=0.1.0-rc2
curl --fail --location \
  "https://repo1.maven.org/maven2/tech/streamfusion/streamfusion-loader/$STREAMFUSION_VERSION/streamfusion-loader-$STREAMFUSION_VERSION.jar" \
  --output "$FLINK_HOME/lib/00-streamfusion-loader.jar"
curl --fail --location \
  "https://repo1.maven.org/maven2/tech/streamfusion/streamfusion-core/$STREAMFUSION_VERSION/streamfusion-core-$STREAMFUSION_VERSION-runtime.jar" \
  --output "$FLINK_HOME/lib/streamfusion-core.jar"
```

Optional modules use their directory names as artifact IDs, for example
`tech.streamfusion:streamfusion-kafka:0.1.0-rc2` and
`tech.streamfusion:streamfusion-json:0.1.0-rc2`. Install the matching stock Flink connector and
format JARs alongside them as described below.

## Kubernetes or Docker

Create a job-neutral image directly from the Maven Central artifacts:

```Dockerfile
ARG FLINK_IMAGE=flink:2.2.1-scala_2.12-java17
FROM ${FLINK_IMAGE}

ARG STREAMFUSION_VERSION=0.1.0-rc2
ADD https://repo1.maven.org/maven2/tech/streamfusion/streamfusion-loader/${STREAMFUSION_VERSION}/streamfusion-loader-${STREAMFUSION_VERSION}.jar /opt/flink/lib/00-streamfusion-loader.jar
ADD https://repo1.maven.org/maven2/tech/streamfusion/streamfusion-core/${STREAMFUSION_VERSION}/streamfusion-core-${STREAMFUSION_VERSION}-runtime.jar /opt/flink/lib/streamfusion-core.jar

ENV GLIBC_TUNABLES=glibc.rtld.optional_static_tls=131072 \
    ROCKSDB_MUSL_LIBC=false
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

Build this image for `linux/amd64`. The current runner-built release JARs contain Linux x86_64 and
macOS Apple Silicon payloads; Linux ARM64 is not part of the published binary set yet.

### Layering connectors and formats

The base image contains the loader and self-contained core runtime JAR and remains connector- and
format-neutral: every optional connector or format is its own
`streamfusion-*` artifact, matching Flink's own connector/format module split. Derive a small image
and install Flink's connector and format JARs, the matching StreamFusion connector JAR, and only
the StreamFusion format JARs your jobs actually use into `/opt/flink/lib` — use that same image for
the JobManager, TaskManagers, and submission client. For example, JSON on Kafka needs four JARs:

```Dockerfile
FROM registry.example/streamfusion-flink:dev
ARG STREAMFUSION_VERSION=0.1.0-rc2
ADD https://repo1.maven.org/maven2/tech/streamfusion/streamfusion-kafka/${STREAMFUSION_VERSION}/streamfusion-kafka-${STREAMFUSION_VERSION}.jar /opt/flink/lib/streamfusion-kafka.jar
ADD https://repo1.maven.org/maven2/tech/streamfusion/streamfusion-json/${STREAMFUSION_VERSION}/streamfusion-json-${STREAMFUSION_VERSION}.jar /opt/flink/lib/streamfusion-json.jar
COPY flink-connector-kafka-5.0.0-2.2.jar flink-json-2.2.1.jar /opt/flink/lib/
```

Replace `streamfusion-json` with `streamfusion-csv`, `streamfusion-raw`, `streamfusion-avro`, or
`streamfusion-protobuf` and add Flink's like-named format JAR — see [Connectors](connectors/index.md)
for the full per-format breakdown. `avro-confluent` uses both `streamfusion-avro` (the shared native
Avro codec) and `streamfusion-avro-confluent-registry` with Flink's
`flink-avro-confluent-registry`. Use
`flink-parquet` with `streamfusion-parquet`, the
same way. Paimon needs `paimon-flink-2.2-2.0.0.jar`, `streamfusion-parquet`, and
`streamfusion-paimon` installed as `01-streamfusion-paimon.jar` — Paimon takes the first `parquet`
format factory it finds and Flink loads `lib/` in sorted name order, so the StreamFusion JAR must
sort before `paimon-flink-*`; see [Apache Paimon](connectors/paimon.md). A missing optional module is always a normal planner fallback to stock Flink, never a
linkage failure — the core image doesn't require any of them.

## Bare metal

For a local Flink distribution instead:

```sh
STREAMFUSION_VERSION=0.1.0-rc2
curl --fail --location \
  "https://repo1.maven.org/maven2/tech/streamfusion/streamfusion-loader/$STREAMFUSION_VERSION/streamfusion-loader-$STREAMFUSION_VERSION.jar" \
  --output "$FLINK_HOME/lib/00-streamfusion-loader.jar"
curl --fail --location \
  "https://repo1.maven.org/maven2/tech/streamfusion/streamfusion-core/$STREAMFUSION_VERSION/streamfusion-core-$STREAMFUSION_VERSION-runtime.jar" \
  --output "$FLINK_HOME/lib/streamfusion-core.jar"
```

Restart Flink after installation, then submit ordinary streaming SQL jobs as usual.

## Contributing from source

For local development, `mvn compile` is Java-only and does not invoke Cargo. `mvn test` builds the
host **debug** native libraries from the Cargo workspace in `native/`. Each extension Maven module
builds its matching Cargo package; the runtime test assembly builds the workspace. Tests load
`libstreamfusion` and the individual `libstreamfusion_<extension>` libraries from
`native/target/debug`, just as a deployment loads each library from its owning JAR. A missing local
extension library cannot bind its methods to the engine library.

Debug builds are roughly an order of magnitude slower than release, so use `mvn test -Pbench` for
benchmarks. Build the portable optimized artifacts when developing or preparing a release:

```sh
bin/build-release.sh
```

The release build enables `mimalloc` by default.

### Native workspace

| Directory | Responsibility |
| --- | --- |
| `native/engine` | Operators, planner bridge, Rust state, and the core `Native` JNI entry points; produces `libstreamfusion`. |
| `native/bridge` | JNI guards, Arrow C Data import/export, handle accounting, Flink numeric/text semantics, and format ABI types. No JNI exports of its own. |
| `native/format-support` | Shared decoder lifecycle, parse-error isolation, key/value composition, CDC gathering, and format facade macros. No engine or third-party format implementation. |
| `native/kafka`, `native/parquet` | Connector-specific JNI entry points and implementation. Kafka owns its existing sink encoders. |
| `native/json`, `native/csv`, `native/raw`, `native/avro`, `native/protobuf` | One decoder library per format JAR. Avro and Avro-Confluent-Registry continue to share the Avro native library. |
| `native/native-build` | Shared build dependency for library-local mimalloc aliases and the checked free/realloc shim. |
| `native/integration-tests` | Rust round trips that exercise both connector encoding and format decoding. |

Shared crates are statically linked into each library. Handles, JVM references, and allocator state
stay local to their owning library; Arrow release callbacks preserve ownership across the C Data
boundary. The Java leak sentinel queries every loaded library's handle registry. The engine enables
`rocksdb-state` by default; `cargo build -p streamfusion --no-default-features` builds an engine
without persistent native state. No connector or format feature selects JNI exports anymore.

```sh
cd native
cargo test --workspace
cargo build -p streamfusion-json
cargo bench -p streamfusion --bench operators
cargo bench -p streamfusion-kafka --bench kafka_sink
cargo bench -p streamfusion-json --bench json_codecs
```

From the repository root, `python3 bin/check-native-workspace.py --libraries native/target/debug`
checks dependency isolation and the built libraries' JNI exports. `native.cargo.args` controls the
Cargo command/profile, and `native.cargo.packages` controls Maven's package selection.

The split removes engine dependencies from standalone extension builds: Parquet's normal dependency
graph contains 79 packages instead of 253. Release validation did not reproduce the historical
binary-size reduction proposed in [issue #51](https://github.com/datafusion-contrib/StreamFusion/issues/51);
the previous export gating already let the linker discard unused engine code.

## Deployment JVM flags

Run the TaskManager JVM with Arrow's safety checks off, as Comet/Spark do — profiling showed
roughly a third of the transpose CPU was per-accessor bounds/refcount checks:

```
-Darrow.enable_unsafe_memory_access=true -Darrow.enable_null_check_for_get=false
```

See [Configuration](configuration.md) for the full `-Dstreamfusion.*` runtime flag surface,
including off-heap sizing for Arrow batches and native operator state.
