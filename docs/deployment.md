# Deployment

StreamFusion currently supports exactly **Flink 2.2.0 and 2.2.1**. The loader fails closed for an
unknown or unversioned planner ABI. Install the loader and core JARs into Flink's `lib` directory
— never into a job JAR — and it accelerates ordinary streaming SQL jobs with no application-side
`NativePlanner.install(...)` call and no query rewriting. Planning itself loads the native library
(the planner compiles each `Calc`'s expressions to verify their result types before admitting it),
so the JARs must be present on the submission client or JobManager as well as the TaskManagers —
which installing them into `lib` on one shared image already ensures.

Builds record the target Flink line and module identity in every payload's manifest. The loader
checks its embedded core and installed StreamFusion JARs before creating the planner classloader,
including renamed extensions. A different line or a missing marker fails loader initialization with an explicit
compatibility error; rebuild or upgrade the loader, core and extensions together. The artifact
coordinates for 2.2 remain unchanged. The `flink-1.18` development profile produces separate
`streamfusion-*-flink1.18` coordinates and admits only Flink 1.18.1; release support remains gated
by the outstanding validation in [#182](https://github.com/datafusion-contrib/StreamFusion/issues/182).
See [Flink line compatibility](flink-compatibility.md) for build commands, dependency selections and
known host differences. Builds and deployments support JDK 11 and JDK 17. Core and the existing connector/format
artifacts target Java 11; the optional Iceberg adapter targets Java 17 and requires
JDK 17 throughout its deployment.
Use the same JDK version on the client, JobManager and TaskManagers. The
[Java compatibility matrix](flink-compatibility.md#java-compatibility) describes build checks
and version-dependent runtime behavior.

Images built by `bin/build-flink-image.sh` perform the same checks before starting a JobManager,
TaskManager, standalone application or history server. The entrypoint checks the host ABI, the
loader's embedded core, and installed payload identities before handing control to Flink's
original entrypoint. An incompatible image exits with a message naming the conflicting lines;
it does not wait for a SQL query. The normal Flink configuration and command handling remain
owned by the original entrypoint. Bare-metal installations and custom images retain the checks
at loader initialization.

The image suite injects conflicting identities into packaged loader, core and renamed extension
JARs on both supported build lines, and verifies nonzero exit before the JobManager starts.
These are startup-admission checks, not cross-version state recovery tests. Cross-line savepoint
upgrade and downgrade validation remain pending in
[#188](https://github.com/datafusion-contrib/StreamFusion/issues/188); no upgrade direction is
announced as supported yet. Testcontainers selects the container runtime from its normal
configuration, including a configured Podman endpoint, without a hard-coded socket path.

The extension-library isolation probes use the same shaded core runtime payload as the image,
plus Flink's provided SLF4J API and each extension's declared additions. A platform-only parent
keeps the application/test classpath from hiding missing packaged dependencies. The thin core
artifact alone is not the deployable runtime: Arrow initialization also needs its bundled classes.

Release artifacts are available from Maven Central and already contain the optimized native
libraries. Fetch the loader and the separate runtime-visible core payload directly into a Flink
distribution; installing StreamFusion does not require a source checkout, Rust, or a local build:

```sh
STREAMFUSION_VERSION=0.1.0-rc4
curl --fail --location \
  "https://repo1.maven.org/maven2/tech/streamfusion/streamfusion-loader/$STREAMFUSION_VERSION/streamfusion-loader-$STREAMFUSION_VERSION.jar" \
  --output "$FLINK_HOME/lib/00-streamfusion-loader.jar"
curl --fail --location \
  "https://repo1.maven.org/maven2/tech/streamfusion/streamfusion-core/$STREAMFUSION_VERSION/streamfusion-core-$STREAMFUSION_VERSION-runtime.jar" \
  --output "$FLINK_HOME/lib/streamfusion-core.jar"
```

Optional modules use their directory names as artifact IDs, for example
`tech.streamfusion:streamfusion-kafka:0.1.0-rc4` and
`tech.streamfusion:streamfusion-json:0.1.0-rc4`. Install the matching stock Flink connector and
format JARs alongside them as described below.

The GitHub release archive includes all optional StreamFusion modules, including Fluss, Delta and Paimon.
Install only the modules your jobs use and their stock connector dependencies; the archive does
not bundle the stock connectors.

## Kubernetes or Docker

Generated Calc/filter bindings contain runtime-only functions; code generation remains in the
planner process. TaskManagers do not need the planner or Calcite to restore or execute these
bindings. An `ExprCodeGenerator` or `CodeGeneratorContext` linkage failure while restoring a
factory through `StreamConfig.getStreamOperatorFactory` indicates a planner/runtime dependency
leak. Upgrade matching compiler and runtime payloads and regenerate the job graph; adding the
whole planner to worker job resources is not the fix. See the isolated-worker regression on the
[Calc/filter page](operators/calc-filter.md#admission-and-routing).

Create a job-neutral image directly from the Maven Central artifacts:

```Dockerfile
ARG FLINK_IMAGE=flink:2.2.1-scala_2.12-java17
FROM ${FLINK_IMAGE}

ARG STREAMFUSION_VERSION=0.1.0-rc4
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

The optional [Doris adapter](connectors/doris.md) builds with the `doris` Maven profile. Install
`streamfusion-doris` and the released `flink-doris-connector-2.2-26.3.0.jar` with the loader and
core. It requires JDK 17 because the stock Doris connector targets Java 17; the adapter itself
targets Java 11. The Flink 2.2 release archive includes this Java-only adapter; it adds no connector
DSO or separate format JAR. The base image does not include it. Stock Arrow sink fallback also
requires the matching Flink `flink-python` JAR from `opt/` at runtime.

The base image contains the loader and self-contained core runtime JAR and remains connector- and
format-neutral: every optional connector or format is its own
`streamfusion-*` artifact, matching Flink's own connector/format module split. Derive a small image
and install Flink's connector and format JARs, the matching StreamFusion connector JAR, and only
the StreamFusion format JARs your jobs actually use into `/opt/flink/lib` — use that same image for
the JobManager, TaskManagers, and submission client. For example, JSON on Kafka needs four JARs:

```Dockerfile
FROM registry.example/streamfusion-flink:dev
ARG STREAMFUSION_VERSION=0.1.0-rc4
ADD https://repo1.maven.org/maven2/tech/streamfusion/streamfusion-kafka/${STREAMFUSION_VERSION}/streamfusion-kafka-${STREAMFUSION_VERSION}.jar /opt/flink/lib/streamfusion-kafka.jar
ADD https://repo1.maven.org/maven2/tech/streamfusion/streamfusion-json/${STREAMFUSION_VERSION}/streamfusion-json-${STREAMFUSION_VERSION}.jar /opt/flink/lib/streamfusion-json.jar
COPY flink-connector-kafka-5.0.0-2.2.jar flink-json-2.2.1.jar /opt/flink/lib/
```

Replace `streamfusion-json` with `streamfusion-csv`, `streamfusion-raw`, `streamfusion-avro`, or
`streamfusion-protobuf` and add Flink's like-named format JAR — see [Connectors](connectors/index.md)
for the full per-format breakdown. `avro-confluent` uses both `streamfusion-avro` (the shared native
Avro codec) and `streamfusion-avro-confluent-registry` with Flink's
`flink-avro-confluent-registry`. Use
`flink-parquet` with `streamfusion-parquet`, or `flink-orc` with `streamfusion-orc`, the
same way. Paimon needs `paimon-flink-2.2-2.0.0.jar`, the selected `streamfusion-parquet` or
`streamfusion-orc` format module, and `streamfusion-paimon` installed as
`01-streamfusion-paimon.jar` — Paimon takes the first matching
format factory it finds and Flink loads `lib/` in sorted name order, so the StreamFusion JAR must
sort before `paimon-flink-*`; see [Apache Paimon](connectors/paimon.md). A missing optional module is always a normal planner fallback to stock Flink, never a
linkage failure — the core image doesn't require any of them.
Iceberg on Flink 2.2/JDK 17 uses the released `iceberg-flink-runtime-2.2` JAR plus
`streamfusion-iceberg`; its data encoder and reader live in that module's own native
library. Rewrite maintenance also links DataFusion’s physical external sort and its utility
dependencies into this library; the SQL planner, session and StreamFusion engine remain
separate. It does not require `streamfusion-parquet`. See [Iceberg](connectors/iceberg.md).

Hudi on Flink 1.18 uses `hudi-flink1.18-bundle-1.2.1`,
`streamfusion-hudi-flink1.18` and `streamfusion-parquet-flink1.18`. The connector
reuses the Parquet native library and adds no separate native payload. It is
included only in Flink 1.18 release archives. Both snapshot reads and admitted
immutable append and admitted MOR upsert writes reuse the separate Parquet module.
The MOR writer does not require the separate Rust MOR reader or a Hudi native DSO.
See [Hudi](connectors/hudi.md).

## Bare metal

For a local Flink distribution instead:

```sh
STREAMFUSION_VERSION=0.1.0-rc4
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

The Flink 2.2 release reactor and deployment archive include the optional Iceberg
JAR and its platform-specific native payload. Host, macOS and container staging,
artifact isolation checks and Maven publication all include it. Flink 1.18 builds
omit Iceberg because the released adapter targets 2.2/JDK 17. The bytecode check
retains the Java 11 limit for core and other modules, with a Java 17 limit only
for Iceberg. Ordinary PR CI runs the Iceberg/upstream streaming selection on JDK 17;
the native workspace check also verifies Iceberg's package and JNI export ownership.

The release build enables `mimalloc` by default. Its thread-local allocator state uses dynamic
TLS because the JVM loads JNI libraries after startup, potentially through more than one
classloader. Linux artifact validation rejects libraries that require a reserved static TLS
block; deployment does not require preloading libraries or changing glibc tunables.

The ORC module builds its released `orc-rust` reader through the normal Cargo/Maven lifecycle
on macOS and Linux. Writing uses the Java ORC library supplied by Flink or Paimon, with a shared
Arrow-to-Hive-vector converter. The ORC C++ adapter, CMake bootstrap and bundled C++ codec archives
have been removed; no ORC/Arrow C++ installation is needed. Other modules retain their normal
native build requirements. See [ORC](connectors/orc.md) for deployment and configuration details.

### Native workspace

| Directory | Responsibility |
| --- | --- |
| `native/engine` | Operators, planner bridge, Rust state, and the core `Native` JNI entry points; produces `libstreamfusion`. |
| `native/bridge` | JNI guards, Arrow C Data import/export, handle accounting, Flink numeric/text semantics, and format ABI types. No JNI exports of its own. |
| `native/format-support` | Shared decoder lifecycle, parse-error isolation, key/value composition, CDC gathering, and format facade macros. No engine or third-party format implementation. |
| `native/kafka` | Kafka-specific JNI entry points, source implementation and existing sink encoders. |
| `native/fluss` | Fluss compacted bucket-key encoding, bucket grouping and Arrow batch statistics, bundled only in `streamfusion-fluss`. Connection management and statistics wire serialization remain in the Java SDK. |
| `native/parquet` | Parquet format encoding and decoding, including reads through host FileIO. |
| `native/orc` | orc-rust decoding, with host-owned I/O and Arrow C Data. Java ORC writing lives in the format/connector JARs. |
| `native/iceberg` | Optional Iceberg task decode, Parquet data-file encode and external rewrite sort; Java retains file/table lifecycle and delete encoding. |
| `native/paimon` | Released paimon-rust source decoding and supported snapshot merging over Java FileIO, bundled in either Paimon adapter. Upstream includes its formats in this DSO; writer codecs remain separate. |
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

StreamFusion sets Arrow Java's optimized access defaults before its first Arrow use, following
Comet's initialization pattern. Normal deployments no longer need to add these options manually:

```
-Darrow.enable_unsafe_memory_access=true -Darrow.enable_null_check_for_get=false
```

The first option skips per-access buffer bounds/liveness checks; the second skips redundant NULL
checks in primitive getters. Validity bitmaps, allocation accounting, retain/release and `setSafe`
capacity growth remain active. Callers must check nullable values and preserve buffer ownership.

Explicit JVM properties, Arrow environment variables and the legacy
`drill.enable_unsafe_memory_access` property retain Arrow's normal precedence. For debugging, start
the TaskManager with `-Darrow.enable_unsafe_memory_access=false` and
`-Darrow.enable_null_check_for_get=true` to restore both checks. These are JVM-wide defaults and
also affect other components using Arrow in that process. Arrow fixes each setting at class
initialization: SQL `SET` and later property changes cannot change it. If another component has
already initialized Arrow, StreamFusion logs the effective state and warns when it differs from
the requested configuration; startup JVM options remain the reliable override in that case.

See [Configuration](configuration.md) for the full `-Dstreamfusion.*` runtime flag surface,
including off-heap sizing for Arrow batches and native operator state.

## Experimental Flink 1.18 images

The development image builder selects the matching official Flink base and qualified payloads:

```sh
bin/build-flink-image.sh --flink-line 1.18 --tag streamfusion-flink:1.18-dev --load --platform linux/amd64
mvn -Pimage-it,flink-1.18 -pl streamfusion-image-it verify \
  -Dstreamfusion.image.name=streamfusion-flink:1.18-dev
```

For an unpacked official distribution, use `bin/install-flink.sh --flink-line 1.18 <FLINK_HOME>`.
The installer checks the supported distribution version and both payload manifests before
copying either JAR. Its default remains the 2.2 line.

Both CI image jobs build optimized native libraries and submit a normal thin user JAR to a
Session cluster. They verify native Calc and grouped aggregation with the native RocksDB backend,
and load each packaged format/connector extension in its own JVM. Flink 1.18 uses the
`state.backend` configuration key; 2.2 uses `state.backend.type`.
The 1.18 line remains experimental: these smoke jobs do not establish cross-line savepoint
upgrade support, which is tracked in [#188](https://github.com/datafusion-contrib/StreamFusion/issues/188).

## Host logging ownership

Flink owns the process logging API and binding. StreamFusion compiles against Flink's SLF4J
1.7.36 API as a provided dependency and does not package SLF4J classes in its deployment JARs.
This prevents Arrow's transitive SLF4J 2 API from shadowing Flink's binding and silently
selecting a no-operation logger. Artifact validation rejects bundled logging API classes,
and the image SQL smoke test requires a real logging provider before running native SQL.

## Connector build classpath ownership

The released Paimon 2.0 connector bundles compatibility copies of Flink `RowData` and
`ArrayData`, including the newer variant methods even in its Flink 1.18 artifact. These
classes must not select the API used to compile StreamFusion's shared source. The Paimon
module declares the target host's `flink-table-common` directly before the connector bundle;
normal dependency version mediation alone cannot detect duplicate classes inside that bundle.
Maven's dependency ordering can expose this difference, so reusing a successful local cache
is not evidence that a fresh image reactor will select the same classes.

`python3 bin/test_paimon_host_classpath.py` resolves the production dependency declarations
against canonical released artifacts on both supported Flink lines. It checks the loaded
`RowData`/`ArrayData` origins and variant APIs and compiles the actual shared `PrunedRowData`
with each compatibility adapter. Its negative control puts the fat connector first and
reproduces the Flink 1.18 `getVariant(int)` compilation failure. Image CI runs this lightweight
Java-only regression and records the Maven version before the native image build.

## Experimental Fluss connector

Source builds and the release archive include the optional `streamfusion-fluss` extension. Install it
alongside the matching core payload and Apache Fluss's released
`org.apache.fluss:fluss-flink-2.2:1.0.0` JAR. It reuses the core Arrow bridge and bundles `libstreamfusion_fluss` for columnar
bucket-key hashing and log-batch statistics. Verified planning substitutions are enabled by default when the module is installed.
Set `-Dstreamfusion.fluss.enabled=false` to retain the stock endpoints; see
the experimental coverage described in [Fluss](connectors/fluss.md).
Primary-key writes remain on Flink's normal Fluss connector.

## Production source ownership

Delta, Fluss and Paimon own their implementation and connector-specific planner classes
under each module's `src/main/java`, beside its SPI registrations and tests. Paimon's
`src/main/java-flink1.18` and `src/main/java-flink2.2` contain the selected host adapters.
Delta requires Flink 2.2; it and Fluss do not add the root compatibility source tree.
Paimon 1.0 retains filtered source generation and its legacy overrides, consuming common
and Flink 1.18 sources from `streamfusion-paimon` rather than the root tree.

This ownership boundary preserves Java packages, service names, JNI symbols, native
resources and deployment coordinates. It adds no native libraries. Kafka and the JSON,
CSV, RAW, Avro, Avro Confluent Registry, Protobuf, Parquet and ORC formats also own their
production sources. Kafka and Avro select host adapters from their own
`src/main/java-flink${flink.line}` directories; the common-only modules do not add the root
compatibility tree. The registry HTTP client retains its existing Kafka package for
compatibility but belongs to the Avro Confluent Registry module, not Kafka.

Runtime tests depend on the actual Kafka and format modules instead of compiling duplicate
implementations or carrying duplicate provider registrations. Module-local SPI files
provide the same installed providers in the test classpath. Cross-module SQL and recovery
tests remain in runtime, including its separate ORC execution with protobuf 2.5; ordinary
Protobuf tests retain their selected host runtime. These test dependencies require no new
test-utils artifact or reverse dependency from the optional modules to runtime.

Kafka, JSON, CSV, Avro, ORC and Hadoop provided dependencies are declared by the modules
that use them, with shared versions in parent dependency management. Core retains the
common file-source interfaces but no longer inherits those specialized production APIs.
Runtime declares their host implementations as test dependencies; connector suites and
canonical-savepoint tests declare the extra APIs and modules their shared fixtures need.
Parent Javadoc no longer injects Parquet or Hadoop to resolve unrelated root sources.

Shared Java sources also require explicit module ownership during compilation. Reproducible
JAR timestamps can precede a fresh checkout's source timestamps; javac then prefers the newer
source of a referenced core class and can implicitly emit that class into a connector's output.
The compiler configuration inherited by every module uses `-implicit:none`: javac may resolve
shared source symbols, but emits classes only for the module's selected source inputs. This
keeps the core runtime and optional connector/format JARs separate without hiding duplicate
classes from artifact validation. Clean rebuilds remove classes emitted by an older configuration.

`python3 bin/test_module_source_ownership.py` recreates an old-timestamp dependency JAR and
newer shared source. Its negative control demonstrates implicit duplication; the configured
compiler flag preserves only the connector's explicit class. A runtime probe executes the
connector and verifies its dependency resolves from the core JAR. Image CI runs the regression
before packaging, and the artifact checker verifies ownership in the real bundle.

`bin/check-artifacts.sh` also runs `bin/check-connector-ownership.py` on the actual Kafka,
Delta, Fluss, Paimon and eight format deployment JARs. It requires every module-local production
type for the selected host, rejects foreign and wrong-version classes, and compares SPI resources with
their maintained module files. Nested classes retain their enclosing source owner.
`python3 bin/test_connector_ownership.py` exercises missing types, leaked core or other-host
types, conflicting source sets and changed/missing SPI registrations as negative controls.
Its manifest probe also preserves module source-directory names when artifact coordinates
include the Flink 1.18 suffix, and retains rejection of mismatched host identities.
The existing bundle checks still require exact native resources, consumer POM identities,
no duplicate deployable classes and the loader's exact embedded core payload.
Runtime's module-origin regression additionally requires one class definition for each
optional implementation and verifies its code source is the owning module, including the
registry client. Paimon 1.0 CI watches the owning Parquet and ORC module paths.

## Portable plans, ordinary SQL and canonical handoff

The unmerged [#569 work](https://github.com/datafusion-contrib/StreamFusion/issues/569) binds
qualified Flink 2.2 window, interval and event-time temporal joins automatically when executing a
stock compiled SQL plan.
Retain the same stock JSON artifact, normal connector dependencies and user code on both engines.
Use Flink's canonical savepoint format to stop the current execution, then restore that savepoint
with the same artifact on the other engine. With StreamFusion installed and enabled, loading the
artifact constructs its private native runtime graph and imports the stock state during normal
initialization. The stock leg needs the stock connector/user JARs and no StreamFusion JARs.

This qualifies the [window-join compiled-plan scope](operators/joins/window-join.md#stock-canonical-savepoint-interoperability),
not unrestricted SQL submission, every operator variant, or every connector delivery mode. Keep
the Flink version and maximum parallelism within the qualified matrix. See the
[canonical state contract](backends/canonical-state.md#automatic-stock-compiled-plan-binding) for
remaining boundaries. Ordinary native checkpoints do not provide a cross-engine handoff artifact.

The [interval workflow](operators/joins/interval-join.md#released-stock-processing-time-state-adapter)
now binds both event-time and processing-time stock compiled plans to their matching native state
adapter. Released restore fixtures pass with actual imported and continued native rows, and all
84 clean-process interval handoffs pass, including residual predicates, no-input re-export, real
checkpoint recovery and 1 → 3 → 2 rescaling. Unrestricted SQL identities, additional types/versions,
failure scenarios and connector protocols retain their separate #569 qualification; keep the
exact tested scope when choosing a handoff plan.

The [temporal workflow](operators/joins/temporal-join.md#canonical-interchange-partial) also binds
stock compiled plans automatically. Twenty-six complete-job round trips qualify INNER/LEFT
continuation, residual predicates, immediate re-export, native checkpoint recovery and 64-key
rescaling through 1 → 3 → 2 tasks at maximum parallelism 128, on both native stores and with both
engine orders. The custom sources and transactional sink stay at one task. Those probes remain pending
until final drain; emitted-output transaction failures have separate ordinary SQL qualification
below. Full-job TTL expiry and wider schema/version/connector guarantees remain unqualified. Keep the same released Flink version,
portable plan and normal user dependencies on both engines; stock needs no StreamFusion JARs.


The qualified temporal, interval and window compiled-plan handoffs also support
`table.exec.uid.generation=DISABLED`. Keep the same original stock plan and qualified Flink
version/maximum parallelism; explicit UIDs or a manual identity map are unnecessary for that
workflow. For the generated-ID rescale cases, set `pipeline.operator-chaining.enabled=false`
from the initial job on both engines. Default chained graphs can change their generated IDs
when parallelism changes, and
stock also rejects that restore. Changing chaining only at restore cannot repair existing IDs.
The [generated-ID matrix](backends/canonical-state.md#generated-operator-ids-in-qualified-compiled-plans)
covers 34 complete jobs plus explicit-UID controls. Ordinary SQL binding has its own qualification
below; it does not inherit every compiled-plan scenario.


Qualified temporal, interval and window joins can also use ordinary SQL submission without a
compiled artifact. Keep the same SQL, schemas, planner options, connector/user code, released
Flink version and maximum parallelism on both engines; stop with a canonical savepoint and restore
through Flink's normal savepoint configuration. StreamFusion retains the original stock logical
roles during planning and installs the conversion automatically. All 26 temporal and 46 window
ordinary handoffs pass with default SQL UIDs; two temporal `ALWAYS` controls pass too. Eight
additional temporal cases recover failures before/after an external sink commit, then switch
engines without duplicate committed effects. Wider ordinary job matrices,
ambiguous graph rewrites, other native families/connectors and wide decimal/high-precision timestamp
keys still need qualification. See the exact
[ordinary SQL scope](backends/canonical-state.md#ordinary-sql-canonical-handoffs). Existing native
processing/checkpoint paths remain available for jobs without a stock canonical binding.


Regular joins now have the same automatic submission workflow on qualified Flink 2.2.1 paths.
Stock compiled JSON stays portable; ordinary SQL needs no compiled artifact. The planner captures
original side layouts/serializers/retention, resolves the original maximum parallelism and preserves
complete stock identities. The regular-binding cohort passes 164 complete-job scenarios across all six join kinds, three
stock side layouts, both native stores/submission forms, repeated switching, no-input re-export,
checkpoint recovery and fixed-maximum rescaling. Eight separate shared-input round trips cover
the later shared-role fix. Keep the exact [regular-join binding scope](operators/joins/regular-join.md#automatic-planner-binding);
broader types, backends, versions, connector protocols, TTL and failure/rescale cases remain under qualification.


Arrival-ordered first-N now follows the same Flink 2.2.1 canonical stop/restore workflow in both
normal SQL and portable stock compiled plans. Its qualified scope includes native memory/RocksDB,
projected/hidden ranks, keyless/empty state, no-input re-export, native checkpoint recovery and
fixed-maximum rescaling, with custom source/sink state preserved. No manual state or operator
mapping is required. Keep connector/UDF dependencies on both engines and retain compatible graph
identities/maxima for native checkpoint upgrades. See the remaining
[first-N limits](backends/canonical-state.md#stock-compatible-first-n-counters);
this does not qualify other rank algorithms or the complete connector/version matrix.


For the qualified Flink 2.2.1 unfiltered normalizer/join graphs, ordinary SQL submission or the
same portable stock compiled plan captures canonical state bindings automatically. Stop with a
canonical savepoint and submit on the other engine using the normal restore option. Stock workers
require only the usual Flink, connector and user code dependencies; StreamFusion workers install
StreamFusion as usual. Both native stores and switch orders, repeated no-input returns and
1→3→2 rescaling at unchanged maxP128 are tested. Sources/sink stay at one task in that rescale
fixture. This does not qualify changing old native graph identities/maxima, arbitrary upstream
native consumers or the full connector/delivery matrix; consult
[canonical coverage](backends/canonical-state.md#automatic-normalizer-binding-and-continued-jobs).


Qualified eager deduplication jobs also acquire Flink 2.2.1 canonical bindings automatically through
ordinary SQL or the same portable compiled plan. No savepoint conversion utility is required.
Stock Flink needs only its normal connector/user dependencies. The qualified process matrix
covers marker/row state, mini-batch full-chain/compact modes, empty/keyless state, no-input returns,
native checkpoint recovery and fixed-maximum rescaling with both native stores and switch orders;
see [eager dedup coverage](backends/canonical-state.md#automatic-eager-dedup-binding) for the remaining
buffered qualification, emission/type variants and delivery/version work. Benchmarking is deferred.


Proctime bare-update deduplication also follows this workflow with its empty stock-state projection,
including mini-batch and TTL variants. Ordinary SQL preserves sink constraint-helper identities
from their actual definitions even when Flink assigns different display IDs. See
[bare-update and helper coverage](backends/canonical-state.md#bare-update-state-and-sink-helper-identities)
for the verified cases and remaining limits. Stock workers still require only normal Flink,
connector and user-code dependencies.


Buffered event-time first-row deduplication now installs its separate Flink 2.2.1 waiting-row,
emitted-marker and event-timer contract through the same submission workflow. It preserves
pending timers and original marker TTL ages. Qualified ordinary/compiled jobs cover both native
stores and switch orders, empty/keyless state, no-input returns, native checkpoint recovery and
fixed-maximum rescaling with custom connectors and both transposes. Broader qualification remains
open; see
[buffered canonical coverage](backends/canonical-state.md#buffered-event-time-first-row-state).


Normal SQL and portable stock compiled plans now bind the initial Flink 2.2.1 value-ordered append
Top-N contract automatically, including fast Top-1 and independent variable bounds. The initial
scope has INT/BIGINT/VARCHAR fields, one sort column and start-one ranges. Stock Flink restores
projected state with only its normal connector/user dependencies. The verified BIGINT process matrix includes both submission forms/stores/orders, no-input/empty
handoffs, native checkpoint recovery and fixed-maxP rescaling, with exact stock changelog and
independent table-state checks. Broader schemas/graphs, rank variants, TTL/failure/version cases
and the documented startup timeouts remain open.
See [append canonical coverage](operators/top-n.md#value-ordered-append-canonical-state-partial).


Normal Flink 2.2.1 SQL and portable compiled plans also bind the qualified fixed/no-TTL UpdateFast
rank subset, including fast Top-1 and one/two sort fields over INT/BIGINT/VARCHAR payloads. Ordinary
updating-rank graphs with unqualified native state owners use their stock template, keeping those
owners' layouts and identities on Flink. Stock restore requires only normal user/connector
dependencies. See [updating canonical scope and pending coverage](operators/top-n.md#updating-canonical-state-partial).
Benchmarking remains deferred.


General retracting ranks also bind automatically on Flink 2.2.1 in ordinary SQL and portable stock
compiled plans. The qualified contract supports INT/BIGINT/VARCHAR payloads, one/two sort fields,
constant OFFSET/large-limit ranges and non-null INT/BIGINT first-observed bounds. It preserves the
three independent stock structures and TTL ages through direct canonical projection. Unsupported
contracts retain stock rank state, and unqualified upstream state owners keep stock layouts.
The 168-scenario complete-job matrix covers both engine orders/stores/submission forms, custom
source/sink state and both transposes, no-input/empty handoffs, checkpoint recovery and rescaling
1→3→2. See [retracting canonical scope and qualification details](operators/top-n.md#retracting-canonical-state-partial).
Benchmarking remains deferred.
