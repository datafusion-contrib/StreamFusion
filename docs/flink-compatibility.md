# Flink line compatibility

The default build targets Flink **2.2.1** and also admits the released **2.2.0** planner ABI.
The `flink-1.18` profile builds a separate **1.18.1** payload from the same source tree.
Both support JDK 11 and JDK 17. Deployable artifacts target Java 11 (class version 55),
including the separately built planner loader. The 1.18 build is a development target until the connector matrix,
real-cluster upgrade checks and required CI/release matrix in
[#187](https://github.com/datafusion-contrib/StreamFusion/issues/187),
[#188](https://github.com/datafusion-contrib/StreamFusion/issues/188) and
[#189](https://github.com/datafusion-contrib/StreamFusion/issues/189) are complete.
The blocking CI matrix covers both Java lines, each native format/connector module, Paimon
Parquet and ORC, and the optimized qualified artifacts with real-loader tests. The upstream matrix
runs both lines, with Delta acceleration restricted to 2.2 and a separate 1.18 host-only Delta
audit. Each run records result totals and distinguishes native execution, expected fallback and
cases without an execution contract. Passing this matrix does not establish real-cluster
cross-line upgrade support or enable publication of the experimental line.

## Java compatibility

Build and run the declared Flink line on JDK 11 or JDK 17. Maven uses `--release 11` on
both build JDKs, so newer build environments cannot introduce newer Java APIs into the
payload. The dependency gate rejects base bytecode above Java 11; the deployment JAR check
also inspects shaded and embedded classes and applies Java 11 multi-release JAR selection.
No connector or format dependency is replaced with a local or forked build.

| Flink payload | JDK 11 | JDK 17 |
| --- | --- | --- |
| 2.2.1 (also admits the 2.2.0 planner ABI) | Build/runtime CI | Build/runtime CI |
| 1.18.1 | Build/runtime CI | Build/runtime CI |

The normal reactor includes core, loader, Kafka, Fluss, and the separate JSON, CSV, raw,
Avro, Confluent Avro, Protobuf, Parquet and ORC modules. The opt-in Delta module remains a
2.2-only integration; Paimon 2.0 supports both lines. Their declared dependencies pass the
same Java 11 gate, and the lake CI runs both JDKs. The legacy Paimon 1.0 compatibility suite
and upstream Flink instrumentation are separate development workflows; their execution
coverage is documented in their respective pages. The instrumentation agent itself uses
JDK 17 and is not installed in a StreamFusion deployment.

Use the same JDK version on JobManagers and TaskManagers. This matters for generated Flink
expressions, Unicode token boundaries, and JDK-dependent string/time behavior. Parity tests
compare native and stock Flink on the same running JDK rather than assuming different JDKs
produce identical results. JDK 11 SQL/JSON uses Unicode 10.0; JDK 17 uses Unicode 13.0.
Both retain the corresponding Jackson numeric and token behavior, including an exhaustive
BMP token-suffix check through JNI.

```sh
# Select a JDK 11 or JDK 17 installation through JAVA_HOME.
mvn -Pdelta,paimon package -DskipTests
python3 bin/check-java-bytecode.py
# Inspect the separate Flink 1.18 artifacts from a clean build.
mvn -Pflink-1.18,paimon clean package -DskipTests
python3 bin/check-java-bytecode.py
```

Java 11 support does not expand native platform coverage or remove the existing 1.18
release-readiness and upgrade constraints above.

Local validation on macOS Apple Silicon used JDK 11.0.27 and 17.0.12 with release/mimalloc
native libraries. Each 2.2/JDK combination passed 386 selected runtime, recovery, loader,
Fluss buffer, Delta row and Paimon cases. Each 1.18/JDK combination passed 372 cases with
11 existing JSON_QUOTE/JSON_UNQUOTE-dependent cases skipped because that released Flink line lacks
the function. These are selected integration regressions, not the full CI suite. Each
combination includes 14 grouped-type cases across two checkpoint restores and 138 Paimon
snapshot key/type cases. The direct JSON token test compares all 65,536 BMP suffixes against
stock Flink on the running JDK. Java 11 packaging checks passed 17 deployment JARs on 2.2
and 16 on 1.18, including effective classes from embedded payloads. Docker-based Fluss
cluster tests require CI or a local Docker daemon and were not executed in this local audit.


## Building and installing

```sh
# Default line; existing coordinates are unchanged.
mvn -pl streamfusion-runtime -am test

# Separate 1.18 artifacts and statically selected compatibility sources.
mvn -Pflink-1.18 -pl streamfusion-runtime -am clean test
```

Select modules by directory when using the profile: the directory remains `streamfusion-runtime`,
while its artifact ID becomes `streamfusion-runtime-flink1.18`. The same suffix applies to the
loader, core and every optional deployment module. Use clean build outputs when switching lines;
Maven's module `target` directories are shared by profile builds.

| Dependency | Default build | `flink-1.18` build |
| --- | --- | --- |
| Flink | 2.2.1 | 1.18.1 |
| Kafka connector | 5.0.0-2.2 | 3.2.0-1.18 |
| Kafka client | 4.2.0 | 3.4.0 |
| Calcite | 1.36.0 | 1.32.0 |
| Protobuf runtime | 4.32.1 | 3.21.7 |
| Paimon connector | `paimon-flink-2.2:2.0.0` | `paimon-flink-1.18:2.0.0` |

The profile selects released dependencies, compatibility source roots, module coordinates and
loader admission together. Dependency enforcement rejects other Flink lines. Each payload's
manifest records its module and line, and the loader checks the embedded core and installed
extensions before implementation classes are loaded. Renaming a JAR cannot bypass this check.
Install one complete line; a mixed install is an error.
Both payload lines use the host's SLF4J 1.7 API and binding; it does not bundle Arrow's transitive
SLF4J 2 API into Flink's global classpath.

Each line also pins its Kafka connector and client. [Kafka](connectors/kafka.md#flink-release-lines)
records these pairings and the integration evidence available for each line.

The Kernel-based Delta implementation belongs only to the 2.2 source root and does not enter
the 1.18 compilation, Javadoc or source artifacts. There is no admitted native Delta connector on 1.18 yet. Do not build or install a 1.18 Delta payload;
this is unavailable functionality, not a verified host fallback.

## Planner and operator semantics

Shared operators remain Arrow batch operators. The 1.18 operator base explicitly enables normal
chaining because that release otherwise starts a new task chain for each custom operator. This
avoids extra row serialization between a source and its Arrow transpose, including the loss of
sub-millisecond timestamp data carried by the host's in-memory rows.

The 1.18 planner represents deduplication with its own physical relation. Its adapter maps that
relation onto the shared native deduplicator. Rowtime keep-first follows that line's eager
changelog behavior. Window deduplication captures the host's public window metadata before
native relation replacement; it does not reflect into private planner fields.
Legacy upsert sinks likewise retain the keys proven by Flink before the input is rewritten;
the sink still uses Flink's released execution node and changelog contract.

The following are host-line differences, not missing native substitutions:

- Per-relation `STATE_TTL` hints do not exist on 1.18. Global idle-state retention still applies.
  Its two-phase global mini-batch aggregate suppresses unchanged results even with TTL enabled;
  the selected emission policy preserves both write-time refresh and expiry.
- `VARIANT`, delta joins and the `SESSION` table function are absent on 1.18. Legacy grouped
  session windows remain a separate supported planner construct.
- Several newer scalar functions and `TO_TIMESTAMP_LTZ` string/default-precision overloads are
  absent from 1.18. Tests mark only those specific host constructs N/A.
  The function-coverage regressions specifically exclude BTRIM, binary STARTSWITH/ENDSWITH/ELT,
  PRINTF, REGEXP_COUNT/INSTR/SUBSTR and REGEXP_EXTRACT_ALL on that released line. STR_TO_MAP,
  Base64 decoding, exact numerics, extrema and the other available forms still run against 1.18's
  own implementation rather than borrowing 2.2 behavior.
- The 1.18 host planner cannot consume update/delete streams in window TVF aggregation. Those
  SQL parity cases are N/A; native retraction handling still has operator-level tests.
- 1.18 has released host code-generation defects for nullable `TINYINT`/`SMALLINT` array lookup,
  sub-hour timestamp rounding and streaming `RANK`/`DENSE_RANK` OVER aggregation. Its external
  TIME conversion also rejects some pre-epoch timestamp casts. Those host-oracle cases are
  explicitly N/A in the shared parity suite; neighboring valid cases still execute.

`ENCODE` on 1.18 declares `BINARY(1)` while returning variable-length bytes. The shared batched
Calc evaluator preserves the host expression and public schema while carrying affected output
columns as Arrow Binary internally. Fused consumers and row sinks preserve the full bytes;
stateful/operator boundaries and native sinks that would interpret those columns as fixed-width
remain explicit fallbacks. An explicit cast to BYTES can establish an ordinary variable-width
boundary. Genuine fixed-width inputs and casts are not globally retyped.

Scalar SQL regression tests assert native routing, resolved output schemas and collected results
for admitted exact-numeric ABS, Java-backed string extrema and dynamic trims, BOOLEAN IF,
PARSE_URL, dynamic SHA2, FROM_BASE64 and the released binary prefix/suffix overloads.
The remaining floating-extrema, binary-backed string, oversized SHA2/ELT and fixed BINARY-result
restrictions retain explicit fallback checks. Generic Calc/filter rejection fixtures use
STRING-to-BOOLEAN TRY_CAST, which still falls back, rather than functions already admitted by
the planner. A fallback assertion failure does not establish a result mismatch: routing and
host/native result parity both need to pass.

Legacy scalar registrations and legacy synchronous/asynchronous lookup sources use the shared
native operators through the 1.18 planner adapters. Existing UDF type/specialization and lookup
changelog restrictions still apply. Parquet sinks admit the host's INT96 timestamp encoding in
both UTC and local-time modes; selecting INT64 is no longer required for native writing. Local
INT96 uses the host's timestamp/calendar conversion once per column batch.

Targeted validation of these adapters passed 262 unchanged upstream 1.18 UDF/lookup
invocations; all 26 audited execution contracts processed rows natively, including the legacy
variants. The focused Java regression selection passed 129 tests (two skipped) on 1.18 and
124 on a clean 2.2 build. The unchanged 1.18 Parquet suite passed all eight cases; the four
INT96 timestamp sink plans were admitted without fallback, and that test class created a native
writer. These scoped runs do not replace the full-suite inventory above.

## JSON and formats

The shared nested ARRAY/ROW JSON parity fixtures use each release line's collection-source
API with identical rows and type information, including multi-batch ownership checks.
Flink 1.18 cannot generate `JSON_QUERY` with a column-valued path. Those cases assert the
same host planning failure, then exercise the nested bridge with a literal selector; 2.2
retains the dynamic selectors.

Both lines use the same native SQL/JSON reader. The 1.18 adapter verifies Jackson 2.14.2's
thread-local recycler on JDK 17; the 2.2 adapter verifies Jackson 2.18.2's thread-local pool.
The shared reader selects the host's numeric and parsing-limit semantics: 1.18 uses Java Double
values for floating JSON numbers and predates Jackson's newer token/depth limits, while 2.2
uses BigDecimal values and those limits. Unsupported runtimes retain the batched host evaluator.
Decimal-bearing `JSON_STRING` and `JSON_OBJECT` on 1.18 still use the host evaluator so decimal
scale and spelling match that release.

JSON, CDC JSON and CSV encoders carry the selected line's decimal-node semantics into the native
formatter. The older Jackson node factory strips decimal trailing zeros even when plain-number
output is selected; current-line plain-number output retains scale. CSV also preserves the older
node's text/quoting behavior and nested decimal spelling.

Flink 1.18's JSON format defaults to its parser decoder. Although the parser-selection config
constant exists, its factory omits that setting from the accepted option whitelist, so explicitly
switching to tree decoding is N/A through SQL on that release. SQL/JSON expression parsing and
connector JSON decoding are distinct contracts.

The 1.18 decoder rejects top-level JSON arrays, including single-element CDC envelopes. Its
plain JSON path retains the parser-compatible retry for scalar coercion and exact float parsing;
array rejection does not disable that retry.

Avro 1.18 exposes only legacy timestamp mapping. Corrected mapping and the newer JSON encoding
option are N/A; the native gate rejects a nonlegacy descriptor rather than approximating it.
Parquet 1.18 retains declared map-key nullability in its footer. The file writer carries that
logical property separately from the Arrow operator boundary and applies it to both the write
schema and Parquet definition levels; map values and keys are checked after reading the file.

Paimon retains its released partition-statistics operator and coordinator; the selected adapter
forwards only the control-event APIs present on the host and preserves the supplied operator context.

## State and recovery

Native state bytes, key-group partitioning and checkpoint ownership are shared across lines.
Columnar partitioner copies preserve their configured channel count because the 1.18 recovery
filter copies after setup. Losing that setting would misroute captured records during rescaling.
On 1.18, the native RocksDB backend uses a temporary heap-backed canonical savepoint projection
because that release's RocksDB backend cannot set a serialized key and an independent key group
through its public API. This projection is used only for native canonical state. Ordinary host
operators retain the RocksDB delegate, and native incremental checkpoints retain their Rust-owned
RocksDB path. The projection's snapshot owns its copied state before live entries are cleared.
It temporarily consumes JVM heap proportional to the canonical serialized state.

For 1.18 native stateful jobs, stock RocksDB selections are transparently replaced by StreamFusion's
native RocksDB backend during native planning. Programmatic embedded and legacy instances retain
their configured host delegate and checkpoint-storage semantics. Custom backend subclasses,
RocksDB options factories and memory factories remain explicit keyed-SQL fallbacks. The version
adapter reads Flink's private resolved-option snapshot because its public API has no export for
previously configured options; inaccessible or changed fields cause a fallback, never defaulted
options. The isolated planner resolves RocksDB option classes from Flink's host classloader,
matching the retained backend. Stateless operators remain eligible.
The full real-cluster recovery and cross-line upgrade matrix remains tracked in the issues above.

Flink's changelog state wrapper remains unsupported on both 1.18 and 2.2 and is a planning
fallback for keyed native operators,
even when it wraps heap state. Its log replay recomputes key groups from serialized keys, which
does not preserve StreamFusion's explicit canonical partition/group pairing. Stateless native
operators remain admitted. Keep `state.changelog.enabled=false` for native keyed execution;
the upstream suite retains Flink's randomization and asserts this fallback when it is enabled.

The 1.18 state admission check recognizes the final StreamFusion RocksDB backend across the
host/planner classloader boundary. A matching backend loaded by the host remains eligible for
native keyed operators; custom-backend and changelog-state exclusions still apply. The loader
regression creates the backend through the normal host configuration before checking the native
aggregate plan, matching image submission rather than a single-classloader unit fixture.
