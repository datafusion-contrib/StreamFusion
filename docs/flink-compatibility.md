# Flink line compatibility

The default build targets Flink **2.2.1** and also admits the released **2.2.0** planner ABI.
The `flink-1.18` profile builds a separate **1.18.1** payload from the same source tree.
Both require Java 17. The 1.18 build is a development target until the connector matrix,
real-cluster upgrade checks and required CI/release matrix in
[#187](https://github.com/datafusion-contrib/StreamFusion/issues/187),
[#188](https://github.com/datafusion-contrib/StreamFusion/issues/188) and
[#189](https://github.com/datafusion-contrib/StreamFusion/issues/189) are complete.
The blocking CI matrix covers both Java lines, each native format/connector module, Paimon
Parquet and ORC, and the optimized qualified artifacts with real-loader tests. The upstream matrix
runs both lines, with Delta acceleration restricted to 2.2 and a separate 1.18 host-only Delta
audit. Full 1.18 baselines and real-cluster upgrade validation are still being verified.

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
- The 1.18 host planner cannot consume update/delete streams in window TVF aggregation. Those
  SQL parity cases are N/A; native retraction handling still has operator-level tests.
- 1.18 has released host code-generation defects for nullable `TINYINT`/`SMALLINT` array lookup,
  sub-hour timestamp rounding and streaming `RANK`/`DENSE_RANK` OVER aggregation. Its external
  TIME conversion also rejects some pre-epoch timestamp casts. Those host-oracle cases are
  explicitly N/A in the shared parity suite; neighboring valid cases still execute.

`ENCODE` on 1.18 declares `BINARY(1)` while returning variable-length bytes. That expression stays
on Flink because the native boundary cannot treat arbitrary bytes as a one-byte fixed vector.
The fallback parity tests check the complete bytes, including UTF-16 encodings and nulls.

## JSON and formats

The shared nested ARRAY/ROW JSON parity fixtures use each release line's collection-source
API with identical rows and type information, including multi-batch ownership checks.
Flink 1.18 cannot generate `JSON_QUERY` with a column-valued path. Those cases assert the
same host planning failure, then exercise the nested bridge with a literal selector; 2.2
retains the dynamic selectors.

The 1.18 Jackson runtime has no verified recycler-buffer contract for native SQL/JSON parsing.
Its probe disables that parser path and the existing whole-Calc JVM evaluator handles the
expressions through one callback per Arrow batch. Decimal-bearing `JSON_STRING` and `JSON_OBJECT`
also use the selected host evaluator so decimal scale and spelling match that release.

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

For 1.18 native stateful jobs use StreamFusion's native memory or native RocksDB backend. A stock
RocksDB delegate cannot carry StreamFusion's synthetic canonical key-group keys on this line.
The planner keeps keyed operators on Flink for that backend and other unverified custom backends,
with an explicit fallback reason; stateless operators remain eligible.
The full real-cluster recovery and cross-line upgrade matrix remains tracked in the issues above.

Flink 1.18's changelog state wrapper remains a planning fallback for keyed native operators,
even when it wraps heap state. Its log replay recomputes key groups from serialized keys, which
does not preserve StreamFusion's explicit canonical partition/group pairing. Stateless native
operators remain admitted. Keep `state.changelog.enabled=false` for native keyed execution;
the upstream suite retains Flink's randomization and asserts this fallback when it is enabled.

The 1.18 state admission check recognizes the final StreamFusion RocksDB backend across the
host/planner classloader boundary. A matching backend loaded by the host remains eligible for
native keyed operators; stock RocksDB and changelog-state exclusions still apply. The loader
regression creates the backend through the normal host configuration before checking the native
aggregate plan, matching image submission rather than a single-classloader unit fixture.
