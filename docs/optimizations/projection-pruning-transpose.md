# Projection pruning into the entry transpose

**Applies to:** the RowData→Arrow entry transpose

## What it is

When a native calc reads only a few columns or nested struct fields of a wide row, the planner
narrows the entry transpose to exactly those leaves and remaps the calc accordingly. The unread
person/auction structs of the Nexmark wide event are never materialized into Arrow at all
(`8523187`).

Row-ordered expressions evaluated by Flink's generated Calc code retain the complete nested
structs passed as arguments. That code uses the original positional row serializers, so pruning
members would change their arity. Unused top-level columns are still pruned; native field-by-name
expressions retain nested-field pruning.

The entry transpose applies this projection **before writing into the owned Arrow batch**.
Only selected fields are read and copied into Arrow; no intermediate deep-copied RowData list
is retained. The reusable projection view releases its source and nested-row references
immediately after the write, including on failure. Upstream GenericRowData and BinaryRowData
can therefore be reused as soon as `processElement` returns without retaining unread payload.

Each emitted batch has independent buffers, following Comet's ownership model. Buffers held by
downstream consumers are never reset or reused. Row-count and latency limits, pre-watermark and
checkpoint flushing, RowKind and zero-column row counts retain their existing behavior. Partial
batches now retain Arrow storage under the normal off-heap allocator rather than copied heap
rows. Closing the operator releases a partial batch without emitting it. No new byte limit is
introduced: memory still depends on the **selected** values' sizes.

## Borrowing at the synchronous consumer

For a direct physical input whose released Flink `InternalTypeInfo` matches the source row
schema, the entry edge uses a consumer-local serializer whose row-copy methods borrow the
incoming row. The transpose writes selected values into independent Arrow buffers before
returning. Flink's extra chained-input deep copy therefore adds no ownership protection on this
edge and is avoided. The source type and every sibling consumer keep their normal serializers;
global object reuse is unchanged. This also avoids copying unread fields before pruning.

The edge is a standard virtual forward partition, not another physical operator or a custom
transformation subclass. Existing virtual partition inputs retain their original serializer,
so a hash or rebalance partition cannot be replaced accidentally. Other type information and
schema mismatches also retain the original input. Network serialization, deserialization and
binary copying delegate to the released row serializer. Its original snapshot is preserved;
restoring that snapshot conservatively restores ordinary row copying. This borrowing serializer
belongs only at the synchronous transpose input, never at a consumer that retains rows or sorts,
keys, or asynchronously processes them.

The design follows Comet's row-to-Arrow reader, which writes a row before advancing its producer.
Flink's consumer-specific chained serializer is the adaptation needed here; see
[the divergence note](https://github.com/datafusion-contrib/StreamFusion/blob/feat/recovered-goal-followups/divergences/46-synchronous-arrow-input.md). Ownership checks run a
source that reuses one row and one array through both chained and network-separated forks. They
verify every buffered Arrow result and a sibling that retains its first row. Additional checks
cover serializer duplication, changelog wire bytes, conservative snapshot restoration and
partition/schema exclusions, alongside transpose and SQL parity checks on released Flink 2.2.1
and 1.18.1.

Release/mimalloc diagnostics on x86-64, JDK 17 and Flink 2.2.1 use a 2 GiB test heap, two warmups
and five alternating stock/native trials. Both transposes, the native Calc and the rowwise
blackhole sink remain in the measured job. Previous production is `1b1b5ed8` with the same
benchmark fixtures and its own release DSO; it falls back to stock expressions for these new
functions. Trial sets ran separately on the same host.

| Query | Rows | Stock seconds | Current native seconds | Previous production seconds |
| --- | ---: | ---: | ---: | ---: |
| STRING to fixed BINARY, 264-byte ASCII source, no NULLs | 2,000,000 | 0.868 | 0.379 | 0.858 |
| ELT on fixed BINARY, no NULLs | 2,000,000 | 0.307 | 0.321 | 0.321 |
| ELT on fixed BINARY, no NULLs | 10,000,000 | 1.018 | 1.280 | not measured |
| Boolean ARRAY_DISTINCT, width eight, domain two, nullable | 10,000,000 | 2.466 | 2.087 | 2.199 |

For the two-million-row cast, native trials span 0.371–0.423 seconds, stock
0.861–0.885, and previous production 0.847–0.883 (its stock control is 0.856).
The combined native cast and entry path is 2.29× faster than stock and 2.26× faster than previous
production. These comparisons include both the new expression and the entry optimization;
they do not isolate the serializer change. ELT remains slower than stock: native trials span
0.318–0.325 seconds at two million rows and 1.273–1.288 at ten million, versus stock
0.303–0.308 and 1.001–1.026 respectively. The ten-million-row fixed-BINARY identity control is
also slower (native 1.171 versus stock 1.005). Entry borrowing does not resolve that exit-boundary
performance limit, and ELT's acceleration gate remains open.

Boolean containers are NULL every eighth row and elements every seventh. The repeated native
run spans 2.081–2.097 seconds against stock 2.440–2.469; previous-production fallback spans
2.171–2.241 with its stock control at 2.183. An earlier candidate run is 2.036 against stock
2.512. Both candidate runs beat both baselines, but the changing stock controls limit the size
of the comparative claim: the repeat is 15.4% below its stock control and 5.1% below previous
production. All trials, identities and both stock controls are retained in the
[measurement CSV](../benchmarks/synchronous-arrow-entry-2026-10-02.csv). These are complete-job
measurements; the Boolean membership optimization is included alongside entry borrowing.

The duplicate-heavy STRING follow-up uses 200,000 rows, width 64, domain eight, a
264-byte suffix with Unicode/NUL prefix and the same NULL frequencies. It takes 3.160 seconds
native (2.968–3.216) versus 3.150 stock (3.137–3.173). Previous production's earlier matching
fallback sample is 3.172 (3.149–3.178), with stock 3.152. The current native median improves over
the pre-entry candidate's 3.494 seconds, but overlapping trials do not establish a win against
both baselines. STRING's performance gate remains unresolved; unique strings have not been
remeasured. The CSV retains all ten new trials.

## Measurement

The following historical measurements compare full-row and projected-row buffering, before
direct Arrow writes replaced the intermediate row list.

`PrunedTransposeBenchmark` uses a source without projection pushdown that reuses full-width rows:
INT, DECIMAL(20,2), an unread STRING and unread BYTES. Its fixed query computes `id + 1`, selects
the decimal and filters `id >= 0`. It asserts that the actual native Calc plan includes a two-field
entry transpose and the exit transpose; both engines use the same rowwise blackhole sink. This
diagnostic does not modify Nexmark. The specialized native filter currently does not push its
column subset into the entry transpose; that separate planner path is outside this measurement.

On ARM64/JDK 17/Flink 2.2.1 with release native artifacts, 100,000 rows, two warmups and five
alternating host/native trials per shape, median job net-runtime seconds were:

| Source row | Bytes per unread field | Main native | Pruned-copy native | Main host | Pruned-copy host |
| --- | ---: | ---: | ---: | ---: | ---: |
| Generic | 0 | 0.084 | 0.090 | 0.068 | 0.065 |
| Generic | 1,024 | 0.098 | 0.098 | 0.072 | 0.076 |
| Generic | 65,536 | 0.774 | 0.348 | 0.320 | 0.327 |
| Binary | 0 | 0.079 | 0.089 | 0.067 | 0.068 |
| Binary | 1,024 | 0.101 | 0.105 | 0.078 | 0.075 |
| Binary | 65,536 | 0.766 | 0.390 | 0.357 | 0.360 |

The wide-payload native runs improve by 2.22x/1.96x. Small payloads show no improvement and the
zero-payload Binary case costs more: selected-field extraction replaces a cheap contiguous copy.
These short pipelines remain slower than Flink; the improvement is relative to the former native
path. Trial sets ran separately on the same host, so small differences should not be overinterpreted.

A separate same-thread allocation diagnostic buffers 512 rows without flushing. The Java
allocation per row includes the input StreamRecord and buffering overhead, excludes fixture
construction and Arrow emission, and is averaged over five trials after three warmups:

| Source row | Bytes per unread field | Main bytes/row | Pruned-copy bytes/row |
| --- | ---: | ---: | ---: |
| Generic | 0 | 338.2 | 154.2 |
| Generic | 1,024 | 2,386.2 | 154.2 |
| Generic | 65,536 | 131,410.2 | 154.2 |
| Binary | 0 | 250.2 | 310.2 |
| Binary | 1,024 | 2,298.2 | 310.2 |
| Binary | 65,536 | 131,322.2 | 310.2 |

At the largest payload, the old 512-row buffer retains 64 MiB of unread field bytes; the new
buffer retains none. This is the retained unread-payload component, not total JVM peak heap.
Allocation in the new copy path no longer grows with unread field size. Ownership regressions
cover immediate producer mutation, nested and empty projections, NULLs, Unicode, precision-38
decimals, nanosecond TIMESTAMP/LTZ, changelog kinds, timers, watermarks and checkpoint barriers.
SQL parity and boundary tests run on released Flink 2.2.1 and 1.18.1.

```bash
SF_BENCHMARK=true mvn -Pbench -pl streamfusion-runtime -am test \
  -Dtest=PrunedTransposeBenchmark -Dsurefire.failIfNoSpecifiedTests=false \
  -Dprune.rows=100000 -Dprune.warmup=2 -Dprune.runs=5
```

## The pass-through bug

Pass-through columnar nodes must not hide the rowwise input from this pruning pass. The mini-batch
assigner sitting between a calc and the source silently disabled it: an unpruned transpose measured
at 7x the transpose work, and native q3 ran 2.4x slower with mini-batch on. The fix pushes the
pruning through the assigner (`ddc4f25`); a planner test pins the pruned arity.

## General rule

Any future pass-through columnar rel needs the same treatment — it must not opaquely block
projection pruning from reaching the transpose on its far side.

The current allocation diagnostic reports pending Arrow storage instead of reflecting into the
removed row list. On the x86-64/JDK 17 direct-write implementation, 512 pending projected rows
retain 81,920 Arrow bytes for every unread-payload size above. Measured heap allocation is about
130.4 bytes/row for GenericRowData and 286.3 for BinaryRowData, averaged over five trials after
three warmups. It now includes Arrow writer creation and writes during row ingestion; these
numbers are not directly comparable to the earlier ARM64 copy-only diagnostic. The fixture and
query are unchanged, and ownership tests independently verify that dropped fields are never read.

## Remaining standalone ELT costs

A 45-second async-profiler 4.5 CPU recording repeats the non-null ten-million-row ELT workload
after two warmups with the released current native DSO, JDK 17/Flink 2.2.1 and a 2 GiB heap.
The plan and runtime verify native Calc and both transposes. The profile selects the native
task threads and removes the thread-name frame before matching components. It contains
42,929 selected CPU samples: entry including downstream work 62.7%, entry excluding Calc
18.3%, Calc including exit 44.4%, Calc excluding exit 18.0%, exit 26.6%, and Rust binary ELT
5.3%. Inclusive counts overlap and must not be added; compiler, coordinator and other threads
are excluded. [Component counts](../benchmarks/entry-elt-cpu-2026-10-02.csv) retain the method.
This is diagnostic sampling, not throughput evidence or a comparison against the old profile.

The per-batch fixed-width capacity experiment follows Comet's writer allocation pattern,
retaining independent buffers and the defaults for variable/nested values. It passes 68
existing Flink 2.2.1 checks and an additional capacity/growth/ownership fixture. At ten million
rows, its ELT median is 1.255 seconds versus a fresh unchanged-production 1.260, with
overlapping trial ranges and changing stock controls (1.013 and 1.017 respectively).
It does not establish a repeatable speedup or close the standalone gate. The prototype and
its new test were removed; no capacity policy change remains.
[All 40 identity/function trials](../benchmarks/fixed-entry-capacity-2026-10-02.csv) and the
[scoped rejection](https://github.com/datafusion-contrib/StreamFusion/blob/feat/recovered-goal-followups/.claude/wontdos/fixed-entry-capacity.md)
are retained. Rust's current ELT adaptation also expands scalar BINARY arguments to batch-sized
arrays before selection; this is a candidate for dedicated scalar/array Criterion profiles and
further profiling, not yet a demonstrated optimization.


### Wide fixed-width capacity sizing (prototype)

A 45-second release/mimalloc CPU profile of the row-fed, nullable BINARY(256)
constant-index ELT job records 6,027 of 56,885 samples (10.6%) in fixed-vector
initialization under entry writer creation. This is new evidence beyond the
previous narrow-row capacity experiment, which showed no reliable speedup.
The prototype follows Comet's Arrow writer: allocate top-level fixed-width
vectors for the entry batch limit, while variable and nested vectors retain
their default allocation policy. Every batch retains independent owned buffers.
Safe writes can grow beyond the hint. All 48 selected Flink 2.2 correctness
cases pass, including fixed-width growth, NULLs, reused input ownership, SQL
parity, and existing transpose checks. The matching Flink 1.18 suite passes
42 cases and explicitly skips six unavailable-function cases. Four additional
batch-configuration checks pass on each Flink version, including limits 1, 5
and 64. Whole-job
acceptance remains pending; the earlier narrow-row rejection still applies.


The first matched wide-row pair uses 2 million BINARY(256) records, NULL every
seventh row, a 2 GiB heap, two warmups and five alternating stock/native trials
per query. Both allocation policies use the identical release/mimalloc native
library with ELT array reuse. Both transposes and blackhole remain in the plan.
[All 60 trials](../benchmarks/binary-wide-fixed-capacity-pair-2026-10-02.csv)
include identity controls; candidate Java source is restored after the pair.

| Query | Sized policy stock / native median (s) | Original policy stock / native median (s) |
| --- | --- | --- |
| Constant-index ELT | 0.328 / 0.401 | 0.311 / 0.440 |
| Dynamic-index ELT | 0.315 / 0.406 | 0.339 / 0.445 |
| Identity | 0.311 / 0.406 | 0.327 / 0.456 |

Native constant-index time falls 9.0% (candidate range 0.398–0.409 seconds,
original 0.428–0.449), and dynamic time falls 8.7% (0.397–0.422 versus
0.436–0.451). Stock controls drift in different directions, by roughly
5–7%. Both native queries remain slower than stock. This supports an
incremental capacity improvement but does not resolve fixed-BINARY admission
or establish stability across run order; the prototype remains unaccepted
pending released-version validation and a reversed-order comparison.


The reversed-order pair also completes successfully with the same configuration
and native library; [all 60 trials](../benchmarks/binary-wide-fixed-capacity-reverse-pair-2026-10-02.csv)
are retained. Original-policy constant-index median is 0.434 seconds
(range 0.431–0.440), versus sized-policy 0.406 (0.402–0.409), a 6.6%
reduction. Dynamic medians are 0.438 (0.432–0.445) versus 0.408
(0.401–0.417), a 6.9% reduction. Stock controls are 0.307 versus
0.336 seconds for constant selection and 0.310 versus 0.326 for dynamic
selection; this drift remains visible. Identity native medians fall from
0.458 to 0.429 seconds. The reduction repeats in both run orders, but both
queries still lose to stock and the feature admission gate remains open.


The sustained narrow-row control retains 10 million non-null BINARY(16) rows,
the same heap, warmups, repeats and native library, and
[all 60 trials](../benchmarks/binary-narrow-fixed-capacity-pair-2026-10-02.csv).
Native constant-index medians are 1.171 seconds sized (1.164–1.172) versus
1.196 original (1.195–1.201); dynamic medians are 1.240 (1.231–1.244)
versus 1.295 (1.279–1.308). Stock constant medians are 1.005 versus 1.024
and dynamic medians 1.020 versus 1.046. Identity native medians are 1.173
versus 1.199 seconds. This pair detects no narrow-profile regression, but
roughly 2% stock drift limits attribution of the smaller changes. It does not
overturn the earlier narrow-only rejection or clear the stock-performance gate.
Benchmark loops skip repeated Javadoc generation after the full correctness
reactors have passed those checks; this does not change timed job execution.


## Fixed-binary entry copy experiment

A measured candidate copied variable-layout fixed-binary fields from a
single-segment heap `BinaryRowData` directly into owned Arrow storage, avoiding
the intermediate byte array from `getBinary()`. Inline, off-heap, multi-segment
and generic rows retained the getter path. The candidate is reverted: its
whole-job measurements do not establish a performance improvement.

Ownership and layout validation passed 20 tests on released Flink 2.2.1;
Flink 1.18.1 passed 14 with six explicit skips for unavailable ELT forms.
Writer checks included input mutation, inline values, nonzero offsets,
segment boundaries, off-heap fallback and growth. These tests establish
correctness of the prototype, not retained direct-copy coverage.

For dynamic ELT over 2,000,000 non-null BINARY(16) rows, release/mimalloc,
2 GiB heap, two warmups and five alternating trials, original/candidate/
restored-original native medians were 0.314/0.333/0.307 seconds. Corresponding
stock medians were 0.283/0.295/0.300 seconds. Both transposes and the blackhole
sink remained in the timed job. Startup and variability are retained; no
acceleration claim is made. [All 60 trials, including identity controls](../benchmarks/fixed-binary-entry-copy-2026-10-02.csv)
are retained.

Flink's Row-to-internal converter constructs GenericRowData, whereas the
candidate targets BinaryRowData. This source audit identifies an applicability
mismatch for the selected source. A later bounded runtime probe supports that
mismatch for nullable BINARY(256) uniform-index ELT and its identity control:
2,000 sampled entry calls per job all carry GenericRowData. At the writer,
2,000 control calls carry PrunedRowData wrapping GenericRowData, while 2,000 ELT
calls carry GenericRowData directly. Writer calls count fields, not distinct rows.
[The four observations](../benchmarks/binary-entry-row-classes-2026-10-04.csv)
are bounded first-call samples, not complete-stream distributions. The diagnostic
job uses released Flink 2.2.1/JDK 17, 20 million rows, width 256, every seventh
value NULL, runtime uniform-first index, both transposes and the row blackhole.
The fixture passes one native-enabled harness test with zero skips/errors;
agent transformation witnesses verify both observed classes. The requested
release native library remains unchanged, but this diagnostic does not log an
exact loaded-library path witness. Agent overhead invalidates its timings. Revisit only with a workload that proves the intended copy is removed
and matched whole-job evidence, rather than assuming every binary field passes
through binary-row storage.


### Reproducing bounded row-class observations

`dev/profiling/RowClassAgent.java` is a diagnostic-only Java agent. It samples
2,000 entry calls and 2,000 binary-writer field calls per enclosing entry instance,
prints `SF_ROW_CLASS` records when the limit is reached and unwraps the projection
adapter. Writer instances are batch-scoped; their counters therefore use entry
context. Use this probe on the row-fed binary scalar harness above, where writes
occur synchronously inside entry processing. It is not a general allocation
profiler and must not be attached to performance-comparison runs.

Build with JDK 17 and the released Byte Buddy dependency:

```sh
row_probe_dir="$PWD/target/row-class-probe"
mkdir -p "$row_probe_dir/classes"
mvn -N -q org.apache.maven.plugins:maven-dependency-plugin:3.7.0:copy -Dartifact=net.bytebuddy:byte-buddy:1.17.6 -DoutputDirectory="$row_probe_dir"
javac --release 17 -cp "$row_probe_dir/byte-buddy-1.17.6.jar" -d "$row_probe_dir/classes" dev/profiling/RowClassAgent.java
printf '%s\n' 'Manifest-Version: 1.0' 'Premain-Class: RowClassAgent' 'Class-Path: byte-buddy-1.17.6.jar' '' > "$row_probe_dir/MANIFEST.MF"
jar --create --file "$row_probe_dir/row-class-agent.jar" --manifest "$row_probe_dir/MANIFEST.MF" -C "$row_probe_dir/classes" .
```

Append `-javaagent:$row_probe_dir/row-class-agent.jar` to the harness's existing
`sf.extraJvmArgs`. Use the `bench` profile, `SF_BENCHMARK=true`,
`ScalarFunctionBenchmark#individualFunctions`, `scalar.engine=native`,
`scalar.functions=ELT_FIXED_BINARY`, `scalar.binary.index=first`,
`scalar.binary.width=256`, `scalar.rows=20000000`, `scalar.nullEvery=7`,
`scalar.warmup=0` and `scalar.runs=1`. Retain the identity control, native plan,
fresh test XML and transform/histogram records. Validate each observed context
has 2,000 calls before interpreting it; retain NULLs, wrappers and both boundary
transforms. The instrumented harness timings are diagnostic artifacts only.

### Fresh owned binary rows retain the host copy

A fresh owned `BinaryRowData` at the Arrow exit cannot eliminate the default
Flink chained-output copy. Released Flink 2.2.1 and 1.18.1
`CopyingChainingOutput.pushToOperator` call the configured serializer's `copy`
for each record regardless of whether the row already owns its storage. The
existing profile includes that path and `BinaryRowData.copy`. Direct allocation
would retain the host copy and introduce an allocation before it; the rejected
generated-segment experiment already removes the temporary getter array while
keeping the reusable writer. This API inspection does not claim new timings or
complete the BINARY performance gate. The scoped rejection is recorded in
`.claude/wontdos/binary-direct-owned-exit-row.md`.
