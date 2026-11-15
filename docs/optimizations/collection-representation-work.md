# Reduce collection representation work

The default-enabled collection evaluator must preserve Flink's Java string caches, nested row kinds
and generic/binary array copy category. Those details require extra physical children for sensitive
types; nullable primitive-only programs keep the ordinary layout and avoid serializer snapshots
that preserve no additional identity. This also leaves their final arrays as ordinary columnar
views instead of rebuilding generic arrays at the exit transpose.

Represented nested-row vectors retain one reader for the batch lifetime. Each returned row is
copied before that reader can move to the next index, preserving ownership without allocating a
new column-batch wrapper and reusable row wrapper for every nested element.

## Initial comparison (2026-10-17)

The initial release profiles use async-profiler CPU sampling at 1 ms and allocation sampling at
1 MiB. They include planning, warmups, stock and StreamFusion trials in one process; counts are
inclusive and overlap. In the append profile, array-writer serializer snapshots appeared in 777
of 30,161 CPU samples and 1,703 of 24,446 allocation samples. In map entries, reader construction
under the nested-row getter appeared in 1,343 of 57,320 CPU samples and 8,424 of 127,310 allocation
samples. These are sampled events, not counts of allocations or copied bytes.
[Profile counters](../benchmarks/extended-functions-profile-2026-10-17.csv) retain the measured scopes.

Unprofiled measurements retain both row/Arrow transposes, two warmups and five alternating trials,
JDK 17, Flink 2.2.1, release/mimalloc, parallelism 1 and a 2 GiB heap on Linux x86-64. The initial and
optimized implementations run in separate JVMs; paired Flink medians are shown to expose drift.
Rows are two million except width-64 sort at 500,000.

| Case | Initial Flink (s) | Initial StreamFusion (s) | Final Flink (s) | Final StreamFusion (s) |
| --- | ---: | ---: | ---: | ---: |
| Append, width 8 | 0.486709 | 0.859151 | 0.488629 | 0.771157 |
| Sort, width 64 | 1.372298 | 1.840761 | 1.342123 | 1.631030 |
| ARRAY constructor | 0.489814 | 0.656635 | 0.506142 | 0.611204 |
| Map entries, width 3 | 1.330885 | 3.411470 | 1.291837 | 3.225086 |
| ROW constructor | 0.507912 | 0.884793 | 0.499804 | 0.801584 |

The reduction is 5–11% of the initial StreamFusion time in these runs. None of these isolated
jobs beats Flink. Both collection and metadata extensions are enabled by default under the
maintainer's explicit acceptance of the measured regressions on 2026-10-17.
The later [standalone comparison](../benchmarks/collections-392-2026-10-22.md) records
the profiled improvement pass for #392. These historical results do not establish a
sustained-throughput gain. Full comparisons against previous StreamFusion,
longer workloads, reproducible commands and every trial belong in the
[scalar benchmark ledger](../benchmarks/scalar-functions.md#extended-function-coverage).

For profiling, add `-Dsf.extraJvmArgs="-Xmx2g -agentpath:$ASPROF_LIBRARY=start,event=cpu,alloc=1m,interval=1ms,file=collections-%p.jfr"`
to the reproduction command, pointing `ASPROF_LIBRARY` at the profiler installation. Keep the PID
suffix: Maven's separate ORC test execution can launch another JVM even when that execution has
no selected benchmark tests. Profiled timings are excluded from the elapsed-time tables.

## Shared views of string-keyed maps

Direct final MAP_KEYS, MAP_VALUES and MAP_ENTRIES projections of an input-reference
MAP<STRING/VARCHAR, nullable basic value> use the existing Arrow view kernels. Basic values
are BOOLEAN, signed integers and DECIMAL. The kernels retain list offsets, parent validity
and the selected entry buffers instead of constructing JVM collections and writing a second
set of Arrow buffers. NULL keys, duplicate entries and their order remain part of the input.

Key and entry projections keep the represented string children, including their original
UTF-16 cache and generic/binary array-copy category. Fresh entry ROWs have INSERT kind.
Nested consumers retain the complete generated evaluator; the existing guards for unverified
columnar producers, state/exchange boundaries and native sinks remain in force. This changes
execution of already-supported expressions rather than adding new SQL signatures.

A Calc containing only direct MAP_VALUES projections and insensitive input-reference/literal
co-projections, with no condition, does not observe the keys' caches. Its entry transpose can
therefore use ordinary UTF-8 key storage while retaining the original map offsets and value
columns. A projection exposing the map, its keys, another sensitive value, or a condition
retains the represented input schema. This decision is captured during planning.

Plain nested ROW views share their immutable column-batch wrapper. Each row view still has its
own position, so retaining one view and reading another cannot retarget the first. Represented
generic ROWs with only the known owning Arrow scalar getters construct a fresh owned row once;
they avoid a second deep copy of values that already own their bytes and objects. Rows containing
nested views and binary row representations retain their existing serializer copies. Tests retain
strings and their UTF-16 caches, binary payloads, wide decimals, full-range timestamps and row kinds
after the reader advances and the entire Arrow batch closes.

## Reserve BIGINT array capacity once

The entry transpose reserves the entire visible BIGINT child range before writing an array,
following Comet's fixed-width writer pattern and the existing INT array path. Nullable boxed
GenericArrayData reads its object storage once rather than redispatching primitive/boxed getters
for every element. Primitive and other ArrayData implementations retain their typed getters.
All paths copy into owned Arrow buffers before upstream can mutate the source. Empty arrays,
NULL parents/elements, count reset and buffer growth retain the ordinary writer contract.

## Standalone materialization comparison (2026-10-22)

Two-million-row release/mimalloc jobs compare actual preceding revision `d951a838`
with implementation `3c47a89d` for collection/BIGINT work and `f7867f55` for the
final generic-row string exit, compiled with the same metrics-enabled harness.
Both transposes, production JNI/IPC, runtime row source and row sink remain timed;
cluster construction and the initial explain are outside. Each expression/cohort
has two warmup pairs and five alternating measured pairs, JDK 17, parallelism one,
a 2 GiB heap and 1,024-row batches. Separate runners serve the two Flink lines and
the follow-up controls and final strings; cross-runner durations are not comparable.

| Flink / case | Prior StreamFusion, s | Current StreamFusion, s | Paired current stock, s |
| --- | ---: | ---: | ---: |
| 2.2 / MAP_VALUES | 2.654 | 1.518 | 1.587 |
| 2.2 / MAP_ENTRIES | 5.260 | 2.677 | 2.150 |
| 2.2 / reverse BIGINT, width 64 | 2.573 | 2.173 | 2.283 |
| 2.2 / prepend BIGINT, width 64 | 2.429 | 1.962 | 2.360 |
| 2.2 / TYPEOF | 0.623 | 0.617 | 0.753 |
| 1.18 / MAP_VALUES | 2.362 | 1.381 | 1.430 |
| 1.18 / MAP_ENTRIES | 4.856 | 2.382 | 1.886 |
| 1.18 / reverse BIGINT, width 64 | 2.364 | 1.962 | 1.797 |
| 1.18 / TYPEOF | 0.630 | 0.587 | 0.651 |

Map projections reduce native medians by 36–51% across both lines. The 2.2 wide
reverse and prepend ranges are below both stock and prior native ranges. Value-only
map ranges overlap stock, while keys/entries remain 25–34% slower than stock and
1.18 reverse remains slower. ROW and ARRAY controls show little benefit or some
regression. TYPEOF prior/current and stock ranges overlap, so no isolated metadata
throughput gain is established.

Paired profiles remove the generated map expression upcalls and unused value-only
key-cache writer. MAP_ENTRIES NativeCalc sampled Java allocations on 2.2 fall from
16.465 to 6.109 GB; the final generic-row TYPEOF exit falls from 1.674 to
1.034 GB, a 38.2% reduction (41.9% on 1.18). BIGINT writer CPU
samples decrease on both lines, while reverse's whole-NativeCalc allocations do
not improve. These inclusive sampled weights are not copied bytes or isolated
transition costs. Required UTF-16 transport and row output materialization remain.

The [complete two-line report](../benchmarks/collections-392-2026-10-22.md) retains
all 420 primary measured jobs and 168 warmups, prior paired stock controls, ranges,
all unfavorable cases, twenty CPU/allocation captures (four rejected), revisions and reproduction
commands. It also retains the interrupted controls and rejected collection-exit
packing prototype separately. The measurements pin their source snapshots around integration onto main
`717b8d94`; their recorded revisions identify the exact binaries tested.

## Arrays consumed immediately by UNNEST

The immediate UNNEST consumer can construct homogeneous scalar arrays with released DataFusion's
Arrow kernel: the container's generic/binary identity cannot escape this boundary. Flink's compact
timestamp writer rules still apply. The computed operand is materialized once per input batch,
and projections are folded into expansion before taking columns. Discarded source arrays and
hidden computed operands therefore avoid being copied once per element; repeated selected columns
share one result array. Empty projections retain cardinality and changelog tags remain independent.

TIMESTAMP_LTZ casts keep the JDK's zone rules. UTC needs no callback; other zones use the existing
batch bridge to obtain offsets, cached between transitions, while date/time arithmetic and array
construction stay in Arrow. The total offset lookup does not force the whole expression back into
row evaluation. Other JVM calls retain their failure-order safeguards. Signed Julian-date arithmetic
and the released version's TIME precision policy remain exact, including extreme timestamps.

The initial complete-job prototype lost to stock Flink: width-four append took 0.869 s versus
0.697 s stock, and the two-element integer constructor took 1.001 s versus 0.751 s. CPU sampling
found Arrow list copying in 530 of 622 inclusive UNNEST samples. Pruning removed that work;
constructor and time-zone specialization then removed unnecessary generated collection evaluation.
The intermediate losses remain in the raw artifact.

Flink 2.2.1/JDK 17 release/mimalloc comparisons use row sources, both transposes, production IPC,
row blackhole sinks, parallelism one, 2 GiB heap, 1,024-row batches, two warmups and three alternating
measured pairs. Each job uses its own metrics cluster; setup and the initial explain are outside
timing, while execution-time planning, deployment and complete processing remain inside. Prior
StreamFusion is actual revision `d85b241a`, with production code and native library frozen.
All twelve primary candidate medians beat both stock and prior StreamFusion. Selected results:

| Case | Rows / source width | Stock median, s | Native median, s | Prior median, s |
| --- | ---: | ---: | ---: | ---: |
| ARRAY_APPEND | 1M / 4 | 0.655305 | 0.606252 | 0.661601 |
| ARRAY_APPEND followed by COUNT | 2M / 4 | 2.785474 | 1.948070 | 2.805246 |
| Two-element BIGINT constructor | 2M / 2 | 0.764788 | 0.558604 | 0.743767 |
| Boolean constructor | 2M / 2 | 0.736582 | 0.574108 | 0.785867 |
| DECIMAL(20,3) constructor | 2M / 2 | 0.956496 | 0.750907 | 0.961498 |
| TIMESTAMP_LTZ constructor | 2M / 2 | 0.770737 | 0.729007 | 0.812969 |
| New York DATE casts | 2M / 2 | 0.958154 | 0.703143 | 0.959377 |
| New York TIME casts | 2M / 2 | 0.943658 | 0.720897 | 0.984899 |
| DATE casts across daylight-saving intervals | 2M / 2 | 0.957382 | 0.733676 | 0.954090 |
| TIME casts across daylight-saving intervals | 2M / 2 | 0.938308 | 0.800881 | 0.950435 |

Five additional two-million-row cases cover TINYINT, SMALLINT, INT, FLOAT and DOUBLE constructors.
Their native medians are 0.656460, 0.597462, 0.586633, 0.588739 and 0.607857 s, respectively;
each beats both its stock and prior-code control. Floating correctness checks separately retain
NaN payloads and signed-zero grouping identity.

Larger and updating comparisons retain the same resources and trial method:

| Case | Rows / source width / keys | Stock median, s | Native median, s | Prior median, s |
| --- | ---: | ---: | ---: | ---: |
| TIMESTAMP_LTZ constructor | 8M / 2 / 65,536 | 2.879445 | 2.672676 | 2.992164 |
| Wide ARRAY_APPEND | 1M / 16 / 64 | 2.089462 | 1.214559 | 2.158536 |
| Retracting LEFT ARRAY_APPEND followed by COUNT | 2M / 4 / 65,536 | 3.072911 | 2.306096 | 3.026504 |

The larger timestamp case has no overlap between candidate and paired stock ranges. A separate
updating LEFT expansion without aggregation retains fallback because Flink inserts the unsupported
`DropUpdateBefore` node. Its failed native-plan assertion and stock warmup are retained; it is not
included among the native speedups.

The normal temporal cases use millisecond IDs; interval cases multiply IDs by one day, spanning
1970–1981 at 4,096 keys. The source still constructs nullable arrays, including empty collections
and NULL elements, even when a particular expression does not read them. Every candidate job
asserts the native operators execute; prior controls assert zero substitutions.

These complete-job durations include startup effects. Timestamp ranges overlap paired stock
ranges, and separate-JVM controls expose drift; these medians are not sustained-throughput
guarantees. The [full trial artifact](../benchmarks/results/computed-unnest-2026-10-18.json) retains
warmups, all measured samples, implementation hashes, configurations and intermediate regressions.

The [CPU profile counters](../benchmarks/results/computed-unnest-profiles-2026-10-18.json) retain
inclusive scopes and profile hashes. In the matching three-million-row timestamp diagnostics,
the generated JVM evaluator's invocation frame falls from 1,029 samples to zero. These profiles
include startup, planning and warmups; their elapsed times do not contribute to the tables.

[Native allocation records](../benchmarks/results/computed-unnest-allocations-2026-10-18.csv)
retain requests and Arrow buffer sharing for all 156 selected fixtures. These describe native
kernel execution with fixture construction excluded; they do not measure bytes copied. The
time-zone fixtures supply offset columns, while complete jobs also include the JDK lookup.

Validation passes 157 selected checks on Flink 2.2.1 and 143 applicable checks on 1.18.1, with
14 version skips, plus eleven release Rust tests and 156 native benchmark fixtures. Coverage includes
both recovery backends and repeated restores, all RowKinds at the expansion boundary, projection
reordering/repetition/cardinality, numeric and temporal edge cases, time-zone transitions, and the
existing 67 expression failure/conditional-evaluation checks. Host serializer-failure probes use independent clusters
for each execution so a failed TaskManager cannot strand the following job.

Reproduce a complete-job case with the release native library built first:

```sh
SF_BENCHMARK=true mvn -pl streamfusion-runtime test -Pbench \
  -Dtest=ComputedUnnestBenchmark -Dsf.testForks=1 \
  '-Dsf.extraJvmArgs=-Xmx2g -Duser.timezone=America/New_York' \
  -Dunnest.operand=date_days -Dunnest.rows=2000000 -Dunnest.width=2 \
  -Dunnest.keys=4096 -Dunnest.warmup=2 -Dunnest.runs=3
```

Run profiles serially, outside other builds/tests/timings. Prior-code controls use identical
fixtures in the frozen revision and `-Dunnest.expectNative=false`. Native fixture validation uses
`python3 bin/bench-native.py --bench data_movement --bench collection_expressions --features mimalloc --filter 'unnest|array_constructor' --smoke`.

## Generated builtin expansion and owned binary ROW output

Computed MAPs and representation-sensitive ARRAYs can expand with Flink's released correlate
generator while remaining inside an Arrow island. This retains the host's constructor, iterator,
predicate and serializer behavior. Source projection removes unused columns before the input
transpose, and the generated output writer receives only the selected correlate columns. LEFT
element predicates use the same generator so unmatched rows receive padding after filtering.
Simple collections retain the existing Arrow take kernel.

Nested binary ROW output carries an owned byte payload alongside the ordinary Arrow children
and representation tag. Both BinaryRowData and NestedRowData use this cache; they are separate
Flink classes. The ordinary reader preserves each class's serializer-copy behavior. An immediate
row exit can instead feed the owned payload into Flink's generated binary projection, avoiding
recursive field reconstruction. It checks every non-null ROW in the batch first. Generic rows,
missing payloads and schemas requiring other representation-sensitive scalar conversions keep
the ordinary exit path. The payload and its metadata survive Arrow IPC and changelog filtering.

UTF-16 caches use checked bulk copies between reusable heap scratch capped at 8 KiB and Arrow's owned UInt16
buffers. A read copies into an owned Java string. This follows Comet's bulk owned-copy pattern
without keeping a native pointer beyond the copy. The code preserves code units directly,
including isolated surrogates, and marks the contiguous non-null child range in one operation.
Scalar cache sidecars allocate only when a Java cache is present; binary-only columns omit the
sidecar. Late cache creation fills earlier rows with NULL and retains the original logical field
index in metadata. Empty Java strings remain present caches, distinct from missing caches.
Stateful consumers choose their output companions from the selected retained values: an OVER
winner can still have an original Java cache when the current input batch has no companions.
The sparse input layout must not suppress that retained representation.

The first two-million-row, four-entry ROW-valued MAP implementation took **4.956 s native versus
3.828 s stock**. CPU sampling found nested serializer copies and row-reader reconstruction in the
generated expansion path. A prototype that cached only BinaryRowData still took **5.005 s versus
3.856 s stock**: map values arrive as NestedRowData. Supporting both classes and checking the
immediate binary exit reduced the native median to **3.352 s versus 3.954 s stock**, with the
native range 3.257–3.519 s below the stock range 3.882–3.955 s. The actual preceding StreamFusion
revision, `8eb53c8b`, took 3.850 s with its separately paired stock control at 3.842 s.

These complete jobs retain the runtime row source, both transposes, generated evaluation,
production IPC and row blackhole sink. They use released Flink 2.2.1, JDK 17,
release/mimalloc, parallelism one, 2 GiB heap, New York time zone, 1,024-row batches, two warmup
pairs and three alternating measured pairs. Cluster creation and initial explain are outside
timing; execution-time planning, deployment and processing remain inside. Startup effects and
separate-JVM control drift limit fine comparisons. Profiled durations do not enter the medians.

Final primary comparisons use two million source rows, four entries per constructor/array,
eight-byte source strings and 4,096 distinct source IDs/values. COUNT groups by the four constant
MAP keys, so this is not a high-cardinality aggregate benchmark. Its gain includes the existing
native COUNT consumer admitted by the new expansion path.

| Flink / query | Stock median, s | Native median, s | Prior median, s |
| --- | ---: | ---: | ---: |
| 2.2.1 / numeric MAP | 2.752839 | 1.324524 | 2.753661 |
| 2.2.1 / numeric MAP then COUNT | 2.766744 | 1.807041 | 2.459782 |
| 2.2.1 / STRING MAP | 4.226764 | 3.674639 | 4.209884 |
| 2.2.1 / ROW-valued MAP | 3.870142 | 3.257920 | 3.849882 |
| 2.2.1 / STRING ARRAY | 2.877080 | 2.353109 | 2.838102 |
| 2.2.1 / LEFT element predicate | 1.124207 | 0.677199 | 1.148685 |
| 1.18.1 / numeric MAP | 2.574973 | 1.256267 | 2.578673 |
| 1.18.1 / numeric MAP then COUNT | 2.477214 | 1.808772 | 2.482529 |
| 1.18.1 / STRING MAP | 4.062077 | 3.618422 | 4.193982 |
| 1.18.1 / STRING ARRAY | 2.847182 | 2.393280 | 2.782944 |

Every candidate range in this primary matrix is below its paired stock range. Prior runs use
separate JVMs: for example, the 2.2 COUNT control's paired stock median was 2.536701 s, compared
with 2.766744 s in the candidate invocation. The raw controls expose this drift; the table does
not isolate the contribution of each optimization. Flink 1.18 does not plan the tested ROW MAP
and LEFT-predicate signatures, so those are host restrictions rather than missing comparisons.

Larger/wider Flink 2.2.1 cases retain the same resources and trial method. Cardinality below
means source IDs/values; COUNT still groups by constant MAP keys.

| Query / source rows / entries / string bytes / cardinality | Stock median, s | Native median, s | Prior median, s |
| --- | ---: | ---: | ---: |
| Numeric MAP / 8M / 2 / 8 / 65,536 | 6.445106 | 2.917515 | 6.335352 |
| Updating numeric MAP then COUNT / 2M / 4 / 8 / 65,536 | 2.781754 | 1.750737 | 2.708050 |
| Updating STRING MAP / 1M / 4 / 128 / 64 | 2.013133 | 1.947118 | 2.031207 |
| ROW-valued MAP / 1M / 8 / 64 / 4,096 | 3.856058 | 3.348729 | 3.798793 |
| Updating STRING ARRAY / 2M / 8 / 32 / 65,536 | 4.102884 | 3.576732 | 4.064715 |
| Updating LEFT predicate / 4M / 8 / 8 / 64 | 3.164162 | 1.858234 | 3.063368 |
| Updating STRING MAP / 4M / 4 / 128 / 64 | 7.692732 | 7.401638 | 7.654708 |

The initial 128-byte updating STRING MAP lost at 2.366729 s versus 1.987632 s stock. Bulk UTF-16
copies alone reduced it to 2.036470 s versus 2.010444 s stock, still a loss. Lazy scalar cache
sidecars then removed unused Arrow vectors and unnecessary cache-aware output views. Writer
scratch is bounded to 8 KiB, including for larger strings. The final one-million-row ranges
overlap; its modest median gain is not a robust throughput claim. The matching four-million-row
confirmation has native range **7.385885–7.515359 s**, below stock **7.676251–7.729726 s** and
prior **7.620529–7.659227 s**. All 17 final candidate medians beat stock and prior behavior.

The [full trial artifact](../benchmarks/results/generated-unnest-2026-10-18.json) retains all
54 profiles and 540 complete unprofiled jobs: warmups, initial losses, intermediate attempts,
final candidate runs and actual prior-code controls on both Flink lines. The source still
constructs nullable strings and nullable/empty arrays even when a query does not read them.
Updating profiles emit all four source RowKinds. Every candidate checks both transposes,
native plans and consumed rows; prior controls require zero native substitutions.

The [CPU profile artifact](../benchmarks/results/generated-unnest-profiles-2026-10-18.json)
retains four profiles and their 16 diagnostic jobs separately. In the matching wide-string
captures, cache-reader inclusive samples fall from 375 to 106 and nested string-writer samples
from 579 to 201. Counts overlap and include startup, planning and both engines; they are not
allocation counts or copied bytes. Sample totals come from the complete flamegraph root,
including its initial compressed record and every JVM thread.

Validation passes 324 selected checks on Flink 2.2.1, including two additional chained ROW/Calc
equality cases after the 322-check suite. Flink 1.18.1 verifies 265 checks with 59 version skips;
the eight UTF-16 unit checks were rerun after correcting two test expectations for Arrow's
allocation-error wrapping. Coverage includes repeated restores on both state backends,
changelog propagation, watermark-carried caches, malformed UTF-16, late cache allocation,
buffer growth, quota rejection cleanup, IPC and owned values after the source/root closes.

The [native allocation records](../benchmarks/results/generated-unnest-allocations-2026-10-18.csv)
cover 18 release fixtures through production IPC encode/decode and UPDATE_BEFORE filtering:
16/1,024/16,384 rows, widths 8/264, NULL parent rows, missing/generic payloads and all four
root RowKinds. These counters report allocation requests and output buffer sharing, not copied
bytes. Fixture setup is excluded from native timing; the complete jobs retain Java conversion.

Reproduce a complete-job case after building the release native library:

```sh
SF_BENCHMARK=true mvn -pl streamfusion-runtime test -Pbench \
  -Dtest=GeneratedUnnestBenchmark -Dsf.testForks=1 \
  '-Dsf.extraJvmArgs=-Xmx2g -Duser.timezone=America/New_York' \
  -Dgenerated.shape=map_row -Dgenerated.rows=2000000 \
  -Dgenerated.entries=4 -Dgenerated.bytes=8 -Dgenerated.keys=4096 \
  -Dgenerated.warmup=2 -Dgenerated.runs=3
```

Prior-code controls copy only the same fixture into the frozen revision and set
`-Dgenerated.expectNative=false`. Native fixture validation uses
`python3 bin/bench-native.py --bench data_movement --features mimalloc --filter cached_rows --smoke`.


## Fixed BINARY map-value views

A direct final MAP_VALUES projection also uses the existing Rust view for VARCHAR-key maps with
nullable BINARY(n) values when the default length-delimited transport is enabled. The kernel
shares the selected binary child, offsets and parent validity; no new Rust formatter or map
representation is introduced. A value-only Calc without a condition or sensitive co-projection
also reuses the existing key-cache omission. Other consumers retain generated evaluation, and
NOT NULL fixed BINARY keys/values remain guarded; nullable binary keys also use the view described below.

At 500,000 runtime-source rows, widths4/256, the release complete-job median falls from
0.581776/0.685394 seconds through generated evaluation to 0.366429/0.448989 seconds through the
Rust view, a 34.5–37.0% reduction. Paired stock medians are 0.312171/0.400533 seconds; the final native
route remains slower. Measurements retain planning, submission, source/sink and both transposes.
[All trials and reproduction](../benchmarks/scalar-functions.md#fixed-binary-map-values) include
previous fallback, controls, ranges and the two-pass optimization limit.

Six Rust smoke fixtures assert child `Arc` identity, sliced offsets, NULL/empty parents and NULL
values at 16/1024 rows and 0/4/256-byte values. The untimed System-allocation probe reports 104
requests/6,481 bytes and zero new Arrow output-buffer bytes per fixture, including first schema
binding. Buffer sharing and allocation requests do not measure copied bytes or end-to-end speedup.

## Fixed BINARY map-key views

Direct final MAP_KEYS on an input-reference map with nullable BINARY(n) keys and nullable basic
values reuses the existing Rust child view when length-delimited transport is enabled. The
kernel shares the binary key child, offsets and parent validity; nested consumers retain the
released Flink evaluator. No new Rust map representation is introduced. NOT NULL fixed BINARY
keys/values and unverified state/writer boundaries retain fallback.

At 500,000 runtime-source rows and key widths4/256, release complete-job native medians fall
from 0.383493/0.490600 seconds through generated evaluation to 0.253453/0.304727 seconds through
the Rust view, a 33.9–37.9% reduction. Paired stock medians are 0.243699/0.274349 seconds; final
native jobs remain slower, and the wide-key native range includes a slower trial. Timings
retain planning, submission, source/sink and both transposes.
[All trials and reproduction](../benchmarks/scalar-functions.md#fixed-binary-map-keys) include
previous fallback, controls, ranges and the completed two-pass optimization limit.

Six Rust smokes assert key-child Arc identity, sliced offsets, duplicate key contents, NULL values and NULL/empty parents
at 16/1024 rows and 0/4/256-byte keys. The untimed System-allocation probe reports 104 requests
for 6,475 bytes and zero new Arrow output-buffer bytes per fixture, including first schema
binding. These counters do not measure copied bytes or end-to-end speedup.

## Preserved scalar-array fan-out

The existing Rust UNNEST kernel recognizes represented STRING carriers as scalars, alongside
component timestamps, rather than expanding their physical children as ROW fields. Its selected
Arrow takes retain ownership, caches and metadata for expanded values and original arrays.
A shared producer-shape check covers collection Calcs and both collection aggregate routes;
nullable elements use native expansion, while NOT NULL elements retain generated getters.
Filters keep this contract. Computed Calcs prune unused whole columns before fan-out, reusing
the existing input-reference remapping and preserving entire nested fields.

Scalar consumers that observe represented string values use the existing generated evaluator.
The input/output contract remains Arrow batches, with no new JNI ownership protocol. Native
ARRAY construction also reuses the binary-element conversion used by map construction, matching
Flink's BinaryArrayWriter cache reset. Applying the conversion after assembly keeps binary-copy
markers on NULL elements; otherwise the first NULL can erase the containing array's category.

The [release comparison](../operators/unnest-correlate.md#release-comparison-for-preserved-arrays-2026-10-28)
records both the aggregate pipeline gains and slower narrow Calc cases, complete
stock/previous/current trial ranges, unchanged released-test witnesses and the
closed two-capture profiling timebox. Generated evaluation and conversion costs
remain explicit follow-up work; native coverage alone is not a speedup claim.
