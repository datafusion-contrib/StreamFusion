# Batch SQL

**Status: partial.** Flink 2.2.1 and 1.18.1 batch Table environments can use native
Calc/filter pipelines, `UNION ALL`, scalar `ORDER BY`, global and typed-key
reductions, DISTINCT, fixed-width MIN/MAX and verified bounded joins, with the
admission restrictions below. Admitted filesystem Parquet inputs can enter as native
Arrow batches; other reader and sink contracts retain the host boundary.
Flink retains SQL optimization, catalogs, source/sink providers, distributed
scheduling and batch recovery. This does not accelerate arbitrary DataStream code.

## Admission

Every interior operator must be an admitted Calc/filter, `UNION ALL`, sort, aggregate,
Expand, bounded limit, analytic window/rank, verified bounded join or blocking
singleton/hash/broadcast exchange. All fields
must use scalar types supported by the existing Arrow boundary, and every expression
must pass the [Calc encoder and declared-result-type checks](calc-filter.md).
ARRAY, MAP, MULTISET, ROW, structured/RAW and unverified collection representations
retain whole-query fallback. Stock sources, constant relations and row sinks may
remain at the perimeter.

A row source with character fields requires a producer that guarantees valid UTF-8. Sources without
that guarantee retain stock batch computation. Connectors with native Arrow output declare their
reader's string contract explicitly before native batch computation can use it.

Row-source expressions requiring original Java UTF-16 string-cache sidecars,
binary-backed STRING results, and legacy variable-width BINARY projections remain
outside batch admission. Positively admitted native UTF-8 sources can use the
generated scalar bridge described below. Other streaming producer/consumer rules
are not inferred for new batch boundaries.

Batch compute admission uses compatibility and the all-or-nothing columnar rule.
Every supported pipeline is eligible, including standalone scalar/filter jobs,
global reductions and sorted limits. There is no performance whitelist based on
operator names, expression names, source representation or work above/below an
aggregate. Each UNION ALL branch must meet the same compatibility checks.
Optional connector substitution remains independent when computation retains stock Flink.
See [Paimon bounded execution](../connectors/paimon.md#bounded-batch-source-and-sink)
and [Iceberg bounded execution](../connectors/iceberg.md#bounded-batch-source-and-sink).

Native execution does not guarantee a speedup. Historical complete-job comparisons
include slower scalar, global-reduction and sorted-limit workloads; these remain
performance evidence rather than planner admission rules. See the
[scalar](../benchmarks/batch-scalars.md), [aggregate](../benchmarks/batch-aggregates.md)
and [sort](../benchmarks/batch-sort-groundwork.md) comparisons.

Sort keys may be TINYINT, SMALLINT, INTEGER, BIGINT, STRING/CHAR, BOOLEAN,
DECIMAL (precision 1–38) or DATE, with any verified combination
of ascending/descending direction and NULLS FIRST/LAST. Scalar payload columns use
the existing Arrow boundary types. Admission probes the actual native Calc output
schema below a sort: view/dictionary or other representations that cannot be
copied into independently owned buffers keep the whole query on stock Flink.
The same keys support finite sorted LIMIT and OFFSET/FETCH through Flink's existing local
and global sort-limit stages. Each local stage retains `offset + fetch` candidates;
the final stage applies the original offset after its singleton Arrow exchange.
Other key types and range sorting remain outside admission.
A new native key/schema capability probe prevents older libraries from admitting
key types they cannot execute. String ordering follows the UTF-8 binary ordering
of Flink's materialized batch-sort rows; supplementary-plane characters and NULLs
are covered by stock/native SQL comparisons.

Admitted aggregates support signed integer and decimal SUM/AVG,
MIN/MAX over signed integers, BOOLEAN, DECIMAL (precision 1–38), DATE and TIME,
scalar COUNT/COUNT(*) and boolean FILTER inputs. The optimizer's integer and decimal SUM0 rewrites are supported too. ONE_PHASE and TWO_PHASE HashAgg/SortAgg plans use
DataFusion aggregates, with projections preserving Flink's declared widths and
accumulator columns. Narrow integer SUM wraps at its declared width; integer AVG
uses BIGINT sum/count states and truncates its final division. Empty local input
emits no partial row, while empty final input emits COUNT/SUM0 zero and NULL for
the other functions. A nonempty input whose FILTER rejects every row still emits
a local neutral state. MIN/MAX preserve the complete logical buffer type, including
decimal precision/scale and TIME precision. Foreign Arrow TIME units are normalized
to Flink's millisecond integer representation before comparison, including its
wrapping narrowing; TIME(0) does not discard milliseconds present in runtime input.

Decimal aggregates wrap the existing Flink-compatible decimal accumulators in
DataFusion UDAFs. SUM resets after overflow; AVG and SUM0 keep overflow sticky.
Decimal AVG exposes the exact host `(sum DECIMAL(38,s), count BIGINT)` local
buffer and uses Flink's HALF_UP final division. SUM0 starts at numeric zero,
ignores NULL input values and propagates NULL partial states on merge. These
contracts also cover Flink's shared SUM/AVG optimizer rewrite.

Standalone global reductions and compositions on either side of them use the
same compatibility admission. No qualifying SHA2 call, sort or user function is
required. The [aggregate comparisons](../benchmarks/batch-aggregates.md) retain
measured standalone costs separately from pipeline gains.

HashAgg GROUP BY admits one or more signed integer, STRING/CHAR, BOOLEAN,
DECIMAL (precision 1–38) or DATE keys,
including NULL keys, with the same verified functions and exact local/final state
layouts. Key columns precede accumulator columns in Arrow partial states. Grouped
empty input emits no rows. Grouped SortAgg uses the same DataFusion aggregate and
then the existing Arrow sorter to restore ascending, NULLS FIRST group ordering,
following Comet's SortAggregate adaptation. Flink's input sorting remains in place.
Auxiliary grouping and intermediate merge-only stages retain stock. DataFusion's existing grouped hash aggregate owns
state, sorted spills and replay through the existing memory pool; no streaming
retraction state is reused.

Key-only HashAgg stages implement DISTINCT over the same verified keys. Flink's
single-argument COUNT/SUM/AVG DISTINCT rewrites may compose with these deduplication
stages when every resulting physical operator is admitted. Aggregate calls that
still carry a DISTINCT flag and unsupported key types retain whole-query stock fallback. Complete standalone GROUP BY and DISTINCT comparisons
are recorded separately from global-reduction and SHA2-pipeline measurements.

Grouping sets and rollups use a columnar Expand over Flink's input-reference and typed-literal
projections. Each projection runs through the existing native Calc evaluator against the same
borrowed batch. The returned Arrow batches own their buffers before the input is released.
Unordered bounded LIMIT/OFFSET stages forward selected Arrow ranges and continue consuming
input after the limit, preserving upstream evaluation and failures.

Batch analytic windows use DataFusion's sorted partition/window plans, following Arroyo and
Comet. Each physical node contains one window group; Flink can split distinct groups into separate nodes. The verified frames begin at UNBOUNDED PRECEDING and end at CURRENT ROW or
UNBOUNDED FOLLOWING, with ROWS or RANGE semantics. COUNT, decimal SUM/SUM0, signed-integer/BOOLEAN/DECIMAL/DATE
MIN/MAX, RANK, DENSE_RANK and ROW_NUMBER use the same signed-integer, STRING,
BOOLEAN, DECIMAL and DATE partition/order keys as the bounded sorter. Decimal accumulation
reuses Flink-compatible arithmetic. Bounded rank ranges filter the computed rank and preserve
peer rows, including plans that omit the rank column from their output.
When an OVER partition key also appears in ORDER BY, its direction and NULL placement
follow Flink's combined collation. Physical rank retains Flink's ascending, NULLS FIRST
partition prefix.

Inner joins with a residual requiring generated scalar evaluation retain their native equality
match and evaluate the residual in an Arrow Calc afterward. Outer/semi/anti residuals keep their
existing admission rules. Fallible AND/OR expressions use Flink's generated whole-Calc evaluator
to retain row short-circuiting. Native UTF-8 source pipelines can use that evaluator for builtins
requiring Java string evaluation; row-source pipelines retain the original string-cache guard.
These callbacks are reported in the plan's JVM function list.

`IS DISTINCT FROM` and `IS NOT DISTINCT FROM` between scalar input references lower
to existing NULL tests and equality. Signed integers, BOOLEAN, DECIMAL, DATE and
VARCHAR are verified; decimal types must match exactly. Floating, nested and CHAR
operands retain their existing admission rules. The lowering does not duplicate
evaluation of computed or effectful expressions.

Shared batches count their deployed chained and network outputs after Flink's reuse processors.
Several logical consumers can share a single serialized network output, and reuse processors
can merge consumers. Counting those consumers would retain unused Arrow views until GC. The deployed count releases each batch when
its actual consumers finish, without increasing the native memory budget.

The existing master and per-operator switches apply. Disabling `calc`, `filter`
on a Calc with a predicate, `union`, `sort`, `groupAggregate`, `expand`, `limit`,
`overAggregate`, or `rank` declines the affected query as a whole.
`NativePlanner.explain(tableEnvironment, sql)` includes batch admission diagnostics.
The deployed planner applies substitution after all batch sink roots are expanded;
`NativePlanner.installBatch(tableEnvironment.getConfig())` supplies the per-root
hook for an already constructed stock planner.

Bounded pipelines prefer 8,192-row Arrow batches, following Comet's batch size.
An explicit `streamfusion.transpose.batchRows` table/JVM setting overrides this
preference; streaming retains its existing defaults. The Parquet Q3 comparison
with 1,024 rows lost to stock, while the 8,192-row comparison established a gain.
The previous trials remain in the [TPC-H evidence](../benchmarks/tpch.md).

## Columnar execution and completion

Batch physical/exec wrappers reuse the existing compiled expression and transpose
runtime. They do not expose streaming execution nodes to Flink's batch graph
processors. An eligible columnar subplan that Flink explicitly reuses preserves its
digest through host cloning. This requires native source sharing and both host reuse
options. The Arrow share operator gives each deployed consumer a retained buffer view.
Virtual exchange nodes stay visible to the operators that verify their distributions.
A shared hash exchange preserves one producer even when Flink's input priorities
require separate blocking hash partitions for its consumers.

Only the perimeter transposes convert rows. UNION ALL forwards Arrow batches, and
unchained native stages use the existing Arrow IPC serializer through Flink's
transport. No process-local handles are enabled by the batch wrappers. A bounded
entry transpose flushes its final short batch at `endInput`; it does not register
streaming latency timers. Producer buffers are released after IPC serialization,
and the consumer owns its reconstructed buffers.

Global sort and final global aggregation use Flink's blocking SINGLETON exchange
at parallelism one. Both the
sort and its downstream transposes preserve the singleton distribution contract;
otherwise a later edge can lose global ordering. Flink persists ordinary Arrow IPC
frames and replays them when a consumer retries. Verified-key HASH exchanges split Arrow batches with the existing projected-key
splitter and use Flink's blocking transport. Their routing count comes from the
deployed batch writer, independently of streaming's recoverable key-group state
namespace. Native final groups explicitly establish their own compatible hash
route, including when stock source traits elided an exchange. This avoids assuming
that streaming BinaryRow hashing equals stock batch's generated field hash.
Range exchanges and other unsupported interior nodes retain stock. Native
batch exchanges explicitly request BATCH; hybrid scheduling remains unverified.
Connector acceleration is separate. The optional native filesystem Parquet source
now replaces admitted fixed-parallelism scan/transpose boundaries with Arrow input;
its [source whitelist](../connectors/parquet.md#source) covers the initial types,
projections and options. Other bounded reader contracts and all batch sink providers
retain Flink's code, including final commits, overwrite policies and constraints.
An available codec alone does not admit another connector or source configuration.

## Bounded join candidate

The deployed planner admits hash and sort-merge equijoins over matching signed integer, STRING,
BOOLEAN, DATE and DECIMAL keys, including duplicates and homogeneous NULL-safe
keys. DataFusion owns the build table and probe stream; Flink's chosen build side
is retained, including restoring the original output column order after swapping
DataFusion's build side. INNER/LEFT/RIGHT/FULL and SEMI/ANTI use the same Arrow
runtime. Broadcast is limited to a nonpreserved build side. Residual predicates reuse the existing expression compiler and DataFusion's
JoinFilter. Keyless/nested-loop joins use DataFusion's NestedLoopJoinExec. Predicate
representations requiring original Java string caches, collections or JVM callbacks
remain outside this initial join whitelist. Mixed per-key NULL equality on keyed
joins retains stock. Bounded joins follow whole-query compatibility admission; explicit per-operator switches
still apply.

A bounded two-input task exposes DataFusion's input demand to Flink's input
selection and keeps each input's EOF separate. An early native result still drains
remaining host input. Hash and broadcast transport use blocking Arrow IPC.
Flink's deadlock resolver remains active; any forwarding boundary it inserts after
an Arrow output uses an Arrow serializer. The host's optional multiple-input fusion
hardcodes RowData, so graphs containing Arrow joins bypass that fusion. Other host
graph processors and normal Flink scheduling/recovery remain active. Installing only
the optimizer hook into an already constructed stock planner declines joins because
it cannot install this graph adaptation.

Hash build state uses the existing shared native memory pool and fails on reservation
denial; this initial DataFusion hash table does **not** spill. DataFusion nested-loop
execution has a chunked disk-spill fallback when temporary files are available. Sort-merge input sorting
uses the existing DataFusion external sorter. A composite NULL-safe hash semi-join
with no residual predicate groups the non-preserved build keys before building its
hash table. No separate pool
or higher memory limit is introduced. The complete SF1 TPC-H release comparison
now passes all 22 queries with exact rows/types, native source and compute work,
and zero entry-transpose rows. All 22 query medians favor native; the geometric
mean speedup is 2.8137×. This is a local shared-host, warm-cache cohort, not an
arbitrary batch SQL or larger-data performance guarantee. See the
[full comparison and limitations](../benchmarks/tpch.md#review-head-production-profile-comparison).

## Remaining gaps

- Other grouping key types and auxiliary grouping,
  remaining DISTINCT aggregate flags, floating SUM/AVG, FLOAT/DOUBLE, STRING/BINARY
  and TIMESTAMP extrema, aggregate user functions, zero-function global/intermediate
  merge-only stages and unverified accumulator layouts. Timestamp input remains
  a two-field Arrow struct to preserve the full Flink range and fractions; scalar
  timestamp aggregation is not inferred from DataFusion's timestamp kernels.
- ORDER BY with other key types, sorted OFFSET without finite FETCH, range sorting,
  analytic functions/frames outside the whitelist above, and other batch sort operators.
- Join predicates requiring unverified JVM/string/collection representations, mixed
  keyed NULL equality, preserved-side broadcasts and external hash-table spilling.
- Expand projections beyond references/literals, UNNEST/correlate, nested boundary types and set-operation plans outside UNION ALL.
- Native bounded connector admission beyond the documented filesystem Parquet, Paimon
  and Iceberg whitelists.
- Native compiled batch plans. The deployed batch planner preserves stock compilation;
  Flink 1.18 keeps its own unsupported compiled-plan behavior.
- Python, pattern matching, async/model functions and other unrecognized interior nodes.

These require further plan/runtime or semantic work; DataFusion having a physical
operator is not sufficient to admit its Flink SQL counterpart.

The [bounded execution adapter](../batch-execution-runtime.md) supplies persistent
DataFusion execution, EOF, spill, streamed output and cancellation for admitted
sorts and admitted global aggregates.

## Validation

Removing the performance whitelist passes 18 focused local SQL tests and 21
unchanged released upstream methods on each supported Flink line, with no skips.
The upstream methods record 20 positive native-work witnesses and one explicit
unsupported floating-aggregate fallback per line. The collect-sink configuration
fixture preserves its expected failure and then records native work on success.
Local controls cover standalone
scalar work, a UNION branch without SHA2, global reductions across both phases,
typed extrema, standalone sorted limits and completed-shuffle recovery. Unsupported
interiors and disabled operators still retain the whole stock compute query.

The unrestricted scalar candidate passed the unchanged published batch SQL Calc
class on both host lines: 93 executed/one upstream-disabled on 2.2.1, and
91 executed/one upstream-disabled on 1.18.1. `testCurrentDate` was excluded under
the repository's existing host-baseline restriction, rather than counted as native
coverage. Candidate native work was observed in 65 and 64 upstream invocations,
respectively; these historical counts predate both the former performance
whitelist and its removal.

The upstream harness requires positive native work for
`testManySelectWithFilter`, `testFilterOnInteger` and `testUserDefinedScalarFunction`,
plus the contracted standalone DATE comparisons. Local SQL controls compare stock/native
results through empty output, short final batches, composed expressions, UNION ALL
and unchained Arrow IPC stages. The unchained control also checks the native
operator's task name so chaining cannot silently bypass serialization.

[Historical runtime witnesses](../benchmarks/results/batch-sql-validation-20261109.json)
retain successful BATCH job statuses, native work and exact fallback reasons for
the three contracted upstream methods on each host line. Additional controls cover
parallelism two and preservation of the host's compiled-plan capability.

Unchanged released `MiscITCase.testNullOrdering` and `testGlobalSorting` require
positive native sort work. `testNullOrderingOnMultiColumn` also requires positive
native sort work. Local controls additionally check ordered results over
multiple Arrow batches, mixed directions/NULL placement, empty input, unchained
parallelism-two producers, forced spill metrics, and consumer failure after a
completed blocking shuffle. The recovery control observes sort/exit attempts 0 and
1 while all row-to-Arrow producers remain on attempt 0.

The [sort measurements](../benchmarks/batch-sort-groundwork.md) retain separate
operation and conversion-inclusive complete-job results, with raw trials and
[validation witnesses](../benchmarks/results/batch-sort-sql-20261109.json).

All six contracted unchanged released `SortLimitITCase` methods require positive
native sort work on both lines. Local standalone sorted-limit controls compare
stock/native results through offsets crossing output batches, NULLs, duplicate
keys, empty input and unchained Arrow IPC. Disabled sort still keeps the whole
compute query on stock. The [historical sorted-limit witnesses](../benchmarks/results/batch-sorted-limit-validation-20261109.json)
retain observations from before removal of the performance whitelist.

Global aggregate controls compare stock results across both host lines, HashAgg/
SortAgg physical state contracts, local/final stages, NULLs, overflow and FILTER.
A final COUNT consumer fails after the blocking partial-state shuffle completes;
its retry returns the original result while the partial producers remain on
attempt zero. The isolated packaged loader covers populated and empty aggregate
results. Contracted unchanged released HashAggITCase/SortAggITCase standalone
methods now require positive native aggregate work. Local SQL controls exercise
standalone global aggregation across both phases and HashAgg/SortAgg plans.

[Aggregate validation witnesses](../benchmarks/results/batch-aggregate-validation-20261109.json)
retain the historical upstream routes, admitted SQL work and earlier candidate
observations separately; their performance-fallback routes predate the current
policy. That historical consolidated local selection passed 51 Java/upstream
cases per host line, plus 46 bounded native controls. Aggregate operator controls
include scalar COUNT validity, FILTER payloads, schema/input cleanup, cancellation,
output surviving producer close and expression-workspace denial under 2 MiB.

The fixed-width extrema extension adds stock/native checks on both lines for
Boolean, DECIMAL(10,2)/DECIMAL(38,18), DATE and TIME across both phases and forced
HashAgg/SortAgg plans. Tests include empty/all-NULL/all-filtered input, canonical
and foreign Arrow time units, TIME(0) runtime milliseconds, and typed partial-state
replay with every output field compared. The packaged planner checks populated
and empty Boolean/decimal states. Unchanged released Table API
`AggregationITCase.testAggregationTypes` and `testWorkingAggregationDataTypes`
require positive native aggregate work and the exact unsupported-result fallback,
respectively. Unsupported floating aggregation remains on stock Flink.
[Extrema witnesses](../benchmarks/results/batch-extrema-validation-20261109.json)
record 38 focused Java/upstream cases per host line and 48 bounded Rust controls.

TPC-H development additionally adapts the existing Flink-compatible decimal SUM
accumulator into DataFusion aggregation, preserving DECIMAL(38,s) partial states,
NULL defaults and overflow/reset behavior. Q6 has exact stock parity and positive
native work at scale factors 0.01 and 1. Its initial complete CSV pipeline is
performance-neutral; this is ongoing work, not a completed acceleration claim.
See the [TPC-H audit and measurements](../benchmarks/tpch.md).

Scalar-subquery SINGLE_VALUE uses a Flink-compatible accumulator that counts NULL rows
as well as non-NULL rows. Empty input returns NULL; a second row raises Flink's multiple-row
error even if both values are equal or NULL. Local and final stages retain the Java value/count
state layout rather than placing DataFusion's private state on the shuffle.

A narrowly admitted inner Cartesian join can be reassociated into two linked
equijoins following DataFusion’s cross-join elimination. The planner restores the
original schema and distribution and retains stock behavior for hints, residuals,
null-safe keys or incompatible ordering/distributions. This avoids Q9’s excessive
intermediate within the existing memory cap. See the [TPC-H evidence](../benchmarks/tpch.md).

Fresh TPC-H Parquet verification after these planner changes passes all 22 queries
at scale factor 0.01 on both supported Flink lines, requiring exact declared types,
rows and native work. Packaged-loader controls cover grouped Arrow exchanges and
two-input joins on both lines. This establishes compute compatibility, while the
[TPC-H performance table](../benchmarks/tpch.md) retains remaining regressions.

Admitted bounded filesystem Parquet inputs can now enter the island as Arrow
batches from the optional [native source](../connectors/parquet.md#source).
The source is chosen after the host reuse graph is complete; fully columnar
consumers share one read through the existing retained-view share operator.
Unsupported source contracts retain Flink's reader and the ordinary entry
transpose. The source does not broaden whole-query compute admission.

The production-profile source validation uses the same release/mimalloc libraries
on both host lines: all 22 TPC-H queries at SF0.01 match stock rows/types, emit
native source rows and have zero entry-transpose rows. Each line also passes five
source ownership/restore/memory controls and two SQL fallback/retry controls.
The entry-counter sensitivity control observes all 2,051 rows when the stock reader
is explicitly selected. The separate native release suite passes 73 bounded tests
with its default RocksDB feature configuration; packaged-loader controls pass five
payload identity checks and two grouped/join checks per line. The
[production-profile validation reports](../benchmarks/tpch.md#review-head-production-profile-comparison)
retain each check's scope rather than assigning the native unit suite the benchmark's
allocator configuration.
