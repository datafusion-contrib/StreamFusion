# Batch SQL

**Status: partial.** Flink 2.2.1 and 1.18.1 batch Table environments can use native
Calc/filter pipelines, `UNION ALL`, scalar `ORDER BY`, global and typed-key
reductions, DISTINCT, fixed-width MIN/MAX and verified bounded joins, with the
admission restrictions below. Admitted filesystem Parquet inputs can enter as native
Arrow batches; other reader and sink contracts retain the host boundary.
Flink retains SQL optimization, catalogs, source/sink providers, distributed
scheduling and batch recovery. This does not accelerate arbitrary DataStream code.

## Admission

Every interior operator must be an admitted Calc/filter, `UNION ALL`, sort, global
aggregate, verified bounded join or blocking
singleton/hash/broadcast exchange. All fields
must use scalar types supported by the existing Arrow boundary, and every expression
must pass the [Calc encoder and declared-result-type checks](calc-filter.md).
ARRAY, MAP, MULTISET, ROW, structured/RAW and unverified collection representations
retain whole-query fallback. Stock sources, constant relations and row sinks may
remain at the perimeter.

Expressions requiring original Java UTF-16 string-cache sidecars, binary-backed
STRING results, and legacy variable-width BINARY projections remain outside batch
admission. Their streaming producer/consumer rules are not inferred for new batch
boundaries.

Each source-to-sink path must also contain an admitted unrestricted sort,
a verified bounded join, keyed reduction/DISTINCT, decimal SUM, a natively encoded
`SHA2` call or a verified user-defined scalar function. Sort and SHA-256 complete-job
comparisons demonstrate a gain with both row/Arrow conversions included. User functions retain the
repository's accepted performance exception and their existing type/lifecycle rules.
Arithmetic, casts, predicates and other admitted scalar expressions may compose
with those operations inside the same columnar island. Simple DATE comparisons
between direct columns/literals use existing DataFusion Date32 comparisons,
avoiding generated Java predicate bridges. Computed temporal operands retain
their existing evaluation rules. Every UNION ALL branch must
meet this rule, unless a qualifying Calc or sort sits above the union. An admitted native
Paimon batch source also qualifies its branch: its input is already Arrow and pays no
row-to-Arrow source conversion. Connector substitution remains independent when computation
retains stock Flink; see [Paimon bounded execution](../connectors/paimon.md#bounded-batch-source-and-sink).

Sort keys may be TINYINT, SMALLINT, INTEGER, BIGINT, STRING/CHAR, BOOLEAN,
DECIMAL (precision 1–38) or DATE, with any verified combination
of ascending/descending direction and NULLS FIRST/LAST. Scalar payload columns use
the existing Arrow boundary types. Admission probes the actual native Calc output
schema below a sort: view/dictionary or other representations that cannot be
copied into independently owned buffers keep the whole query on stock Flink.
The same keys support finite sorted LIMIT and OFFSET/FETCH through Flink's existing local
and global sort-limit stages. Each local stage retains `offset + fetch` candidates;
the final stage applies the original offset after its singleton Arrow exchange.
A sorted limit alone does not qualify a source-to-sink path: the measured cheap
standalone jobs did not beat stock after their row/Arrow boundaries. They keep
stock with `batch sort limit: standalone pipeline has no verified speedup`.
Sorted limits can compose with another qualifying operation, such as SHA2 or an
accepted user function. Other key types and range sorting remain outside admission.
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

A global signed-integer aggregate alone does not qualify an island: representative complete jobs did
not establish a reliable standalone gain after profiling, removing an unnecessary
input copy and testing larger batches. Such queries retain stock with
`batch aggregate: standalone pipeline has no verified speedup`. Aggregates can
compose with qualifying SHA2/full sorts evaluated below the reduction, or accepted
scalar user functions. SHA2 or a one-row sort above an aggregate does not amortize
its input boundary; scalar user functions retain their accepted performance exception
on either side.
The measured SHA2 compositions outperform the previous whole-query stock fallback;
this is a pipeline gain, not an isolated aggregate-kernel claim. See the
[aggregate comparisons](../benchmarks/batch-aggregates.md).

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
still carry a DISTINCT flag, grouping sets/Expand and unsupported key types retain
whole-query stock fallback. Complete standalone GROUP BY and DISTINCT comparisons
are recorded separately from global-reduction and SHA2-pipeline measurements.

Cheap standalone scalar pipelines with rowwise sources retain stock Flink with the reason:

```text
batch Calc: standalone scalar pipeline has no verified speedup
```

The initial unrestricted candidate passed broad upstream Calc parity checks, but
complete arithmetic and regex jobs lost to stock Flink. Larger Arrow batches did
not establish a general speedup. There is no performance opt-in for these paths;
their native implementations remain usable in admitted pipelines. See the
[batch scalar measurements](../benchmarks/batch-scalars.md).

The existing master and per-operator switches apply. Disabling `calc`, `filter`
on a Calc with a predicate, `union`, `sort`, or `groupAggregate` declines the affected query as a whole.
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
processors. Native digests retain the single-consumer Arrow reuse barrier.
An explicitly shared unary aggregate subplan can preserve its digest through host
cloning and stamp the verified consumer count using the existing Arrow share
operator. This requires native source sharing and both host reuse options; other
subplans keep their barriers. Each branch receives its own retained buffer view.

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
joins retains stock. Bounded joins qualify an otherwise verified pipeline; explicit per-operator switches
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
uses the existing DataFusion external sorter. No separate pool
or higher memory limit is introduced. The complete SF1 TPC-H release comparison
now passes all 22 queries with exact rows/types, native source and compute work,
and zero entry-transpose rows. All 22 query medians favor native; the geometric
mean speedup is 2.8137×. This is a local shared-host, warm-cache cohort, not an
arbitrary batch SQL or larger-data performance guarantee. See the
[full comparison and limitations](../benchmarks/tpch.md#review-head-production-profile-comparison).

## Remaining gaps

- Other grouping key types, auxiliary grouping, grouping sets/Expand,
  remaining DISTINCT aggregate flags, floating SUM/AVG, FLOAT/DOUBLE, STRING/BINARY
  and TIMESTAMP extrema, aggregate user functions, zero-function global/intermediate
  merge-only stages and unverified accumulator layouts. Timestamp input remains
  a two-field Arrow struct to preserve the full Flink range and fractions; scalar
  timestamp aggregation is not inferred from DataFusion's timestamp kernels.
  Verified global states are supported only inside otherwise qualifying pipelines.
- ORDER BY with other key types, standalone sorted limits without a
  qualifying operation, OFFSET without finite FETCH, unsorted LIMIT/OFFSET, range sorting,
  OVER/rank and other batch sort operators.
- Join predicates requiring unverified JVM/string/collection representations, mixed
  keyed NULL equality, preserved-side broadcasts and external hash-table spilling.
- Expand/UNNEST/correlate, nested boundary types and set-operation plans outside UNION ALL.
- Native bounded connector admission beyond the documented filesystem Parquet and
  Paimon whitelists, including batch Iceberg substitution and other native batch sinks.
- Native compiled batch plans. The deployed batch planner preserves stock compilation;
  Flink 1.18 keeps its own unsupported compiled-plan behavior.
- Python, pattern matching, async/model functions and other unrecognized interior nodes.

These require further plan/runtime or semantic work; DataFusion having a physical
operator is not sufficient to admit its Flink SQL counterpart.

The [bounded execution adapter](../batch-execution-runtime.md) supplies persistent
DataFusion execution, EOF, spill, streamed output and cancellation for admitted
sorts and admitted global aggregates.

## Validation

The unrestricted scalar candidate passed the unchanged published batch SQL Calc
class on both host lines: 93 executed/one upstream-disabled on 2.2.1, and
91 executed/one upstream-disabled on 1.18.1. `testCurrentDate` was excluded under
the repository's existing host-baseline restriction, rather than counted as native
coverage. Candidate native work was observed in 65 and 64 upstream invocations,
respectively; these counts predate the narrower performance admission above.

The upstream harness now checks explicit fallback for the cheap
`testManySelectWithFilter` and `testFilterOnInteger` fixtures, and positive native
work for `testUserDefinedScalarFunction`. Local SQL controls compare stock/native
results through empty output, short final batches, composed expressions, UNION ALL
and unchained Arrow IPC stages. The unchained control also checks the native
operator's task name so chaining cannot silently bypass serialization.

[Checkpoint runtime witnesses](../benchmarks/results/batch-sql-validation-20261109.json)
retain successful BATCH job statuses, native work and exact fallback reasons for
the three contracted upstream methods on each host line. Additional controls cover
parallelism two and preservation of the host's compiled-plan capability.

Unchanged released `MiscITCase.testNullOrdering` and `testGlobalSorting` require
positive native sort work. `testNullOrderingOnMultiColumn` requires the exact
unsupported-key fallback. Local controls additionally check ordered results over
multiple Arrow batches, mixed directions/NULL placement, empty input, unchained
parallelism-two producers, forced spill metrics, and consumer failure after a
completed blocking shuffle. The recovery control observes sort/exit attempts 0 and
1 while all row-to-Arrow producers remain on attempt 0.

The [sort measurements](../benchmarks/batch-sort-groundwork.md) retain separate
operation and conversion-inclusive complete-job results, with raw trials and
[validation witnesses](../benchmarks/results/batch-sort-sql-20261109.json).

Sorted-limit candidate runs passed all six unchanged released upstream methods on
both lines before the performance gate. Current contracts require stock fallback
for the four cheap signed-key fixtures and key-type fallback for the two STRING
fixtures. The admitted SHA2 compositions have separate positive native SQL
controls, including offsets crossing output batches and a packaged-loader test.
[Sorted-limit witnesses](../benchmarks/results/batch-sorted-limit-validation-20261109.json)
distinguish those candidate native observations from the final admission results.

Global aggregate controls compare stock results across both host lines, HashAgg/
SortAgg physical state contracts, local/final stages, NULLs, overflow and FILTER.
A final COUNT consumer fails after the blocking partial-state shuffle completes;
its retry returns the original result while the partial producers remain on
attempt zero. The isolated packaged loader covers populated and empty aggregate
results. Unchanged released HashAggITCase/SortAggITCase methods retain exact
standalone fallback contracts; passing those fixtures does not count as current
native aggregate coverage. Admitted local SQL and release jobs assert positive
aggregate input separately.

[Aggregate validation witnesses](../benchmarks/results/batch-aggregate-validation-20261109.json)
retain the final exact upstream routes, admitted SQL work and earlier candidate
observations separately. The consolidated local selection passes 51 Java/upstream
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
retain exact standalone-cost and unsupported-result fallback contracts respectively;
they are host parity controls, not positive native extrema coverage.
[Extrema witnesses](../benchmarks/results/batch-extrema-validation-20261109.json)
record 38 focused Java/upstream cases per host line and 48 bounded Rust controls.

TPC-H development additionally adapts the existing Flink-compatible decimal SUM
accumulator into DataFusion aggregation, preserving DECIMAL(38,s) partial states,
NULL defaults and overflow/reset behavior. Q6 has exact stock parity and positive
native work at scale factors 0.01 and 1. Its initial complete CSV pipeline is
performance-neutral; this is ongoing work, not a completed acceleration claim.
See the [TPC-H audit and measurements](../benchmarks/tpch.md).

Scalar-subquery SINGLE_VALUE guards are native only for a planner-proven
at-most-one-row input: Calc/singleton exchange over a final global aggregate. The
adapter uses DataFusion first-value semantics in that narrow case. Other
SINGLE_VALUE inputs retain stock Flink and its multiple-row error.

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
