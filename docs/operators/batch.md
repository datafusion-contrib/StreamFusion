# Batch SQL

**Status: partial.** Flink 2.2.1 and 1.18.1 batch Table environments can use native
Calc/filter pipelines, `UNION ALL`, signed-integer `ORDER BY`, global integer
reductions and fixed-width MIN/MAX, with the admission restrictions below.
Flink retains SQL optimization, catalogs, source/sink providers, distributed
scheduling and batch recovery. This does not accelerate arbitrary DataStream code.

## Admission

Every interior operator must be an admitted Calc/filter, `UNION ALL`, sort, global
aggregate or blocking
singleton exchange. All fields
must use scalar types supported by the existing Arrow boundary, and every expression
must pass the [Calc encoder and declared-result-type checks](calc-filter.md).
ARRAY, MAP, MULTISET, ROW, structured/RAW and unverified collection representations
retain whole-query fallback. Stock sources, constant relations and row sinks may
remain at the perimeter.

Expressions requiring original Java UTF-16 string-cache sidecars, binary-backed
STRING results, and legacy variable-width BINARY projections remain outside batch
admission. Their streaming producer/consumer rules are not inferred for new batch
boundaries.

Each source-to-sink path must also contain an admitted unrestricted sort, a natively encoded `SHA2`
call or a verified user-defined scalar function. Sort and SHA-256 complete-job comparisons demonstrate
a gain with both row/Arrow conversions included. User functions retain the
repository's accepted performance exception and their existing type/lifecycle rules.
Arithmetic, casts, predicates and other admitted scalar expressions may compose
with those operations inside the same columnar island. Every UNION ALL branch must
meet this rule, unless a qualifying Calc or sort sits above the union.

Sort keys may be TINYINT, SMALLINT, INTEGER or BIGINT, with any verified combination
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
For example, a STRING key reports `batch sort: only signed integer keys are verified`.

Global aggregates without grouping keys support signed integer SUM/AVG,
MIN/MAX over signed integers, BOOLEAN, DECIMAL (precision 1–38), DATE and TIME,
scalar COUNT/COUNT(*) and boolean FILTER inputs. The optimizer's integer
SUM0 rewrite is supported too. ONE_PHASE and TWO_PHASE HashAgg/SortAgg plans use
DataFusion aggregates, with projections preserving Flink's declared widths and
accumulator columns. Narrow integer SUM wraps at its declared width; integer AVG
uses BIGINT sum/count states and truncates its final division. Empty local input
emits no partial row, while empty final input emits COUNT/SUM0 zero and NULL for
the other functions. A nonempty input whose FILTER rejects every row still emits
a local neutral state. MIN/MAX preserve the complete logical buffer type, including
decimal precision/scale and TIME precision. Foreign Arrow TIME units are normalized
to Flink's millisecond integer representation before comparison, including its
wrapping narrowing; TIME(0) does not discard milliseconds present in runtime input.

An aggregate alone does not qualify an island: representative complete jobs did
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

Cheap standalone scalar pipelines retain stock Flink with the reason:

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

## Columnar execution and completion

Batch physical/exec wrappers reuse the existing compiled expression and transpose
runtime. They do not expose streaming execution nodes to Flink's batch graph
processors. Native digests retain the single-consumer Arrow reuse barrier.

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
frames and replays them when a consumer retries. Hash/range/broadcast exchanges and
other unsupported interior nodes retain stock execution.
Connector acceleration is separate: this milestone retains stock bounded readers
and stock sink providers, including their final commits, overwrite policies and
constraints. Native Parquet/ORC codecs do not imply a native filesystem batch scan.

## Remaining gaps

- Grouped/DISTINCT aggregation, noninteger SUM/AVG, FLOAT/DOUBLE, STRING/BINARY
  and TIMESTAMP extrema, aggregate user functions, zero-function/intermediate
  merge-only stages and unverified accumulator layouts. Timestamp input remains
  a two-field Arrow struct to preserve the full Flink range and fractions; scalar
  timestamp aggregation is not inferred from DataFusion's timestamp kernels.
  Verified global states are supported only inside otherwise qualifying pipelines.
- ORDER BY with keys other than signed integers, standalone sorted limits without a
  qualifying operation, OFFSET without finite FETCH, unsorted LIMIT/OFFSET, range sorting,
  OVER/rank and other batch sort operators.
- Batch joins, their input-consumption order, distribution and external-memory strategy.
- Expand/UNNEST/correlate, nested boundary types and set-operation plans outside UNION ALL.
- Native bounded connector admission and native batch sink substitution.
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
