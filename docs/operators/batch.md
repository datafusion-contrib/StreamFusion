# Batch SQL

**Status: partial.** Flink 2.2.1 and 1.18.1 batch Table environments can use native
Calc/filter pipelines, `UNION ALL` and signed-integer `ORDER BY`, with the admission
restrictions below.
Flink retains SQL optimization, catalogs, source/sink providers, distributed
scheduling and batch recovery. This does not accelerate arbitrary DataStream code.

## Admission

Every interior operator must be an admitted Calc/filter, `UNION ALL`, sort or blocking
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

Each source-to-sink path must also contain an admitted sort, a natively encoded `SHA2`
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
Other key types, LIMIT/OFFSET and range sorting remain outside this admission.
For example, a STRING key reports `batch sort: only signed integer keys are verified`.

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
on a Calc with a predicate, `union`, or `sort` declines the affected query as a whole.
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

Global sort uses Flink's blocking SINGLETON exchange at parallelism one. Both the
sort and its downstream transposes preserve the singleton distribution contract;
otherwise a later edge can lose global ordering. Flink persists ordinary Arrow IPC
frames and replays them when a consumer retries. Hash/range/broadcast exchanges and
other unsupported interior nodes retain stock execution.
Connector acceleration is separate: this milestone retains stock bounded readers
and stock sink providers, including their final commits, overwrite policies and
constraints. Native Parquet/ORC codecs do not imply a native filesystem batch scan.

## Remaining gaps

- GROUP BY/DISTINCT, hash/sort aggregation and intermediate accumulator schemas.
- ORDER BY with keys other than signed integers, sorted LIMIT/OFFSET, range sorting,
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
sorts. Aggregation is currently an adapter control, not admitted batch SQL.

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
