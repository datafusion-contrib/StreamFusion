# Batch execution audit

This is the historical assessment before implementation. Current local coverage is
documented in [Batch SQL](operators/batch.md), with measurements in
[Batch scalar comparisons](benchmarks/batch-scalars.md) and
[batch sort comparisons](benchmarks/batch-sort-groundwork.md).

Audited 2026-11-09 at StreamFusion `4e3a20ce3`. This is an implementation assessment,
not a claim of new supported coverage. Scope: Flink SQL/Table batch execution on the
repository's Flink 2.2.1 and 1.18.1 lines, using its released DataFusion 54 dependency.
Arbitrary DataStream operators require a separate acceleration boundary.

Batch SQL is a promising extension. Much of the expression engine, Arrow transport,
JNI ownership and connector payload code can be reused. DataFusion can replace much
of the custom stateful machinery needed for streaming. However, enabling the existing
streaming rewrite in batch mode would not produce a working batch accelerator. The
missing pieces are a batch planner path, a persistent bounded execution adapter,
batch-compatible exchanges, verified memory/spill behavior, and batch-specific admission
and evidence.

## What runs today

There are three distinct cases:

| Case | Current behavior |
|---|---|
| Finite input executed through a streaming Table environment | Eligible for existing streaming acceleration, subject to ordinary admission. Uses streaming plans and semantics. |
| SQL/Table environment created in batch mode | Uses stock Flink. There is no native batch SQL planner. |
| Connector maintenance, such as Paimon compaction or Iceberg file rewrites | Has specialized native payload paths. These are useful components and experience, not general batch SQL coverage. |

The deployed [planner factory](https://github.com/datafusion-contrib/StreamFusion/blob/4e3a20ce3688d056e4d8c3006e668f90edf0e4cf/src/main/java/tech/streamfusion/planner/StreamFusionPlannerFactory.java)
selects `NativeStreamPlanner` only when `RUNTIME_MODE == STREAMING`. Both compatibility
implementations extend Flink's `StreamPlanner`.
`NativePlanner.install` modifies only `FlinkStreamProgram`;
`PhysicalPlanScan` implements `FlinkOptimizeProgram<StreamOptimizeContext>` and its
registry matches `StreamPhysical*` nodes. The native physical bases implement
`StreamPhysicalRel`, and execution nodes such as `NativeCalcExecNode` implement
`StreamExecNode`.

There is already explicit stock-batch evidence: the unchanged upstream
`batch.sql.CalcITCase#testSelectStar` completed with zero native work and a
`batch_host_only` route on both supported releases. This is documented in the
[upstream suite](upstream-flink-suite.md), not newly executed by this audit.

## Why bounded streaming is not sufficient

Bounded input makes batch algorithms possible, but SQL batch uses a separate physical
plan family. Flink's released [2.2.1 batch planner](https://github.com/apache/flink/blob/release-2.2.1/flink-table/flink-table-planner/src/main/scala/org/apache/flink/table/planner/delegation/BatchPlanner.scala)
uses `BatchCommonSubGraphBasedOptimizer` and requires `BatchExecNode` roots. It also
runs graph processors for deadlock handling, multiple inputs, dynamic filtering,
exchange handling and adaptive joins. The
[1.18.1 implementation](https://github.com/apache/flink/blob/release-1.18.1/flink-table/flink-table-planner/src/main/scala/org/apache/flink/table/planner/delegation/BatchPlanner.scala)
has a similar split, with a different processor set.

Batch execution normally uses persisted blocking exchanges and stage replay rather
than streaming checkpoint recovery. Multi-input consumption order and end-of-input
are execution contracts. Some batch DataStream operators also have key-scoped
watermark/timer behavior that streaming operators cannot assume away. See Flink's
[execution-mode documentation](https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/dev/datastream/execution_mode/)
and [batch shuffle documentation](https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/ops/batch/batch_shuffle/).

Running a finite job in streaming mode remains useful for existing workloads. It
does not establish support for batch plans, final-only aggregation, unrestricted
batch sorting, or batch scheduling and recovery.

## Reuse and operator opportunities

| Area | Reusable foundation | Work still required |
|---|---|---|
| Calc/filter | Rex expression encoding, Flink compatibility functions, compiled native expressions, JVM UDF binding | Batch physical/exec wrappers; check batch evaluation context and preserve existing expression admission. |
| Representation | Arrow batches, C Data ownership, shared allocators, row/Arrow transposes | Batch transition nodes and proof of flush, lifetime and cancellation behavior. The row-to-Arrow operator already implements `BoundedOneInput.endInput`. |
| UNION ALL / Expand / UNNEST | Existing columnar transformations and native kernels | Batch matchers and lifecycle checks; UNNEST retains producer/consumer and collection restrictions. |
| GROUP BY / DISTINCT | DataFusion aggregation and existing Flink aggregate semantics | Batch aggregate lowering; local/final state layouts; empty-input behavior; verified spill by aggregate mode/type. |
| Equi-join | DataFusion hash join; existing key, residual-filter and Arrow infrastructure | Persistent build/probe execution, input ordering, broadcast/hash distribution, outer/semi/anti semantics and memory admission. |
| ORDER BY / sorted LIMIT | DataFusion sort, top-k/limit and merge operators | Preserve collation, global versus partition order, null placement and OFFSET/FETCH. General batch sort expands SQL coverage beyond streaming's time-sort restrictions. |
| OVER / rank | DataFusion window execution | Partition/order/frame translation and Flink result parity. Finite input permits more shapes, but does not automatically admit them. |
| Set operations | DataFusion primitives and Flink's existing relational rewrites | Handle the actual optimized plan, including duplicate/null semantics and ALL variants. These may appear as join/aggregate plans. |
| Connector payloads | Native Parquet/ORC codecs, Java-owned filesystem IO, admitted lakehouse readers/writers | Separate bounded-source and batch-sink admission. Keep host planning, security and commit protocols. |

This supports the expectation of **fewer custom stateful algorithms**. Ordinary batch
aggregation need not emit retract/update pairs; a batch join need not maintain both
sides indefinitely or retract earlier unmatched output. Most batch plans do not need
streaming TTL, per-record changelog reconciliation or native streaming savepoint
interoperability. Existing streaming implementations should remain available for
their intended mode rather than becoming the default batch algorithms.

DataFusion availability is not Flink compatibility. Existing code in
`native/engine/src/flink_functions`, `flink_float.rs`, `flink_key.rs` and the timestamp
bridge demonstrates the remaining semantic boundary: decimals and overflow, floating
keys, null-safe comparisons, nested values, temporal representation, aggregate result
types, conditional evaluation and JVM functions. Batch has its own host algorithms,
so streaming parity is useful evidence but cannot certify every batch result.
Floating reductions also need an explicit comparison contract appropriate to Flink's
parallel batch execution; changing reduction order must not silently relax the
repository's result-parity requirements.

## Required architecture work

### 1. Add a batch planner path

Introduce a batch planner adapter for each supported Flink line, retaining Flink's
SQL validation, optimization, cost decisions, catalogs and connector contracts.
Perform native substitution over the expanded batch roots, following the existing
complete-plan approach. Batch native physical nodes and execution nodes must satisfy
the batch interfaces; changing the factory's mode condition alone is insufficient.

Share expression encoding, type admission, diagnostics and representation interfaces
where practical. Keep mode-specific matching and translation explicit. The current
Calc matcher already takes Calcite's general `Calc`, but its substitution constructs
streaming nodes and contains streaming-specific fusion rules, so it is not directly
reusable as a batch rule.

Preserve the [whole-query columnar island policy](operators/index.md): unsupported
interior computation returns the query to stock Flink; stock sources and sinks may
remain at the perimeter. A narrow Calc milestone therefore accelerates fully admitted
ETL queries, not the Calc fragments of an otherwise unsupported analytical query.
Changing that policy would be a separate architectural decision.

Check interactions with Flink's graph processors explicitly. Multiple-input fusion,
dynamic-filter dependencies and adaptive join rewrites can inspect concrete stock
node types or change consumption order. Keep unsupported shapes on stock execution;
do not globally disable Flink optimizations as an unreported cost of acceleration.
Keep compiled-plan support scoped to what each host release supports; the 1.18 batch
planner explicitly rejects compiled plans. Streaming canonical-plan lowering is not
a prerequisite for an ordinary batch SQL milestone.

### 2. Drive persistent bounded DataFusion execution

The production Calc path evaluates compiled physical expressions directly. Existing
time-bounded joins run DataFusion `HashJoinExec` on buffered batches; the helper in
`native/engine/src/join_common.rs` calls `collect` and consolidates output. Neither is
a general, persistent batch-fragment runtime.

Add an adapter that owns a compiled execution plan and its input/output streams for
the operator's lifetime. It needs input batches, explicit end-of-input per port,
incremental output draining, cancellation and structured failure propagation.
Drain output in bounded batches; avoid accumulating an entire sort or join result in
a JNI result vector or one consolidated batch.

Use the Comet embedding pattern as the starting point. Its
`native/core/src/execution/operators/scan.rs` and execution planner provide input
batch/EOF handling and poll-driven execution. Flink's push/mailbox model still needs
an adapter: when DataFusion needs more input, return control to Flink. Blocking the
task thread while waiting for another input on that same thread can deadlock a
two-input join. Input selection/build priority must agree with Flink's batch graph
and its deadlock processing. Spill IO and native work must remain cancellable.

A practical target is one native fragment per compatible task region between Flink
exchanges, with explicit ports at stage boundaries. The first stateless milestone
can reuse current operators without requiring full fragment fusion. Flink should
continue to own distributed scheduling and network exchange; embedding DataFusion
does not require a second distributed scheduler.

### 3. Add batch-compatible Arrow exchanges

The current `NativeColumnarExchangeExecNode` implements `StreamExecNode`, constructs
key-group routing and explicitly requests `StreamExchangeMode.PIPELINED`. It chooses
initial destination parallelism from the execution environment. Batch lowering must
honor the selected exchange mode, distribution, input priority and actual scheduled
parallelism, including adaptive scheduling where admitted.

`ArrowBatchSerializer` already has a real IPC byte representation, so a new network
format is not inherently required. Its optional local format, however, serializes a
process-local handle and retains the Arrow buffers until a consumer claims them.
`NativeConfig.zeroCopyExchange` checks local deployment and unaligned checkpoints;
it does not check blocking batch exchange lifetime.

Use IPC initially for persisted/blocking batch exchanges, including local runs. A
handle cannot serve as durable shuffle data across task retries, and retaining a
whole stage's buffers until the next stage starts defeats blocking shuffle's memory
benefit. Verify serialization, producer teardown, replay, fan-out ownership and
consumer cleanup independently of the existing streaming shuffle tests.

Do not substitute DataFusion's local hash partitioning for Flink's network
distribution without proof. Both join sides must route equal keys together;
native/host boundaries must agree with the host distribution and sort contracts.

### 4. Verify memory and spill per algorithm

The existing [memory authority](memory-management.md) is a strong foundation:
`FlinkTaskOffHeapPool` backs DataFusion reservations, and `OperatorMemory` creates a
`TaskContext` with that pool. Retain the single TaskManager off-heap authority;
derive batch operator resource declarations consistently with it. Also account for
retained inputs, queued outputs and scratch memory without charging shared Arrow
buffers twice.

Existing streaming state is not a general external-memory batch engine. Some
connector code already uses DataFusion's `DiskManager` for disposable spills
(`native/engine/src/append_buffer.rs`), but this does not establish spill support
for batch SQL operators.

DataFusion 54's [sort](https://github.com/apache/datafusion/blob/54.0.0/datafusion/physical-plan/src/sorts/sort.rs)
has external sorting; its
[grouped aggregation](https://github.com/apache/datafusion/blob/54.0.0/datafusion/physical-plan/src/aggregates/row_hash.rs)
has mode-dependent spill handling. Configure host task-local directories, disk
limits, cleanup and metrics, and exercise the exact modes and types admitted.

**Hash join is a material limitation.** The inspected DataFusion 54
[hash-join build path](https://github.com/apache/datafusion/blob/54.0.0/datafusion/physical-plan/src/joins/hash_join/exec.rs)
retains build batches and a hash table under memory reservations; reservation denial
is not a general spilling hash join. Partitioning reduces typical per-task build
size but does not bound a skewed partition. Large joins therefore need a verified
external-memory route, such as sort-merge with proven spill behavior, or stock
fallback. Enabling a disk manager alone does not solve this.

If no reliable bound or safe memory strategy can be established at planning time,
retain the stock path. A runtime out-of-memory exception is not a successful
connector/operator admission policy, and execution cannot safely switch engines
after emitting output. Treat larger-than-memory joins as a separate milestone.

### 5. Preserve aggregate and type contracts across stages

Flink local aggregates and DataFusion partial aggregates do not necessarily serialize
the same accumulator schema. Translate an admitted aggregate pipeline consistently,
including intermediate schemas across Arrow exchanges; never pass native partial
state to a stock final aggregate merely because both implement SUM or AVG.

Test global aggregation over empty input, grouped empty input, all-null groups,
COUNT/SUM/AVG result types, decimal widening/overflow, DISTINCT/FILTER, grouping sets
and supported user aggregates. Similar checks apply to join null filtering, outer
padding, semi/anti joins, sort ties and window frames. Reuse Flink compatibility
functions instead of handing SQL text to a separate DataFusion SQL planner.

## Connector scope

Start compute support using stock bounded sources and sinks with edge transposes.
This avoids making native scan work a prerequisite for the first batch SQL query.
It also retains the conversion costs a real rowwise deployment pays.

| Boundary | Audit finding and consequence |
|---|---|
| Filesystem Parquet/ORC scan | Both currently use stock Flink. Native codecs exist, but general native filesystem SQL scans need discovery/split/projection/filter/limit/partition and schema admission. |
| Paimon snapshot | Native streaming readers already decode/merge snapshots. `PaimonSourceMatcher` explicitly rejects non-streaming or bounded source mode. Reuse the payload implementation only after verifying bounded split completion, pushdowns, snapshot selection and row kinds. |
| Iceberg scan | SQL matchers are streaming-specific and reject non-streaming table mode. Keep Java planning, deletes, snapshot selection, FileIO and metadata semantics when adding bounded admission. |
| Kafka / Fluss | Existing streaming admission does not certify bounded reads. Verify end offsets or snapshot completion and the original connector's bounded contract. Stock bounded sources remain usable at the compute perimeter. |
| Native sinks | Existing planner extensions match streaming sinks. Verify batch provider selection, `endInput`/finish, final commit without periodic checkpoints, abort/retry and constraints before admitting them in batch. |
| Overwrite / partitioned writes | Important batch use cases; native filesystem admission currently excludes `INSERT OVERWRITE`. Retain the host path until overwrite and commit semantics are verified. |

Preserve the module split: core stays connector/format-neutral. Native Parquet
encoding does not imply a native filesystem source, and specialized maintenance
acceleration does not imply native batch SQL.

## Suggested delivery sequence

These are proposed milestones, not measured estimates or promises of speedup.

| Milestone | Deliverable and exit condition | Relative effort |
|---|---|---|
| 1. Batch SQL foothold | Batch planner/wrappers, ordinary Calc/filter ETL, stock bounded sources/sinks, edge transposes, complete fallback diagnostics. A successful job is `BATCH` and has positive native compute work. Verify both Flink lines and the deployed loader path. | Smallest useful scope; moderate integration work. |
| 2. Batch aggregation | Persistent bounded runtime, Arrow batch exchanges with IPC, initial scalar aggregates and consistent partial/final lowering. Prove completion, replay and forced spill where claimed. | Substantial; establishes the shared execution foundation. |
| 3. Joins and ordering | Initial equi-joins with an explicit memory strategy; global sort and sorted LIMIT. Add outer/semi/anti and external-memory join routes only as proven. | Substantial; input ordering and memory are the main risks. |
| 4. Workload-driven breadth | Window/rank/set-operation coverage and native bounded file/lakehouse scans or sinks selected from stakeholder queries. | Variable; connector semantics and type parity dominate. |

If the stakeholder's workload is dominated by joins or GROUP BY, milestone 1 is an
integration proof rather than the useful end state. Prioritize milestone 2 or 3
using representative query plans. Native bounded file reads may be worth advancing
alongside them when scan/decode dominates; compare with Flink's existing vectorized
readers rather than assuming native decode must be faster.

The most useful next inputs for sizing are representative SQL/EXPLAIN plans, source
and sink connectors, data sizes/key cardinalities/skew, memory limits and the target
Flink line. Calendar estimates before those inputs and a batch runtime spike would
be speculation.

## Validation and acceptance

Extend the existing upstream harness rather than treating its current passing batch
cases as acceleration evidence. The suite agent currently installs StreamFusion
only for streaming mode and only replaces a stock `StreamPlanner`;
`dev/flink-suite/sql_inventory.py` also classifies BatchPlanner execution as outside
the streaming target. Add batch installation, explicit native/fallback contracts,
operator work attribution and batch-aware inventory reporting together. Preserve
explicit host-only expectations for cases outside admission.

For each milestone:

- Run unchanged upstream batch SQL/Table tests on 2.2.1 and 1.18.1, with positive
  native work for admitted cases and exact fallback expectations otherwise. Explain
  labels or merely opening an operator are insufficient.
- Check empty input, short final batches, independently finishing join inputs,
  bounded queue pressure, cancellation and task failure. Exercise parallelism above
  one, blocking network exchange and replay; add adaptive/hybrid variants only when
  their contracts are admitted. Require output completion, not just consumed input.
- Read back committed sink contents, partitions and schemas; inject failure before
  final commit and verify retry/abort behavior. Prove spill cleanup after success,
  cancellation and failure, including disk/memory exhaustion.
- Benchmark release builds against stock **batch** Flink and the previous
  StreamFusion behavior with identical input, resources and queries. Retain real
  scans, JNI, transposes, shuffle, spill and sink commit in end-to-end timing. Report
  repeated measurements, startup costs, peak memory and spill bytes. Include
  scan/Calc ETL, high-cardinality aggregation, skewed joins and ordering workloads
  appropriate to the admitted milestone.

No new runtime tests or benchmarks were executed for this audit. Evidence consists
of repository source/documentation inspection, the installed released DataFusion
54 source, batch execution-node inventories from the released Flink JARs, and
upstream release source checks. The sibling Flink checkout is a newer snapshot, so
release-specific conclusions above use the pinned releases instead. Comet's input
adapter and Arroyo's join structure were consulted as architectural references;
no external repository was modified.
