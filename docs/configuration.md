# Configuration

Planner settings use Flink's normal configuration surface and can be supplied in `flink-conf.yaml`,
the SQL client, or `TableConfig` (for example, `SET 'streamfusion.native.enabled' = 'false'`). A
same-named `-Dstreamfusion.*` system property remains a compatibility fallback. Task-runtime tuning
settings are still JVM properties before 1.0; they are called out below and will become
serialized operator configuration rather than process-global state before a stable release.

## Acceleration control

- **`-Dstreamfusion.native.enabled=false`** — master switch; run entirely on stock Flink.
- **`-Dstreamfusion.operator.<name>.enabled=false`** — keep one operator on the host (e.g. leave a
  lone cheap `filter` on a row source, which can't earn back the transpose round-trip). A switch
  covers every shape that reaches the same native operator — e.g. `groupAggregate` also covers the
  two-phase global half, `windowRank` also covers window deduplication, and `dropUpdateBefore`
  controls the stateless changelog filter. All default on.
  Optional connector operators only activate once the matching connector extension and format JAR
  are installed; otherwise the plan falls back to Flink's own path.
- **`streamfusion.experimental.jvmGroupAggregates`** (system property, default true) — enable
  verified synchronous single-phase JVM UDAFs with VARCHAR/INT/BIGINT inputs/results. Set false
  for the preceding host fallback. Cache and borrowed-exit performance experiments retain their
  existing defaults. See [GROUP BY](operators/group-by.md#jvm-aggregate-investigation).
- **`streamfusion.experimental.rustCollectionAggregates`** (system property, default true) —
  use Rust state for the verified single-phase integer/BOOLEAN COLLECT and Flink 2.2 ARRAY_AGG
  signatures. Set false to retain the preceding JVM handler when restoring its managed-state
  savepoints; automatic JVM/native collection-state migration is not supported. The existing
  `jvmGroupAggregates` switch and collection-consumer guards still apply. See
  [collection state](operators/group-by.md#integer-collect).
- **`streamfusion.experimental.nestedScalarUdfs`** (system property, default true) — enable
  verified external Java Row and Map callbacks and the matching nested RowKind/raw BINARY
  transport. MAP callbacks use Flink-generated whole-Calc evaluation; computed collection outputs
  retain representation guards before unverified state/exchange boundaries.
  Configure the same value in planner and task JVMs; false restores the preceding transport and
  nested-signature fallback. See [Calc/filter](operators/calc-filter.md).
- **`streamfusion.dropUpdateBefore.standalone.enabled`** (default true) — enable a lone
  changelog filter after verified Rust-side Arrow admission. Explicit `false` retains stock
  for the standalone shape. Complete row-source jobs can still lose because of perimeter costs.
  Filters inside an admitted native pipeline retain ordinary admission. See
  [drop update-before](operators/drop-update-before.md).
- **`streamfusion.calc.sharedStatefulUdf.enabled`** (default true) — complete-row native
  Calc evaluation for shared non-deterministic scalar calls, preserving Flink invocation order.
  Explicit `false` retains the shared-call host route. Slower JVM UDF execution is an accepted
  tradeoff for correct coverage. See [Calc/filter](operators/calc-filter.md).
- **`streamfusion.calc.temporalUdf.enabled`** (default true) — complete-row generated Calc
  evaluation for verified `LocalDateTime` TIMESTAMP and `Instant` TIMESTAMP_LTZ UDF signatures.
  Explicit `false` forces those UDFs to stock. Correctness/type/lifecycle gates remain in force;
  slower UDF execution alone does not block coverage. See
  [temporal UDF coverage](operators/calc-filter.md#temporal-scalar-functions).
- **`streamfusion.experimental.directTemporalCalc`** (system property, default true) — evaluate
  complete JVM Calcs with timestamp inputs and optional BOOLEAN, TINYINT, SMALLINT, INT, BIGINT,
  FLOAT or DOUBLE columns directly on Arrow batches. Explicit false
  restores the previous JNI callback; changelog input and represented output schemas always use
  that callback. This does not change temporal UDF type coverage.
- **`streamfusion.experimental.temporalAliases`** (system property, default true) — share
  unchanged timestamp output buffers for unconditional, timestamp-only direct projections of
  matching width and a common allocator root. Every function still executes. Explicit false uses
  ordinary output writers.
- **`streamfusion.experimental.collectionExpressions`** (default true) — enable the
  collection kernels and generated collection/constructor programs described in
  [Calc/filter coverage](operators/calc-filter.md#collection-expressions). The planner serializes
  the required Java string-cache and nested row-kind layout into the entry transpose and generated
  result schema. Standalone row-source queries can be slower; the maintainer accepted this
  tradeoff on 2026-10-17. Set false to keep these extensions on Flink.
- **`streamfusion.experimental.metadataExpressions`** (default true) — enable `TYPEOF` and
  `CURRENT_DATABASE`. Aggregate pipelines improve in the measured workloads, while standalone
  projections still lose to stock Flink. This tradeoff was accepted on 2026-10-17; set false
  to disable the extension. See [metadata coverage](operators/calc-filter.md#type-and-database-metadata).
- **`-Dstreamfusion.fluss.enabled=false`** — disable the optional Fluss extension’s source/sink
  substitutions. This JVM property defaults to true when `streamfusion-fluss` is installed;
  only the verified combinations in [Fluss](connectors/fluss.md) accelerate.
- **`-Dstreamfusion.expression.<NAME>.allowIncompatible=true`** — opt into the faster pure-Rust
  path for expressions that otherwise use a byte-exact JVM upcall or fall back (`UPPER`/`LOWER`,
  `REGEXP_EXTRACT`, `DATE_FORMAT`/`EXTRACT` over `TIMESTAMP_LTZ`, `ROUND` on float, transcendental
  math). Floating math and non-DECIMAL ROUND use exact generated evaluation by default;
  these flags select the existing approximate Rust alternatives. Off by default — parity-first.
  See the per-operator/expression pages under
  [Operators](operators/index.md) for exactly which functions this affects and how they can diverge.
- **`-Dstreamfusion.plan.shareSources=false`** — disable the substitution pass's own source-sharing
  (two branches scanning the same native source normally collapse into one read); restores one
  source per branch. It also disables retained-view sharing of unary batch aggregate
  subplans. Flink's `table.optimizer.reuse-source-enabled=false` and
  `table.optimizer.reuse-sub-plan-enabled=false` also disable native source sharing.
- **`streamfusion.transpose.batchRows`** (streaming default 1024; bounded batch pipelines prefer 8192; proctime complex COUNT input islands prefer 4096) — maximum rows buffered by each
  row-to-Arrow boundary. Must be positive. Read from the job's Flink configuration at planning
  time and serialized into the operator; the same-named JVM property is a fallback. Explicit
  table/JVM settings override the preferred batch size. Watermarks,
  checkpoints, end-of-input and the latency timer can flush a smaller batch. This does not set
  Flink's logical mini-batch size or the post-exchange coalescer's target size. Accepted rows are
  copied directly into owned Arrow storage, including partial batches; operator close releases
  any unflushed buffers. Its `conversionTime` counter estimates row-write time using random 1/64
  sampling, plus exact allocation and batch-finalization timing.
- **`-Dstreamfusion.exchange.zeroCopyLocal=...`** — vouch for a single-TaskManager deployment so the
  columnar exchange can hand off batches by ownership transfer instead of Arrow IPC on a same-process
  edge. See [Zero-copy local shuffle](optimizations/zero-copy-local-shuffle.md).
- **`-Dstreamfusion.exchange.coalesceRows`** (default 4096) / **`-Dstreamfusion.exchange.coalesceLatencyMs`**
  (default 50) — re-assemble processing-sized batches after the keyed exchange fragments them across
  parallel subtasks. See [Post-exchange batch coalescing](optimizations/post-exchange-coalescing.md).

## Memory

See [Memory management](memory-management.md) for the authoritative pool, covered consumers,
exhaustion behavior, sizing, and metrics.

- **`taskmanager.memory.task.off-heap.size`** — the single TaskManager-wide authority for
  StreamFusion memory. It must be greater than zero. Native operator state and DataFusion working
  memory and Arrow FFI buffers reserve from this shared cap. A denied reservation fails with a
  `NativeMemoryLimitException` naming this normal Flink setting.
- **`state.backend.rocksdb.memory.*`** — Flink's normal RocksDB memory-control options size the
  native stores' shared block cache and write-buffer manager under the RocksDB backend; there are
  no StreamFusion-specific state memory settings. See [RocksDB backend](backends/rocksdb.md).

### Arrow Java access checks

StreamFusion applies the following defaults before Arrow schema conversion, runtime allocation,
or native library initialization. These are Arrow JVM settings, not Flink table options:

| JVM property | StreamFusion default | Effect |
| --- | --- | --- |
| `arrow.enable_unsafe_memory_access` | `true` | Skip per-access buffer bounds and liveness checks. |
| `arrow.enable_null_check_for_get` | `false` | Skip primitive-getter NULL checks; callers still check validity. |

Explicit system properties take precedence over environment variables. For unsafe access, Arrow's
legacy `drill.enable_unsafe_memory_access` property takes precedence over
`ARROW_ENABLE_UNSAFE_MEMORY_ACCESS` when the current property is absent. The NULL-check environment
variable is `ARROW_ENABLE_NULL_CHECK_FOR_GET`. StreamFusion leaves these explicit settings intact,
including Arrow's case-sensitive interpretation of `true` and `false`.

To diagnose invalid accesses, set `-Darrow.enable_unsafe_memory_access=false` and
`-Darrow.enable_null_check_for_get=true` at JVM startup. No StreamFusion-specific switch is needed.
The properties are process-wide and may affect other Arrow users in the same JVM; each Arrow
classloader fixes its effective values at class initialization. StreamFusion logs the effective
checks and warns if earlier Arrow initialization prevents the requested settings from taking effect.
Later SQL or system-property changes do not reconfigure initialized classes. Memory budgeting,
reference-counted ownership, NULL bitmaps and safe vector growth remain unchanged. See
[Arrow access optimization](optimizations/arrow-unsafe-memory-checks.md).

### Off-heap sizing

Size `taskmanager.memory.task.off-heap.size` for the peak aggregate of all StreamFusion consumers in
one TaskManager, not per operator:

- Arrow FFI buffers are process-wide and bounded by in-flight batches.
- Native operator state and DataFusion working reservations vary with the query. The in-memory state
  backend fails when it cannot reserve more. Under the RocksDB backend, state lives in RocksDB's own
  memtables and block cache (bounded by `state.backend.rocksdb.memory.*`), and only the per-batch
  working set reserves from this cap.

Kafka client buffers are owned by Flink's Java source/sink and remain under the normal Flink/JVM
memory model; StreamFusion charges only its Arrow batches and native codec/operator work here.

### Live metrics

StreamFusion exports the shared pool and operator state to the Flink UI/metrics reporter:

- `nativeOffHeapCapacityBytes` — configured TaskManager task off-heap capacity.
- `nativeOffHeapReservedBytes` / `nativeOffHeapAvailableBytes` — current aggregate usage/headroom.
- `nativeOffHeapPeakBytes` — process-wide reservation high-water mark.
- `nativeOffHeapDeniedReservations` — number of rejected growth requests.
- `nativeArrowAllocatorBytes` — the process-wide Arrow FFI allocator's current footprint.
- `nativeStateBytes` — tracked state drawing on that budget, sampled per batch.

## Window coverage

CHAR/VARCHAR MIN/MAX now run natively by default in every otherwise supported window shape,
including fixed processing-time windows, sessions and legacy group windows. The optional
`streamfusion.window.string-extrema.single-phase.enabled=false` forces stock execution;
`true` matches the default. The Rust-side string selection optimization removes the former
high-cardinality HOP restriction; the separate session kernel also passes the Rust-side gate.
Two-phase local/global string extrema do not require this option.
See [Window aggregate](operators/window-aggregate.md#string-minmax).

Single-phase attached-window re-aggregation now runs natively for all otherwise supported
append-only event-time TUMBLE/HOP/CUMULATE layouts, including regional calendars, wider overlap
and upstream aggregates beyond COUNT. The optional
`streamfusion.window.attached.single-phase.enabled=false` forces stock execution; `true`
now matches the default. FILTER, retracting inputs and unsupported types still fall back.
[The admission audit](benchmarks/feature-admission-2026-10-19.md) compares stock RowData with
matching prebuilt Arrow inputs, with intrinsic conversions included. Complete-job timings
remain separate evidence, including retained slower wide-overlap jobs.

## Diagnostics

Completed planning passes log recorded fallback reasons and final function-engine assignments
at **INFO** by default, including stock compiled-plan lowering. Repeated messages within a pass
include an occurrence count. The former `streamfusion.logFallbackReasons` flag is no longer read;
use the normal logger configuration to control verbosity.

Assignments list distinct SQL function and UDF calls under `native` and `jvm`; casts are retained
because their type and failure semantics can select JVM evaluation, while arithmetic, comparison
and boolean operators are omitted. A JVM assignment also records why a host-exact upcall or
whole-Calc generated evaluator was selected. The planning summary counts both operator fallbacks
and Calc/filter nodes containing JVM functions. Logging does not change execution admission.

Ordinary SQL `EXPLAIN` and submitted operator descriptions retain the original Flink details:
native sources and sinks keep table identifiers, fields and source/sink abilities; Calc/filter
nodes keep `select` and `where`. Native source/sink transformation names use `NativeSource(...)`
and `NativeSink(...)`, including those details. Intermediate serialization/routing stages keep
their existing names. Calc/filter names also include `nativeFunctions` and `jvmFunctions`, even when
`table.exec.simplify-operator-name-enabled` is enabled. These lists describe the final expression
encoding, including a whole-Calc JVM evaluator when required for shared function state or
representation contracts. Independent competing exceptions do not require that evaluator. A surrounding
`NativeCalc` node alone does not prove that every scalar function runs in Rust. Detailed JVM
selection reasons remain available in the diagnostic log and `NativePlanner.explain(...)` summary.

See [Benchmarks](benchmarks.md) for how to reproduce the throughput numbers, and
[Optimizations](optimizations/index.md) for how these flags map to specific performance techniques.


## Binary selection

The JVM system property `streamfusion.experimental.binaryElt` defaults to `true`. Verified
INTEGER-indexed binary `ELT` expressions run without an opt-in. Explicit `false` retains
stock execution; use the same setting in planner and task JVMs. This also transports SQL `BINARY(n)` values through length-delimited
Arrow Binary vectors, preserving raw payload lengths without changing their SQL types.
The nested-scalar option independently selects the same transport when enabled.

Ordinary safe selection uses the existing Rust kernel; expressions requiring released
Flink evaluation retain that generated path. Unsupported index overloads retain fallback.
Legacy variable-length cast guards and nested scalar admission rules are unchanged.
Default admission follows the Rust/Arrow versus Java/RowData functionality gate. The
[historical binary measurements](operators/calc-filter.md#binary-casts-and-fixed-width-generated-expressions)
retain slower conversion-inclusive standalone results; they are separate from this gate.

The optional bounded native Parquet source honors
`streamfusion.operator.fileSource.enabled` (default true for admitted paths).
It requires fixed batch parallelism and its documented [source admission](connectors/parquet.md#source);
unsupported tables retain Flink's reader and the ordinary transpose.
