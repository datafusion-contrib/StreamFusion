# Roadmap / backlog index

Read this first. It is the ordered map of what is done and what remains; each item points to a
detailed ticket. Keep it in step as tickets are completed or added; delete a ticket's pointer
here when the ticket is deleted.

## Where we are (done + parity-verified)
- **Native expression engine — done** (was ticket 19, now retired; the residual tail lives in
  `docs/coverage-and-fallbacks.md` §3 + `divergences/07`). A hand-encoded `RexNode`→DataFusion evaluator
  compiled once per operator handles the planner's `Calc` (optional filter + arbitrary projections):
  arithmetic incl. `/`/`%` and narrow-int width, the comparisons/boolean/null predicates, searched
  `CASE`/`COALESCE`/`NULLIF`, `ROW`-field access, `PROCTIME()`, a broad string/temporal function set,
  **exact `Decimal128` `+`/`-`/`*`**, widening + **narrowing/float→int** `CAST` (wrapping/saturating
  kernel) and `CHAR`/`VARCHAR`→`VARCHAR` passthrough; precision/locale-divergent ops (`ROUND`/transcendental,
  pure-Rust case/regex) are opt-in behind `allowIncompatible`. Remaining tail (parity-gated, minor):
  number↔string `CAST`, narrowing-`VARCHAR`/cast-to-`CHAR(n)`, byte-exact decimal `/`/`%`, obscure funcs.
- **JVM-upcall UDFs — done, distributed-safe.** A function the native engine can't evaluate itself runs
  via a native→JVM columnar Arrow upcall (`NativeUdf`/`JvmUdf`, one JNI crossing per batch, byte-identical
  to Flink): non-builtin Flink `ScalarFunction`s (q14) and the host-exact builtins `REGEXP_EXTRACT` /
  `UPPER` / `LOWER` (q21, native by default; the faster pure-Rust regex/case paths stay opt-in behind
  `allowIncompatible`). The function is carried into the operator as a serializable descriptor and
  registered on the JVM that runs it at `open()` (freed at `close()`), so it works on a distributed task
  manager, not just the planner's JVM — no plan-time global registry.
- **Operators, native + identical to Flink:** filter/`WHERE` (via the native expression engine),
  tumbling / hopping / session / cumulative window aggregates (one- and two-phase), event-time
  `OVER` aggregation, event-time interval and window joins (INNER/LEFT/RIGHT/FULL + residual
  non-equi, outer null-pads at watermark eviction), Parquet sink (exactly-once),
  and Parquet + ORC sources. The sources read through DataFusion's file scan (like comet) with the
  framework's file source owning enumeration/split-assignment/checkpointing — splittable at
  row-group/stripe granularity, projection pushed into the decode. Compatibility chart in `readme.md`
  is the source of truth.
- **Changelog / retract — done.** `RowKind` crosses the boundary as a four-way byte column
  (divergences/13), and three operators both emit and consume a retract changelog: the non-windowed
  `GROUP BY` aggregate (SUM/COUNT/MIN/MAX retract), the regular (updating) equi-join
  (INNER/LEFT/RIGHT/FULL/SEMI/ANTI + a residual non-equi predicate), and
  append-only streaming Top-N (`ROW_NUMBER`), which the global `FETCH`/`LIMIT` reuses — `ORDER BY …
  LIMIT n` (`SortLimit`) and plain `LIMIT n` (`Limit`) both lower to a global no-partition
  ROW_NUMBER rank, so they run on the same native Top-N operator with an empty partition key (no new
  operator). A fourth, **changelog normalization** (`ChangelogNormalize`, upsert/CDC-with-PK source →
  regular changelog), keeps the last row per unique key (a port of Flink's keep-last-on-changelog
  `ProcTimeDeduplicateKeepLastRowFunction`). The streaming engines RisingWave/Proton informed these
  (divergences/14). All three are **columnar** (Arrow in/out) per the CLAUDE.md principle — the
  row↔Arrow conversion is paid only at host edges, so a native changelog chain has no per-operator
  transpose. Streaming Top-N now also projects the **rank number** (the `AppendOnlyTopNFunction`
  shift cascade — UPDATE_BEFORE/UPDATE_AFTER per shifted rank, plus the appended rank column).
  Feature tails — `RANK`/`DENSE_RANK`, an offset, and retracting-input Top-N — are tracked in the
  coverage tracker (ticket 11).
- **Window aggregate input schemas:** all five aggregates over every non-decimal
  numeric value type (custom accumulators keep the host's type/precision) plus decimal MIN/MAX/COUNT;
  multiple value columns (`SUM(a), SUM(b)`); bigint/int/string/boolean/date/timestamp/decimal grouping
  keys; and `COUNT(*)` (one- and two-phase). Parity matrix in `docs/aggregate-type-support.md`.
  Decimal `SUM`/`AVG` stay on the host (precision rules). Join/`OVER` partition keys now carry the
  full grouping-key set (bigint/int/string/boolean/date/timestamp/decimal) — the native key path is
  type-general, so OVER, interval join, and window join were widened from bigint/int/string to match
  the group-by/window aggregate (float/double stay out — key equality on them is ill-defined).
- **Columnar flow, end to end:** `ArrowBatch` stream type + IPC serializer, the two
  transpose operators, the transition-inserter pass, `ColumnarInput`/`ColumnarOutput` markers, a
  **columnar watermark assigner**, a **columnar keyed shuffle** (native by-key split +
  `ColumnarKeyGroupPartitioner`, own consistent hash — divergences/10), and a **columnar windowing
  table function** (stateless `TUMBLE`/`HOP`/`CUMULATE` window assignment, fanning rows out for
  hopping/cumulative; shares the window aggregate's assignment math). Columnar today: source, sink,
  filter/calc, all window aggregates (one- and two-phase), `OVER`, both joins, the windowing TVF,
  **event-time sort** (`ORDER BY rowtime`), **keep-first deduplication** (rowtime `ROW_NUMBER … = 1`),
  **window Top-N / window deduplication** (over the windowing TVF), **`UNION ALL`** (a pure
  stream merge — no operator, just a `UnionTransformation` over the inputs' Arrow streams), and
  **`GROUPING SETS`/`CUBE`/`ROLLUP`** (a stateless `Expand` fan-out feeding the native GROUP BY) — a
  windowed/keyed pipeline (source → watermark assigner → exchange → operator) flows Arrow with no
  transpose. The GROUP BY now also emits **NULL group keys** (grouped-out keys / Flink's null-as-key),
  a fix that fell out of GROUPING SETS.
- **Fully-columnar native islands (the standing invariant — shipped):** every native operator but a
  source/sink is `Arrow → Arrow`; `RowData` appears only where the native region meets a *rowwise*
  source/sink, via the two transpose operators, never between native operators. Acceleration is
  **whole-query all-or-nothing** — if any non-source/sink operator would run row-wise, the planner
  substitutes nothing and the query runs as stock Flink (a rowwise source/sink is bridged by a
  perimeter transpose, not a fall-back trigger). Top-N and the updating join keep row-materialized
  *internal* state for sort/retract correctness — fine, since their boundary is Arrow.
  **Sub-plan reuse is disabled** when native is installed (`NativePlanner.install`): an island's
  zero-copy hand-off assumes each Arrow batch is consumed once (the consumer closes its off-heap
  buffers), so a reused branch fanning batches to two consumers would double-free → null rows.
  Disabling reuse keeps the plan a tree; output is identical (only the execution graph changes).
- **Joins delegate the match to DataFusion** (`HashJoinExec` over the batches we buffer), like
  Arroyo; we own buffering + watermark eviction (divergences/12).
- **Profiling/benchmarks (ticket 20):** Criterion native micro-benches; `ThroughputBenchmark`
  vs Flink; the `bench` Maven profile (release native — mandatory for benchmarks).
- **Acceleration config + visibility:** per-expression `allowIncompatible`, the master
  `streamfusion.native.enabled`, and per-operator `streamfusion.operator.<name>.enabled` flags
  (`NativeConfig`); fallback-reason reporting (`PhysicalPlanScan.fallbackReasons()`,
  `-Dstreamfusion.logFallbackReasons`, `NativePlanner.explain`). Mirrors Comet; see readme
  "Controlling acceleration" / "Seeing why a query fell back".
- **Release benchmarks vs Flink (clean):** Parquet copy 4.97×, Parquet sink 2.24×, windowed-over-
  columnar 1.82×, interval join 1.71×, OVER 1.56×, tumbling 1.24×, bare filter 0.75×. Sink coalescing
  lifted both Parquet paths; the row-major + pre-sized transpose build lifted the row-source ops (a
  native row decoder was investigated and rejected — ticket 28). Only the lone stateless filter stays
  below 1× (its `RowData → Arrow → RowData` round-trip); leave it on the host via the per-operator flag.
- **Native decode-to-Arrow at ingest — done.** File sources (Parquet + ORC) read through DataFusion's
  file scan with the framework owning enumeration/splits/checkpointing, splittable at row-group/stripe
  granularity with projection pushdown; and a streaming decode operator (`NativeBytesDecodeOperator`)
  decodes Kafka bytes → Arrow for JSON, Confluent/bare Avro, CSV, protobuf, and Debezium/OGG CDC
  envelopes (→ our `$row_kind$` changelog). The residual tail lives in ticket 32 (a time-based flush for
  sub-batch unbounded streams; Maxwell/Canal auto-routing; CSV/JSON *file* sources).
- **Nexmark matrix vs Flink — running.** The full q0–q22 suite (q6 excluded, ticket 39) runs
  native-substituted vs stock Flink across four source rungs (generator, Kafka JSON/Avro/Protobuf);
  per-query routed-fraction/fallback reasons via `NexmarkExplainTest`, throughput matrix in the readme.
  It is the standing prioritization + regression gate; the coverage/perf gaps it surfaces feed the
  backlog below.

## Next, roughly in order
1. **Richer columnar endpoints** (ticket 24): beyond local Parquet — Iceberg and remote
   filesystems (`hdfs:`/`s3:`) for the native source/sink; currently `file:` only. **Deferred by
   direction until generalized operator support lands** — broaden what we can run (the ticket 11
   operators and any remaining expression tail) before broadening where we read/write.
2. **Operator-level perf** (ticket 20 backlog): the scalar `GroupKey` remaining in the smaller
   keyed loops (dedup, `OVER` partitions, Top-N, exchange split) — swap to arrow-row keys only
   with a bench showing it pays. (All aggregators now use arrow-row keys — keyed tumbling 2.2×;
   session `update` batches gap-connected runs — dense shape 20× vs per-row; the `RowData → Arrow`
   transpose was made row-major + pre-sized, ~25% faster; a native decoder was investigated and
   rejected on benchmark grounds — ticket 28.)

## Production-readiness (not yet load-bearing)
- **Memory accounting**: shipped for every stateful native operator (mini-batch local pre-aggregate
  included) — the transformation declares an operator-scope managed-memory weight, the operator
  reserves the fraction from Flink's `MemoryManager` for its lifetime, and the native side enforces
  it as a bounded DataFusion pool with incrementally tracked state bytes
  (`NativeMemoryLimitException` on exceed, restore included; up-front reservation, not comet's
  per-grow upcall). DataFusion-executed join fragments run under the same pool; file scans are
  deliberately not pool-wired (DF 53's scan path registers no consumers) and the FFI Arrow
  allocator is deliberately outside the budget — scope and rationale in divergences/16.
- **Mailbox threading** (ticket 01): native execution should integrate with the task mailbox
  (non-blocking), not block the task thread.
- **Memory verification**: shipped — every test doubles as a native leak check (live handles and
  the Arrow FFI allocator must drain to zero at close; the first audit caught and fixed the
  dropped-in-flight-record failover leak, now freed by the columnar record's Cleaner backstop),
  accounted operators export their budget/state/allocator bytes as Flink operator metrics, an
  opt-in soak (`SF_SOAK=true`) asserts memory plateaus over a long evicting job, and
  `docs/native-memory-profiling.md` carries the heap-profiler recipe with a findings log (first
  run: clean).

## Breadth / longer horizon
- **Arroyo operator coverage tracker** (ticket 11): what remains is async UDF (ticket 01) plus operator
  feature tails (rank-number / `RANK` / retracting-input Top-N, OVER
  frames / `FIRST_VALUE`/`LAST_VALUE`, proctime variants). (Two-phase cumulative windows, event-time
  joins incl. outer/semi/anti, the non-windowed GROUP BY aggregate, the regular updating join,
  append-only Top-N, keep-first deduplication, window Top-N / window deduplication, and event-time
  sort are now done.)
- **Fully native Kafka source, no JNI** (ticket 33, back burner): subscribe in Rust, decode →Arrow,
  lifting the connector semantics (partition/offset/checkpoint/watermark) from Arroyo. Removes the one
  off-heap copy ticket 32 pays; only worth it once that decode path proves the copy is the bottleneck.
- **Nexmark with Apache Fluss as the source** (ticket 36): add Fluss (columnar streaming storage) as a
  fourth source in the Nexmark matrix; its columnar format may let the native island ingest Arrow with
  little/no row transpose — the perimeter transpose is a visible share of the remaining stateful-query
  cost, so Fluss is the source most likely to show the engine's largest end-to-end margin.
- **Native lookup join** (ticket 40): **sync + async DONE.** `NativeLookupJoinOperator` (sync) and
  `NativeAsyncLookupJoinOperator` (async) accelerate a processing-time lookup join (Nexmark q13) — Arrow
  probe batches stay in the island while the connector's real `LookupFunction`/`asyncLookup` runs
  (byte-identical to Flink); INNER + LEFT, field-ref keys, no calc/residual. Async fires a batch's
  distinct-key lookups concurrently and awaits them on the task thread (Arroyo/RisingWave within-batch
  model — no operator mailbox). Follow-ups in ticket 40: calc-on-temporal-table + residual condition,
  constant keys, columnar/preload assembly, distributed serialization.
- **Nexmark q6 — documented exclusion** (ticket 39): the one query Flink 2.2.1 itself can't run (invalid
  as written; wrapped, Flink rejects the bounded `OVER` over the Top-N's non-time-attribute `dateTime` —
  FLINK-19059, the retraction limit the nexmark README cites, is fixed in 2.1.0, so that barrier is gone,
  but a different one remains). No host plan to mirror or parity-check. Not StreamFusion's to fix.
- **Disaggregated state store** (ticket 37): move operator state off-heap to a remote/tiered store
  (likely Fluss's PK-table KV) with a local working-set cache — decoupling state size from worker RAM
  and enabling incremental checkpoints + lazy rescale (Flink 2.0 ForSt / RisingWave Hummock direction).
  The operators' state is already arrow-row bytes, so the stored format is mostly in place.
- **Columnar sinks + remaining boundary types** (ticket 34): the nested-type transpose is **done** —
  the row↔Arrow converter carries `ROW`/`ARRAY`/`MAP`/`VARBINARY`, so nested protobuf/Avro/JSON now
  route. What remains: a custom columnar sink beyond Parquet (no ORC sink / columnar collect yet), and
  the protobuf representation reconciliations still gated (`enum` int-vs-name, unsigned/`fixed` ints,
  `bytes` parity, proto3 missing-field defaults).
