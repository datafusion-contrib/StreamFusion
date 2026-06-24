# Roadmap / backlog index

Read this first. It is the ordered map of what is done and what remains; each item points to a
detailed ticket. Keep it in step as tickets are completed or added; delete a ticket's pointer
here when the ticket is deleted.

## Where we are (done + parity-verified)
- **Operators, native + identical to Flink:** filter/`WHERE` (via the native expression engine),
  tumbling / hopping / session / cumulative window aggregates (one- and two-phase), event-time
  `OVER` aggregation, event-time INNER interval and window joins, Parquet sink (exactly-once),
  Parquet source. Compatibility chart in `readme.md` is the source of truth.
- **Window aggregate input schemas:** all five aggregates over every non-decimal
  numeric value type (custom accumulators keep the host's type/precision) plus decimal MIN/MAX/COUNT;
  multiple value columns (`SUM(a), SUM(b)`); bigint/int/string/boolean/date/timestamp/decimal grouping
  keys; and `COUNT(*)` (one- and two-phase). Parity matrix in `docs/aggregate-type-support.md`.
  Decimal `SUM`/`AVG` stay on the host (precision rules). Join/`OVER` partition keys still carry only
  bigint/int/string (the wider-key carriage exists; admitting it there just needs tests).
- **Columnar flow, end to end:** `ArrowBatch` stream type + IPC serializer, the two
  transpose operators, the transition-inserter pass, `ColumnarInput`/`ColumnarOutput` markers, a
  **columnar watermark assigner**, and a **columnar keyed shuffle** (native by-key split +
  `ColumnarKeyGroupPartitioner`, own consistent hash — divergences/10). Columnar today: source, sink,
  filter/calc, all window aggregates (one- and two-phase), `OVER`, and both joins — a windowed/keyed
  pipeline (source → watermark assigner → exchange → operator) flows Arrow with no transpose.
- **Joins delegate the match to DataFusion** (`HashJoinExec` over the batches we buffer), like
  Arroyo; we own buffering + watermark eviction (divergences/12).
- **Profiling/benchmarks (ticket 20):** Criterion native micro-benches; `ThroughputBenchmark`
  vs Flink; the `bench` Maven profile (release native — mandatory for benchmarks).
- **Acceleration config + visibility:** per-expression `allowIncompatible`, the master
  `streamfusion.native.enabled`, and per-operator `streamfusion.operator.<name>.enabled` flags
  (`NativeConfig`); fallback-reason reporting (`PhysicalPlanScan.fallbackReasons()`,
  `-Dstreamfusion.logFallbackReasons`, `NativePlanner.explain`). Mirrors Comet; see readme
  "Controlling acceleration" / "Seeing why a query fell back".
- **Release benchmarks vs Flink (clean):** Parquet copy 4.68×, Parquet sink 2.24×, windowed-over-
  columnar 1.82×, interval join 1.71×, OVER 1.56×, tumbling 1.24×, bare filter 0.75×. Sink coalescing
  lifted both Parquet paths; the row-major + pre-sized transpose build lifted the row-source ops (a
  native row decoder was investigated and rejected — ticket 28). Only the lone stateless filter stays
  below 1× (its `RowData → Arrow → RowData` round-trip); leave it on the host via the per-operator flag.

## Next, roughly in order
1. **Expression layer stage 3 tail** (ticket 19): general projection (the planner's `Calc` node —
   an optional filter plus projections in one node — handled natively) and a broad function set are
   done (`/` `%`, COALESCE/NULLIF/NULL, narrow-int arithmetic, and the common string/exact-math
   functions, with precision/locale-divergent ones opt-in). Remaining: narrowing/float→int/string
   `CAST`, and any further obscure functions — each parity-gated.
2. **Richer columnar endpoints** (ticket 24): beyond local Parquet — Iceberg and remote
   filesystems (`hdfs:`/`s3:`) for the native source/sink; currently `file:` only. **Deferred by
   direction until generalized operator support lands** — broaden what we can run (item 1 and the
   ticket 11 operators) before broadening where we read/write.
3. **Operator-level perf** (ticket 20 backlog): per-row `GroupKey` allocation in aggregators, session
   `update` one-row `take` batching. (The `RowData → Arrow` transpose was made row-major + pre-sized,
   ~25% faster; a native decoder was investigated and rejected on benchmark grounds — ticket 28.)
4. **Nexmark benchmark vs Flink** (ticket 30): run the standard q0–q22 Flink SQL suite native-
   substituted vs stock Flink (release), per-query routed-fraction + fallback reasons + throughput
   ratio. Use it as the prioritization engine for both coverage (which queries fall back, and why)
   and perf (which route but trail Flink) — re-run as a regression/impact gate after each change.

## Production-readiness (not yet load-bearing)
- **Memory accounting** (ticket 05): native `RootAllocator`s per operator are not accounted
  against Flink's `MemoryManager`.
- **Mailbox threading** (ticket 01): native execution should integrate with the task mailbox
  (non-blocking), not block the task thread.
- **Changelog / retract** (ticket 06): only insert-only streams are native today.

## Breadth / longer horizon
- **Arroyo operator coverage tracker** (ticket 11): append-only dedup, window Top-N, event-time
  sort remain; non-windowed GROUP BY / regular joins / streaming Top-N need retract (ticket 06).
  (Two-phase cumulative windows and event-time joins are now done.)
- **Fully native Kafka source, no JNI** (back burner): subscribe in Rust, decode Avro→Arrow, only if
  the connector semantics can be lifted from Arroyo.
