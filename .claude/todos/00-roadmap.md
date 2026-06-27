# Roadmap / backlog index

Read this first. It is the ordered map of what is done and what remains; each item points to a
detailed ticket. Keep it in step as tickets are completed or added; delete a ticket's pointer
here when the ticket is deleted.

## Where we are (done + parity-verified)
- **Operators, native + identical to Flink:** filter/`WHERE` (via the native expression engine),
  tumbling / hopping / session / cumulative window aggregates (one- and two-phase), event-time
  `OVER` aggregation, event-time INNER interval and window joins, Parquet sink (exactly-once),
  and Parquet + ORC sources. The sources read through DataFusion's file scan (like comet) with the
  framework's file source owning enumeration/split-assignment/checkpointing — splittable at
  row-group/stripe granularity, projection pushed into the decode. Compatibility chart in `readme.md`
  is the source of truth.
- **Changelog / retract — done.** `RowKind` crosses the boundary as a four-way byte column
  (divergences/13), and three operators both emit and consume a retract changelog: the non-windowed
  `GROUP BY` aggregate (SUM/COUNT/MIN/MAX retract), the regular (updating) INNER equi-join, and
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
5. **Native decode-to-Arrow at ingest** (ticket 32): skip the per-record `RowData` materialization on
   source formats, two source kinds handled differently. **File sources — done for ORC + Parquet:**
   read through DataFusion's file scan with the framework's file source owning enumeration/splits/
   checkpointing, splittable at row-group/stripe granularity, projection pushed down (Avro OCF dropped —
   arrow-avro can't read Flink's top-level-union output; CSV/JSON files remain, lower priority).
   **Streaming/Kafka (next):** keep Flink's connector, pay one off-heap copy into a native decode
   operator (arrow-json/csv/avro share one push API; CDC envelopes → our `$row_kind$` changelog;
   protobuf via prost-reflect). JSON decode kernel landed (~5.3 Melem/s). Removing the Kafka copy
   entirely is the fully-native source (ticket 33).

## Production-readiness (not yet load-bearing)
- **Memory accounting** (ticket 05): native execution memory is not accounted against Flink's
  `MemoryManager`; needs a DataFusion `MemoryPool` with named per-operator consumers bridged to it
  (comet's model). This is distinct from the FFI Arrow allocator (now one shared, long-lived
  allocator across all operators, as comet does) — accounting is the pool's job, not the allocator's.
- **Mailbox threading** (ticket 01): native execution should integrate with the task mailbox
  (non-blocking), not block the task thread.

## Breadth / longer horizon
- **Arroyo operator coverage tracker** (ticket 11): what remains is async-gated (lookup join, async
  UDF — ticket 01) plus operator feature tails (rank-number / `RANK` / retracting-input Top-N, OVER
  frames / `FIRST_VALUE`/`LAST_VALUE`, proctime variants). (Two-phase cumulative windows, event-time
  joins incl. outer/semi/anti, the non-windowed GROUP BY aggregate, the regular updating join,
  append-only Top-N, keep-first deduplication, window Top-N / window deduplication, and event-time
  sort are now done.)
- **Fully native Kafka source, no JNI** (ticket 33, back burner): subscribe in Rust, decode →Arrow,
  lifting the connector semantics (partition/offset/checkpoint/watermark) from Arroyo. Removes the one
  off-heap copy ticket 32 pays; only worth it once that decode path proves the copy is the bottleneck.
- **Join breadth** (ticket 35): **DONE.** The regular updating join does INNER/LEFT/RIGHT/FULL/SEMI/ANTI +
  a residual non-equi predicate (per-row match-degree, RisingWave's degree table = Flink's
  `numOfAssociations`); the interval and window joins do INNER/LEFT/RIGHT/FULL + a residual non-equi
  predicate (folded into the DataFusion join filter, Arroyo's pattern), with outer null-pads emitted at
  watermark eviction via per-row match tracking. Semi/anti don't arise as time-bounded joins. All
  parity-tested.
- **Columnar sinks + nested boundary types** (ticket 34): the row↔Arrow transpose carries only
  scalar/temporal columns, so native operators that produce a nested `ROW`/`ARRAY`/`MAP` column can't
  hand it to a rowwise sink — complex protobuf/Avro/JSON messages therefore fall back today. The end
  state is custom columnar sinks (no transpose for columnar workflows); the bridge fix is nested-type
  support in the transpose. Lifts the flat-scalar gate on protobuf decode.
