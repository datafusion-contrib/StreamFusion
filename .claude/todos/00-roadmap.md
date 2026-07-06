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
  obscure funcs only. (Shipped 2026-07-03, retiring ticket 42: byte-exact decimal `/`/`%` — a fused
  kernel reproducing Flink's two HALF_UP rounding steps — plus number↔string `CAST`,
  narrowing-`VARCHAR`/cast-to-`CHAR(n)`, and float→`DECIMAL`, all running Flink's own `CastExecutor`
  through the columnar JVM upcall, byte-identical by construction; the tier-3 aggregate items —
  `SUM`/`MIN`/`MAX` `DISTINCT`, decimal `AVG`, single-phase windowed decimal `SUM`/`AVG` — landed the
  same day, with windowed DISTINCT aggregates gated off a wrong-results plain-fold path.)
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
  Top-N also handles an `OFFSET` and a retracting input; `RANK`/`DENSE_RANK` are parity (Flink
  rejects them in streaming).
- **Window aggregate input schemas:** all five aggregates over every numeric value type incl.
  decimal (custom accumulators keep the host's type/precision), single- and two-phase alike —
  the custom SUMs carry Flink's own nullable-sum buffer as their single-field partial, so the
  windowed local/global split admits the full value-type family (AVG stays single-phase; shipped
  2026-07-05, closing ticket 41 — the overflow semantics are Flink's exactly: reset-after-overflow
  for SUM, NULL partials skipped in the merge, pinned by overflow-boundary parity tests);
  multiple value columns (`SUM(a), SUM(b)`); bigint/int/string/boolean/date/timestamp/decimal grouping
  keys; and `COUNT(*)` (one- and two-phase). Parity matrix in `docs/aggregate-type-support.md`.
  Join/`OVER` partition keys now carry the
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
  **event-time sort** (`ORDER BY rowtime`), **deduplication** (all four rowtime/proctime
  keep-first/keep-last variants),
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
  **Sub-plan reuse stays enabled, scoped by digest barriers** (shipped 2026-07-04): every native
  rel carries a per-instance digest term, so Flink's post-optimize reuse can never merge a
  columnar subtree (an island's zero-copy hand-off assumes each Arrow batch is consumed once —
  a merged branch would double-free), while the rowwise prefix (source + Row→RowData conversion)
  merges normally — a multi-view/self-join query reads and converts its source once, not per
  branch (was an exactly-2x conversion tax on q3/q4/q5/q7/q8/q9/q20; fixing it measured q3 +17%,
  q9 +9%, q20 +6% on the generator profile loop).
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
  native row decoder was investigated and rejected — wontdos/28). Only the lone stateless filter stays
  below 1× (its `RowData → Arrow → RowData` round-trip); leave it on the host via the per-operator flag.
- **Native decode-to-Arrow at ingest — done.** File sources (Parquet + ORC) read through DataFusion's
  file scan with the framework owning enumeration/splits/checkpointing, splittable at row-group/stripe
  granularity with projection pushdown; and a streaming decode operator (`NativeBytesDecodeOperator`)
  decodes Kafka bytes → Arrow for JSON, Confluent/bare Avro, CSV, protobuf, and all four CDC JSON
  envelopes — Debezium/OGG, and Maxwell/Canal via the findValue key-presence scan (→ our
  `$row_kind$` changelog); the format-option audit matched the CSV and JSON decode envelopes to
  Flink's converters exactly (divergences/21). The residual tail lives in ticket 32 (CSV/JSON
  *file* sources and the smaller CDC follow-ups).
- **Nexmark matrix vs Flink — running.** The full q0–q22 suite (q6 excluded, wontdos/39) runs
  native-substituted vs stock Flink across four source rungs (generator, Kafka JSON/Avro/Protobuf);
  per-query routed-fraction/fallback reasons via `NexmarkExplainTest`, throughput matrix in the readme.
  It is the standing prioritization + regression gate; the coverage/perf gaps it surfaces feed the
  backlog below.

## Next, roughly in order (re-prioritized 2026-07-04 after the operator profiling round)

0. **Profile-driven operator perf round toward the 5-10x target** (research:
   `.claude/research/nexmark-operator-profiles-2026-07.md` — differential flame graphs of every
   Nexmark query, native vs Flink, plus Arroyo/RisingWave/Proton technique survey and the
   provenance of Alibaba's 5-10x claim). The ranked levers, each its own ticket
   (shipped 2026-07-04: ticket 45 — the forked rowwise prefix, q3 +17%/q9 +9%/q20 +6%; ticket 46 —
   Top-N emit dedup, q19 +13%, and (2026-07-05) net-diff emission under mini-batch plans,
   divergences/20; ticket 47 — typed DISTINCT sets + cached changelog emit, q16 +17%/q17 +4%/q15
   +3%; the
   dedup SipHash item on ticket 20 via the crate-wide ahash default; the q21 upcall regex cache,
   +12.5%; 2026-07-05: the borrowed-byte probes for the group-agg/dedup/Top-N maps, the
   upcall builtins handing off bytes not `String` — q21's residual — and the full
   `ScalarValue`-vintage retirement (the keyed OVER loops +121–162%, retracting Top-N +228%,
   exchange split +208%, keep-first probe +6% on Criterion; ticket 49 retired, residual
   scalar-keyed maps listed on ticket 20). The round's **matrix re-quote landed 2026-07-05**:
   the full standard matrix (readme table — 19 of 23 clear 1× from RowData, q14 newly over the
   line; Kafka floor 1.65×) plus the first **full-suite tuned run** (benchmarks.md — all 23
   native under mini-batch tuning, no fallbacks, 20 of 23 beat tuned Flink): what remains —
   ticket 40's bounded-dim preload (deprioritized on the
   2026-07-05 q13 profile — the Nexmark dim is an in-memory test connector, so the win only shows
   on real external dims). Closed on the 2026-07-05 profiles: the join block store (wontdos/48 —
   the joiner's stored-row decode no longer registers) and paned HOP (wontdos/51 — the two-phase
   split already panes; the residual slice merge is 3–4% of q5).
1. **Native Kafka source: gate FLIPPED (2026-07-03)** (ticket 33). Per-partition watermarks/idleness
   and specific-offsets/topic-pattern startup shipped; `kafkaSource` defaults on and the `kafka`
   cargo feature is a default build feature (probe-guarded for opt-out builds). Remaining tails in
   the ticket: `key.format`, SASL/SSL build features, Linux mimalloc link-alias verification,
   multi-broker measurement. The Kafka matrix was re-run with watermarks flowing mid-stream and the
   readme/benchmarks Kafka numbers re-quoted (2026-07-05 full-matrix run: floor 1.65x,
   peak q11 3.9–5.6x — watermark
   regeneration costs nothing measurable).
2. **Legacy group windows** (ticket 43): map `GROUP BY TUMBLE/HOP(...)` onto the existing native
   window operators — the event-time `SESSION` exception is the template.
3. **Cheap wins, interleaved:** the format-option parity audit (ticket 32). (Shipped 2026-07-03:
   `avro-confluent` routing — registry-fed writer schemas by frame id, decode path; the lookup-join
   extensions: calc/residual/pre-filter/constant-keys + distributed execution, via Flink's own
   generated runners driven per Arrow batch — ticket 40 now holds only the columnar-assembly perf
   item; `ignore-parse-errors` skip mode, native for the JSON-decoded formats incl. CDC, gated to
   fallback for CSV/protobuf; and the decode operator's time-based + pre-barrier flushes.)
4. **Richer columnar endpoints** (ticket 24): beyond local Parquet — Iceberg and remote
   filesystems (`hdfs:`/`s3:`) for the native source/sink; currently `file:` only. **Deferred by
   direction until generalized operator support lands** — broaden what we can run (the ticket 11
   operators and any remaining expression tail) before broadening where we read/write.
5. **Operator-level perf** (ticket 20 backlog): the last scalar-keyed maps — the window Top-N
   ranker, the changelog normalizer, the temporal join, and the mini-batch local aggregate — swap
   to arrow-row keys only with a bench showing it pays. (Everything else now uses arrow-row keys:
   all aggregators — keyed tumbling 2.2×;
   the 2026-07-05 retirement moved the keyed OVER loops, retracting Top-N, keep-first dedup, and
   the exchange split — OVER +121–162%, retracting Top-N +228%, exchange +208% on Criterion;
   session `update` batches gap-connected runs — dense shape 20× vs per-row; the `RowData → Arrow`
   transpose was made row-major + pre-sized, ~25% faster; a native decoder was investigated and
   rejected on benchmark grounds — wontdos/28.)

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
- **Arroyo operator coverage tracker** (ticket 11): what remains is async UDF (ticket 01) plus the
  OVER aggregate tail (`AVG`, `COUNT(*)`, decimal value columns). (Bounded/proctime OVER frames,
  `FIRST_VALUE`/`LAST_VALUE`, proctime interval/window joins, joins incl. outer/semi/anti, the
  non-windowed GROUP BY aggregate, the regular updating join, Top-N incl. rank number / offset /
  retracting input, all four deduplication variants, window Top-N / window deduplication, and
  event-time sort are now done.)
- **Native Kafka source** (ticket 33): built, and now decisively faster than the shallow path on
  every format (divergences/19 — malloc override, `check.crcs` default, callback drain + inline
  decode; raw consume 1.21x the Java client). Remaining before the `kafkaSource` gate can default
  on: per-partition watermarks/idleness, specific-offsets / topic-pattern startup, key.format,
  SASL/SSL build features, Linux `mimalloc` link-alias verification, multi-broker measurement.
- **Nexmark with Apache Fluss as the source** (ticket 36): add Fluss (columnar streaming storage) as a
  fourth source in the Nexmark matrix; its columnar format may let the native island ingest Arrow with
  little/no row transpose — the perimeter transpose is a visible share of the remaining stateful-query
  cost, so Fluss is the source most likely to show the engine's largest end-to-end margin.
- **Native Fluss log source** (ticket 44): the Kafka pattern mirrored onto Fluss — reuse its Flink
  enumerator (dynamic partition discovery included) verbatim, swap the split reader for a JNI reader
  over fluss-rust's Arrow batch scanner. Fluss's log is Arrow on the wire, so ingest can be
  zero-decode/zero-transpose; feasibility confirmed 2026-07-05
  (`.claude/research/fluss-native-source-findings.md`). Enables ticket 36's measurement.
- **Native lookup join** (ticket 40): **coverage DONE** (2026-07-03). The native operators drive
  Flink's own generated lookup runners over each Arrow probe batch, so a processing-time lookup join
  (Nexmark q13) — INNER + LEFT, sync + async, field-ref *and constant* keys, pre-filter, dim-side
  calc, residual condition — is byte-identical to the host by construction and serializes to task
  managers like Flink's own exec node. Async fires a batch's lookups concurrently and awaits on the
  task thread (within-batch model — no operator mailbox). Only the upsert-materialized lookup falls
  back; ticket 40 now holds the columnar-assembly / bounded-dim-preload perf item.
- **Disaggregated state store** (ticket 37): move operator state off-heap to a remote/tiered store
  (likely Fluss's PK-table KV) with a local working-set cache — decoupling state size from worker RAM
  and enabling incremental checkpoints + lazy rescale (Flink 2.0 ForSt / RisingWave Hummock direction).
  The operators' state is already arrow-row bytes, so the stored format is mostly in place.
- **Columnar sinks + remaining boundary types** (ticket 34): the nested-type transpose is **done** —
  the row↔Arrow converter carries `ROW`/`ARRAY`/`MAP`/`VARBINARY`, so nested protobuf/Avro/JSON now
  route. What remains: a custom columnar sink beyond Parquet (no ORC sink / columnar collect yet), and
  the protobuf representation reconciliations still gated (`enum` int-vs-name, unsigned/`fixed` ints,
  `bytes` parity, proto3 missing-field defaults).

## Decided against (records live in `.claude/wontdos/`)
- **Nexmark q6** (wontdos/39): the one query Flink 2.2.1 itself can't run — invalid as written, and
  fixed up it hits a still-present Flink limit (bounded `OVER` over a non-time-attribute sort). No
  host plan to mirror or parity-check; not StreamFusion's to fix.
- **Native row→Arrow transpose / fused keyed shuffle** (wontdos/28): prototyped and rejected on
  benchmark grounds — native decode only ties the honest JVM build before paying JNI costs, and
  Comet builds Arrow on the JVM too. The measurement instead shipped the row-major pre-sized
  converter (~25% faster), which aligns with the reference.
