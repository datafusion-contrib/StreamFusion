# Benchmarks

Acceleration claims in this project are measured, not asserted. This is where the
method and the numbers live.

## Native operator micro-benchmarks

[`native/benches/operators.rs`](../native/benches/operators.rs) measures each native
operator's steady-state hot loop over an in-memory Arrow batch, isolated from the JVM
bridge and from Flink's job scheduling (which otherwise dominates and hides operator
cost). Built on [Criterion](https://github.com/bheisler/criterion.rs).

Run:

```bash
cd native && cargo bench
```

Criterion reports time per batch with a confidence interval and compares against the
previous run, so a regression in a hot loop is visible commit-to-commit. Each bench
declares its row count as throughput, so Criterion also prints elements/s.

Current benches:
- `filter/gt_literal` — the compiled-predicate filter (`v > 0`) over a 4096-row batch,
  half passing. The predicate is compiled once before the loop, so this measures
  evaluation + the Arrow filter kernel, not planning.
- `tumbling/sum_update_flush` — a tumbling `SUM` over 16 windows: one `update` of a
  4096-row batch followed by a `flush` of all closed windows, from fresh state each
  iteration.
- `tumbling/sum_keyed_update_flush` — the same, grouped by a bigint key (64 distinct
  values), so it exercises the per-row grouping-key path the unkeyed bench does not.
  The `_accounted` variant attaches a managed-memory budget, measuring the per-touched-group
  footprint tracking an operator pays when the host hands it one (default off in the plain bench,
  so the unaccounted number is the like-for-like baseline).
- `session/sum_keyed_update_flush` — a session `SUM` grouped by key (gap merge). Its rows are
  spaced beyond the gap, so every row opens its own one-row session — the worst case for session
  state (4096 open sessions). `session/sum_keyed_dense_update_flush` is the complementary shape:
  each key's rows chain within the gap into one long session, the common real workload.
- `over/running_sum_keyed`, `over/row_number_keyed` — the columnar `OVER` push+flush, for a
  running `SUM` (DataFusion accumulator per row) and `ROW_NUMBER` (per-key counter).
- `interval_join/equi_key_push`, `window_join/equi_key_flush` — the two joins with a unique key
  (1:1 match, no cross product), so they measure the DataFusion hash-join construction per batch.

### Results

Numbers are only comparable within a machine; record the host (CPU) alongside. The release
profile pins `codegen-units = 1` (see `native/Cargo.toml`): with the default parallel split,
hot-loop numbers swung ~50% from unrelated code additions elsewhere in the crate, so numbers
measured before the pin (or without it) are not comparable to these.

**Apple M1 Max** (median of 100 Criterion samples):

| Benchmark | Rows/batch | Time/batch | Elements/s | Notes |
|---|---|---|---|---|
| `filter/gt_literal` | 4096 | 2.5 µs | ~1.63 Gelem/s | compiled predicate, ~50% selectivity |
| `tumbling/sum_update_flush` | 4096 | 77 µs | ~53.5 Melem/s | 16 windows, no key |
| `tumbling/sum_keyed_update_flush` | 4096 | 110 µs | ~37.4 Melem/s | 16 windows, 64 bigint keys |
| `tumbling/sum_keyed_update_flush_accounted` | 4096 | 106 µs | ~38.5 Melem/s | same, managed-memory budget attached (≤1% overhead) |
| `interval_join/equi_key_push` | 4096 | 63 µs | ~65 Melem/s | INNER, 1:1, equi-key + interval filter |
| `window_join/equi_key_flush` | 4096 | 130 µs | ~31.5 Melem/s | INNER, 1:1, equi-key + window bounds |
| `over/running_sum_keyed` | 4096 | 515 µs | ~8.0 Melem/s | running aggregate, specialized fold, 64 keys |
| `over/row_number_keyed` | 4096 | 410 µs | ~10.0 Melem/s | per-key counter, 64 keys |
| `session/sum_keyed_update_flush` | 4096 | ~2.2 ms | ~1.9 Melem/s | one-row sessions, 64 keys (high-variance) |
| `session/sum_keyed_dense_update_flush` | 4096 | 101 µs | ~40.4 Melem/s | gap-chained sessions, 64 keys |
| `json_decode/three_field_object` | 4096 | 610 µs | ~6.7 Melem/s | ~46 B docs, simd-json tape walk |
| `json_decode/nexmark_bid_shape` | 4096 | 985 µs | ~4.2 Melem/s | ~210 B docs, 4 of 7 fields skipped |

The gap between filter and aggregation is the signal: the filter is a compiled
expression plus one Arrow kernel, while an aggregator groups every row by its key and
holds per-group accumulator state across batches.
Profiling-driven cuts so far (tumbling, 4096-row batch):

- reusing the per-row window buffer instead of allocating one per row (244 → 181 µs);
- moving the row's key into its last window instead of cloning it for every window
  (181 → 171 µs unkeyed, 395 → 323 µs keyed);
- a fast hash (`ahash`) for the grouping map instead of the stdlib SipHash
  (171 → ~106 µs unkeyed, 323 → ~252 µs keyed);
- one codegen unit for the release build (~106 → ~84 µs unkeyed; most operators gained
  10–17%, and the numbers stopped drifting with unrelated code churn);
- the joins stopped rebuilding a full DataFusion `SessionContext` (its entire function
  registry) per pushed batch — a bare `TaskContext` (or the operator's cached pool-wired
  one, when accounted) is all a hash join needs (interval join ~115 → ~63 µs, window join
  ~184 → ~130 µs at equal codegen settings);
- the Kafka JSON/CDC decode swapped arrow-json's scalar tokenizer for a simd-json (SIMD
  stage-1) parse walked straight into typed Arrow builders — ~8% on tiny 3-field documents,
  ~27% on a realistic Nexmark-bid-sized document (1.36 ms → 985 µs; decimal-bearing schemas
  keep the arrow-json raw-literal path for exactness — see `divergences/18`);
- the session aggregator stopped slicing the value column one row at a time: rows are grouped
  per key and segmented (in timestamp order) into gap-connected runs — the connected components
  the row-at-a-time walk would build — so a run pays one `take` + one accumulator update
  (2.04 ms → 217 µs, 9.4×, on the dense gap-chained shape; the one-row-session shape is
  per-session-bound and unchanged). The open-session merge scan also became a bounded
  `BTreeMap` range probe instead of a walk of every open session, which matters when a key
  holds many not-yet-closed sessions;
- the windowed aggregators (tumbling/hopping/cumulative and session) swapped their
  `Vec<ScalarValue>` group keys for the arrow-row memcomparable encoding the non-windowed
  GROUP BY already used: keys are encoded once per batch, the per-batch grouping map holds
  borrowed byte-row views (no per-row allocation), and flush decodes stored keys straight
  back into output columns (keyed tumbling 245 → 110 µs, 2.2×; dense session 217 → 101 µs;
  the managed-memory-accounted variant gained the same).

Net so far: the unkeyed tumbling path is ~3.2× faster (244 → ~77 µs) and the keyed path ~3.6×
(395 → ~110 µs). The scalar `GroupKey` now survives only in the smaller keyed loops (dedup,
`OVER` partitions, Top-N, the exchange split) — candidates for the same row-key swap if their
benches say it pays; see the [profiling
ticket](../.claude/todos/20-profiling-and-benchmarks.md).

The running `OVER` aggregate was the per-row outlier (~2.6 Melem/s, a DataFusion accumulator
`update_batch` + `evaluate` per row); replacing it with a specialized typed running fold —
matching the accumulators exactly (wrapping integer sum, null-skipping) but without the per-row
call — took it to ~8 Melem/s (3×). The session aggregator's dense (gap-chained) shape now runs at
tumbling-level throughput (~40 Melem/s); its sparse shape (~1.9 Melem/s, high-variance) is bound
by genuinely per-session costs — accumulator creation and flush materialization for 4096 one-row
sessions — not by the update loop.

## End to end vs. Flink

`ThroughputBenchmark` (opt-in: `SF_BENCHMARK=true mvn test -Pbench -Dtest=ThroughputBenchmark`)
runs the same query over a large generated source (5M rows; override with `SF_ROWS`) into a
sink, once with native substitution installed and once on stock Flink, single slot. It reports
best-of-3 rows/s for each and the native/Flink ratio. A warmup run absorbs JIT and minicluster
startup so the measured runs reflect execution.

**The `-Pbench` profile is mandatory** — it builds and loads the *release* native library.
Without it, `mvn test` uses the debug build (fast to compile, ~10–20× slower to run), which
makes every native number misleadingly low. (Measured: the columnar copy below ran 0.48× on
the debug build and 3.0× on release — same code.)

| Operator | Query | Flink | Native | Native vs. Flink |
|---|---|---|---|---|
| Parquet copy (columnar source → sink) | `INSERT INTO parquet SELECT * FROM parquet` | 1.35 M rows/s | 6.34 M rows/s | **4.68×** |
| Parquet sink (row source) | `INSERT INTO parquet SELECT *` | 1.23 M rows/s | 2.76 M rows/s | **2.24×** |
| Windowed aggregate over a columnar source | `SUM` by 1s window from a Parquet table | 1.80 M rows/s | 3.29 M rows/s | **1.82×** |
| Interval join (event-time) | `a JOIN b ON a.k=b.k AND a.rt BETWEEN b.rt ± 1s` | 0.37 M rows/s | 0.63 M rows/s | **1.71×** |
| `OVER` running `SUM` (row source) | `SUM(v) OVER (ORDER BY rt)` | 0.91 M rows/s | 1.42 M rows/s | **1.56×** |
| Tumbling (row source) | `SUM` by 1s window | 1.69 M rows/s | 2.10 M rows/s | **1.24×** |
| Filter (`WHERE`) | `SELECT * FROM f WHERE v > 50` | 3.23 M rows/s | 2.41 M rows/s | **0.75×** |

The gain tracks how much of the pipeline stays columnar. Fully-columnar paths lead — the copy
**4.68×**, the windowed aggregate over a columnar source **1.82×**, the event-time interval join
**1.71×** (Flink's interval join is slow; ours delegates the match to a DataFusion hash join). The
**Parquet sink reaches 2.24×** even from a row source: it writes Arrow → Parquet natively and
coalesces batches into size-targeted files
(rolling on a row target / checkpoint) instead of one file per batch, so per-file overhead no longer
scales with batch count — this also lifted the columnar copy (2.61 → 4.68×). Other row-source ops
still pay a `RowData → Arrow` transpose at the input, ~25% cheaper since the converter was made
row-major + pre-sized ([wontdos/28](../.claude/wontdos/28-native-row-transpose-and-shuffle.md)): `OVER`
running `SUM` **1.56×**, tumbling **1.24×**. The lone stateless **filter stays below 1× at 0.75×** —
a single cheap predicate cannot earn back the `RowData → Arrow → RowData` round-trip. A lone operator
crosses 1× once fed by a columnar source or chained with other native operators (no transpose between
them) — the columnar-flow work ([divergences/08](../divergences/08-columnar-flow-transitions.md)).

### How we got these numbers (a profiling lesson)

The first end-to-end numbers were *far* worse — the columnar copy measured **0.45×**, which
made no sense for a zero-transpose pipeline. Rather than tune blindly, we profiled, and the
chain of measurements is worth recording:

1. **Pure-native ceiling**: a Rust-only Parquet copy of 5M rows ran in **0.36s (14 M rows/s)**
   — so native compute was never the bottleneck; the JVM job was ~13× slower than the compute.
2. **Fixed vs. variable**: at 100K rows native and Flink tied (~0.66s, all fixed job overhead);
   the gap only appeared at scale, so it was a per-row/per-batch cost.
3. **Component timing**: the sink's `Native.writeParquet` dominated (**5.8s of 7.3s**), ~17×
   slower per batch than the *same* native write standalone. Export/serialization were
   negligible (the operators chained, so no IPC).
4. **GC ruled out**: a `-verbose:gc` run showed exactly **one** 5.7ms pause — not GC.
5. **Root cause**: the Maven build loaded the **debug** native library (`cargo build`, no
   `--release`). Debug Rust on Parquet byte-encoding is ~10–20× slower. Building release
   (`-Pbench`) moved the copy from **0.45× to 3.19×** — same code.

The lesson is baked into the harness: benchmarks must run under `-Pbench` (release), and
`mvn test` keeps the fast debug build for the correctness loop only.

## Nexmark

The Nexmark suite is the honest end-to-end read: the source is the rowwise `nexmark` datagen (the
wide event row — `event_type` plus nested `person`/`auction`/`bid` structs) and the sink is
`blackhole` (also rowwise), exactly the published Nexmark plan, so a native island pays a
`RowData → Arrow` transpose at the source **and** an `Arrow → RowData` transpose at the sink. We keep
both transposes in the measured path on purpose — a real deployment feeds us rowwise records and
drains to a rowwise sink, so this is the honest number, not the favorable columnar-source/sink case.
Object reuse is on for both engines (a standard tuned-prod setting).

### q0–q4 (rowwise source + blackhole sink)

The first five queries, 2 M events, single slot — `SF_BENCHMARK=true mvn test -Pbench
-Dtest=NexmarkBenchmark`. q1's decimal arithmetic is exact and native by default (Decimal128 multiply
+ a HALF_UP cast to DECIMAL(23,3), matching Flink).

| Query | Shape | Flink | Native | Native vs. Flink |
|---|---|---|---|---|
| q2 | filter `WHERE MOD(auction, 123) = 0` | 1.91 M ev/s | 2.87 M ev/s | **1.50×** |
| q1 | `0.908 * price` (exact decimal) | 1.92 M ev/s | 2.15 M ev/s | **1.12×** |
| q0 | pass-through projection of `bid` fields | 2.00 M ev/s | 2.17 M ev/s | **1.08×** |
| q4 | regular join → `MAX` per auction → `AVG` per category | 1.12 M ev/s | 1.15 M ev/s | **1.03×** |
| q3 | regular (updating) join `auction ⋈ person` on seller | 2.93 M ev/s | 1.57 M ev/s | **0.54×** |

**q0/q1/q2 beat stock Flink** even on the rowwise perimeter. Four changes got them there, all profiled
on q0: disabling Arrow's per-accessor bounds/refcount checks (deployment flag); object reuse (drops
Flink's per-handoff defensive copy); a zero-copy `ColumnarRowData` at the exit transpose; and — the big
one — **nested projection pushdown at the entry transpose**, which converts only the columns and struct
sub-fields the calc reads rather than the whole wide row, so unread structs never touch Arrow. That
roughly doubled native throughput and was the difference between ~0.6× and >1×.

**q4 reaches parity** (0.69→1.03×): its join is a *regular* updating join (the `B.dateTime BETWEEN
A.dateTime AND A.expires` bound is a data column, not an interval) feeding two `GROUP BY`s. Batching the
INNER join's whole input (one columnar residual-predicate eval, emit by `filter_record_batch`, rows
moved into state rather than re-cloned) removed the per-pair `ScalarValue` and clone churn. **q3 stays
below 1×**: the same regular join but with *unbounded, ever-growing* state (one popular seller matching
many auctions), and the residue is the per-row state store — a fresh `OwnedRow` per buffered row where
Flink reuses pooled `BinaryRowData`. A free-list allocator for the keyed-multiset buffers is the next
lever ([divergences/08](../divergences/08-columnar-flow-transitions.md)).

### q0–q2 from a Kafka source (native decode)

The native decoder is itself a (Rust) bytes→Arrow transpose. Flink does **not** push projection into
the Kafka scan, so its format decodes the whole record; we push the query's projection into the decode
so it builds only the read columns/fields. `SF_BENCHMARK=true mvn test -Pbench
-Dtest=NexmarkKafkaBenchmark` (Testcontainers Kafka). 2 M events, native decode vs Flink's own format:

| Query | JSON (Flink → Native) | Avro (Flink → Native) | Protobuf (Flink → Native) |
|---|---|---|---|
| q0 pass-through | 0.67 → 0.86 M ev/s — **1.27×** | 0.81 → 1.33 M ev/s — **1.64×** | 1.15 → 1.45 M ev/s — **1.26×** |
| q1 currency | 0.77 → 0.85 M ev/s — **1.10×** | 0.82 → 1.34 M ev/s — **1.63×** | 1.14 → 1.49 M ev/s — **1.30×** |
| q2 filter | 0.80 → 0.93 M ev/s — **1.17×** | 0.83 → 1.52 M ev/s — **1.83×** | 1.17 → 1.60 M ev/s — **1.36×** |

**Every format now clears 1× (JSON 1.1–1.3×, Avro 1.6–1.8×, Protobuf 1.3×) — each after attacking
what its profile said it was bound by.** All formats share a large Kafka-I/O + thread-sync cost
(~38–45%) with the Flink run; the decode itself is bound by different work. **JSON was
tokenize-bound** (~19% of CPU in `arrow-json`'s scalar tape parse of the whole document, only ~5%
building the Arrow arrays — so projection pruning couldn't help, and Flink's mature deserializer held
it to ~parity, 0.97–1.02×); swapping the tokenizer for a **simd-json** SIMD parse walked straight
into Arrow builders ([divergences/18](../divergences/18-simd-json-decode.md)) lifted it to
1.10–1.27×. **Avro is build/copy-bound** (~27% `memmove` + ~15% decode, of which `append_null` for
the mostly-null `person`/`auction` union branches was ~15% alone — pushing the projection into the
decode removed that build/copy of unread fields). **Protobuf** is also build/copy-bound (~25%
`memmove` + ~16% ptars decode); pruning via a **pruned descriptor** (ptars builds a column per
descriptor field and skips wire tags it has no field for) flipped it from 0.88–0.94× to 1.26–1.36×.

### The row→columnar ladder (Kafka)

How far into Rust the source-side work moves, on the same q0/q1/q2 over the same produced bytes, all vs
stock Flink. Three rungs, each one layer more native (projection pushed in at every rung that can):

1. **JVM transpose** — Flink consumes *and* decodes to `RowData` with its own format, then a JVM
   `RowData → Arrow` transpose feeds the native calc.
2. **Rust transpose, JVM poll** — Flink's `KafkaSource` polls raw bytes, a native operator decodes them
   straight to Arrow (the shallow decode path).
3. **Rust poll + Rust transpose** — the native rdkafka source: Rust owns the consume *and* the decode.
   No Flink Kafka client, no `RowData`.

`SF_BENCHMARK=true mvn test -Pbench -Dnative.cargo.args="build --release --features kafka"
-Dtest=NexmarkKafkaLadderBenchmark`. 2 M events, ×vs stock Flink (best rung **bold**):

| Format | Flink (ev/s) | JVM transpose | Rust transpose, JVM poll | Rust poll + Rust transpose |
|---|---|---|---|---|
| JSON q0 | 0.75 M | 1.07× | 1.22× | **2.07×** |
| JSON q1 | 0.78 M | 1.03× | 1.12× | **1.96×** |
| JSON q2 | 0.82 M | 1.07× | 1.13× | **1.97×** |
| Avro q0 | 0.90 M | 0.98× | 1.59× | **2.43×** |
| Avro q1 | 0.90 M | 0.92× | 1.49× | **2.47×** |
| Avro q2 | 0.88 M | 1.07× | 1.67× | **2.63×** |
| Protobuf q0 | 1.20 M | 1.07× | 1.28× | **2.05×** |
| Protobuf q1 | 1.23 M | 0.99× | 1.21× | **1.98×** |
| Protobuf q2 | 1.23 M | 1.17× | 1.35× | **2.03×** |

**The full native source is the best rung on every format — roughly 2× stock Flink** and 1.5–1.7×
the shallow decode rung. An earlier version of this table had the source rung *trailing* the
shallow rung on Avro/Protobuf, capped at a ~1.35 M ev/s ceiling; the consume fast path
(divergences/19 — one-lock callback drain, inline decode instead of a decode thread, metadata
warm-up before assign, and the `check.crcs` default) removed that ceiling, and the source rung
now runs 2.2–2.5 M ev/s end to end. These numbers are the default build — the benchmark-grade
allocator override (another ~19% of raw consume) is not included.

**Reference — the transpose floor (no Kafka).** The same q0/q1/q2 with the source replaced by the
in-process `nexmark` datagen emitting `RowData` directly — no Kafka client, no format decode, just the
columnar island over a free source and `blackhole` sink (`-Dtest=NexmarkBenchmark`). The ceiling for
what columnar execution buys when I/O and decode are free:

| Query | Flink (RowData) | Native (JVM transpose, no decode) | speedup |
|---|---|---|---|
| q0 pass-through | 1.93 M ev/s | 2.11 M ev/s | **1.09×** |
| q1 currency | 1.76 M ev/s | 1.97 M ev/s | **1.12×** |
| q2 filter | 1.75 M ev/s | 2.84 M ev/s | **1.62×** |

Both engines run 2–3× faster in absolute ev/s than any Kafka rung — that gap is exactly the Kafka
consume + decode the ladder is about. The native speedup is pure columnar execution: modest on the
projections (transpose-bound) and large on the filter (native discards rows in Arrow before they are
ever materialized to `RowData`).

### The full accelerating set, every source

`NexmarkMatrixBenchmark` runs **every query StreamFusion accelerates** (q0–q5, q7–q23 — only q6 is out;
see [.claude/wontdos/39-nexmark-q6-exclusion.md](../.claude/wontdos/39-nexmark-q6-exclusion.md)) over **every
source it can be fed by** — the rowwise generator, a local Parquet file, and Kafka json/avro/protobuf
across the ladder — all vs stock Flink, same steelmanned perimeter. 500K events.

`SF_BENCHMARK=true mvn test -Pbench -Dnative.cargo.args="build --release --features kafka"
-Dtest=NexmarkMatrixBenchmark` (Testcontainers Kafka; native source needs the `kafka` feature). Column
toggles: `SF_MATRIX_GENERATOR` / `SF_MATRIX_PARQUET` / `SF_MATRIX_KAFKA` (`false` skips one).

The matrix runs with the native managed-memory cap **in force**: the shared test cluster declares a
deployment-like managed-memory size (flink-test-utils' default gave each slot ~10 MB, which the
accounted updating joins outgrow at 500K events; a real TaskManager's 40%-of-process managed memory
holds that state easily, so the benchmark cluster is sized to match). Reserving managed memory is
bookkeeping, not allocation — the budget costs nothing until state actually grows into it.

All the stateful operators run **columnar on Arrow byte-state**: Top-N, keep-last dedup, the updating
join, and the group/`DISTINCT` and windowed (tumbling/hopping/cumulative/session) aggregates key and
buffer their state as memcomparable arrow-row bytes (à la RisingWave's value-encoded state + Arroyo's
`RowConverter`), not boxed `Vec<ScalarValue>`.

_Numbers are one **combined run** — every query in a single JVM, best of 2 after a warmup, 500K events.
A combined run accumulates heap/GC pressure that disproportionately slows the alloc-heavier native side,
so these **understate** native for the aggregate/dedup queries; it is the conservative read._

**Generator** (the transpose floor — no I/O, no decode), native vs Flink, sorted by speedup (q21 appears
twice — the byte-parity default and the opt-in native regex/case path, see † below):

| Query | Shape | Native vs. Flink |
|---|---|---|
| q11 | session-window `COUNT` per bidder | **2.73×** |
| q12 | proctime tumble `COUNT` per bidder | **1.53×** |
| q0 | pass-through projection of `bid` | **1.34×** |
| q7 | tumble `MAX` ⋈ bid | **1.32×** |
| q5 | Hot Items (window re-agg + window join) | **1.32×** |
| q4 | regular join → `MAX` → `AVG` per category | **1.29×** |
| q2 | filter `WHERE MOD(auction, 123) = 0` | **1.27×** |
| q1 | `0.908 * price` — exact `Decimal128` (byte-parity) | **1.17×** |
| q15 | multi-`DISTINCT` `COUNT`s per day | **1.14×** |
| q22 | `SPLIT_INDEX(url, '/', n)` projection | **1.09×** |
| q14 | `HOUR`/`CASE` + `count_char` UDF + decimal | **1.00×** |
| q10 | `DATE_FORMAT` projection | 0.99× |
| q17 | group agg + `AVG`/`MIN`/`MAX`/`SUM` per day | 0.99× |
| q9 | regular join → `ROW_NUMBER` (≤ 1) | 0.99× |
| q13 | lookup join (bounded dimension) | 0.96× |
| q19 | `ROW_NUMBER` topN (≤ 10) | 0.92× |
| q8 | tumble windowed-distinct ⋈ join | 0.92× |
| q23 | three-way join `bid ⋈ person ⋈ auction` | 0.87× |
| q16 | multi-`DISTINCT` per channel/day | 0.84× |
| q18 | `ROW_NUMBER` dedup (≤ 1) | 0.82× |
| q3 | updating join `auction ⋈ person` | 0.80× |
| q20 | updating join (`category = 10`) | 0.73× |
| q21 | `CASE` + `REGEXP_EXTRACT`/`LOWER` — JVM upcall (byte-parity) | 0.71× |
| q21 † | …same, pure-native Rust regex/case (opt-in, non-parity) | **1.50×** |

**Parquet file** — the columnar-source case: the native island reads Arrow straight from the
`filesystem`/`parquet` scan, so there is no `RowData → Arrow` transpose at ingest (only the sink
transpose remains). Same queries, same order as the generator table above:

| Query | Native vs. Flink | | Query | Native vs. Flink |
|---|---|---|---|---|
| q8 | **4.60×** | | q23 | **2.15×** |
| q3 | **4.21×** | | q10 | **1.98×** |
| q12 | **4.05×** | | q13 | **1.76×** |
| q2 | **3.69×** | | q15 | **1.52×** |
| q1 | **3.67×** | | q17 | **1.44×** |
| q0 | **3.40×** | | q9 | **1.35×** |
| q14 | **3.22×** | | q19 | **1.17×** |
| q5 | **2.92×** | | q18 | **1.10×** |
| q22 | **2.92×** | | q16 | 0.85× |
| q11 | **2.88×** | | | |
| q20 | **2.84×** | | q21 | **1.58×** (5.68× native regex/case) |
| q7 | **2.41×** | | | |
| q4 | **2.34×** | | | |

Every query but q16 clears 1× by a wide margin — **2–4.6×** — because the ingest transpose is gone: the
scan feeds Arrow batches directly into the operator, and only the `blackhole` sink pays a transpose.
The queries that are transpose-bound on the generator (q8 at 0.92×, q3 at 0.80×, q20 at 0.73×) are
exactly the ones that jump the most here (q8 4.60×, q3 4.21×, q20 2.84×) — confirming their generator
cost was the `RowData` perimeter, not the operator. Parquet's rowtime is a plain `TIMESTAMP(3)`, so the
`DATE_FORMAT`/`HOUR` queries (q10/q14/q15/q16/q17) run natively (over the Kafka `TIMESTAMP_LTZ` they run
natively too now — see the Kafka table's `§` note). Only q16's multi-`DISTINCT` accumulator (still
`ScalarValue`-boxed) stays below 1×.

**Ten clear 1.0× even on this conservative combined run, and another seven (q8/q9/q10/q13/q14/q17/q19)
sit within noise of parity.** The window-aggregate queries moved when the aggregators went to
arrow-row keys and the session update went run-batched: **q5 1.00→1.32, q8 0.70→0.92, q11
2.41→2.73**. The **updating-join family was the earlier big mover**: a CPU profile put ~40% of
the worst query (q9) in the joiner. Making the INNER join batch its whole input — gather all candidate
pairs against the fixed probe side, evaluate the residual predicate once columnar, emit by
`filter_record_batch`, and move rows into state instead of re-cloning — lifted **q9 0.39→0.97, q4
0.64→1.07, q7 0.91→1.37, q23 0.66→0.96**. The streaming Top-N shed its allocator churn (defer the
per-row `owned()` until a row enters, share the with-rank cascade's repeat-emitted rows via `Arc`):
**q19 0.77→0.91**. The lever throughout was a differential profile's clearest signal — on every
changelog operator native spends 10–22% of CPU in the system allocator where Flink spends ~1% (Flink
reuses pooled `BinaryRowData`, its cost landing in GC). Cutting those allocations, not swapping the
allocator, closed the gap ([divergences/08](../divergences/08-columnar-flow-transitions.md)).

What still trails 1× is three distinct residues: q8 is transpose-bound (a window join with only a ~9%
native island — its windowed-distinct half got 0.70→0.92 from the arrow-row keys, the rest is the
perimeter); q16's multi-`DISTINCT` accumulator still churns `ScalarValue`; and q20/q3 are wide
updating joins whose remaining cost is the per-row state store that Flink pools.

**† q21 is reported on both paths.** By default its `REGEXP_EXTRACT` and `LOWER` run through a
byte-identical **JVM upcall** (one JNI crossing per batch) — the **0.76×** row, the price of staying
exactly Flink-equal on functions whose Rust regex / case-folding can diverge at a locale/regex edge.
`-Dstreamfusion.expression.allowIncompatible=true` runs them on the **pure-native Rust** path at
**1.57×** — a 2× swing, and the honest cost of the guarantee. Both are documented in
[divergences/07](../divergences/07-expression-encoding-and-compile-once.md).

**‡ q1's approximate-decimal toggle buys nothing.** The exact `Decimal128` multiply (byte-parity) is not
the bottleneck, so the approximate `double` path measures within noise of it (occasionally slower in a
combined run) — exact-by-default costs nothing and the non-parity toggle isn't worth enabling. Reported
as a single row.

**§ `DATE_FORMAT`/`HOUR` over the Kafka `TIMESTAMP_LTZ` now runs natively** (q10/q14/q15/q16/q17 — these
were skipped here before). The default routes the LTZ case through Flink's own zone-aware datetime code
via a JVM upcall (byte-parity); a pure-Rust `chrono-tz` path is opt-in under `allowIncompatible` but
measures within noise (the datetime call isn't the bottleneck), so parity is free — see
[divergences/17](../divergences/17-ltz-datetime-session-zone.md). Reported as a single row.

**Kafka**, best rung per format (native speedup vs that format's own Flink baseline; rung in parens —
`jvm` = JVM transpose, `decode` = Rust decode / JVM poll, `source` = full native rdkafka source), sorted
by the JSON speedup:

| Query | JSON | Avro | Protobuf |
|---|---|---|---|
| q11 | **1.95×** (decode) | **2.18×** (decode) | **2.21×** (decode) |
| q7 | **1.30×** (jvm) | **1.39×** (decode) | **1.31×** (decode) |
| q0 | **1.25×** (decode) | **1.47×** (decode) | **1.14×** (decode) |
| q4 | **1.22×** (decode) | **1.39×** (decode) | **1.26×** (decode) |
| q1 | **1.19×** (decode) | **1.41×** (decode) | **1.16×** (decode) |
| q15 § | **1.18×** (decode) | **1.35×** (decode) | **1.09×** (decode) |
| q9 | **1.15×** (jvm) | **1.07×** (jvm) | **1.21×** (jvm) |
| q2 | **1.14×** (decode) | **1.43×** (decode) | **1.19×** (decode) |
| q22 | **1.13×** (jvm) | **1.52×** (decode) | **1.20×** (decode) |
| q5 | **1.11×** (decode) | **1.45×** (decode) | **1.32×** (decode) |
| q16 § | **1.10×** (jvm) | **1.07×** (jvm) | **1.01×** (jvm) |
| q23 | **1.10×** (decode) | **1.42×** (decode) | **1.27×** (jvm) |
| q12 | **1.09×** (decode) | **1.51×** (decode) | **1.23×** (decode) |
| q8 | **1.08×** (decode) | **1.30×** (decode) | **1.12×** (decode) |
| q20 | **1.07×** (jvm) | **1.39×** (decode) | **1.12×** (decode) |
| q21 | **1.04×** (source) | **1.15×** (decode) | 0.94× (decode) |
| q21 † | **1.21×** (decode) | **1.63×** (decode) | **1.41×** (decode) |
| q10 § | **1.03×** (decode) | **1.18×** (decode) | **1.03×** (decode) |
| q13 | **1.00×** (jvm) | **1.24×** (decode) | **1.02×** (decode) |
| q17 § | **1.00×** (jvm) | **1.25×** (decode) | **1.07×** (decode) |
| q19 | **1.00×** (jvm) | **1.17×** (jvm) | **1.17×** (jvm) |
| q14 § | 0.97× (jvm) | **1.28×** (decode) | **1.07×** (decode) |
| q3 | 0.95× (decode) | **1.26×** (decode) | **1.10×** (decode) |
| q18 | 0.94× (jvm) | **1.14×** (decode) | 0.96× (decode) |

Two things the Kafka columns add: **the source rung compounds the operator verdict** — the Rust decode
stacks on top of the operator work (q11 reaches **2.2×** on the binary formats and **1.95×** on JSON;
several queries that trailed on the bare generator turn clearly positive once the decode saving is
added). The JSON column was re-measured after the decoder swapped arrow-json's scalar tokenizer for a
simd-json parse walked straight into Arrow builders (`divergences/18`): previously the JVM-transpose
rung led nearly every JSON row (the Rust decode was tokenize-bound at ~parity); now the Rust decode is
JSON's best rung on most queries, the same shape as the binary formats. And **the changelog-heavy
queries still win on the JVM-transpose rung** (q9/q19: a compute/emit-bound operator gets no lift from
decoding faster, it only fills sooner) — native decode is the lever for source- and aggregate-bound
queries, and a no-op (not a hazard) for changelog-bound ones.

_Apple M1 Max; numbers are comparable only within a machine._
