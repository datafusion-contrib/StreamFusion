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
- `local_group_by_logical_bundle/logical/*` — local two-phase `SUM` over 64 hot keys with
  logical bundle sizes 1, 32, 256, 4,096, and 50,000. Size 1 is the immediate/non-coalescing
  baseline; `physical_batch` pins the former one-flush-per-Arrow-batch behavior.
- `group_by_logical_bundle/*` — single-phase `SUM` over 64 hot keys, comparing per-row changelog
  construction, one final diff per 256-row physical batch, and one diff per 4,096-row bundle.
- `session/sum_keyed_update_flush` — a session `SUM` grouped by key (gap merge). Its rows are
  spaced beyond the gap, so every row opens its own one-row session — the worst case for session
  state (4096 open sessions). `session/sum_keyed_dense_update_flush` is the complementary shape:
  each key's rows chain within the gap into one long session, the common real workload.
- `over/running_sum_keyed`, `over/row_number_keyed`, `over/bounded_rows_sum_keyed` — the columnar
  `OVER` push+flush, for a running `SUM` (specialized fold), `ROW_NUMBER` (per-key counter), and a
  bounded `ROWS 10 PRECEDING` frame (per-key buffer + frame recompute).
- `retract_topn/{immediate,physical_256,logical_4096}` — the retracting Top-N (changelog input,
  full buffers): steady-state inserts into 64 pre-populated partitions, comparing per-row output,
  a diff after every 256-row Arrow batch, and one diff over the logical 4,096-row bundle.
- `append_topn_logical_bundle/*` — append-only ascending Top-10 over 64 partitions with sustained
  boundary churn. It compares the per-record cascade, a net diff after each 256-row physical
  batch, and one net diff over the 4,096-row logical bundle, with and without projected rank.
- `unique_updating_join_logical_bundle/{immediate,physical_256,logical_4096}` — an INNER join whose
  two join keys are unique, under a left-side replacement storm over 64 keys; compares per-record
  output, physical-batch transition folding, and one logical-bundle transition per key.
- `dedup/keep_first_emitted_probe` — keep-first dedup in its steady state: all 256 keys already
  emitted, so each row is one emitted-set probe and a drop.
- `exchange/split_by_key_8` — the columnar shuffle's by-key split: hash each row's key to one of
  8 partitions and gather the sub-batches.
- `interval_join/equi_key_push`, `window_join/equi_key_flush` — the two joins with a unique key
  (1:1 match, no cross product), so they measure the DataFusion hash-join construction per batch.
- `date_format/compiled` vs `date_format/per_row_parse` — the DATE_FORMAT hot loop after and
  before the compile-once change (pattern parsed once vs re-parsed inside every row's Display),
  kept as an A/B pair so the win stays visible.

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
| `over/running_sum_keyed` | 4096 | 183 µs | ~22.3 Melem/s | running aggregate, specialized fold, 64 keys |
| `over/row_number_keyed` | 4096 | 131 µs | ~31.4 Melem/s | per-key counter, 64 keys |
| `over/bounded_rows_sum_keyed` | 4096 | 452 µs | ~9.1 Melem/s | ROWS 10 PRECEDING frame recompute, 64 keys |
| `retract_topn/immediate` | 4096 | 3.29 ms | ~1.24 Melem/s | changelog Top-N, per-row before/after diff |
| `dedup/keep_first_emitted_probe` | 4096 | 16 µs | ~255 Melem/s | steady state: every key already emitted |
| `exchange/split_by_key_8` | 4096 | 57 µs | ~72 Melem/s | by-key split into 8 partitions |
| `session/sum_keyed_update_flush` | 4096 | ~2.2 ms | ~1.9 Melem/s | one-row sessions, 64 keys (high-variance) |
| `session/sum_keyed_dense_update_flush` | 4096 | 101 µs | ~40.4 Melem/s | gap-chained sessions, 64 keys |
| `date_format/compiled` | 4096 | 378 µs | ~10.8 Melem/s | pattern compiled once (`per_row_parse` pins the old loop at 670 µs) |
| `json_decode/three_field_object` | 4096 | 610 µs | ~6.7 Melem/s | ~46 B docs, simd-json tape walk |
| `json_decode/nexmark_bid_shape` | 4096 | 985 µs | ~4.2 Melem/s | ~210 B docs, 4 of 7 fields skipped |

Local GROUP BY count-boundary baseline on the same Apple M1 Max (median of 100 Criterion samples,
64 bigint keys):

| Logical rows/bundle | Rows/iteration | Time/iteration | Elements/s | vs size 1 |
|---:|---:|---:|---:|---:|
| 1 | 4,096 | 4.250 ms | 0.964 M | 1.00× |
| 32 | 4,096 | 960.2 µs | 4.266 M | 4.43× |
| 256 | 4,096 | 413.0 µs | 9.918 M | 10.29× |
| 4,096 | 4,096 | 230.7 µs | 17.751 M | 18.42× |
| physical Arrow batch (4,096) | 4,096 | 229.7 µs | 17.829 M | 18.50× |
| 50,000 | 50,000 | 2.721 ms | 18.373 M | 19.06× |

The exact logical 4,096-row path and the old physical-batch path are statistically equivalent,
showing that the boundary controller adds no measurable kernel overhead when boundaries coincide.
The curve also quantifies the opportunity: output materialization and per-bundle setup dominate
small bundles, while throughput plateaus around 4K rows for this 64-key shape.

Single-phase GROUP BY on the same Apple M1 Max (median of 100 Criterion samples, 4,096 rows, 64
bigint keys, 256-row physical batches):

| Mode | Time/iteration | Elements/s | Logical speedup |
|---|---:|---:|---:|
| per-row immediate changelog | 568.1 µs | 7.210 M | 3.25× |
| net diff per physical batch | 414.3 µs | 9.886 M | 2.37× |
| net diff per logical bundle | 174.9 µs | 23.414 M | 1.00× |

The logical path retains only Arrow key-buffer references plus one first output tuple per touched
group, mutates durable accumulators for every row, and materializes final aggregate tuples once.

Append-only Top-N logical-bundle baseline on the same Apple M1 Max (median of 100 Criterion
samples, 4,096 descending values across 64 partitions, ascending Top-10):

| Mode | Rank projected | Time/iteration | Elements/s | Logical vs mode |
|---|---:|---:|---:|---:|
| per-record cascade | no | 903.0 µs | 4.536 M | 1.40× |
| net diff per 256-row physical batch | no | 1.740 ms | 2.354 M | 2.70× |
| net diff per 4,096-row logical bundle | no | 645.6 µs | 6.345 M | 1.00× |
| per-record cascade | yes | 1.925 ms | 2.128 M | 3.41× |
| net diff per 256-row physical batch | yes | 1.290 ms | 3.176 M | 2.28× |
| net diff per 4,096-row logical bundle | yes | 565.1 µs | 7.248 M | 1.00× |

The physical-diff baseline deliberately reproduces the former Arrow-batch-sensitive cadence. A
five-second release sample of `logical_diff_rank` attributes most samples to `TopNRanker::push`,
with `arrow_row::Row::owned` and allocator traffic prominent; after output coalescing, row
ownership in state mutation is the next measured target.

Retracting Top-N logical-bundle baseline on the same Apple M1 Max (20 Criterion samples, 4,096
steady-state inserts across 64 pre-populated partitions, Top-10, 256-row physical batches):

| Mode | Time/iteration | Elements/s | Logical vs mode |
|---|---:|---:|---:|
| immediate per-row diff | 3.292 ms | 1.244 M | 2.54× |
| net diff per 256-row physical batch | 1.926 ms | 2.127 M | 1.48× |
| net diff per 4,096-row logical bundle | 1.297 ms | 3.158 M | 1.00× |

All modes mutate the same full retracting buffers. The logical mode retains one visible-window
preimage per touched partition and emits only its final membership/rank transition.

Unique-key updating-join logical-bundle baseline on the same Apple M1 Max (20 Criterion samples,
4,096 left-side delete/insert rows over 64 keys matched by a stable unique right side):

| Mode | Time/iteration | Elements/s | Logical vs mode |
|---|---:|---:|---:|
| immediate per-record join changelog | 455.9 µs | 8.985 M | 3.08× |
| net transitions per 256-row physical batch | 541.1 µs | 7.570 M | 3.66× |
| net transitions per 4,096-row logical bundle | 147.9 µs | 27.692 M | 1.00× |

The first profiled logical implementation measured 15.03 M rows/s. Its sample was dominated by
allocating and freeing an owned key for every repeated staging-map update; borrowed-key probing
raised the final result to 27.69 M rows/s while leaving durable payload ownership unchanged.

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
(395 → ~110 µs). The 2026-07-05 round retired the remaining scalar-keyed loops onto the same
arrow-row byte state: the three keyed `OVER` loops (running sum 422 → 183 µs, ROW_NUMBER
342 → 131 µs, bounded frame 688 → 452 µs), the retracting Top-N (10.2 → 3.1 ms — byte sort keys
replace the scalar comparator, `Arc`-shared payloads make the per-row before/after snapshots
refcount bumps), keep-first dedup's emitted set (+6%), and the exchange split (174 → 57 µs,
hashing the encoded key bytes). The last scalar-keyed maps (window Top-N, changelog normalizer,
temporal join, mini-batch local aggregate) are bench-gated candidates on the [perf backlog
issue](https://github.com/datafusion-contrib/StreamFusion/issues/14).

The running `OVER` aggregate was the per-row outlier (~2.6 Melem/s, a DataFusion accumulator
`update_batch` + `evaluate` per row); replacing it with a specialized typed running fold —
matching the accumulators exactly (wrapping integer sum, null-skipping) but without the per-row
call — took it to ~8 Melem/s (3×), and the arrow-row key swap above to ~22 Melem/s. The session
aggregator's dense (gap-chained) shape runs at
tumbling-level throughput (~40 Melem/s); its sparse shape (~1.9 Melem/s, high-variance) is bound
by genuinely per-session costs — accumulator creation and flush materialization for 4096 one-row
sessions — not by the update loop.

## End to end vs. Flink

`ThroughputBenchmark` (opt-in: `SF_BENCHMARK=true mvn -pl :streamfusion-runtime test -Pbench -Dtest=ThroughputBenchmark`)
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
| Parquet copy (columnar source → sink) | `INSERT INTO parquet SELECT * FROM parquet` | 1.51 M rows/s | 19.4 M rows/s | **12.85×** |
| Parquet sink (row source) | `INSERT INTO parquet SELECT *` | 1.26 M rows/s | 4.12 M rows/s | **3.26×** |
| Windowed aggregate over a columnar source | `SUM` by 1s window from a Parquet table | 1.80 M rows/s | 3.29 M rows/s | **1.82×** |
| Interval join (event-time) | `a JOIN b ON a.k=b.k AND a.rt BETWEEN b.rt ± 1s` | 0.37 M rows/s | 0.63 M rows/s | **1.71×** |
| `OVER` running `SUM` (row source) | `SUM(v) OVER (ORDER BY rt)` | 0.91 M rows/s | 1.42 M rows/s | **1.56×** |
| Tumbling (row source) | `SUM` by 1s window | 1.69 M rows/s | 2.10 M rows/s | **1.24×** |
| Filter (`WHERE`) | `SELECT * FROM f WHERE v > 50` | 3.23 M rows/s | 2.41 M rows/s | **0.75×** |

The gain tracks how much of the pipeline stays columnar. Fully-columnar paths lead — the copy
**12.85×**, the windowed aggregate over a columnar source **1.82×**, the event-time interval join
**1.71×** (Flink's interval join is slow; ours delegates the match to a DataFusion hash join). The
**Parquet sink reaches 3.26×** even from a row source: it encodes Arrow → Parquet natively and
rolls part files exactly like the host (on checkpoint and on the configured size/time policies —
the sink now runs inside Flink's own streaming file writer, so file lifecycle overhead matches the
host's and the entire margin is the encoding), which also lifted the columnar copy (4.68 → 12.85×:
the old sink rolled a file per million rows; checkpoint-driven rolling writes one). Other row-source ops
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

The first five queries, 2 M events, single slot — `SF_BENCHMARK=true mvn -pl :streamfusion-runtime test -Pbench
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
so it builds only the read columns/fields. `SF_BENCHMARK=true mvn -pl :streamfusion-runtime test -Pbench
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
3. **Rust poll + Rust transpose** — the production native source: rdkafka consumes and the separately
   installed format artifact decodes inside the same poll call, dispatched through the versioned
   cross-DSO driver ABI (divergences/25).

`SF_BENCHMARK=true mvn -pl :streamfusion-runtime test -Pbench -Dnative.cargo.args="build --release --features mimalloc,kafka,json,avro,protobuf"
-Dtest=NexmarkKafkaLadderBenchmark`. 2 M events (2026-07-12), ×vs stock Flink (best rung **bold**; the
`mimalloc` feature — the recommended Kafka build — link-aliases the library's allocator, worth
+12–22% on the source rung, divergences/19):

| Format | Flink (ev/s) | JVM transpose | Rust transpose, JVM poll | Rust poll + Rust transpose |
|---|---|---|---|---|
| JSON q0 | 0.79 M | 1.04× | 1.20× | **2.30×** |
| JSON q1 | 0.78 M | 1.09× | 1.16× | **2.33×** |
| JSON q2 | 0.79 M | 1.07× | 1.21× | **2.41×** |
| Avro q0 | 0.89 M | 1.02× | 1.59× | **3.00×** |
| Avro q1 | 0.88 M | 0.99× | 1.62× | **3.02×** |
| Avro q2 | 0.87 M | 1.06× | 1.73× | **3.22×** |
| Protobuf q0 | 1.26 M | 1.03× | 1.22× | **2.09×** |
| Protobuf q1 | 1.23 M | 1.06× | 1.26× | **2.31×** |
| Protobuf q2 | 1.21 M | 1.15× | 1.35× | **2.38×** |

The full native source is the best rung on every format — **2.1–3.2× stock Flink**, 1.8–2.9 M ev/s
end to end — measurably *faster* than the pre-split fused source on the same machine (JSON q0 2.30×
vs 1.94× re-measured side by side), so the format-artifact modularity now costs nothing. Two caveats
this table's history earned: an early source rung trailed the shallow rung until the consume fast
path landed (divergences/19), and the 2026-07-11 modular split briefly decoded in a downstream
operator, which halved this rung until the in-poll driver-ABI decode restored it (divergences/25).
The matrix harness reads the same BIGINT epoch-millis corpus but declares the Nexmark `WATERMARK`
on its table (this ladder doesn't) — until native per-split source watermarks landed (2026-07-12,
divergences/25), that watermark silently kept the matrix's Kafka scans on Flink entirely, so matrix
Kafka cells of that period measured only the downstream island. Compare rungs within one harness
only, and verify the plan contains the native source before trusting a source rung.

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

`SF_BENCHMARK=true mvn -pl :streamfusion-runtime test -Pbench -Dnative.cargo.args="build --release --features mimalloc,kafka"
-Dtest=NexmarkMatrixBenchmark` (Testcontainers Kafka; the Kafka test build enables its feature, and
`mimalloc` — the recommended build — rebinds the library's allocator, divergences/19). Column
toggles: `SF_MATRIX_GENERATOR` / `SF_MATRIX_PARQUET` / `SF_MATRIX_KAFKA` (`false` skips one), plus
`SF_MATRIX_FLUSS` (`true` *adds* the opt-in Fluss rung — off by default; see below).

The matrix runs with the native managed-memory cap **in force**: the shared test cluster declares a
deployment-like managed-memory size (flink-test-utils' default gave each slot ~10 MB, which the
accounted updating joins outgrow at 500K events; a real TaskManager's 40%-of-process managed memory
holds that state easily, so the benchmark cluster is sized to match). Reserving managed memory is
bookkeeping, not allocation — the budget costs nothing until state actually grows into it.

All the stateful operators run **columnar on Arrow byte-state**: Top-N, keep-last dedup, the updating
join, and the group/`DISTINCT` and windowed (tumbling/hopping/cumulative/session) aggregates key and
buffer their state as memcomparable arrow-row bytes (à la RisingWave's value-encoded state + Arroyo's
`RowConverter`), not boxed `Vec<ScalarValue>`.

### Current release matrix (2026-07-12)

Run with `SF_BENCHMARK=true SF_MATRIX_FLUSS=true mvn -pl :streamfusion-runtime test -Pbench
-Dnative.cargo.args="build --release --features mimalloc,kafka,parquet,json,csv,raw,avro,protobuf,fluss"
-Dtest=NexmarkMatrixBenchmark`. This is one combined 500K-event JVM run, best of two after a warmup;
both engines use object reuse, the default Flink configuration, and the same source bytes. Kafka reports
the complete native poll-and-decode rung rather than an intermediate best-of ladder rung.

| Query | Generator | Parquet | Fluss | Kafka JSON | Kafka Avro | Kafka Protobuf |
|---|---|---|---|---|---|---|
| q0 | **1.46×** | **3.65×** | **2.71×** | **2.64×** | **3.48×** | **2.71×** |
| q1 | **1.22×** | **3.74×** | **2.71×** | **2.70×** | **3.43×** | **2.70×** |
| q2 | **1.35×** | **2.83×** | **2.94×** | **2.16×** | **2.50×** | **2.03×** |
| q3 | 0.98× | **3.46×** | **2.12×** | **2.04×** | **2.10×** | **1.76×** |
| q4 | **1.60×** | **3.66×** | **1.67×** | **2.60×** | **3.06×** | **2.74×** |
| q5 | **1.31×** | **3.92×** | **1.74×** | **2.94×** | **3.74×** | **2.94×** |
| q7 | **1.60×** | **4.37×** | **2.86×** | **3.37×** | **3.99×** | **3.68×** |
| q8 | 0.86× | **4.17×** | **2.56×** | **2.54×** | **2.70×** | **2.63×** |
| q9 | **1.30×** | **1.83×** | **1.60×** | **2.25×** | **2.19×** | **2.20×** |
| q10 | **1.46×** | **3.92×** | **3.21×** | **2.79×** | **2.80×** | **2.26×** |
| q11 | **2.78×** | **5.19×** | **4.06×** | **4.50×** | **5.15×** | **4.64×** |
| q12 | **1.46×** | **3.55×** | — | **2.31×** | **2.60×** | **2.14×** |
| q13 | **1.11×** | **2.60×** | **2.16×** | **2.37×** | **2.61×** | **2.28×** |
| q14 | **1.08×** | **3.40×** | **2.51×** | **2.79×** | **3.29×** | **2.98×** |
| q15 | **1.63×** | **2.26×** | **1.13×** | **2.94×** | **2.87×** | **2.20×** |
| q16 | **1.32×** | **1.42×** | 0.99× | **1.83×** | **1.84×** | **1.50×** |
| q17 | **1.43×** | **1.82×** | **1.20×** | **2.73×** | **2.72×** | **2.43×** |
| q18 | **1.26×** | **2.41×** | **1.68×** | **2.64×** | **3.18×** | **2.99×** |
| q19 | **1.41×** | **1.59×** | **2.71×** | **1.88×** | **1.73×** | **1.77×** |
| q20 | 0.95× | **4.01×** | **2.40×** | **2.81×** | **3.55×** | **2.95×** |
| q21 | **1.08×** | **2.38×** | **1.78×** | **2.52×** | **2.99×** | **2.69×** |
| q21 † | **1.86×** | **5.41×** | **4.32×** | **2.58×** | **3.02×** | **3.00×** |
| q22 | **1.46×** | **4.37×** | **3.09×** | **2.38×** | **2.82×** | **2.54×** |
| q23 | **1.14×** | **4.38×** | **2.30×** | **2.10×** | **2.94×** | **2.26×** |

This table is one combined run taken after the 2026-07-12 hot-path round (batched BinaryRow key
encoding, the transpose's intrinsified string encode, the `DATE_FORMAT` digit renderer, and O(1)
accounted-state sizing — `docs/optimizations.md`), the in-poll driver-ABI Kafka decode
(divergences/25), and native per-split source watermarks. The last of these is what re-quoted the
Kafka columns: the matrix's Kafka table declares the canonical Nexmark watermark, which previously
kept its scans on Flink entirely — the earlier chart's near-parity Kafka cells were measuring
Flink's consume+decode with only the downstream island native. With the watermark regenerated
inside the native source, every Kafka cell wins, 1.50× (q16 protobuf) to 5.15× (q11 Avro). The
generator column reads 20 of 23 wins; the trailers (q3/q8, with q20 just under parity) are the
perimeter-transpose/join-state cluster. All Parquet queries win (floor 1.42×, q16), and every
measurable Fluss cell but q16 (0.99×) is a win. `†` is the non-parity native regex/case path; the
default q21 remains the byte-parity JVM-upcall path.

### Historical matrix (2026-07-05)

The following tables are retained to show the previous fused-source measurements. They are not current
release claims; use the matrix above for current comparisons.

**Generator** (the transpose floor — no I/O, no decode), native vs Flink, sorted by speedup (q21 appears
twice — the byte-parity default and the opt-in native regex/case path, see † below):

| Query | Shape | Native vs. Flink |
|---|---|---|
| q11 | session-window `COUNT` per bidder | **2.79×** |
| q7 | tumble `MAX` ⋈ bid | **1.61×** |
| q12 | proctime tumble `COUNT` per bidder | **1.52×** |
| q19 | `ROW_NUMBER` topN (≤ 10) | **1.50×** |
| q5 | Hot Items (window re-agg + window join) | **1.47×** |
| q15 | multi-`DISTINCT` `COUNT`s per day | **1.42×** |
| q23 | three-way join `bid ⋈ person ⋈ auction` | **1.38×** |
| q16 | multi-`DISTINCT` per channel/day | **1.36×** |
| q0 | pass-through projection of `bid` | **1.33×** |
| q17 | group agg + `AVG`/`MIN`/`MAX`/`SUM` per day | **1.32×** |
| q4 | regular join → `MAX` → `AVG` per category | **1.31×** |
| q2 | filter `WHERE MOD(auction, 123) = 0` | **1.30×** |
| q9 | regular join → `ROW_NUMBER` (≤ 1) | **1.18×** |
| q22 | `SPLIT_INDEX(url, '/', n)` projection | **1.18×** |
| q1 | `0.908 * price` — exact `Decimal128` (byte-parity) | **1.13×** |
| q18 | `ROW_NUMBER` dedup (≤ 1) | **1.13×** |
| q10 | `DATE_FORMAT` projection | **1.11×** |
| q13 | lookup join (bounded dimension) | **1.07×** |
| q14 | `HOUR`/`CASE` + `count_char` UDF + decimal | **1.02×** |
| q21 | `CASE` + `REGEXP_EXTRACT`/`LOWER` — JVM upcall (byte-parity) | 0.96× |
| q3 | updating join `auction ⋈ person` | 0.95× |
| q8 | tumble windowed-distinct ⋈ join | 0.87× |
| q20 | updating join (`category = 10`) | 0.84× |
| q21 † | …same, pure-native Rust regex/case (opt-in, non-parity) | **1.54×**

**Parquet file** — the columnar-source case: the native island reads Arrow straight from the
`filesystem`/`parquet` scan, so there is no `RowData → Arrow` transpose at ingest (only the sink
transpose remains). Same queries, sorted by speedup:

| Query | Native vs. Flink | | Query | Native vs. Flink |
|---|---|---|---|---|
| q11 | **5.39×** | | q12 | **3.23×** |
| q8 | **4.37×** | | q0 | **3.21×** |
| q7 | **4.22×** | | q1 | **3.07×** |
| q23 | **3.91×** | | q10 | **2.54×** |
| q4 | **3.61×** | | q18 | **2.27×** |
| q22 | **3.58×** | | q13 | **2.26×** |
| q3 | **3.57×** | | q17 | **2.23×** |
| q2 | **3.56×** | | q15 | **2.07×** |
| q5 | **3.45×** | | q9 | **1.94×** |
| q20 | **3.40×** | | q19 | **1.75×** |
| q14 | **3.30×** | | q16 | **1.37×** |
| q21 | **2.77×** (6.14× native regex/case) | | | |

Every query clears 1× — most **2–5.4×**, the floor q16 at 1.37× — because the ingest transpose is
gone: the scan feeds Arrow batches directly into the operator, and only the `blackhole` sink pays a
transpose. The queries that are transpose-bound on the generator (q8 at 0.87×, q3 at 0.95×, q20 at
0.84×) are exactly the ones that jump the most here (q8 4.37×, q3 3.57×, q20 3.40×) — confirming their
generator cost was the `RowData` perimeter, not the operator. Parquet's rowtime is a plain
`TIMESTAMP(3)`, so the `DATE_FORMAT`/`HOUR` queries (q10/q14/q15/q16/q17) run natively (over the Kafka
`TIMESTAMP_LTZ` they run natively too now — see the Kafka table's `§` note). q16 — long the one Parquet
query below 1× (its multi-`DISTINCT` accumulator churned `ScalarValue`) — cleared it when the
`mimalloc` build rebound the library's allocator, and again moved (1.10→1.34) when the DISTINCT sets
went typed and the state probes went borrowed-byte.

**Nineteen clear 1.0× even on this conservative combined run** (sixteen before the 2026-07 profiling
round, eighteen after its first pass — the differential flame-graph work recorded in
`.claude/research/nexmark-operator-profiles-2026-07.md`, whose shipped levers are itemized in
`docs/optimizations.md`: shared rowwise prefix under scoped sub-plan reuse, allocation-free state
probes across the join/aggregate/dedup/Top-N maps, typed DISTINCT sets + cached changelog emit,
decode-deduplicated Top-N emit, the transpose string single-copy, the lookup join's collect-time
Arrow writes, and the byte-path parity upcalls). The round's second pass measured its movers on the
75-second profile loop: **q21's parity path +12%** (the byte marshalling + primitive ASCII fold),
**q23 +8.5%**, **q18 +5.4%**, **q16 +3.4%**. The
window-aggregate queries moved earlier when the aggregators went to arrow-row keys and the session
update went run-batched (**q5 1.00→1.47, q8 0.70→0.87, q11 2.41→2.79** cumulatively). The
**updating-join family was the earlier big mover**: a CPU profile put ~40% of the worst query (q9)
in the joiner. Making the INNER join batch its whole input — gather all candidate pairs against the
fixed probe side, evaluate the residual predicate once columnar, emit by `filter_record_batch`, and
move rows into state instead of re-cloning — lifted **q9 0.39→0.97, q4 0.64→1.07, q7 0.91→1.37,
q23 0.66→0.96** at the time. The lever throughout was a differential profile's clearest signal — on
every changelog operator native spent 10–22% of CPU in the system allocator where Flink spends ~1%
(Flink reuses pooled `BinaryRowData`, its cost landing in GC). Cutting those allocations, not
swapping the allocator, closed the gap
([divergences/08](../divergences/08-columnar-flow-transitions.md)).

What still trails 1× on this rung: q8 is transpose-bound (a window join with only a ~9% native
island); q20 is the widest updating join (its state probes are allocation-free and its stored-row
decode no longer registers on the profile — the remainder is intrinsic hash-join work over the
rowwise perimeter, see wontdos/48); and q3 (0.95×) and q21's byte-parity upcall (0.96×) sit at the
line. q14 crossed it this run (1.02×); q13's lookup join,
long below 1×, cleared it when its collector started writing straight into the Arrow builders.

**† q21 is reported on both paths.** By default its `REGEXP_EXTRACT` and `LOWER` run through a
byte-identical **JVM upcall** (one JNI crossing per batch): the compile cost
is cached, the string boundary stays in UTF-8 bytes with a primitive ASCII fold, and the argument
columns marshal once per batch (0.75× → 0.86× → ~parity across the round; this combined run reads
0.96×, and the isolated 75s profile loop puts it above 1× — the upcall path is the most sensitive
to the combined run's accumulated GC pressure). The price of staying
exactly Flink-equal on functions whose Rust regex / case-folding can diverge at a locale/regex edge
is ~1.6× against the opt-in: `-Dstreamfusion.expression.allowIncompatible=true` runs the
**pure-native Rust** path at **1.54×**. Both are documented in
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

**Kafka**, the full native rdkafka source rung — after the consume fast path (divergences/19) it is
the best rung on **every row**, so the table reports it directly (native speedup vs that format's own
Flink baseline), sorted by the JSON speedup:

| Query | JSON | Avro | Protobuf |
|---|---|---|---|
| q11 | **3.93×** | **5.18×** | **5.55×** |
| q7 | **2.89×** | **4.11×** | **3.21×** |
| q15 § | **2.76×** | **3.06×** | **2.52×** |
| q0 | **2.71×** | **3.42×** | **2.58×** |
| q10 § | **2.69×** | **2.64×** | **2.26×** |
| q18 | **2.52×** | **3.02×** | **2.58×** |
| q22 | **2.49×** | **2.83×** | **2.28×** |
| q17 § | **2.49×** | **2.59×** | **2.23×** |
| q14 § | **2.47×** | **3.50×** | **2.66×** |
| q21 † | **2.46×** | **3.01×** | **2.62×** |
| q21 | **2.44×** | **2.98×** | **2.64×** |
| q4 | **2.43×** | **3.27×** | **2.66×** |
| q1 | **2.39×** | **3.35×** | **2.62×** |
| q20 | **2.38×** | **3.40×** | **2.92×** |
| q5 | **2.32×** | **3.35×** | **3.04×** |
| q12 | **2.31×** | **2.55×** | **2.14×** |
| q9 | **2.24×** | **2.13×** | **2.35×** |
| q8 | **2.22×** | **2.94×** | **2.58×** |
| q13 | **2.20×** | **2.75×** | **2.14×** |
| q23 | **2.09×** | **2.85×** | **2.38×** |
| q2 | **2.04×** | **2.48×** | **2.09×** |
| q19 | **1.98×** | **1.89×** | **1.85×** |
| q3 | **1.97×** | **2.38×** | **1.80×** |
| q16 § | **1.86×** | **1.87×** | **1.65×** |

**Historically, every Kafka row cleared 1.65×, all but a handful cleared 2×, and the peak was q11 at
3.9–5.6×.** These numbers include the former fused source's per-partition watermark regeneration (the matrix tables declare a
`WATERMARK`, pushed into the scan): windows fire incrementally mid-stream exactly as on stock Flink,
and the per-batch max-rowtime scan that feeds it costs nothing measurable. The same watermark work
collapses the two middle rungs on these tables — the decode rung declines a watermarked table (it
cannot regenerate the pushed watermark), so its per-rung numbers now equal the JVM-transpose rung's;
the un-watermarked ladder tables above are unaffected. An
earlier version of this table reported "best rung per format", because the source rung was capped by
a per-poll ceiling and the shallow decode (or even the JVM transpose) rung often led; the consume
fast path removed that ceiling and made the source rung strictly dominant — including for the
changelog-heavy queries (q9/q19) that previously gained nothing from faster decode, and
q3/q14/q18/q21, whose JSON rows were below 1× on their old best rung and now sit at ~2×+. The floor
of the table is q16 and the changelog-bound q3/q19 — operator-bound queries where the consume saving
is diluted, not reversed.

**Fluss** — the opt-in fourth source rung (`SF_MATRIX_FLUSS=true`), the columnar-on-the-wire
source: the same wide event row is preloaded into a local Fluss test cluster and read back by
both engines in the identical default streaming runtime — stock Flink-on-Fluss vs the native
fluss-rs log-table reader. Boundedness comes from a counting blackhole sink —
raw `RowData`, the same perimeter as the other rungs' `blackhole`, releasing the driver's latch
at the finish line — so each cell measures time-to-Nth-row (or time-to-marker) at `SF_ROWS`
scale. The native reader requires the `fluss` cargo
feature in the build, added alongside the recommended `mimalloc`: `SF_BENCHMARK=true
SF_MATRIX_FLUSS=true mvn -pl :streamfusion-runtime test -Pbench -Dnative.cargo.args="build --release --features
mimalloc,fluss" -Dtest=NexmarkMatrixBenchmark`. Building the `fluss` feature currently needs
`protoc` (`protobuf-compiler`) because fluss-rs generates its RPC protos at build time.

Because the log table is unbounded, the rung needs a deterministic Nth sink row to cancel at.
The benchmark table declares the generator's own 4s bounded-out-of-orderness `WATERMARK` (the
Fluss catalog persists it), so the windowed event-time queries run on both engines — Flink keeps
the watermark as an assigner node above the Fluss scan (no push-down, unlike Kafka), and that
assigner runs natively above the native source. A preloaded sentinel event (an `event_type`
outside 0..2 with a far-future rowtime, invisible to every view) advances the watermark past
every real window end, closing the same windows the bounded generator calibration's end-of-input
flush closes, so the counts line up. Three queries have no usable row count and use a **poison
marker** finish line instead: a traced copy of the preload appends one poison auction+bid pair
(ids outside every real range; the bid's channel is `apple`) after all real events, and the run
cancels when the pair's output row reaches the sink — in a parallelism-1 pipeline that row is
necessarily emitted after every real row, so time-to-marker measures the same full drain,
without a count:

- **q4/q9** — their two-input join feeds an update-collapsing aggregate/rank: Flink skips the
  `-U/+U` pair when an input row doesn't change the aggregate value, so the changelog row *count*
  depends on the join's input interleaving — non-deterministic even between two stock Flink runs
  (a 500K run calibrated 362,710 rows off the generator and observed 316,092 on Fluss, the job
  idle). Values and final state are identical; only the update cadence varies. The marker (q4:
  the poison category's aggregate row; q9: the poison auction's rank row) sidesteps the count
  entirely.
- **q21** — emits zero rows over this generator's data (its channels are `channel-N` and its
  URLs carry no `channel_id=`), so the poison bid's `apple` channel makes the marker row its
  first and only output.

One query skips: **q12** — a proctime window's output count is wall-clock-dependent, and any
marker's own window would close ~10s (the window size) after the drain, so a finish line would
time the window, not the engines. It stays measured on the bounded rungs, whose end-of-input
flush fires proctime windows immediately. Upstreaming `scan.bounded.mode` to Fluss
([issue #10](https://github.com/datafusion-contrib/StreamFusion/issues/10)) would retire the
count, sentinel, and marker machinery at once and admit q12.

Run of 2026-07-06 (500K events, best of 2 after a warmup, time-to-Nth-row / time-to-marker,
native vs the stock Fluss connector in the identical default streaming environment, both over
the watermarked table, both sinking to the counting blackhole), sorted by speedup:

| Query | Native vs. Flink-on-Fluss | Flink (ev/s) | Native (ev/s) |
|---|---|---|---|
| q11 | **4.02×** | 0.72 M | 2.89 M |
| q22 | **3.07×** | 0.99 M | 3.06 M |
| q2 | **2.77×** | 1.49 M | 4.12 M |
| q23 | **2.72×** | 0.59 M | 1.60 M |
| q1 ‡ | **2.65×** | 1.37 M | 3.64 M |
| q0 | **2.58×** | 1.38 M | 3.54 M |
| q10 § | **2.53×** | 1.17 M | 2.97 M |
| q14 § | **2.53×** | 1.37 M | 3.46 M |
| q7 | **2.46×** | 0.57 M | 1.41 M |
| q19 | **2.43×** | 0.37 M | 0.91 M |
| q5 | **2.25×** | 0.79 M | 1.77 M |
| q21 † | **2.14×** | 0.83 M | 1.77 M |
| q8 | **2.12×** | 0.93 M | 1.98 M |
| q13 | **2.11×** | 1.28 M | 2.70 M |
| q20 | **2.02×** | 0.83 M | 1.68 M |
| q18 | **1.88×** | 0.97 M | 1.84 M |
| q9 | **1.59×** | 0.58 M | 0.92 M |
| q3 | **1.55×** | 1.38 M | 2.14 M |
| q4 | **1.43×** | 0.65 M | 0.93 M |
| q17 § | **1.26×** | 0.96 M | 1.21 M |
| q16 § | 1.00× | 0.79 M | 0.79 M |
| q15 § | 0.98× | 0.94 M | 0.92 M |

**Twenty of twenty-two clear 1×, floor 0.98×.** An earlier quote of this table (same day, same
build) had the distinct-agg family at 0.78–0.85× and q19 at 0.97× — an artifact of the rung's
original sink: the count-to-N cancel ran through `toChangelogStream`, whose `TableToDataStream`
conversion turns every internal `RowData` into an external `Row` (boxing, UTF-8 decode,
`LocalDateTime` materialization). Both engines paid it equally, but a large shared perimeter
constant compresses every ratio toward 1× — worst exactly for the changelog-heavy queries that
emit ~2 sink rows per input row. Replacing it with the counting blackhole (raw `RowData`, the
same swallow as every other rung's `blackhole`, plus the latch) restored the rung's
comparability: q19 0.97×→2.43×, q23 1.41×→2.72×, q15 0.78×→0.98×, q16 0.85×→1.00×, q17
0.84×→1.26×. The profiled operator levers (the changelog aggregate's allocation churn and
`DATE_FORMAT`'s per-row formatting — `.claude/research/fluss-source-profile-2026-07.md`) remain
the path to push q15/q16 past the line.

The opt-in variants measure within noise of their byte-parity defaults on this rung — except
**† q21**, whose work is regex-dominated: the byte-parity JVM-upcall default reads **2.14×** and
the opt-in pure-Rust regex/case path **4.98×**, the honest cost of the parity guarantee (the
same split the Parquet rung shows at 2.77× vs 6.14×). q4/q9/q21 are the marker-measured cells.

The table's log rides Fluss's defaults, including ZSTD-compressed Arrow batches (as the Parquet
rung's file rides Flink's Snappy default) — both engines decode the same bytes. Turning
compression off is not a lever: with `'table.log.arrow.compression.type' = 'NONE'` q0 native
*drops* 2.83× → 2.18× (stock unchanged, q15 within noise), because fetches are byte-capped and
an uncompressed log needs ~4× the fetch round-trips for the same rows — the zstd decode the
profile shows is the price of fewer RPCs, not waste.

**The zero-transpose hypothesis holds.** The wire format is Arrow, so the native reader feeds
the island directly — no ingest transpose, no decode — and the stateless queries hit the highest
absolute native rates of any streaming rung (q2 at 4.1 M ev/s). The stock connector is itself
the strongest baseline of any rung (a lazy columnar read of the same Arrow log — its per-row
`ColumnarRow`→`RowData` conversion is the gap the stateless 2.5–3.1× measures), which is why
these ratios sit below Parquet's despite higher absolute rates: stock-on-Fluss is simply much
faster than stock-on-Parquet.

**Two masked native bugs surfaced the first time this rung ran unbounded** — worth recording
because every earlier rung was bounded, where the end-of-input `MAX_WATERMARK` flush forgives
mid-stream watermark mistakes:

1. **A missing sub-plan-reuse barrier on the Fluss scan.** Every native rel carries a digest
   barrier so Flink's post-optimize reuse can never merge two branches onto one columnar
   producer (the Arrow hand-off is zero-copy, single-consumer); the Fluss source node lacked
   one, so multi-view queries merged into one source broadcasting the same batch to two
   consumers — a use-after-free the watermark assigner turned into a hard crash.
2. **A shift-zone asymmetry in the window re-aggregation path (q5's shape).** Flink's rule:
   plain-`TIMESTAMP` rowtime windows compute on epoch millis with UTC digits; only
   `TIMESTAMP_LTZ` shifts boundaries into the session zone. The local window aggregate's exec
   node passed the session zone unconditionally, and its window-attached ingest (the only
   consumer of that zone) "un-shifted" boundaries that were never shifted — every re-aggregated
   window landed the session-zone offset in the future, where only a bounded run's final flush
   ever released it. Both engines' results were still identical on the bounded rungs, which is
   why parity never caught it; the unbounded rung is the first consumer of mid-stream firing
   for that shape.

### The tuned (mini-batch) matrix — the full suite

Production Flink deployments routinely enable mini-batch for stateful queries, so the matrix has a
**tuned mode**: `table.exec.mini-batch.*` (2s allow-latency, size 50000) on **both** engines — the
steelman rule, and the config behind the only public per-query Alibaba comparison. Generator source
(the tuned question is engine-vs-engine, not the perimeter) and **5M events** so the flush cadence
amortizes (at 500K the run is shorter than one flush interval and measures latency artifacts).
`table.optimizer.distinct-agg.split.enabled` stays default-off: it
is a skew mitigation for parallel deployments (these runs are parallelism 1) and its incremental
plan chain deliberately has no native path (`wontdos/52-distinct-split-chain.md`).
`SF_BENCHMARK=true SF_MATRIX_TUNED=true SF_ROWS=5000000
SF_MATRIX_QUERIES=q0,…,q23 mvn -pl :streamfusion-runtime test -Pbench -Dnative.cargo.args="build --release --features
mimalloc" -Dtest=NexmarkMatrixBenchmark#tunedMiniBatchMatrix` (the query list defaults to the
changelog family — mini-batch changes only those plans — but the full-suite run below doubles as
the coverage check that **every** query still routes native under production tuning; run
2026-07-05, no fallbacks).

| Query | Shape | Native vs. tuned Flink |
|---|---|---|
| q0 | pass-through projection of `bid` | **1.23×** |
| q1 | `0.908 * price` — exact `Decimal128` (byte-parity) | **1.16×** |
| q2 | filter `WHERE MOD(auction, 123) = 0` | **1.45×** |
| q3 | updating join `auction ⋈ person` | 0.69× |
| q4 | regular join → `MAX` → `AVG` per category | **2.85×** |
| q5 | Hot Items (window re-agg + window join) | **1.18×** |
| q7 | tumble `MAX` ⋈ bid | **1.43×** |
| q8 | tumble windowed-distinct ⋈ join | 0.76× |
| q9 | regular join → `ROW_NUMBER` (≤ 1) | **2.15×** |
| q10 | `DATE_FORMAT` projection | **1.18×** |
| q11 | session-window `COUNT` per bidder | **3.01×** |
| q12 | proctime tumble `COUNT` per bidder | **1.70×** |
| q13 | lookup join (bounded dimension) | **1.09×** |
| q14 | `HOUR`/`CASE` + `count_char` UDF + decimal | **1.05×** |
| q15 | multi-`DISTINCT` `COUNT`s per day (`DATE_FORMAT` group) | **1.26×** |
| q16 | multi-`DISTINCT` per channel/day | **1.18×** |
| q17 | group agg + `AVG`/`MIN`/`MAX`/`SUM` per day | 1.00× |
| q18 | `ROW_NUMBER` dedup (≤ 1) | **2.02×** |
| q19 | `ROW_NUMBER` topN (≤ 10) | **2.36×** |
| q20 | updating join (`category = 10`) | **1.34×** |
| q21 | `CASE` + `REGEXP_EXTRACT`/`LOWER` — JVM upcall (byte-parity) | 0.97× |
| q22 | `SPLIT_INDEX(url, '/', n)` projection | **1.25×** |
| q23 | three-way join `bid ⋈ person ⋈ auction` | **3.01×** |

The changelog-family margins are **wider** than the default-config generator column, not narrower:
at 5M events the state-heavy queries dominate their runtime with operator work (the per-event
JIT/setup share shrinks), and under mini-batch the native side emits the net logical-bundle Top-N diff
(divergences/20) where Flink's rank — which has no mini-batch variant — still pays the per-record
cascade (q19 2.36× tuned vs 1.48× default). The non-changelog queries plan identically tuned or
not (mini-batch inserts nothing into them), so their column is effectively the generator rung at
5M events — the same transpose-bound stragglers trail here for the same reason (q3 a thin island
over a wide transposed perimeter, q8's window join, q21's per-batch JVM upcall at 5M-event scale);
the mini-batch config itself costs the native side nothing since calc pruning pushes through the
assigner.

The first tuned run reported q4/q15/q16/q17 as fallbacks — the tuned column doubling as the
mini-batch coverage check, exactly as designed. That coverage has since landed (two-phase FILTER
clauses, filtered distinct views, string MIN/MAX partials, retraction-bearing partials with the
count1 record counter), and all four now run fully native — as does the whole suite, including
the windowed two-phase splits over every value type (the 2026-07-05 nullable-sum-buffer work).
q15/q16 are worth noting: `GROUP BY
day` is a single live grouping key carrying every record's bidder/auction distinct sets — the
hot-key shape `distinct-agg.split` exists to mitigate — and the native no-split plan beats tuned
Flink on it (see `wontdos/52-distinct-split-chain.md`).

_Apple M1 Max; numbers are comparable only within a machine._
