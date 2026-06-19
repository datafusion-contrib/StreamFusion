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

### Results

Numbers are only comparable within a machine; record the host (CPU) alongside.

**Apple M1 Max** (median of 100 Criterion samples):

| Benchmark | Rows/batch | Time/batch | Elements/s | Notes |
|---|---|---|---|---|
| `filter/gt_literal` | 4096 | 2.56 µs | ~1.60 Gelem/s | compiled predicate, ~50% selectivity |
| `tumbling/sum_update_flush` | 4096 | 110 µs | ~37.4 Melem/s | 16 windows, no key |
| `tumbling/sum_keyed_update_flush` | 4096 | 262 µs | ~15.7 Melem/s | 16 windows, 64 bigint keys |
| `session/sum_keyed_update_flush` | 4096 | 2.70 ms | ~1.5 Melem/s | gap merge, 64 bigint keys |

The gap between filter and aggregation is the signal: the filter is a compiled
expression plus one Arrow kernel, while the tumbling aggregator groups every row by a
`GroupKey` (`Vec<ScalarValue>`) — and the keyed case still costs ~2.4× the unkeyed one.
Profiling-driven cuts so far (tumbling, 4096-row batch):

- reusing the per-row window buffer instead of allocating one per row (244 → 181 µs);
- moving the row's key into its last window instead of cloning it for every window
  (181 → 171 µs unkeyed, 395 → 323 µs keyed);
- a fast hash (`ahash`) for the grouping map instead of the stdlib SipHash
  (171 → 110 µs unkeyed, 323 → 262 µs keyed).

Net so far: the unkeyed path is ~2.2× faster (244 → 110 µs) and the keyed path ~1.5×
(395 → 262 µs). The remaining per-row `GroupKey` allocation is the next target
(row-format or dictionary-encoded keys) — see the [profiling
ticket](../.claude/todos/20-profiling-and-benchmarks.md).

The session aggregator is ~10× slower per element than tumbling because its `update`
slices the value column one row at a time (a `take` per row) rather than once per group;
batching that slice is its first optimization target. Its grouping hash is a negligible
fraction here, so the `ahash` swap was deliberately not applied to it.

## End to end vs. Flink

`ThroughputBenchmark` (opt-in: `SF_BENCHMARK=true mvn test -Dtest=ThroughputBenchmark`)
runs the same query over a large generated source (5M rows) into a `blackhole` sink,
once with native substitution installed and once on stock Flink, single slot. It reports
best-of-3 rows/s for each and the native/Flink ratio. The discarding sink keeps the
operator the bottleneck, not client transfer; a warmup run absorbs JIT and minicluster
startup so the measured runs reflect execution.

| Operator | Query | Flink | Native | Native vs. Flink |
|---|---|---|---|---|
| Filter (`WHERE`) | `SELECT * FROM f WHERE v > 50` | 3.07 M rows/s | 1.79 M rows/s | 0.58× |
| Tumbling | `SUM` by 1s window | 1.54 M rows/s | 1.24 M rows/s | 0.81× |
| Parquet sink | `INSERT INTO parquet SELECT *` | 1.18 M rows/s | 0.61 M rows/s | 0.52× |

**Native is currently slower than Flink end to end**, even though the native compute is
fast in isolation (above). The cost is the `RowData ↔ Arrow` conversion: a single
substituted stage fed by a row source converts in (and back out), and that round-trip
outweighs the native saving. The ratio improves with compute intensity (filter 0.58× →
tumbling 0.81×) — the conversion is a fixed per-row tax and only amortizes when more work
rides on each converted batch.

The Parquet sink (0.52×) is the sharpest lesson: a columnar *sink* fed by a *row* source
still pays the transpose at the sink, while Flink writes `RowData → Parquet` directly — so
a columnar sink alone does not win. The decisive case is a **columnar source**: reading
Parquet/Iceberg as Arrow means the data never becomes `RowData`, so a Parquet→Parquet job
stays columnar end to end while Flink round-trips through rows. That, plus keeping
adjacent native operators in Arrow (chaining, then Arrow over the shuffle — the Iron
Vector model), is where the ratio crosses 1×. Until then these single-stage, row-fed
numbers are expected below 1×; they are the baseline the columnar-source and chaining work
must beat. Re-run whenever the boundary or conversion path changes.
