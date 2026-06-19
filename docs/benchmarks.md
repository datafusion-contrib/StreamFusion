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

## End-to-end parity timing

The parity harness already runs every checked query twice (host and native). Wall-clock
there is dominated by minicluster startup, so it is not a reliable operator-level
signal; the Criterion benches above are the operator measurement. End-to-end timing is
tracked separately when a representative streaming benchmark (e.g. Nexmark) is stood up.
