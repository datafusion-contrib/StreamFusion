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

### Results

Numbers are only comparable within a machine; record the host (CPU) alongside.

**Apple M1 Max** (median of 100 Criterion samples):

| Benchmark | Rows/batch | Time/batch | Elements/s | Notes |
|---|---|---|---|---|
| `filter/gt_literal` | 4096 | 2.56 µs | ~1.60 Gelem/s | compiled predicate, ~50% selectivity |
| `tumbling/sum_update_flush` | 4096 | 241 µs | ~17 Melem/s | 16 windows, single key |

The ~100× gap between the two is the signal: the filter is a compiled expression plus
one Arrow kernel, while the tumbling aggregator allocates a `GroupKey`
(`Vec<ScalarValue>`) per row and hashes it to group — the per-row key cost flagged in
the [profiling ticket](../.claude/todos/20-profiling-and-benchmarks.md). That is the
first low-hanging target; benchmark before and after any change to it.

## End-to-end parity timing

The parity harness already runs every checked query twice (host and native). Wall-clock
there is dominated by minicluster startup, so it is not a reliable operator-level
signal; the Criterion benches above are the operator measurement. End-to-end timing is
tracked separately when a representative streaming benchmark (e.g. Nexmark) is stood up.
