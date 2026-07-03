# Native lookup join — remaining extension: columnar assembly / bounded-dim preload

**Status:** v1 DONE (2026-07-01); async DONE (2026-07-01); calc-on-temporal-table, residual +
pre-filter conditions, constant lookup keys, and distributed execution DONE (2026-07-03) — the native
operators now drive **Flink's own generated lookup runners** (`LookupJoinRunner` /
`AsyncLookupJoinRunner`, incl. the WithCalc variants) over each Arrow probe batch, so key building
(field refs + constants), pre-filter, dim-side calc, residual condition, and LEFT null-padding are
byte-identical to the host by construction, and the generated code + embedded function instances
serialize to task managers exactly as Flink's own exec node ships them. The async operator fires
every probe row's lookup concurrently within the batch (bounded by Flink's
`table.exec.async-lookup.buffer-capacity`, the host's own backpressure) and awaits on the task thread
— still the Arroyo/RisingWave within-batch model, no mailbox, nothing in flight across a checkpoint
barrier (see ticket 01 for why within-batch beats the `AsyncWaitOperator` port). The only remaining
matcher gate is the **upsert-materialized** (keyed-state) lookup, which needs a changelog probe the
island doesn't admit anyway.

## What remains (perf, not coverage)

- **Columnar assembly / preload for bounded dims.** The operator copies probe+dim rows per output
  row (through the runner's `JoinedRowData`). For a bounded side input, preload the whole dim into a
  native hash table once (RisingWave/Arroyo cache the dim side) and probe fully columnar — zero
  per-batch JVM crossing in steady state. Otherwise keep the per-row lookup but assemble the output
  columnar (take probe cols by index + matched dim cols) instead of row copies. Only worth doing with
  a benchmark showing the row assembly is a measurable share of q13-shaped queries.
- **Cross-batch async overlap** (`AsyncWaitOperator` port) — only if per-lookup latency is so high
  that blocking on a single batch stalls checkpoints unacceptably; not the case today.
