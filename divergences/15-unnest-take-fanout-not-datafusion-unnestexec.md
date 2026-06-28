# 15 — UNNEST as a take-based fan-out, not DataFusion's `UnnestExec`

Arroyo implements `UNNEST` by rewriting to a DataFusion `LogicalPlan::Unnest` and letting DataFusion's
`UnnestExec` run it. We do **not** mirror that, and instead fan rows out with an Arrow `take` (the same
shape as our windowing TVF `assign_windows` and `Expand`). Reasons:

1. **Arroyo owns its whole plan; we don't.** Arroyo rewrites to a DataFusion plan and lets DataFusion
   *define* the unnest's output schema. We must match Flink's fixed `Correlate` output —
   `[input cols.., element]`, with the source array column **preserved** — and thread our `$row_kind$`
   changelog meta-column through. DataFusion's `UnnestExec` replaces the unnested column *in place*,
   maps the surviving columns *by name*, and has no notion of `$row_kind$`. Bending it to Flink's
   append-the-element-and-keep-the-array contract would mean duplicating the array column, building a
   name-matched output schema, threading `$row_kind$` as a non-unnested column, and reordering — more
   machinery than the fan-out itself.

2. **DataFusion's batch kernel is private.** Only the `UnnestExec` `ExecutionPlan` is public
   (`unnest_list_array`/`build_batch` are private), so "call DataFusion's unnest on a batch" isn't
   available — we'd have to stand up a `MemorySource` + `collect` per batch (a tokio round-trip) the
   way we do for the hash joins, purely to get at the kernel.

3. **It's the established in-repo pattern.** Our other stateless per-row fan-outs — the windowing TVF
   and `Expand` — already fan out with `ListArray`/`take`. UNNEST is the same shape: take the child
   values by element index, repeat the input columns by row index. ~15 lines, no plan construction.

Scope today: **INNER `UNNEST` of a single `ARRAY` column** whose element is one supported column
(`UNNEST(array_of_ints/strings/…)`). A `MAP`/`MULTISET` unnest, an `ARRAY<ROW>` unnest (element
flattens to several columns), `WITH ORDINALITY`, a `LEFT` (outer) unnest, and a filter pushed into the
`Correlate` as a `condition` all fall back. INNER semantics: a null/empty array yields no rows; a null
element inside a non-null array yields a null row — matching Flink.
