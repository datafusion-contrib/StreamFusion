# Multiple grouping keys

**Status:** open — design ready
**Source:** ticket 04; highest-value remaining input-schema gap

## Why
Real `GROUP BY` aggregations group by several dimension columns alongside the
window. Today only a single bigint key is supported (`keyColumn`, an `i64` in the
native aggregators). Multi-dimensional grouping is common, so this is the
headline remaining item.

## Scope (v1)
- **Multiple bigint keys**, 0..N. Non-integer (int/string) keys are a follow-up:
  int keys widen into the same path; string keys need a byte-encoded key.
- All window paths (combined tumbling/hopping/cumulative, session, two-phase
  local/global), because the key lives in the shared native aggregators and the
  shared operator base — carving out one path is not simpler.
- Parity-trivially-safe: grouping is exact partitioning, no value semantics.

## Native
- The per-group key becomes a composite: `HashMap<i64, …>` → `HashMap<Vec<i64>, …>`
  in both `TumblingAggregator` (`AlignedWindow.keys`) and `SessionAggregator`
  (`sessions`). The `update` grouping key gains the `Vec<i64>`.
- Key arity is inferred from the `key0..key{n-1}` columns present in each update
  batch and stored; `flush`/`flush_partial`/`snapshot` emit that many key columns
  (`key0..`), `update_partial`/`restore` read them. An empty flush before any
  update emits zero key columns — safe because the operator's row loop is then
  empty (it never dereferences the key vectors).

## JVM
- Thread `int[] keyColumns` in place of `int keyColumn` through the matcher,
  the four rels, the four exec nodes, and the operators. `keyColumns.length == 0`
  is the unkeyed case (today's `-1`), `== 1` is today's single key.
- `NativeWindowOperatorBase.updateRaw` builds `key0..key{n-1}` bigint vectors;
  `emitFinal` reads and emits N key fields in the host's column order
  (`[key0..key{n-1}, aggs…, window_start, window_end]`).
- Local/global operators build/read N key columns; `partialColumns` and
  `sliceEnd` indices shift by the extra keys (they are already grouping-relative).
- Matcher: accept `grouping.length >= 0` with **all** grouping columns bigint
  (drop the `> 1` rejection); `keyColumns(grouping)` returns the grouping array.

## Acceptance criteria
- `GROUP BY k1, k2, window_start, window_end` (two bigint keys) with one and with
  multiple aggregates routes to native with identical results to host Flink, on
  tumbling, hopping, session, cumulative, and two-phase tumbling.
- Single-key and unkeyed queries stay green (they are N=1 and N=0).
- Non-bigint or >0 string keys fall back.

## Notes
- The blast radius is ~17 files / ~68 `keyColumn` references, mostly mechanical
  threading. The risk is in the two native aggregators' key handling and the
  emit column order; the parity harness across all window types is the gate.
