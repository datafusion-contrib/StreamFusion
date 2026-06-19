# Native operator chaining (keep columnar between native operators)

**Status:** design — needs architecture sign-off before the build.
**Source:** the end-to-end benchmark ([ticket 20](20-profiling-and-benchmarks.md)):
single-operator substitution is *slower* than Flink (filter 0.58×, tumbling 0.81×)
because every substituted operator converts `RowData → Arrow` on the way in and back
on the way out. The native compute is fast; the per-boundary conversion is the tax.
Chaining removes the conversions *between* adjacent native operators, which is the
only way the project beats Flink.

## The problem with the current model
Each native operator is its own Flink `OneInputStreamOperator<RowData, RowData>`. Two
adjacent native operators therefore convert Arrow→RowData (upstream out) then
RowData→Arrow (downstream in) for nothing — the data was already columnar. The ratio
rises with compute per row (filter → tumbling), confirming the conversion is a fixed
per-row cost that only amortizes when more work rides on each converted batch.

## How Comet does it (and why we can't copy it directly)
Comet replaces operators with columnar (`CometExec`) equivalents, then a transition
pass inserts `ColumnarToRow`/`RowToColumnar` **only** where a columnar operator meets a
row operator. Adjacent Comet operators exchange `ColumnarBatch` and never convert. This
rides on Spark's built-in columnar execution framework (`ColumnarRule`,
`supportsColumnar`, `ColumnarBatch` as a first-class exchange type).

**Flink has no equivalent.** Its runtime is row-based (`RowData`/`StreamRecord`); there
is no `supportsColumnar` contract and no framework that auto-inserts transitions. So we
cannot lean on the engine the way Comet leans on Spark — we have to provide the
columnar boundary ourselves.

## Decision — fuse, don't exchange
Two ways to keep data columnar between native operators:

1. **Columnar stream element + transitions.** Make Arrow a `StreamRecord` payload type
   between native operators and insert conversion operators at native↔host boundaries —
   a hand-built version of Comet's transition pass. Cost: a columnar record type,
   serializers for it (Arrow IPC across any non-chained edge), and transition
   insertion in the planner. Fights Flink's row-based runtime at every turn.
2. **Fuse a native subtree into one Flink operator** (chosen). The planner replaces a
   maximal **shuffle-free connected component** of native-able operators with a *single*
   Flink operator that runs the whole sub-pipeline in Arrow internally, converting
   `RowData → Arrow` once at the component's outer input and `Arrow → RowData` once at
   its outer output. Within the operator there is no boundary, so no inter-op conversion.
   This is Comet's "native subtree as one `CometExec`" mapped onto Flink without needing
   a framework columnar API.

Fusion is chosen because it works *with* Flink's row-based operator model (one operator
in, one operator out, RowData on the wire) and needs no columnar serializer until we
tackle the shuffle. It also yields the maximal win (zero conversion inside the subtree).
This is a deliberate divergence from Comet's framework-assisted transitions — see the
divergences note.

## Phased plan (each green + parity-tested + benchmarked vs Flink)
1. **Stateless fusion.** Fuse a chain of stateless native ops (e.g. `WHERE` feeding a
   projection, or two filters) into one operator that applies the sequence to each Arrow
   batch. The planner grows from per-node substitution to per-connected-component. Bench
   a filter chain vs Flink — should already move the ratio up.
2. **Stateless prefix into a stateful window.** The common high-value case: a filter (or
   projection) feeding a window aggregate. The fused window operator runs the native
   filter on each input batch *before* aggregating, all in Arrow. The window already does
   enough work that this should cross 1× vs Flink — the first real win.
3. **Arrow across the shuffle.** Two-phase aggregation has a `keyBy` between local and
   global. Carry Arrow over that network edge (Arrow IPC) so local→global stays columnar.
   Hardest phase; gated on 1–2 proving out.

## Acceptance criteria
- A query with two adjacent native operators substitutes as one fused native operator,
  with RowData↔Arrow conversion only at its outer boundaries (verify via the plan).
- Identical results to Flink across the parity suite.
- `ThroughputBenchmark` shows the fused chain beating its per-operator counterpart, and
  phase 2 crossing 1× vs Flink on at least one query.
