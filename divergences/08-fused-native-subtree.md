# Keep columnar by fusing a native subtree, not by a columnar exchange type

**Kind:** structural — how adjacent native operators avoid re-converting.
**Diverges from:** Comet (mechanism, not goal).
**Forced by parity:** no — forced by Flink's runtime model and a throughput finding.

## Their decision
Comet keeps data columnar between native operators by riding Spark's built-in columnar
execution framework: operators advertise `supportsColumnar`, exchange `ColumnarBatch`,
and a `ColumnarRule` transition pass inserts `ColumnarToRow`/`RowToColumnar` only where a
columnar operator meets a row operator. Adjacent Comet operators never convert.

## What we do, and why
Flink has no columnar execution framework — its runtime exchanges `RowData` rows, with
no `supportsColumnar` contract and no transition pass to lean on. So we cannot reproduce
Comet's mechanism. Instead we **fuse a maximal shuffle-free connected component of
native operators into a single Flink operator** that runs the whole sub-pipeline in Arrow
internally, converting `RowData → Arrow` once at its outer input and back once at its
outer output. No boundary exists inside the fused operator, so no inter-operator
conversion happens — the same outcome as Comet's adjacent-columnar operators, reached
without a framework columnar type.

The alternative — making Arrow a `StreamRecord` payload type between native operators and
hand-inserting row↔columnar transitions — was rejected: it fights Flink's row-based
runtime (a columnar record type, its serializers, transition insertion) for no extra
benefit over fusion within a shuffle-free segment, and it still needs Arrow IPC the
moment an edge crosses the network. Fusion works *with* the one-in/one-out operator model
and defers network serialization to the one place it is unavoidable (the shuffle).

## Why this exists at all
Per-operator substitution measured *slower* than Flink (filter 0.58×, tumbling 0.81× —
see [ticket 20](../.claude/todos/20-profiling-and-benchmarks.md)) because each operator
pays a `RowData ↔ Arrow` round-trip. Fusion removes the round-trips between native
operators, which is the prerequisite for beating Flink. The plan and phases live in
[ticket 21](../.claude/todos/21-native-operator-chaining.md).

## Scope / consequences
- The unit of substitution becomes a connected component, not a single node; the planner
  grows a fusion pass over the native-able nodes between shuffles.
- A fused operator that contains a stateful node (a window) owns that node's state and
  checkpointing, exactly as the standalone operator did — fusion changes the input path
  (a native prefix runs before the aggregation, in Arrow), not the state model
  ([04](04-synchronous-stateful-execution.md)).
- Arrow across the shuffle (two-phase local→global) is a later phase and is where a
  columnar serializer finally becomes necessary.
