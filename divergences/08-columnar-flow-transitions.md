# Keep columnar by tagging operators columnar/rowwise, with transitions at the boundaries

**Kind:** structural — how adjacent native operators avoid re-converting.
**Diverges from:** nothing (it *follows* Comet); Flink just lacks the framework Comet leans on.
**Supersedes:** an earlier draft of this note that chose subtree *fusion* — see "Why not fusion".

## Their decision (Comet)
Comet keeps data columnar between native operators by riding Spark's columnar execution
framework: each operator advertises whether it is columnar, columnar operators exchange
`ColumnarBatch`, and a `ColumnarRule` inserts `ColumnarToRow`/`RowToColumnar` transitions
*only* where a columnar operator meets a row operator. Adjacent columnar operators never
convert; no operator knows anything about its neighbours beyond "is this edge columnar?".

## What we do
The same model. Each native operator is **columnar**: it consumes and produces Arrow
batches (a columnar stream-record type), not `RowData`. Adjacent columnar operators flow
Arrow straight through — within a chained task Flink hands the batch to the next operator
in-memory (by reference with object reuse, else an in-memory `copy()`); a *byte*
serializer (Arrow IPC) runs only across a network edge. At a **rowwise↔columnar boundary**
the planner inserts a transpose operator (`RowData → Arrow` entering the columnar region,
`Arrow → RowData` leaving it). A columnar source produces Arrow with no input transpose; a
columnar sink consumes Arrow with no output transpose. An operator is simply rowwise or
columnar — *no further detail about what it is adjacent to matters*, and the framework
composes them.

The batch serializer's `copy()` is **identity** (it returns the same batch): our operators
emit a fresh batch per record and never retain or mutate it after emit, and the serializer
is used only on native↔native edges, so the general copy contract does not need a real
clone. This avoids forcing the global object-reuse flag (which would also change the host
operators' semantics) — a deliberate choice the chained-pass relies on.

Flink has no columnar-execution framework to lean on (its runtime exchanges `RowData`),
so we provide the two missing pieces ourselves: the columnar stream-record type, and the
transition insertion in the planner. That is the whole of it — everything else is just
operators flowing into operators.

## Why not fusion
An earlier version of this note chose to *fuse* a maximal native subtree into one Flink
operator. That was wrong: fusion means pattern-matching specific operator *combinations*
(a Parquet source feeding a Parquet sink, a filter feeding a window, …) and emitting a
bespoke fused node for each — a hardcoded replacement rule per shape, which does not
generalize and is a smell. Tagging each operator columnar/rowwise and letting them flow,
with transitions only at the boundary, needs no per-combination knowledge: a columnar
source, a columnar filter, and a columnar sink compose because they are all columnar, not
because the planner recognizes the trio.

## Scope / consequences
- Native operators move from a `RowData → RowData` interface (converting internally at
  both ends, as they do today) to an `Arrow → Arrow` interface over the columnar
  stream-record type. The internal per-operator conversion goes away; conversion lives
  only in the transpose operators at region boundaries.
- The planner inserts a transpose wherever a substituted (columnar) operator meets a
  rowwise one — including at columnar sources/sinks, where there is none to insert.
- A columnar operator that contains a stateful node still owns that node's
  state/checkpointing ([04](04-synchronous-stateful-execution.md)); columnar flow changes
  the *edge* type, not the state model.
- Arrow across a shuffle (two-phase local→global) is where the columnar record type needs
  its IPC byte serializer; within a chained task only an in-memory hand-off (reference or
  `copy()`) happens, never byte serialization.
- Riding Flink's record queues gives batches a lifetime hole Comet doesn't have (Spark's
  columnar batches stay inside task-owned iterators, closed by a task-completion listener):
  Flink drops in-flight records at teardown with no close hook, which for an off-heap batch
  means leaked buffers on every failover. The columnar record type carries a `Cleaner`
  backstop for exactly this — a batch dropped without any consumer having taken its root is
  freed once unreachable, and taking the root disarms it (ownership contract in the
  `ArrowBatch` javadoc).
