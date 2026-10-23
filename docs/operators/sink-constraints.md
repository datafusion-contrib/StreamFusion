# Sink constraints

**Status: partial.** Doris Arrow sinks support top-level NOT NULL `ERROR`/`DROP` and
CHAR/VARCHAR/BINARY/VARBINARY length `IGNORE`, `TRIM_PAD` and `ERROR` policies. Other connector
admission remains unchanged: the shared fast enforcer implements top-level NOT NULL `ERROR`.

## Admitted boundaries

Both [Paimon](../connectors/paimon.md) adapters use the shared operator for their already-admitted
NOT NULL ERROR targets. It runs before local merging and bucket routing, so a later update cannot
hide an earlier invalid row. Their admission remains unchanged. Paimon's existing `native-paimon-not-null-enforcer` transformation name and
position are unchanged.

Connector planning still derives required fields from the physical target schema and verifies
formats, types, changelog modes and options independently. No row-to-Arrow conversion is inserted
just to reach this operator.

Doris uses the same fast NOT NULL ERROR path when no length checking is needed. Other admitted
policies call released Flink 2.2's `ConstraintEnforcerExecutor` through the connector-neutral
`NativeSinkConstraintEnforcer`. Input rows are reusable views of the Arrow buffers, so there is
no whole-batch input transpose or duplicate implementation of Flink's trimming/padding rules.
The stage runs before sink-key routing and upsert materialization.

## Runtime semantics and ownership

The normal path checks Arrow null counts on constrained columns and forwards the original batch
and stream record. Inspection does not take an Arrow consumer share, create a replacement batch,
or materialize RowData. RowKinds, timestamps, routing and source metadata stay on the same batch.

On a violation, the operator searches in row order and then target-column order, matching stock
Flink's first offending row, column and exception message. Any valid prefix is forwarded before
the error; the exception type follows the target Flink line as well. The failing row and later rows
are not forwarded. Prefix slices retain their RowKinds, row-aligned ordering tags, routing and
source metadata. Slicing is an error-path operation, not a
claim that every Arrow layout slices without allocating.

The rejected consumer releases its owned root synchronously. Other consumers of a shared batch
and the emitted prefix retain independent references to their buffers. A downstream failure while
accepting an unclaimed prefix also closes that prefix. Arrow's `slice` uses transfer pairs to retain
the sliced buffers; closing the original root does not release a prefix's references. Tests read
fixed-width, variable-width and nested prefix values after closing all original consumers and check
that the retained buffers are freed only after the prefix closes.

For Doris DROP/length enforcement, an unchanged batch keeps its identity. Dropping rows gathers
the selected columns and aligned metadata; rewriting a value copies its affected column while
ordinary sibling buffers remain shared. String-cache sidecars are updated with the changed values.
A failure emits the already accepted prefix, including earlier rewrites, then releases the rejected
input. Released Flink's executor controls constraint order, Unicode length and padding, and errors.
This includes the released executor's nullable-length failure behavior, verified against the
stock connector through an actual Arrow source.

The operator is stateless and forwards watermarks through Flink's ordinary operator implementation.
It retains no Arrow batches in checkpoint state. Connector commit, recovery and delivery guarantees
are unchanged.

## Remaining gaps and validation boundary

- Other connector matchers retain their existing NOT NULL `DROP` and type-length fallbacks.
- Nested enforcement remains unsupported by this adapter; Doris retains stock execution when its nested-enforcer option is enabled.
- Other sinks do not gain admission merely because a shared enforcer is available.

Operator tests compare exceptions and valid prefixes with the released Flink enforcer and cover
identity, shared ownership, nullable children, empty batches, RowKinds, metadata, failure cleanup,
watermarks and stateless restore. This extraction does not establish an end-to-end throughput gain
or independently revalidate Paimon's service recovery guarantees.

[Doris complete-job measurements](../benchmarks/doris-constraints-rocks-2026-10-23/README.md)
retain stock/previous/current trials. At one million input rows, DROP and TRIM_PAD improve the
stock medians by 1.30× and 1.28×; the small cases overlap substantially.
