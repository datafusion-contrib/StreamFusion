# Borrowing rows only at synchronous Arrow entry

Comet's `RowArrowReader.nextBatch` calls `writer.write(rowIter.next())` before advancing to the
next row, and owns the resulting Arrow buffers. StreamFusion's entry transpose already follows
that lifetime. Flink adds a deep copy at chained consumers when global object reuse is disabled,
including this synchronous writer. We avoid that redundant copy only on the writer's input edge.

A standard virtual forward partition supplies a consumer-local type information wrapper. Its
serializer borrows rows for object-copy calls, while delegating wire operations to the released
Flink row serializer. Upstream type information and sibling consumers are unchanged. We preserve
the original serializer snapshot so restoration safely returns to copying without changing wire
bytes. This is a stateless input optimization, not a replacement state serializer.

The adapter admits only a direct physical transformation with matching internal row schema.
Wrapping existing virtual partitions could mask their hash/rebalance behavior, so those inputs
retain normal copying. Flink's released graph translator dispatches on exact transformation
classes; we use its standard partition class instead of subclassing a one-input transformation.
We do not enable global object reuse or extend borrowing to consumers that retain rows.

Consulted before design: Comet's `CometSparkToColumnarExec` and `RowArrowReader`, released Flink
2.2.1 and 1.18.1 serializer/type-information APIs, and Flink's chained-output and transformation
translation contracts. Tests exercise reused rows/arrays through a real fork both with and
without chaining, wire bytes and changelog kinds, conservative snapshot restoration, and
partition/schema exclusions. See the [entry transpose ledger](../docs/optimizations/projection-pruning-transpose.md)
for performance evidence and the remaining ELT limitation.
