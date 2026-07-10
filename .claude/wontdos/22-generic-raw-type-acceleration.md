# Generic Flink `RAW<T>` acceleration

**Status:** WONTDO (2026-07-10). Extracted from [#22](https://github.com/datafusion-contrib/StreamFusion/issues/22)
after the BinaryRow/key-group and raw keyed-state work shipped.

`RAW<T>` is an arbitrary Flink `TypeSerializer` payload. Its equality and keyed-state placement
depend on the exact serialized bytes, while its serializer snapshot controls safe state evolution.
This makes it a source-protocol concern, not merely another Arrow physical type.

**Why we are not building generic native support.** A row-fed host source could materialize those
bytes at the RowData-to-Arrow transpose, but a native source never sees a Java object or its Flink
serializer. Fluss's Arrow-facing type system has `Bytes` and `Binary` but no `RAW` type or Flink
serializer snapshot. Paimon's type system likewise has no `RAW`, and its Flink row adapter rejects
`getRawValue`. The native Kafka decoders only understand their wire schemas; none carries an
arbitrary Flink `TypeSerializer` contract.

Making native and row-fed paths agree would require a new, durable source contract containing both
the exact serializer bytes and a versioned serializer snapshot for every RAW field, plus checkpoint
migration for serializer changes. Treating the bytes as ordinary `VARBINARY` is incorrect: Flink's
BinaryRow encoding gives RAW a distinct, always-variable representation, so short values would hash
to different key groups.

**Cost of the decision:** any column or nested field of `RAW<T>` keeps the whole affected native
island on Flink's host path. That preserves Flink semantics and checkpoint compatibility without a
new source format or partial, source-dependent acceleration.

**Revisit if:** a source format exposes Flink serializer bytes together with a stable serializer
snapshot, or a workload demonstrates that generic RAW-keyed SQL is common enough to justify defining
and maintaining that cross-source protocol.
