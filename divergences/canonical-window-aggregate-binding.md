# Canonical window aggregate binding

Flink keeps accumulator rows and data views in named slice or session namespaces and stores
progress separately. Native operators retain their running columnar folds and indexes. Canonical
savepoints project those live structures directly into the released Flink contract; native
checkpoints remain a separate recovery mechanism. No intermediate native snapshot is required.

The planner captures the original stock owner identity, serializers, maximum parallelism, clock,
time zone and aggregate information before replacing the owner. Local partials are drained at
barriers and remain stateless in the stock graph. Global partial buffers retain the released slice
and final-window distinction and must drain before forwarding watermarks.

Legacy unbound native INT grouping keys widen to INT64. Stock-bound window keys use INT32 so
imported stock rows and subsequent input have exactly the same Arrow row-converter schema. The
existing generated-projection key descriptor selects this schema in typed RocksDB constructors
for fixed windows and sessions; legacy descriptors retain their original carriage. No new magic
key-type code or legacy state reinterpretation is introduced.

Reference implementations consulted: Flink synchronous slicing/merging processors and generated
accumulator serializers, Arroyo tumbling/session aggregating windows, and Comet Arrow C Data
ownership transfer. Flink-specific namespaces, timer reconstruction and UNION watermark progress
are required for stock restore. Arroyo does not provide that stock state contract. Canonical
qualification remains partial under [#569](https://github.com/datafusion-contrib/StreamFusion/issues/569),
with benchmarking explicitly deferred by the maintainer.
