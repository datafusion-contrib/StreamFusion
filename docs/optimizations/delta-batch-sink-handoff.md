# Delta batch-native sink handoff

**Applies to:** native Delta append and merge-on-read writes

The StreamFusion-owned Delta Sink V2 writer accepts each Arrow output batch as one
ownership-carrying record. It walks the batch synchronously with one reusable row cursor for
primary-key and RowKind inspection, then stores only row positions for inserts and update-after
images that must reach a data file. Update-before and key-only delete records therefore avoid both a
`StreamRecord` and a retained Java object.

Previously the boundary emitted one `StreamRecord<RowData>` per Arrow row. Delta buffered those
wrappers, grouped them by their original batch, and reconstructed columnar selections before the
native Parquet write. The current handoff preserves the original Arrow ownership throughout. Dense
selections enter the same native encoder as the plain Parquet sink; sparse merge-on-read selections
stay as Kernel columnar batches and use the released Kernel writer without passing through Flink
rows.

This adapter uses published `delta-flink` merge and commit APIs. Earlier measurements depended on an
unpublished connector build and are intentionally not carried forward; the released-only path must
be benchmarked independently.
