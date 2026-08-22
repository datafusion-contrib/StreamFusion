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
carry row positions into Rust, gather each Arrow column once, and then enter the same standard
parquet-rs `ArrowWriter`. Neither path transposes the data-file payload through Flink rows.

This adapter uses published `delta-flink` merge and commit APIs. The current released-only 2M-event
Nexmark sink diagnostic completed all 23 supported queries at a **1.522×** suite geomean over the
stock Delta writer; see [Benchmarks](../benchmarks.md#parquet-and-delta-sink-diagnostics).
