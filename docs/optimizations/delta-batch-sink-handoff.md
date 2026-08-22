# Delta batch-native sink handoff

**Applies to:** native Delta append and merge-on-read writes

The Delta Sink V2 boundary accepts each Arrow output batch as one ownership-carrying record. The
writer walks it synchronously with one reusable row cursor for primary-key and RowKind inspection,
and creates retained row views only for inserts and update-after images that the checkpoint writer
must keep. Update-before and key-only delete records therefore avoid both a `StreamRecord` and a
retained Java object.

Previously the boundary emitted one `StreamRecord<RowData>` per Arrow row. Delta buffered those
wrappers, grouped them by their original batch, and reconstructed columnar selections before the
native Parquet write. The new handoff preserves the original Arrow ownership throughout and also
skips empty partition-map construction for unpartitioned tables.

On focused q19 at 500,000 Kafka JSON events, parallelism and partitions 4, memory state, and
mini-batching off, the best StreamFusion Delta time fell from 2.249 seconds to 1.821 seconds
(**19% faster**).

The 2M-event profile then found two checkpoint costs behind the remaining Delta/Parquet gap. First,
merge-on-read repeatedly reopened Parquet files written by the same live sink writer to locate prior
primary-key images. The connector now carries a runtime-only key-to-file-row index across
checkpoints and applies cumulative deletion vectors directly; after failover the absent cache safely
falls back to the table scan. Second, the native Parquet handler exported every full source Arrow
batch before gathering Delta's sparse survivor selection. Selected merge-on-read batches now use
Kernel's sparse writer through an iterator that explicitly closes each StreamFusion-owned Arrow
batch; dense append batches retain the native writer.

On the matched 2M-event q19 benchmark, StreamFusion Delta improved from 6.074 seconds to 2.461
seconds (**59% faster**) and beat stock Flink Delta at 2.708 seconds (1.10x). The same
current-build changelog Parquet run completed in 1.653 seconds versus 5.423 seconds for stock Flink.
The remaining Delta-versus-Parquet difference is the expected cost of maintaining a transaction log,
primary-key state, and deletion vectors rather than merely appending physical changelog rows.
