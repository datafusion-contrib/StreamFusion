# Record-level JOIN state with whole-bucket hydration

Decision: retain the current persistent JOIN layout after the release-build investigation of
#244. Whole-bucket write amplification is real, but replacing it with per-record entries while
retaining the current complete-bucket access contract makes these measured workloads slower.
This decision concerns the storage-only substitution, not all possible per-record JOIN designs.
Issue [#244](https://github.com/datafusion-contrib/StreamFusion/issues/244) remains open for a
different access pattern; this diagnostic does not resolve whole-bucket write amplification.

## Reference and prototype

Arroyo's `JoinWithExpiration` stores each side separately, retrieves matching batches by key and
runs the join over the resulting column batches. StreamFusion adds Flink's retraction counts,
association degrees and write-time TTL semantics. Existing RocksDB companion tables for aggregate
multisets avoid complete-set hydration by point-probing only changed elements; the generic JOIN
state interface currently requires a complete bucket. Copying their physical layout alone does
not copy that important access-pattern advantage.

The retained ignored Rust diagnostic compares the production bucket store with a test-only
record layout `[key group][side][key length][key][payload] -> [timestamp,count,degree]`. Both use
the same released RocksDB, options, binary key encoder, payload/meta, WAL policy and working-set
lifetime. Each iteration updates one record, then drops the resident bucket. The prototype scans
and hydrates all entries and already knows which single row changed: it omits production journal,
migration and metadata-marker costs, making it optimistic for the proposed storage substitution.
It is not a supported backend, and it cannot read production checkpoints.

## Measurement

ARM64, release+mimalloc, uncompressed RocksDB, 16 MiB write buffer, 8 MiB block cache, normal
level compaction. Two warmups and five measured trials per layout; a trial performs 64 logical
bundles. The median foreground loop excludes an explicit memtable flush between trials. The
source/sink and SQL executor are absent: these are state microbenchmarks, not end-to-end gains.

| Shape | Keys / rows per key / payload bytes | Bucket seconds | Record seconds | Record/bucket |
| --- | --- | ---: | ---: | ---: |
| Unique | 128 / 1 / 64 | 0.001733 | 0.003833 | 2.21x |
| Uniform small bucket | 128 / 8 / 64 | 0.001628 | 0.004429 | 2.72x |
| Hot key | 1 / 4,096 / 1,024 | 0.269512 | 0.655145 | 2.43x |

The hot-key loop's logical write bytes fall from 274,728,512 to 68,416. Logical hydrated read
bytes remain 274,728,512 / 280,231,936, and both retain 4,194,304 bytes of row payload while a
bucket is loaded (not a peak-allocator/RSS measurement). The standalone 64-encode probe takes
16.27 ms for buckets versus less than 1 microsecond for record metadata. Foreground memtable-write
time falls from 14.33 ms to 0.15 ms. Those savings do not overcome iterator and hydration overhead.

RocksDB counters averaged across the five hot-key measurement intervals (including the flush):

| Counter | Bucket bytes | Record bytes |
| --- | ---: | ---: |
| Foreground SST block reads | 4,292,660 | 1,094 |
| Flush SST writes | 67,843,062 | 5,374 |
| Compaction SST writes | 17,174,824 | 209,542 |
| Compaction SST reads | 85,857,384 | 218 |

SST counters measure RocksDB file I/O, not physical device traffic; caches and the filesystem can
serve reads. Compactions run asynchronously, so their work may straddle measurement intervals.
The writes confirm substantial amplification, but do not establish an I/O-bound deployment.

```bash
cd native
cargo test -p streamfusion --release --features mimalloc --offline \
  join_record_profile -- --ignored --nocapture
```

## Reopen conditions

Revisit with a workload showing an end-to-end I/O bottleneck, or an operator/storage design that
point-probes the changed input-side rows while lazily iterating the opposite side. Any production
change still needs per-row TTL, count/degree and retraction parity, old incremental checkpoint
reading, canonical savepoints, both rescale directions and both released Flink test lines. No
such semantics or migration support is claimed for this diagnostic. Keep current formats and
recovery behavior until that design demonstrates a material net benefit.
