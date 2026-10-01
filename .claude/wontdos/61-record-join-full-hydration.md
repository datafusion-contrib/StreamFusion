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

## Q23 Kafka-to-Paimon profile, 2026-09-29

A Flink 1.18.1 release-build Nexmark run with 2M Kafka events, Paimon append sink,
disk state, parallelism four, mini-batching disabled, equal 512 MiB managed memory,
and equal 3 GiB JVM heap limits reproduced a whole-bucket bottleneck. The initial
measured best times were 12.709 s stock versus 22.114 s native (0.57×), with
5,520,000 output rows on both sides. This WSL2 host had concurrent workloads;
retain both trials and remeasure on an idle host before attributing all wall-clock
variation to the implementation.

The matched native CPU profile placed about 73% of samples under join ingestion
and 68% under immediate join processing. Self samples included approximately
18% Snappy decompression and 5% Snappy compression, plus allocation, copying,
hashing, and bucket destruction. The current `begin_batch` hydrates both whole
input-side and opposite-side buckets; `end_bundle` serializes dirty buckets and
clears both working sets, repeating this work on the next Arrow batch.

This is evidence to investigate the selective access pattern in the reopen
conditions above, not justification for adopting the rejected full-hydration
record prototype. A production redesign must point-read changed input rows,
stream opposite-side records into bounded output chunks, and persist only changed
record metadata. Duplicate counts, unique-key replacement, association degrees,
per-row TTL, retractions, and uncommitted same-batch updates must remain visible.
The old bucket checkpoints must remain readable, canonical savepoints must retain
their logical format, and clipping must preserve key-group ownership in both
rescale directions. An unbounded resident cache, increased memory budget,
mini-batch activation, or changed Nexmark SQL does not satisfy this investigation.

A partial bucket cannot be substituted silently behind `KeyedStateStore<JoinBucket>`:
its `remove`/empty-bucket contract assumes the complete multiset is resident. Retracting
the final *probed* row must not erase unrelated committed rows, while a planner-proven
unique-key replacement must erase the complete prior key range. A selective store needs
an explicit record-access contract that distinguishes these operations. For INNER joins,
only the input side changes during an immediate Arrow batch; the opposite side can be
read as a stable bounded cursor. Degree-bearing outer/semi/anti joins additionally need
journal-aware opposite-side updates. Retaining the current verified path for those
families is preferable to claiming untested partial-state semantics.

A matched release experiment reusing the raw serializer's temporary buffer within
writeback was rejected. Stock measured 24.502/18.157 s and native 48.117/22.038 s,
with equal 5,520,000 output rows. The native best time did not improve versus
21.838/22.435 s before; the change was reverted. A new paired-store checkpoint/TTL
round-trip test was retained as useful recovery coverage. The full native Rust
suite passed (558 tests, one ignored diagnostic), and the Q7/Q12 regression suite
passed 114 tests on each released Flink line. Those correctness results do not
resolve the Q23 performance issue.

## Selective access implementation (2026-09-30)

The storage-only rejection above remains valid. A new immediate-INNER path uses
input record point lookups, bounded opposite-side reuse, and lazy record scans; it
does not hydrate full input buckets. Old-checkpoint migration, canonical backend
transitions, key-group split/merge, counts, unique replacement, NULL policies, and
per-side TTL have regression coverage. The matched Q23 Kafka/Paimon disk run
measured stock 11.339/13.016 s and native 4.055/4.084 s (2.80× best), with identical
5,520,000 row counts. The full 23-query rerun passed with matching output counts
and faster native timings (2.46× geometric mean); Q23 measured 4.95× best in that
run, with substantial stock timing variability. See the live
technique in `docs/optimizations/rocksdb-write-through.md` and retained trial CSV.


The September 30 controlled cache/layout diagnostic retains unfavorable cases:
unique-key inputs favored the bucket store, and a 50,000-row group exceeded the
production cache allowance and paid repeated scans. The memory cap is a resource
safeguard, not the mechanism removing whole-group write amplification. See
`docs/optimizations/rocksdb-write-through.md` for all policies and raw trials.
The original rejection of unconditional full hydration remains applicable.
