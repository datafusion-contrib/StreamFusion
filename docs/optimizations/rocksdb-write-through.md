# RocksDB write-through on Flink's write path

**Applies to:** every native operator running on the RocksDB state backend's typed store — all
operators today, event-time and proctime, including the multiset aggregate shapes (MIN/MAX
retraction, DISTINCT) with per-element companion tables and every OVER shape (unbounded folds,
bounded ROWS/RANGE frame buffers, proctime, DISTINCT seen-sets); the snapshot path remains only
for shapes whose state has no fixed-type native codec

Found by issue [#26](https://github.com/datafusion-contrib/StreamFusion/issues/26): at 10M events
the backend spent 80% of CPU in its own memory-pressure flush, and a StreamFusion-only tuning knob
(`write-buffer-mb`) swung the result from 3x slower than Flink to 2x faster.

The original store retained dirty entries in a Java-governed map above RocksDB — a second memtable.
When it hit its threshold it drained the whole map on the task thread, encoded every value as a
standalone Arrow IPC stream, forced a memtable flush into small L0 files, and cleared the read
cache. All of that was overhead management for a buffer RocksDB already has.

The store now follows Flink's write path with the batching advantage kept:

- **Write-through per bundle.** Dirty entries are written to the RocksDB memtable (WAL off) at
  every bundle boundary — one coalesced write per touched key per bundle, where Flink pays one per
  record. RocksDB's background threads own all flushing and compaction; the barrier only commits
  the current bundle's residue, so checkpoint sync time no longer scales with the interval's
  write volume. The working map is a per-bundle read/dedup cache, nothing more.
- **One columnar conversion per bundle.** Values are compact arrow-row bytes: the whole dirty set
  encodes in a single `RowConverter` pass, and `begin_batch` hydrates misses with one batched read
  plus a single batch decode — replacing a per-value Arrow IPC stream (schema framing per value,
  parsed even inside the TTL compaction filter). The TTL timestamp is now a fixed 8-byte value
  prefix, making the compaction filter one integer read per entry.
- **Optimized, pinned batch reads.** Point-read stores use rust-rocksdb's
  `batched_multi_get_cf` on the default column family. The older `multi_get` binding reaches
  RocksDB's legacy vector-returning API; the batched API groups SST lookups to share block work
  and pipeline cache misses. It also borrows input key bytes and returns `DBPinnableSlice`
  values, avoiding the legacy binding's copied keys and intermediate value buffers. Cache-backed
  values remain pinned until the batch has been decoded into owned operator state; other read
  paths can still copy into the pinnable slice. Keys need not be sorted, and results retain input
  order. The default column family receives the same translated Flink options as before,
  including its block cache, compression, write buffers, and TTL filter.
- **Columns directly from aggregate state.** The group codec builds integer and floating-point
  Arrow columns directly from accumulator iterators, and restores each group directly from the
  decoded columns. This removes the per-group scalar vectors, their row-to-column transpose,
  and the extra vectors and scalar clones on restore. Decimal and string state keep the general
  scalar-to-array converter. `RowConverter` still produces exactly the existing persisted bytes;
  the optimization is around it, not a replacement row format.
- **Flink's memory governance.** One shared block cache and write-buffer manager per slot, sized by
  `state.backend.rocksdb.memory.*` with Flink's exact split formulas, replaces per-store 256 MB
  caches. Total native state memory stops scaling with operator count.

The `streamfusion.state.rocksdb.write-buffer-mb` knob and the memory-pressure flush are deleted;
there is no StreamFusion-specific state memory tuning.

Measured (Nexmark state-backend A/B, 200K events, parallelism 2, best of 1, vs Flink RocksDB):

- q4: 1.41s → 1.30s (4.20x → 5.05x); with the old knob at 1 MiB (the pathology at small scale) the
  old code degraded to 1.91s — that failure mode no longer exists.
- q7: 1.33s → 1.21s (2.31x → 2.70x).

## Batched-read and codec measurements

Measured on an Apple M1 Max (10 cores, 64 GiB), Java 17, with the release `bench` profile,
2M Nexmark events, parallelism 4, mini-batching off, and one-second checkpoints. These runs use
the existing exactly-once Kafka harness and its native serialization boundaries described in
[Benchmarks](../benchmarks.md); they are separate from generator-to-blackhole measurements.
No benchmark queries, sources, sinks, or backend tuning were changed.
The baseline was commit `62be413`.

Q17 CPU profiles used `exactlyOnceKafkaSinkProfileLoop`, `profile.backend=rocksdb`, a 30-second
loop, and async-profiler CPU sampling at 1 ms. Counts below are inclusive samples divided by
completed 2M-event jobs (10 baseline, 10 pinned-only, 11 with both changes). They estimate CPU
work, not elapsed time; rows overlap and must not be added.

| Q17 path | Baseline | Pinned reads | Pinned reads + column codec |
|---|---:|---:|---:|
| RocksDB MultiGet | 966 | 554 | 577 |
| Group aggregate update | 1,752 | 1,320 | 1,251 |
| Dirty-state writes | 244 | 247 | 167 |
| State-value decode | 40 | 47 | 29 |

Pinned reads reduced sampled read CPU by about **43%** and group-update CPU by **25%**.
The column codec then reduced the sampled dirty-state write path by **32%** and value decode
by **38%** relative to pinned reads alone. Median execution time after the first job in each
profile loop was 2.40s, 2.40s, and 2.25s respectively. The CPU savings therefore do not translate
proportionally into end-to-end throughput: Kafka, checkpoints, and the rest of the pipeline
remain in the measured path. Arrow-row conversion itself accounted for only about 10–12
samples per job; the useful codec improvement was removing the surrounding temporary values.

The unprofiled matrix used one warmup and the best of two timed runs per engine. Native gain is
the ratio of the native before/after times. The Flink control timings are included to show the
run-to-run variability, particularly on Q18; these are observations from this host and workload.

| Query | Native before (s) | Native after (s) | Native throughput gain | Flink before (s) | Flink after (s) |
|---|---:|---:|---:|---:|---:|
| Q4 | 10.275 | 5.098 | **2.02×** | 54.441 | 50.672 |
| Q17 | 2.370 | 2.118 | **1.12×** | 4.247 | 3.919 |
| Q18 | 11.169 | 6.735 | **1.66×** | 42.465 | 36.343 |

The subsequent full 23-query run (2M events, parallelism 4, one warmup, best of two) completed
without failures or fallbacks. StreamFusion's throughput geomean over Flink RocksDB was **2.75×**
with mini-batching off and **3.41×** with it enabled, up from the previous headline's 2.47× and
2.92×. The full per-query ratios and methodology are in [Benchmarks](../benchmarks.md).

Reproduce the unprofiled state-backend comparison with:

```bash
SF_BENCHMARK=true SF_MATRIX_STATE_BACKENDS=true \
SF_MATRIX_QUERIES=q4,q17,q18 SF_ROWS=2000000 SF_WARMUP=1 SF_RUNS=2 \
mvn -pl :streamfusion-runtime test -Pbench \
  -Dtest='NexmarkMatrixBenchmark#stateBackendComparison'
```

For the Q17 CPU recording, set `ASPROF_LIB` to the installed async-profiler library and
`PROFILE_FILE` to an output file, then run:

```bash
SF_BENCHMARK=true SF_PROFILE_KAFKA_SINK=true SF_ROWS=2000000 \
mvn -pl :streamfusion-runtime test -Pbench \
  -Dtest='NexmarkMatrixBenchmark#exactlyOnceKafkaSinkProfileLoop' \
  -Dprofile.query=q17 -Dprofile.backend=rocksdb -Dprofile.seconds=30 \
  "-Dsf.extraJvmArgs=-agentpath:${ASPROF_LIB}=start,event=cpu,interval=1ms,cstack=fp,collapsed,file=${PROFILE_FILE}"
```

## Record state for immediate inner joins

The Q23 CPU profile attributed 45.8% of sampled CPU to batch-state hydration and
20.7% to bundle commit; Snappy decompression appeared in 30.3% of samples. These
are inclusive percentages and overlap. Hydrating and rewriting both full
per-key multisets on every batch made cost grow with the accumulated history.

With mini-batching disabled, an inner join reads the input row's count through a
batched point lookup and iterates the opposite key's records. Its persistent key
contains the Flink key group, side, length-delimited equality key, and Arrow row
payload. The value contains the count, association metadata, and last-write time.
A bundle writes only changed records, rather than rewriting the entire key's
multiset. An empty input point lookup does not imply an empty logical key.

Opposite-side reuse uses a per-bundle cache charged to the shared host memory
pool, without a separate fixed byte ceiling. Groups that cannot obtain a memory
reservation use a lazy iterator and bounded output chunks;
allocation pressure drains pending output before retrying the same record. Expired
records are skipped before payload allocation. Outer, semi, anti, and mini-batched
joins retain their existing bucket implementation.

Existing bucket checkpoints migrate when a key is first accessed. Canonical
savepoints retain the existing logical encoding, including counts and per-row TTL,
so backend transitions and key-group split/merge restore remain portable. Migration,
cache, pending writes, and canonical materialization reserve host memory.

The matched Q23 run (Flink 1.18.1, Kafka/Paimon, disk state, mini-batching off,
2M inputs, P4, 512 MiB managed memory and 3 GiB heap per engine) measured
11.339–13.016 s for Flink and 4.055–4.084 s for StreamFusion: 2.80× by best
measured trial, with 5,520,000 output rows from each. Previous StreamFusion
bucket-state trials were 21.838–22.435 s in an earlier matched run. Those runs
were separate and machine variability limits direct before/after attribution.
[All trials](../benchmarks/paimon-disk-q23-flink118-2026-09-29.csv), including
warmups and rejected experiments, are retained. The complete 23-query rerun
measured Q23 at 19.911/30.660 s Flink versus 4.026/4.215 s native (4.95× best);
Flink variability is substantial. [Full-suite trials](../benchmarks/paimon-disk-full-flink118-2026-09-30.csv)
show wins for all queries, with Q3 a narrow 1.12× full-suite win. A separate
five-run Q3 verification measured 1.20× best and 1.29× by mean time; see the
[benchmark details](../benchmarks.md#flink-118-diskpaimon-headline-rerun-2026-09-30).


### Separating record layout from probe reuse, 2026-09-30

A release diagnostic compares the old bucket store with the record store using
no opposite-side cache, the production 8 MiB allowance, and a 64 MiB allowance.
The allowance is a ceiling per store, not reserved memory. Disabling reuse also
skips the cache-prefill scan. SQL, input, and the persistent record layout are
identical across the three record variants; the bucket variant retains its
original full-group reuse.

The state-level diagnostic uses a 64 MiB enforced native pool, an 8 MiB RocksDB
block cache, two 16 MiB write buffers, and uncompressed state. It includes Arrow
conversion, join output consumption, and state updates, but excludes Kafka, JNI,
Paimon, startup, and seeding. Seeded state can remain in memtables. Each case has
one warmup and five measured trials, with policy order rotated. The table shows
median seconds; every policy produced the same row count and checksum.

| Shape / payload bytes | Bucket | Record, no reuse | Record, 8 MiB | Record, 64 MiB |
|---|---:|---:|---:|---:|
| Unique / 0 | 0.0755 | 0.1534 | 0.1585 | 0.1548 |
| Unique / 256 | 0.0900 | 0.1873 | 0.1846 | 0.1924 |
| Repeated / 0 | 0.0781 | 0.1864 | 0.0877 | 0.0880 |
| Repeated / 256 | 0.2655 | 0.2869 | 0.1595 | 0.1596 |
| Skewed / 0 | 0.0563 | 0.0942 | 0.0620 | 0.0612 |
| Skewed / 256 | 2.0976 | 0.1459 | 0.0972 | 0.0952 |
| 50,000-row group / 0 | 0.1256 | 0.3622 | 0.3635 | 0.1277 |
| 50,000-row group / 256 | 0.4500 | 0.9465 | 0.9475 | 0.4557 |

[Raw state-level trials](../benchmarks/record-join-probe-cache-2026-09-30.csv)
retain all warmups and measured trials. Unique-key inputs favor the bucket layout;
record layout is not universally faster. Repeated probes benefit from reuse.
When a group exceeds 8 MiB, lazy rescans are about 2–3 times slower here than
keeping that group under the larger allowance. The cap protects memory rather
than creating the throughput improvement. Wide skewed bucket timings vary with
storage/compaction behavior and should not be generalized into a universal ratio.


End-to-end Q23 trials keep Kafka, JNI, both transposes, both joins, and Paimon
in the measured path. Each fresh JVM has one warmup; the first series has two
measured trials per policy and the reverse-order repeat has five. Configuration:
Flink 1.18.1, release native library, disk, mini-batching off, 2M events, parallelism
four, four Kafka partitions, 512 MiB managed memory and 3 GiB JVM heap. The existing
shared-cluster task off-heap ceiling is 48 GiB for every policy (not allocated
up front); these runs do **not** demonstrate a 512 MiB total native-memory budget.

| Sequence / policy | Measured native seconds | Mean seconds |
|---|---|---:|
| First / record, 8 MiB | 4.295, 4.207 | 4.251 |
| First / record, no reuse | 9.092, 9.337 | 9.215 |
| First / record, 64 MiB | 4.345, 4.360 | 4.353 |
| First / old bucket | 23.408, 22.518 | 22.963 |
| Repeat / record, 8 MiB | 7.718, 9.130 | 8.424 |
| Reverse / record, 64 MiB | 4.166, 4.139, 4.192, 4.240, 3.873 | 4.122 |
| Reverse / record, 8 MiB | 7.681, 8.025, 11.850, 11.094, 4.078 | 8.546 |

All completed policy runs returned 5,520,000 rows from each engine. The harness
checks counts on the final trial, so other CSV count cells are intentionally blank.
[All end-to-end trials](../benchmarks/paimon-disk-q23-cache-ab-flink118-2026-09-30.csv)
include Flink controls, warmups, and native-library hashes. Flink measured trials
range from 11.065 to 44.289 seconds; host variability is substantial. The same
8 MiB binary was used in all three phases and its timings varied widely. No
stable factor should be attributed to that cache based on the favorable first
phase alone. The record store without reuse was approximately 2.49 times faster
than the old bucket store by these phase means, establishing an independent
layout benefit for Q23, while retaining the limits of separate timed phases.
Reuse can further help, and the larger allowance was more consistent in this
experiment; cache admission and working-set effects have not been causally
profiled. These measurements preceded removal of the fixed 8 MiB production ceiling.
Production reuse now follows the shared native memory pool; fixed cache allowances
remain test-only controls for reproducing this experiment. A regression test
verifies reuse of a group larger than 8 MiB under a 64 MiB pool, correct join
output, and release of reservations. The small-pool regression still verifies
lazy fallback and bounded output when a complete group cannot fit.
