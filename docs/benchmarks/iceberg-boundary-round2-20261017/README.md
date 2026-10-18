# Four Iceberg boundary candidates

This investigation follows the [first boundary optimization](../iceberg-boundary-20261017/README.md).
Supported streaming acceleration remains on by default. Java still controls
partition transforms, file rolling, delete encoding, FileIO and commits. Payloads
remain Arrow through Rust Parquet encoding.

All component runs use the [fast loop](../iceberg-io.md): release + mimalloc,
JDK 17, Flink 2.2.1, Java Iceberg 1.12.0, iceberg-rust 0.10.1, four buckets,
262,144 rows, batch size 8,192, two warmups and five alternating repetitions,
4 GiB heap and identical Parquet settings. The baseline is commit `60c864fa`.
Observations, source/library fingerprints and compressed logs accompany each cohort.

## Outcomes

1. **Selection ownership:** the Rust encoder now consumes the JNI selection vector.
   Sparse Arrow indices take ownership of that allocation; contiguous selections
   only slice the input columns and skip index-array construction. Empty, sparse,
   reordered and duplicate selections retain their previous semantics. This removes
   an unnecessary production JNI-to-encoder selection clone. The initial sink
   median falls 135.34→131.82 ms, but the final repeat is 140.10 ms; ranges overlap.
   Allocation reduction is the supported claim, not a demonstrated throughput gain.
2. **Combined write/size reporting:** a prototype returned the size alongside memory
   usage through one reusable Java long-array slot, eliminating the separate
   `estimatedBytes` JNI call. Native sink medians were 138.33 and 128.77 ms across
   repeats versus 135.34 ms before. There is no consistent benefit; the prototype
   is removed. Java's original 1,000-row rolling checks and size query remain intact.
3. **Dynamic identifiers:** a task-local transient cache holds up to 1,024 parsed
   table identifiers. Every row still invokes the user router and gets a fresh
   control record with its current branch/spec/distribution/parallelism/upsert and
   equality fields. Catalog/schema/spec metadata and payload views are not cached.
   Clearing the derived cache preserves the normal refresh/recovery behavior.
   The destination-generation microbenchmark falls 34.12→9.90 ms (3.45× previous);
   stock is 41.47 ms (4.19× current). The final checked reference retains the
   original borrowed-row-kind guard. Earlier new-code cohorts measured 13.86 / 9.39 ms.
   This isolates generator work and excludes the host processor, shuffle, writer
   and commits; it is not a complete dynamic job speedup.
4. **Direct source reads:** the actual configured checksummed local Hadoop FileIO
   exposes `ChecksumFSInputChecker`, which neither implements `ByteBufferReadable`
   nor advertises `in:readbytebuffer`. Iceberg's base `SeekableInputStream` has no
   ByteBuffer read method. The [capability probe](source-capabilities.txt) and its
   [source](Probe.java) retain the evidence. Direct reads cannot accelerate this
   workload through its existing stream contract. No filesystem replacement or
   checksum bypass is introduced. Direct-capable external FileIO remains separate
   work, requiring its own contract and measurements.

## Component times

Times are medians [min–max] in milliseconds. Read source code is unchanged in this
round; its timing differences are cohort variability.

| Cohort / operation | Stock | Native |
|---|---:|---:|
| Baseline read | 108.34 [101.16–114.99] | 48.42 [41.37–54.41] |
| Baseline write | 198.93 [182.19–201.16] | 135.34 [133.03–139.04] |
| Selection write | 191.19 [184.68–202.90] | 131.82 [129.12–137.63] |
| Combined-size write | 188.26 [186.73–201.24] | 138.33 [133.81–140.64] |
| Combined-size repeat write | 192.62 [185.23–209.89] | 128.77 [127.78–135.21] |
| Final write | 190.71 [178.90–203.92] | 140.10 [131.66–142.36] |
| Previous dynamic generator | 38.43 [36.70–41.44] | 36.54 [34.88–64.90] |
| Early cached dynamic generator | 37.11 [35.44–38.80] | 9.39 [9.19–18.61] |
| Checked previous generator | 39.84 [35.24–63.91] | 34.12 [33.18–78.37] |
| Checked cached generator | 41.47 [35.99–55.06] | 9.90 [9.40–10.57] |

Every file-task trial retains full payload/ID readback, native-footer and resource
release assertions. Dynamic generator trials compare every table/branch destination
against the original uncached generator. Unit/integration cases cover nullable data,
rolling, abort/recovery and identifier reuse/eviction with changing destination
metadata. The new `--cases dynamic-route` case takes about two seconds and can use
`--baseline` and profiling like the other cases.

The six release Rust units and 126 Criterion fixtures pass. The contiguous encode
fixtures request one fewer allocation (56 fewer requested bytes) than the previous
native fixture; sparse encode fixture counts are unchanged because the standalone
fixture must create its own owned selection. The production JNI path additionally
passes its existing vector into the encoder instead of cloning it. These request
counts are not copied-byte measurements or an end-to-end speedup.

The full serial release Java/upstream suite passes **303 tests**, with **13 batch
exclusions**, across 37 classes. All 12 captured-channel recovery/rescaling cases,
four source recovery cases, SQL and normal/dynamic/range controls pass.
[Summary/library hashes](validation/summary.json), [reports](validation/reports.tar.gz)
and compressed logs retain the evidence. The complete
[Iceberg Nexmark rerun](../iceberg-optimized-20261017/README.md) passes all
120 cells / 480 paired validations after this validation, retaining mixed gains
and remaining regressions.

The `dynamic-*-checked` cohorts are the final generator comparison. Earlier
generator-only cohorts omitted the borrowed-row-kind guard in their uncached
reference; their raw measurements remain as exploratory evidence. Source hashes
and class inventories identify the production implementation and saved baseline.
