# Iceberg-to-Iceberg Nexmark after boundary optimization

The complete unchanged streaming matrix passes **120 cells / 480 paired validations**,
including warmups. It uses two million original events, parallelism four,
four-bucket Iceberg inputs/outputs, identifier-field upserts where the query updates
rows, equal 4 GiB job heaps, one warmup and three alternating measured pairs per cell.
All 23 queries, four state/mini-batch modes and seven existing labelled expression
variants are retained. Snapshot compression remains off. Supported acceleration
remains on by default.

## Repeated results

The README query selection contributes each query once per mode, choosing its
existing labelled variant where present. Exact SQL remains separately reported.
Ratios are geometric means of stock/native median elapsed-time ratios; higher is faster.

| State / mini-batch | README selection | Exact SQL | Previous native / current native, README selection |
|---|---:|---:|---:|
| Memory / off | 1.22× | 1.23× | 1.03× |
| Memory / on | 1.81× | 1.69× | 1.19× |
| RocksDB / off | 2.80× | 2.51× | 1.22× |
| RocksDB / on | 2.94× | 2.74× | 0.90× |

The [previous complete matrix](../iceberg-final-20261017/README.md) reports
README-selection ratios 1.24× / 1.44× / 2.63× / 2.97×. The
[previous/current table](previous-comparison.md) retains both stock-normalized
ratios and the direct native-time comparison. Current native medians improve
geometrically in three modes and regress about 10% in RocksDB/on. Stock timings
also move, and some cells have large ranges. These cohorts measure the combined
boundary changes since the previous full matrix; they do not isolate each edit's
causal contribution.

[All cell medians/ranges and sampling conventions](results.md),
[raw observations](observations.jsonl), [phase/memory metrics](metrics.csv),
[method](method.json) and [audit](audit.json) retain every measured sample.
The README first-memory/best-of-first-two-disk sampling convention is secondary:
1.24× / 2.01× / 2.82× / 3.46×. The repeated medians above are primary.
The headline Kafka benchmark and SQL corpus are unchanged.

## Remaining slow cases

These results remain in the normal default-on matrix. Times are medians [min–max]
in seconds; the complete table includes every other regression too.

| Query / mode | Stock | Native | Stock/native |
|---|---:|---:|---:|
| q18 memory/off | 4.223 [3.651–38.692] | 29.996 [4.101–54.018] | 0.14× |
| q20 memory/off | 1.799 [1.768–1.973] | 17.729 [10.670–30.198] | 0.10× |
| q20 memory/on | 2.231 [2.159–9.741] | 20.483 [18.679–23.013] | 0.11× |
| q23 RocksDB/on | 20.247 [18.421–41.059] | 39.928 [3.346–39.946] | 0.51× |

q7 memory/off/on also gives 0.20× / 0.19×. The timing phases expose substantial
lifecycle costs in the larger memory cases: q18 native corpus/settle/drain medians
are 2.312 / 11.621 / 16.320 seconds; q20 memory/off is
0.664 / 9.102 / 9.473, and memory/on is 0.659 / 9.174 / 8.427.
Separate phase medians need not sum to the total median. These phases include query
state/checkpoint work and Java sink lifecycle work; they do not attribute all time
to the data-file encoder. q23 RocksDB/on's native corpus range also remains variable.
[Earlier CPU/wall/kernel profiles](../iceberg-profile-20261017/README.md) and
[issue #391](https://github.com/datafusion-contrib/StreamFusion/issues/391) retain
the state/checkpoint investigation. This rerun does not resolve those regressions.

## What changed and how to iterate

The [first boundary optimization](../iceberg-boundary-20261017/README.md) reuses
fixed source schemas/JSON serializers, shares one imported Arrow batch across file
drains, and caches exact Java integer-partition groups and writer ranges.
The [four-candidate follow-up](../iceberg-boundary-round2-20261017/README.md) transfers
selection ownership into the Rust encoder and caches parsed dynamic identifiers.
The generator-only median improves 34.12→9.90 ms (3.45× previous); repeated writer
throughput is inconclusive. Combined write/size reporting has no consistent benefit
and was removed. The configured checksummed local FileIO has no direct-buffer
capability, so its stream/checksum behavior remains intact.

The normal SQL sink is used in this full matrix. The dynamic generator gain is a
separate component measurement. Java retains discovery, partition/key decisions,
rolling checks, delete encoding, FileIO, checkpoints and commits. Payloads stay
Arrow through native operators/shuffles and Rust Parquet data-file encoding.
The [fast loop](../iceberg-io.md) takes about 8–9 seconds for reads/writes and two
seconds for routing/generator cases on this host. Use it during connector edits;
this full release matrix takes about **3 hours 4 minutes**, including the long
stock RocksDB trials and untimed committed-output checks.

## Reproducibility and validation

The [audit](audit.json) verifies 960 trials, 480 passing pairs, 360 measured pairs,
all native source/writer/task witnesses, zero stock task fallbacks, 480 native graphs
without row/Arrow transposes and 480 native plans without operator fallback. q12
uses the existing per-bidder count-conservation rule for processing-time windows.
All prior full-matrix settings match. The current cohort uses commit `8cd39960`
and the exact release libraries used by the 303-test validation suite:

- Core: `17bfce78697dec52ee111f83df323cc8017c8a810bbf064f837b6abffd8849f8`
- Iceberg: `dcf4520d34302f66d506f275a378428e2c3d9ab1404c1214d460b9e8d103e8c9`

The host is the same i7-12650H WSL2/ext4 machine, JDK 17, Flink 2.2.1, Java Iceberg
1.12.0 and iceberg-rust 0.10.1. Current observations are one warmed shared-JVM cohort;
the previous matrix resumed across three JVM cohorts. Java codec/JIT/GC and storage
variability remain part of these end-to-end results. There is no profile in the
timed run and no changed query, input corpus or checkpoint-compression setting.

[Input verification](input-reproducibility.json) compares all 480 input files with
the previous complete matrix. Data pages are byte-identical; released Parquet
metadata matches after sorting unordered encoding lists. Whole-file bytes differ
because of those footer lists. [Verifier source](CompareIcebergInputs.java) and
compressed output are retained. Per-cell archives under `cohorts/cohort-001/`
contain plans, graphs, final table metadata and checkpoint sizes; `inputs.tar.gz`
contains input metadata and file/data-page hashes. Payload/state files are excluded.

The [release validation](../iceberg-boundary-round2-20261017/validation/summary.json)
passes 303 Java/upstream streaming tests with 13 batch-only exclusions across 37
classes, including all captured-channel/source recovery fixtures. Six Rust units
and 126 native Criterion fixtures pass. The optimization ledger records the
[techniques and source inspiration](../../optimizations/iceberg-boundary-overhead.md).
